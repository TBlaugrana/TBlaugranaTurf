#!/usr/bin/env python3
"""
Serveur pour pmu_bot.html — version "cloud" (GitHub + Railway).

Contrairement a la version locale (ou c'etait le navigateur qui interrogeait
l'API PMU toutes les 500ms tant que l'onglet restait ouvert), ici c'est LE
SERVEUR qui fait tout le travail de suivi en continu, dans un thread de fond,
independamment du fait qu'un navigateur soit connecte ou non :
  - il charge le programme du jour, choisit automatiquement la prochaine course
  - il interroge les rapports probables (cotes) toutes les ~500ms
  - il calcule l'ecart Gagnant/Place, la vitesse de variation, le top 5, etc.
  - il garde tout ca dans un etat en memoire (dict `STATE`)

La page pmu_bot.html ne fait plus que lire cet etat via GET /api/state toutes
les ~700ms et l'afficher. Fermer l'onglet (ou le telephone) n'arrete donc plus
le suivi : au retour, l'etat a jour est simplement redemande au serveur.

Deploiement Railway : le serveur ecoute sur 0.0.0.0:$PORT (Railway fournit la
variable d'environnement PORT). En local, sans PORT defini, il ecoute sur
0.0.0.0:8000 comme avant.
"""
import csv
import http.server
import socketserver
import http.client
import io
import zipfile
import json
import math
import os
import threading
import time
import urllib.parse
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

# Fuseau horaire de reference pour tout affichage/calcul d'heure. Le serveur
# (Railway) tourne en UTC ; sans ca, les heures affichees avaient 2h de moins
# que l'heure reelle en France (heure d'ete, UTC+2).
PARIS_TZ = ZoneInfo("Europe/Paris")

PORT = int(os.environ.get("PORT", 8000))

# Message affiche en zone "snapLine" tant qu'aucune strategie (Gagnant/Place)
# n'a encore produit de cheval sur la course en cours. Remplace l'ancien
# libelle technique "Snapshot T1 (...)" -- purement cosmetique, n'affecte
# aucun calcul (les fenetres de strategie restent celles de STRATEGY_CONFIG).
STRATEGY_WAIT_MESSAGE = (
    "⏳ Calcul en cours — un cheval peut apparaitre jusqu'a 30s apres "
    "le depart programme."
)

PMU_PROG_HOST = "online.turfinfo.api.pmu.fr"
PMU_PROG_PREFIX = "/rest/client/61/"
PMU_CIT_HOST = "offline.turfinfo.api.pmu.fr"
PMU_CIT_PREFIX = "/rest/client/7/"

REFRESH_INTERVAL_S = 0.5        # delai minimal entre deux cycles de poll des cotes
FETCH_TIMEOUT_S = 6             # abandonne un appel PMU trop lent
SPEED_WINDOW_S = 45             # fenetre glissante pour calculer la vitesse de variation (Gagnant)
SPEED_THRESHOLD_PCT_PER_MIN = 25 # seuil (variation RELATIVE, en %/min) au-dela duquel le badge eclair est affiche
REPROG_INTERVAL_S = 45          # recharge le programme en tache de fond toutes les 45s
DEPART_CHANGE_THRESHOLD_S = 15  # ecart d'heure de depart a partir duquel on considere un retard/avance
RACE_STALE_S = 6 * 60           # bascule vers la course suivante 6 min apres son depart
DELTA_WINDOW_S = 15             # fenetre de comparaison : cote Gagnant actuelle vs il y a 15s
GAINERS_TOP_N = 6                # nombre de chevaux affiches (plus forte progression sur 15s)

# Le snapshot T0 (reference servant a calculer "% Chute") n'est plus fige au
# tout premier poll, mais seulement a l'heure de depart officielle de la
# course (la "cote de depart"). Avant ce moment, le tableau affiche un mode
# "warm-up" : TOUS les chevaux tries du plus au moins favori (cote live
# croissante), sans masquage ni badges, avec un message d'attente indiquant
# le temps restant avant le depart. Des que l'heure de depart est atteinte,
# le snapshot est pris et le tableau bascule sur le mode normal (classement
# par % de chute depuis T0, top N, valuebet...).

# Plage de cote Gagnant en DIRECT (cote decimale, endpoint /participants)
# acceptee dans le tableau : un cheval dont la cote live sort de cette plage
# est masque — trop court (moins de COTE_RANGE_MIN, deja hyper favori, peu
# d'interet) ou trop long (plus de COTE_RANGE_MAX, tocard sans chance
# reelle). Reevalue a CHAQUE poll sur la cote LIVE : un cheval qui rentre
# dans la plage apparait immediatement, un cheval qui en sort disparait
# immediatement (pas de marquage definitif, contrairement a bigmove/valuebet).
# Cette restriction n'est appliquee QUE pour le Trot (cf. TROT_COTE_RANGE_ENABLED
# / GALOP_COTE_RANGE_ENABLED plus bas) : en Plat/Obstacle, aucune restriction.
COTE_RANGE_MIN = 1.0
COTE_RANGE_MAX = 10.0

# Lissage exponentiel (EMA) applique a la probabilite implicite (Gagnant) avant
# tout calcul de delta/vitesse, pour filtrer le bruit tick-par-tick (la cote
# PMU est interrogee 2x/s et peut micro-fluctuer sans signification). Span de
# 5 a 10s recommande ; on prend une valeur mediane. L'EMA est ici a "temps
# continu" (pondere par le delai reel ecoule entre deux points, pas par un
# nombre fixe de ticks) car l'intervalle entre deux cotes n'est pas garanti
# constant (latence reseau, cache PMU, etc.).
EMA_SPAN_S = 7.0

# En dessous de cette duree d'historique accumule, on ne calcule PAS de
# vitesse : extrapoler un taux logarithmique sur une fenetre de 1-2 secondes
# puis le ramener "par minute" (x30, x60...) avant de repasser par exp() fait
# exploser artificiellement le resultat (ex: un cheval qui bouge de +0.1 point
# en 1 seconde donnerait un taux astronomique une fois annualise a la minute,
# sans rapport avec un vrai mouvement de marche). Il faut laisser le temps a
# l'historique de s'accumuler un minimum avant que "vitesse" ait un sens.
SPEED_MIN_WINDOW_S = 10.0

# Seuil de "gros ecart" exprime en variation RELATIVE (%) sur la fenetre de
# 15s, et non plus en points absolus : un outsider qui passe de 2% a 4% de
# probabilite implicite (+100% relatif, mais seulement +2 points absolus)
# est un signal au moins aussi fort qu'un favori qui passe de 40% a 42%
# (+5% relatif). Le calcul en relatif remet les deux cas a la meme echelle.
BIGMOVE_THRESHOLD_PCT = 15.0
BIGMOVE_ALERT_LEAD_S = 60        # les alertes ne se declenchent que dans la derniere minute avant le
                                  # depart (avant, les mouvements sont frequents mais les enjeux sont faibles)

# Signal VALUEBET : a l'evaluation (T+<eval>, cf. TROT_VALUEBET_EVAL_AFTER_DEPART_S
# / GALOP_VALUEBET_EVAL_AFTER_DEPART_S plus bas, selon la discipline), on ne
# marque plus TOUS les chevaux qui franchissent un seuil de chute -- on ne
# marque QUE le cheval qui a subi la plus grosse chute de cote (T0 -> Live)
# parmi les chevaux eligibles (cf. evaluation dans handle_odds). Un seul
# signal valuebet par course, jamais plusieurs simultanes.

# ---------------------------------------------------------------------------
# Parametres dependants du TYPE DE COURSE (discipline PMU) : la methode de
# detection du signal valuebet differe desormais entre le Trot (Attele /
# Monte) et le "Galop" (Plat, Obstacle, Haie, Steeple-chase, Cross...) :
#
#   - GALOP (Plat / Obstacle / Haie / Steeple-chase) : snapshot T0 pris
#     GALOP_SNAPSHOT_BEFORE_DEPART_S avant le depart (30s avant), evaluation
#     du valuebet a GALOP_VALUEBET_EVAL_AFTER_DEPART_S apres le depart
#     programme (2 min) -> on regarde qui a le plus chute entre T-30s et
#     T+2min. Aucune restriction de plage de cote (GALOP_COTE_RANGE_ENABLED
#     = False) : tous les partants sont eligibles, quelle que soit leur cote.
#
#   - TROT (Attele / Monte) : snapshot T0 pris exactement au depart officiel
#     (TROT_SNAPSHOT_BEFORE_DEPART_S = 0), evaluation du valuebet
#     TROT_VALUEBET_EVAL_AFTER_DEPART_S apres ce depart (30s) -> on regarde
#     qui a le plus chute entre T0 et T+30s. Seuls les chevaux dont la cote
#     LIVE reste dans [COTE_RANGE_MIN, COTE_RANGE_MAX] (1 a 10) sont
#     eligibles et affiches (TROT_COTE_RANGE_ENABLED = True).
# ---------------------------------------------------------------------------
TROT_SNAPSHOT_BEFORE_DEPART_S = 0        # T0 = au depart officiel (Trot Attele/Monte), pas avant
GALOP_SNAPSHOT_BEFORE_DEPART_S = 30      # 30 secondes avant le depart (Plat/Obstacle/Haie/Steeple-chase)
TROT_COTE_RANGE_ENABLED = True           # Trot : on masque les chevaux hors [COTE_RANGE_MIN, COTE_RANGE_MAX]
GALOP_COTE_RANGE_ENABLED = False         # Galop : aucune restriction de plage de cote
TROT_VALUEBET_EVAL_AFTER_DEPART_S = 30   # Trot : evaluation valuebet a T0+30s (T0 = depart)
GALOP_VALUEBET_EVAL_AFTER_DEPART_S = 120 # Galop : evaluation valuebet a T+2min apres le depart programme


def is_trot_discipline(discipline):
    """True si la discipline (telle que fournie par l'API PMU) correspond a
    du Trot (Attele ou Monte). En pratique, le champ "discipline" renvoye par
    l'API PMU vaut directement "ATTELE" ou "MONTE" (PAS "TROT_ATTELE" ni
    "TROT_MONTE" comme on aurait pu le supposer -- confirme par capture
    d'ecran : une course Attele affiche bien "ATTELE" comme discipline).
    On garde aussi la reconnaissance de "TROT"/"TROT_ATTELE"/"TROT_MONTE" en
    prefixe par securite, au cas ou l'API renverrait un jour ce format-la
    (ou pour un import/restauration d'etat plus ancien). Toute autre
    discipline (PLAT, HAIE, STEEPLE-CHASE, CROSS...) est traitee comme
    "Galop" (memes parametres pour Plat/Obstacle)."""
    d = (discipline or "").strip().upper()
    return d.startswith("TROT") or d.startswith("ATTELE") or d.startswith("MONTE")


# ---------------------------------------------------------------------------
# Strategies "Gagnant" / "Place" affichees sur la page (2 sections empilees,
# une par type de pari). Un seul cheval retenu par strategie : celui qui a
# la PLUS GROSSE CHUTE DE COTE (cote decimale brute -- meme definition que
# pct_chute existant plus bas dans handle_odds : (cote_debut - cote_fin) /
# cote_debut * 100) entre un instant de DEBUT et un instant de FIN, tous
# deux fixes relativement a l'heure de depart PROGRAMMEE (self.depart_ts),
# parmi les chevaux dont la cote a l'instant de FIN ("cote d'arrivee")
# tombe dans [cote_min, cote_max]. Si aucun cheval ne correspond (ou si la
# fenetre n'est pas encore terminee), rien n'est renvoye et le client
# affiche "Aucun cheval ne correspond au critere pour le moment.".
#
# Fonctionnalite ENTIEREMENT ADDITIVE : calcule a partir d'un historique de
# cotes DEDIE (self.strategy_odds_history, cf. update_strategy_history),
# separe de self.odds_history (retention courte, 45s, utilise par le
# suivi/vitesse/valuebet existants -- non modifie). Ne lit ni n'ecrit aucun
# etat partage avec le reste du Tracker, hormis en lecture seule
# (self.depart_ts, self.selected_discipline, self.favoris_order,
# self.horse_names).
#
# Discipline (regroupement demande) :
#   PLAT : tout sauf Trot et Haie/Steeple/Cross (cf. classify_discipline)
#   TROT : Attele + Monte
#   HAIE : Haie + Steeple-chase + Cross
# ---------------------------------------------------------------------------
STRATEGY_HISTORY_RETENTION_S = 6 * 60  # 6 min : couvre la fenetre la plus large (Trot Gagnant : T-5min a T-2min)

STRATEGY_CONFIG = {
    "PLAT": {
        "gagnant": {"start_offset_s": -30,  "end_offset_s": 0,   "cote_min": 6,  "cote_max": 10},
        "place":   {"start_offset_s": -90,  "end_offset_s": 30,  "cote_min": 1,  "cote_max": 100},
    },
    "TROT": {
        "gagnant": {"start_offset_s": -300, "end_offset_s": -120, "cote_min": 1, "cote_max": 6},
        "place":   {"start_offset_s": 0,    "end_offset_s": 30,   "cote_min": 1, "cote_max": 10},
    },
    "HAIE": {
        "gagnant": {"start_offset_s": -60,  "end_offset_s": 30,  "cote_min": 6, "cote_max": 10},
        "place":   {"start_offset_s": -150, "end_offset_s": 30,  "cote_min": 1, "cote_max": 6},
    },
}


def classify_discipline(discipline):
    """Classe la discipline PMU brute en 'PLAT' / 'TROT' / 'HAIE' pour le
    choix de la config de strategie a appliquer (cf. STRATEGY_CONFIG) :
      - TROT : Attele + Monte (meme regroupement que is_trot_discipline)
      - HAIE : Haie + Steeple-chase + Cross
      - PLAT : tout le reste (valeur par defaut)
    Fonction dediee aux 2 sections de strategie -- n'affecte pas et ne
    remplace pas is_trot_discipline (utilisee par le suivi/valuebet
    existant, inchangee)."""
    d = (discipline or "").strip().upper()
    if d.startswith("TROT") or d.startswith("ATTELE") or d.startswith("MONTE"):
        return "TROT"
    if d.startswith("HAIE") or d.startswith("STEEPLE") or d.startswith("CROSS"):
        return "HAIE"
    return "PLAT"

# on garde en memoire assez d'historique pour satisfaire la fenetre la plus longue
HISTORY_RETENTION_S = max(SPEED_WINDOW_S, DELTA_WINDOW_S)

# Si True, les reunions dont le pays n'est pas la France sont completement
# ignorees des le chargement du programme (elles ne sont jamais candidates
# a la selection automatique et n'apparaissent jamais dans l'etat renvoye
# au bot). Mettre a False pour revenir au comportement d'origine (toutes
# les reunions, France + etranger).
HIDE_FOREIGN_RACES = True

# Codes pays consideres comme "France" (l'API PMU utilise normalement "FRA").
# Liste au cas ou un autre libelle serait renvoye pour certaines reunions.
FRANCE_COUNTRY_CODES = {"FRA"}

# ---------------------------------------------------------------------------
# Persistance sur disque : permet de retrouver l'etat (course suivie,
# historique des cotes...) si le process redemarre (crash, redeploiement,
# mise en veille du service...). Sans ca, un simple redemarrage faisait
# repartir tout le suivi de zero (cotes a 0 / "Chargement..." le temps de
# reaccumuler de l'historique).
# ---------------------------------------------------------------------------
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tracker_state.json")
SAVE_INTERVAL_S = 5              # ecrit l'etat sur disque au plus toutes les 5s

# ---------------------------------------------------------------------------
# Journal silencieux des picks de STRATEGIE (pour analyse ulterieure : ROI,
# taux de reussite, etc., calcules SEPAREMENT pour chaque strategie). N'af-
# fecte ni l'interface ni le fonctionnement du suivi : c'est un simple
# fichier ecrit en tache de fond, jamais lu ni expose par /api/state.
#
# IMPORTANT (evolution) : ce journal s'appelait a l'origine "valuebet_log"
# et enregistrait le signal "valuebet" (plus grosse chute de cote a
# l'evaluation). Il a ete transforme pour enregistrer desormais le cheval
# retenu par CHACUNE des 2 strategies affichees sur la page ("Simple
# Gagnant" et "Simple Place", cf. STRATEGY_CONFIG / compute_strategy_pick),
# course par course et discipline par discipline, avec une colonne
# "strategie" ("gagnant" / "place") pour pouvoir isoler la rentabilite de
# chaque strategie a la fin de la journee. Le nom du fichier/des variables
# ("valuebet_log", VALUEBET_LOG_FILE...) a ete volontairement conserve pour
# ne rien casser ailleurs (endpoints /api/valuebet-log*, export .zip...).
# Le signal "valuebet" (badge/marquage a l'ecran) continue lui d'exister
# tel quel pour l'affichage -- seul CE journal ne l'enregistre plus.
#
# A chaque bascule vers la course suivante, on fige le pick final de chaque
# strategie sur la course qu'on quitte (pas avant : la fenetre de la
# strategie doit etre terminee), puis on va chercher en arriere-plan les
# rapports definitifs PMU de cette course (une seule fois, partages entre
# les 2 strategies) pour pouvoir calculer un ROI plus tard.
# ---------------------------------------------------------------------------
VALUEBET_LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "valuebet_log.jsonl")
VALUEBET_LOG_LOCK = threading.Lock()
RAPPORTS_FETCH_ATTEMPTS = 20      # nombre de tentatives pour recuperer les rapports definitifs
RAPPORTS_FETCH_RETRY_DELAY_S = 60  # delai entre deux tentatives (~20 min de fenetre au total : les rapports definitifs peuvent mettre du temps, surtout en cas d'enquete des commissaires)

# ---------------------------------------------------------------------------
# Journal des series temporelles de cotes (photo periodique de tous les
# chevaux en chute, autour du depart programme). Fichier SEPARE du journal
# valuebet (odds_timeseries.jsonl) : n'affecte ni l'interface, ni le
# fonctionnement du suivi, ni le journal valuebet existant.
# ---------------------------------------------------------------------------
ODDS_TIMESERIES_LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "odds_timeseries.jsonl")
ODDS_TIMESERIES_LOG_LOCK = threading.Lock()
ODDS_TIMESERIES_SAMPLE_INTERVAL_S = 30   # un point toutes les 30s (permet des colonnes par demi-minute)
ODDS_TIMESERIES_WINDOW_BEFORE_S = 5 * 60  # a partir de T-5min (T = heure de depart PROGRAMMEE, cf. self.depart_ts)
ODDS_TIMESERIES_WINDOW_AFTER_S = 5 * 60   # jusqu'a T+5min



def _iter_dicts_with_typepari(obj, current_typepari=None):
    """Parcourt recursivement une structure JSON (list/dict imbriques) et
    fait remonter, pour chaque dict rencontre, le dernier "typePari" (ou
    equivalent) vu au-dessus de lui dans l'arbre. Utilise par
    extract_dividende ci-dessous pour ne pas dependre d'une forme JSON
    unique : l'API PMU peut structurer rapports-definitifs differemment
    selon le type de pari, et je n'ai pas pu verifier un payload reel."""
    if isinstance(obj, dict):
        tp = obj.get("typePari") or obj.get("type") or obj.get("libelleTypePari") or current_typepari
        yield obj, tp
        for v in obj.values():
            yield from _iter_dicts_with_typepari(v, tp)
    elif isinstance(obj, list):
        for item in obj:
            yield from _iter_dicts_with_typepari(item, current_typepari)


def extract_dividende(rapports, num, keyword):
    """Cherche, dans la reponse brute /rapports-definitifs (dont le detail
    exact peut varier selon le type de pari — je n'ai pas pu tester contre
    un payload PMU reel), le dividende pour un cheval (num) et un type de
    pari dont le libelle contient `keyword` (ex: "GAGNANT", "PLACE").

    Best-effort volontairement tolerant : essaie plusieurs noms de champs
    possibles pour "combinaison" (identifiant du cheval) et pour le
    dividende lui-meme, a n'importe quelle profondeur de la structure.
    Renvoie None des que rien ne matche plutot que de planter — la colonne
    JSON brute (rapportsRawJson) reste de toute facon disponible en secours
    pour verifier/completer a la main."""
    if rapports is None:
        return None
    combinaison_keys = ("combinaison", "combinaisons", "numPmu", "numeroPmu", "numero", "numeros")
    dividende_keys = ("dividendePourUnEuro", "dividendePourUneMise", "dividende", "rapportDirect",
                       "rapport", "montant", "rapportPourUnEuro")
    try:
        for d, tp in _iter_dicts_with_typepari(rapports):
            tp_str = str(tp or "").upper()
            if keyword not in tp_str:
                continue
            matched = False
            for ck in combinaison_keys:
                if ck not in d:
                    continue
                val = d[ck]
                if isinstance(val, list):
                    if str(num) in [str(x) for x in val]:
                        matched = True
                elif str(val) == str(num):
                    matched = True
                if matched:
                    break
            if not matched:
                continue
            for dk in dividende_keys:
                if d.get(dk) is not None:
                    return d[dk]
    except Exception:
        return None
    return None


def delta_proba(cote_avant, cote_apres):
    """Calcule le gain de probabilite implicite (1/cote) entre deux cotes,
    en points de pourcentage. Contrairement au % de chute brut, cette
    metrique est directement comparable entre un favori et un outsider :
    une meme chute en % ne represente pas le meme mouvement de conviction
    du marche selon la cote de depart (relation 1/cote non lineaire)."""
    try:
        if not cote_avant or not cote_apres or cote_avant <= 0 or cote_apres <= 0:
            return None
        return (1.0 / cote_apres - 1.0 / cote_avant) * 100
    except Exception:
        return None


def proba_implicite(cote):
    """Probabilite implicite brute (en %) deduite d'une cote : 1/cote * 100.
    Renvoie None si la cote est invalide/absente."""
    try:
        if not cote or cote <= 0:
            return None
        return (1.0 / cote) * 100
    except Exception:
        return None


def build_valuebet_csv():
    """Lit le journal valuebet_log.jsonl et le met a plat en CSV, UNE LIGNE
    PAR CHEVAL RETENU PAR UNE STRATEGIE (colonne "strategie" : "gagnant" ou
    "place", cf. compute_strategy_pick / STRATEGY_CONFIG). Une course peut
    donc produire 0, 1 ou 2 lignes (une par strategie ayant trouve un
    cheval correspondant a son critere).

    Colonnes "mise"/"gain"/"profit" calculees directement (mise fixe de 1
    unite par cheval retenu ; gain = dividende PMU correspondant au type de
    pari de la strategie -- Gagnant ou Place -- si le cheval a rapporte,
    0 sinon) pour permettre un simple tableau croise dynamique (par
    "strategie" et/ou "disciplineGroupe") et obtenir la rentabilite de
    chaque strategie en fin de journee sans calcul supplementaire.

    Compatibilite : les anciennes lignes du journal (format d'avant la
    refonte, signal "valuebet") n'ont pas de champ "cheval" et sont
    ignorees silencieusement ici (rien a en tirer pour un calcul de
    rentabilite par strategie)."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "date", "reunion", "course", "label", "strategie", "discipline", "disciplineGroupe",
        "hippodrome", "paysCode", "paysLabel", "etrangere", "nbPartants", "heureDepart",
        "loggedAt", "num", "nom",
        "coteDebut", "probaImpliciteDebut", "coteFin", "probaImpliciteFin",
        "pctChute", "deltaProba", "classementFinal",
        "dividende", "mise", "gain", "profit",
        "rapportsType", "rapportsRawJson",
    ])
    try:
        with open(VALUEBET_LOG_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except Exception:
                    continue
                cheval = entry.get("cheval")
                if not cheval:
                    continue  # ancienne ligne (pre-refonte, signal valuebet) : rien a exploiter ici
                strategie = entry.get("strategie", "")
                num = cheval.get("num")
                rapports = entry.get("rapports")
                rapports_raw = json.dumps(rapports, ensure_ascii=False) if rapports is not None else ""
                logged_at = entry.get("loggedAt")
                logged_at_str = (datetime.fromtimestamp(logged_at, tz=PARIS_TZ).isoformat()
                                  if logged_at else "")
                cote_debut = cheval.get("coteDebut")
                cote_fin = cheval.get("coteFin")
                keyword = "GAGNANT" if strategie == "gagnant" else "PLACE"
                dividende = extract_dividende(rapports, num, keyword)
                mise = 1
                gain = dividende if dividende is not None else 0
                profit = gain - mise
                writer.writerow([
                    entry.get("date", ""),
                    entry.get("reunion", ""),
                    entry.get("course", ""),
                    entry.get("label", ""),
                    strategie,
                    entry.get("discipline", ""),
                    entry.get("disciplineGroupe", ""),
                    entry.get("hippodrome", ""),
                    entry.get("paysCode", ""),
                    entry.get("paysLabel", ""),
                    entry.get("etrangere", ""),
                    entry.get("nbPartants", ""),
                    entry.get("heureDepart", ""),
                    logged_at_str,
                    num,
                    cheval.get("nom", ""),
                    cote_debut,
                    proba_implicite(cote_debut),
                    cote_fin,
                    proba_implicite(cote_fin),
                    cheval.get("pctChute", ""),
                    delta_proba(cote_debut, cote_fin),
                    cheval.get("classementFinal", ""),
                    dividende if dividende is not None else "",
                    mise,
                    gain,
                    profit,
                    entry.get("rapportsType", ""),
                    rapports_raw,
                ])
    except FileNotFoundError:
        pass  # aucun pick de strategie enregistre pour l'instant -> CSV avec juste l'entete
    return buf.getvalue()


def _col_name_for_offset(m):
    """Nom de colonne pour un decalage `m` (en minutes, multiple de 0.5) par
    rapport au depart programme. Garde les noms historiques pour les
    minutes entieres (CoteT-5, CoteT-0, CoteT+1, ...) et le nom special
    CoteFinal pour T+5 ; ajoute les demi-minutes sous la forme CoteT-4min30
    / CoteT+0min30."""
    if m == 5.0:
        return "CoteFinal"
    if m == 0:
        return "CoteT-0"
    sign = "-" if m < 0 else "+"
    abs_m = abs(m)
    whole = int(abs_m)
    if abs_m == whole:
        return f"CoteT{sign}{whole}"
    return f"CoteT{sign}{whole}min30"


def build_odds_timeseries_csv():
    """Lit le journal odds_timeseries.jsonl (une ligne JSON par course
    francaise suivie, contenant TOUT le peloton avec un point toutes les
    ODDS_TIMESERIES_SAMPLE_INTERVAL_S secondes de T-5min a T+5min autour du
    depart PROGRAMME) et le met en forme "large" : UNE LIGNE PAR CHEVAL,
    avec une colonne de cote toutes les 30s (CoteT-5 .. CoteT-0min30 ..
    CoteT-0 .. CoteT+0min30 .. CoteFinal), plutot qu'une ligne par
    (cheval, instant). Chaque colonne prend l'echantillon le plus proche de
    l'instant vise (tolerance = la moitie de l'intervalle d'echantillonnage ;
    au-dela, vide). Format volontairement simple : date et label (ex. R1C1)
    bien separes, discipline explicite, et seulement les rapports Simple
    Gagnant / Simple Place (pas de classement d'arrivee)."""
    # Decalages cibles, de T-5min a T+5min par pas de 30s (= 0.5 min)
    minute_offsets = [i / 2 for i in range(-10, 11)]
    col_names = {m: _col_name_for_offset(m) for m in minute_offsets}
    tolerance_s = ODDS_TIMESERIES_SAMPLE_INTERVAL_S / 2  # evite toute ambiguite entre deux colonnes voisines

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(
        ["date", "label", "discipline", "heureDepart", "num", "nom"]
        + [col_names[m] for m in minute_offsets]
        + ["rapportSimpleGagnant", "rapportSimplePlace", "rapportsRawJson"]
    )
    try:
        with open(ODDS_TIMESERIES_LOG_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except Exception:
                    continue
                rapports = entry.get("rapports")
                rapports_raw = json.dumps(rapports, ensure_ascii=False) if rapports is not None else ""
                depart_iso = entry.get("heureDepart")
                depart_ts = None
                if depart_iso:
                    try:
                        depart_ts = datetime.fromisoformat(depart_iso).timestamp()
                    except Exception:
                        depart_ts = None
                for h in (entry.get("horses") or []):
                    samples = h.get("samples") or []
                    # pour chaque instant cible, on prend l'echantillon le
                    # plus proche (tolerance = moitie de l'intervalle
                    # d'echantillonnage, pour ne jamais chevaucher la colonne voisine)
                    cote_par_minute = {}
                    if depart_ts is not None:
                        for m in minute_offsets:
                            target_s = depart_ts + m * 60
                            best = None
                            best_diff = None
                            for s in samples:
                                t = s.get("t")
                                if t is None:
                                    continue
                                diff = abs(t - target_s)
                                if diff <= tolerance_s and (best_diff is None or diff < best_diff):
                                    best = s.get("coteInstant")
                                    best_diff = diff
                            cote_par_minute[m] = best
                    writer.writerow(
                        [
                            entry.get("date", ""),
                            entry.get("label", ""),
                            entry.get("discipline", ""),
                            depart_iso or "",
                            h.get("num", ""),
                            h.get("nom", ""),
                        ]
                        + [cote_par_minute.get(m, "") if cote_par_minute.get(m) is not None else "" for m in minute_offsets]
                        + [
                            h.get("rapportSimpleGagnant", ""),
                            h.get("rapportSimplePlace", ""),
                            rapports_raw,
                        ]
                    )
    except FileNotFoundError:
        pass  # aucune donnee enregistree pour l'instant -> CSV avec juste l'entete
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Etat partage, lu par les threads HTTP (GET /api/state) et ecrit uniquement
# par le thread de suivi (tracker_loop). Protege par un verrou.
# ---------------------------------------------------------------------------
STATE_LOCK = threading.Lock()
STATE = {
    "courseInfo": "Chargement…",
    "statusLine": "Demarrage du suivi…",
    "snapLine": "",
    "toteLabel": "",
    "departTs": None,
    "rows": [],
    "updatedAt": None,
    "strategyGagnant": None,  # pick unique Strategie Gagnante (num/nom/cotes) ou None -- cf. compute_strategy_picks
    "strategyPlace": None,    # pick unique Strategie Placee (num/nom/cotes) ou None -- cf. compute_strategy_picks
}

# ---------------------------------------------------------------------------
# Indicateur de sante minimal, expose via GET /api/health -- permet de
# verifier en direct que les deux mecanismes de suivi tournent VRAIMENT
# (pas juste "le processus n'a pas crashe" -- une panne silencieuse type
# "le thread tourne mais toutes les requetes echouent en boucle" ne se
# voit pas autrement que par un poll qui ne se met plus a jour). Ajoute
# suite a un incident ou le suivi s'est arrete de collecter des la
# mi-journee sans que rien ne l'indique avant le lendemain (analyse du CSV).
# Pas de verrou dedie : simples int/str/None, ecriture atomique en Python.
# ---------------------------------------------------------------------------
HEALTH = {
    "tracker_last_ok_ts": None,
    "tracker_last_error": None,
    "lookahead_last_ok_ts": None,
    "lookahead_last_error": None,
    "lookahead_active_courses": 0,
    # --- Diagnostic fin ajoute suite a l'incident "odds_timeseries.jsonl ne
    # se remplit plus alors que /api/health affiche tout au vert" : l'ancien
    # health ne prouvait que "poll_once() n'a pas leve d'exception", pas que
    # des donnees etaient reellement collectees ET ecrites sur disque. Ces
    # champs rendent chaque etape (fetch PMU -> buffer -> flush -> ecriture
    # fichier) observable independamment, sans avoir a fouiller les logs.
    "lookahead_last_sample_ts": None,       # dernier instant ou au moins 1 cote a ete ajoutee a un buffer
    "lookahead_total_samples": 0,           # compteur cumule (depuis le demarrage du process) d'echantillons ajoutes
    "lookahead_last_empty_fetch_ts": None,  # dernier instant ou un fetch PMU a reussi mais renvoye 0 cheval exploitable
    "lookahead_consecutive_empty_fetches": 0,
    "lookahead_last_flush_ts": None,
    "lookahead_last_flush_label": None,
    "lookahead_last_flush_chevaux": None,
    "lookahead_last_flush_points": None,
    "lookahead_last_write_ok_ts": None,      # derniere ecriture REUSSIE dans odds_timeseries.jsonl
    "lookahead_last_write_label": None,
    "lookahead_last_write_error": None,      # derniere erreur du worker d'ecriture (label + exception)
}


def set_state(**kwargs):
    with STATE_LOCK:
        STATE.update(kwargs)


def get_state_json():
    with STATE_LOCK:
        return json.dumps(STATE)


# ---------------------------------------------------------------------------
# Connexion HTTPS persistante vers l'API PMU, reutilisee entre les appels --
# mais UNE PAR THREAD (threading.local), jamais partagee entre threads.
#
# BUG CORRIGE : avant, une seule connexion par HOTE etait partagee (dict
# global `_conns`, sans verrou) entre TOUS les threads qui appellent
# http_get_json -- le Tracker principal (poll toutes les 0.5s), le
# LookaheadOddsLogger (poll toutes les 2s, pour potentiellement plusieurs
# courses en parallele), et les threads worker() de recuperation des
# rapports definitifs (jusqu'a 20 tentatives par course terminee). Un objet
# http.client.HTTPSConnection n'est PAS thread-safe : deux threads qui font
# request()/getresponse() en meme temps sur la MEME connexion melangent les
# echanges (reponse corrompue, exception, timeout...). Cote appelant
# (_poll_one notamment), l'exception etait juste avalee et tout le sample de
# cote pour CETTE minute et TOUTE la course etait perdu -- exactement le
# symptome observe dans le CSV (colonnes CoteT-1/CoteT-0/CoteFinal entierement
# vides pour une course donnee). Desormais chaque thread a sa propre
# connexion par hote : plus aucun partage, donc plus de corruption possible.
# ---------------------------------------------------------------------------
_thread_local = threading.local()


def http_get_json(host, path):
    conns = getattr(_thread_local, "conns", None)
    if conns is None:
        conns = {}
        _thread_local.conns = conns
    conn = conns.get(host)
    if conn is None:
        conn = http.client.HTTPSConnection(host, timeout=FETCH_TIMEOUT_S)
        conns[host] = conn
    try:
        conn.request("GET", path, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        resp = conn.getresponse()
        data = resp.read()
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status} sur {host}{path}")
        return json.loads(data)
    except Exception:
        conns.pop(host, None)  # force une reconnexion propre au prochain appel (dans ce meme thread)
        raise



def date_pmu(d):
    return d.strftime("%d%m%Y")


def fetch_rapports_definitifs(date_str, reunion, course):
    """Recupere les rapports definitifs (Gagnant, Place, etc.) d'une course
    donnee, utilises uniquement pour le journal valuebet (calcul de ROI a
    posteriori). Peut renvoyer None si les rapports ne sont pas encore
    publies (l'appelant reessaie plus tard)."""
    path = (f"{PMU_CIT_PREFIX}programme/{date_str}/R{reunion}/C{course}"
            f"/rapports-definitifs?specialisation=OFFLINE")
    return http_get_json(PMU_CIT_HOST, path)


def fetch_rapports_provisoires(date_str, reunion, course):
    """Repli sur les rapports provisoires (publies plus vite que les
    definitifs, mais susceptibles d'etre revus en cas d'enquete des
    commissaires) si les definitifs ne sont toujours pas disponibles apres
    toutes les tentatives."""
    path = (f"{PMU_CIT_PREFIX}programme/{date_str}/R{reunion}/C{course}"
            f"/rapports-provisoires?specialisation=OFFLINE")
    return http_get_json(PMU_CIT_HOST, path)


def fetch_participants_arrivee(date_str, reunion, course):
    """Recupere l'endpoint /participants une fois la course terminee : il
    expose normalement le classement final de chaque partant (en plus des
    cotes, deja utilisees ailleurs pour le suivi en direct)."""
    path = (f"{PMU_CIT_PREFIX}programme/{date_str}/R{reunion}/C{course}"
            f"/participants?specialisation=OFFLINE")
    return http_get_json(PMU_CIT_HOST, path)


def extract_classement(participants_data, num):
    """Cherche le classement final (position d'arrivee) d'un cheval dans la
    reponse /participants post-course. Best-effort, comme extract_dividende :
    je n'ai pas pu verifier le nom exact du champ sur un payload PMU reel une
    fois la course terminee, donc plusieurs noms de champs plausibles sont
    essayes. Renvoie None si rien ne matche (le cheval reste alors identifie
    seulement via dividendeGagnant/dividendePlace, qui eux sont confirmes
    fonctionner)."""
    if not isinstance(participants_data, dict):
        return None
    classement_keys = ("ordreArrivee", "place", "rang", "position", "classement", "numeroOrdreArrivee")
    try:
        for p in (participants_data.get("participants") or []):
            if not isinstance(p, dict):
                continue
            if str(p.get("numPmu")) != str(num):
                continue
            for k in classement_keys:
                if p.get(k) is not None:
                    return p[k]
    except Exception:
        return None
    return None


def is_annulee(obj):
    s = obj.get("statut") or obj.get("statutCourse") or obj.get("statutReunion") or ""
    return isinstance(s, str) and "ANNULE" in s.upper()


def is_etrangere(reunion):
    """True si la reunion se deroule dans un pays autre que la France.

    L'API PMU expose normalement un champ "pays" sur chaque reunion, par ex.
    {"code": "FRA", "libelleLong": "FRANCE"}. Si ce champ est absent ou dans
    un format inattendu, on ne masque PAS la reunion par prudence (mieux vaut
    afficher une course etrangere par erreur que de faire disparaitre des
    reunions francaises a cause d'un format non reconnu)."""
    pays = reunion.get("pays")
    if not isinstance(pays, dict):
        return False
    code = (pays.get("code") or "").strip().upper()
    if not code:
        return False
    return code not in FRANCE_COUNTRY_CODES

def log_odds_timeseries_async(date_str, reunion, course, label,
                           timeseries_buffer, horse_names, course_info=None):
    """Ecrit en tache de fond le journal des series temporelles de cotes
    (fichier SEPARE, ODDS_TIMESERIES_LOG_FILE) : un point toutes les
    ODDS_TIMESERIES_SAMPLE_INTERVAL_S secondes pour TOUT le peloton, entre
    T-5min et T+5min (T = heure de depart PROGRAMMEE). Version simplifiee
    a la demande de l'utilisateur : plus de classement d'arrivee (best
    effort, jamais fiable) -- seulement les rapports Simple Gagnant et
    Simple Place, qui suffisent a savoir si un cheval a gagne/ete place."""
    if not timeseries_buffer:
        return
    hippodrome = (course_info or {}).get("hip")
    discipline = (course_info or {}).get("discipline")
    nb_partants = (course_info or {}).get("nbPartants")
    depart_ms = (course_info or {}).get("depart")
    heure_depart = (datetime.fromtimestamp(depart_ms / 1000.0, tz=PARIS_TZ).isoformat()
                     if depart_ms else None)

    def worker():
        # BUG CORRIGE : comme pour log_race_strategy_picks_async (cf. son
        # propre commentaire "BUG CORRIGE"), seule l'ecriture finale du
        # fichier etait protegee par un try/except. Si une exception
        # inattendue survenait AVANT (fetch des rapports, calcul du rang de
        # cote, construction de la liste `horses`...) -- par ex. un format
        # de payload PMU jamais rencontre pour une course/discipline
        # particuliere -- le thread worker() plantait silencieusement, sans
        # AUCUN message dans les logs : le journal odds_timeseries.jsonl
        # cessait alors de recevoir de nouvelles lignes, sans la moindre
        # trace de ce qui s'etait passe (exactement le symptome observe :
        # aucune ligne [ODDS-TS-LOG] dans les logs depuis plusieurs
        # courses). Desormais TOUT le corps du worker est protege par un
        # try/except qui logue systematiquement l'erreur.
        try:
            # Rapports (definitifs, avec repli provisoires + boucle de reessai
            # -- les rapports definitifs ne sont presque jamais prets
            # instantanement, il faut laisser du temps a PMU).
            rapports = None
            last_rapports_err = None
            for attempt in range(1, RAPPORTS_FETCH_ATTEMPTS + 1):
                try:
                    rapports = fetch_rapports_definitifs(date_str, reunion, course)
                    if rapports:
                        break
                except Exception as e:
                    last_rapports_err = e
                time.sleep(RAPPORTS_FETCH_RETRY_DELAY_S)
            if rapports is None:
                try:
                    rapports = fetch_rapports_provisoires(date_str, reunion, course)
                except Exception as e:
                    last_rapports_err = e
            if rapports is None:
                print(f"[ODDS-TS-LOG] {label} : rapports indisponibles apres "
                      f"{RAPPORTS_FETCH_ATTEMPTS} tentatives (+ repli provisoires echoue). "
                      f"Derniere erreur : {last_rapports_err!r}")

            # Rang de cote dans le peloton complet (1 = favori officiel de la
            # course = cote finale la plus basse), calcule a partir de la
            # derniere cote connue de chaque cheval (dernier sample).
            cote_finale_par_num = {num: samples[-1]["coteInstant"] for num, samples in timeseries_buffer.items() if samples}
            ranked = sorted(cote_finale_par_num.items(), key=lambda kv: kv[1])
            rang_par_num = {num: i + 1 for i, (num, _) in enumerate(ranked)}

            horses = []
            for num, samples in timeseries_buffer.items():
                if not samples:
                    continue
                horses.append({
                    "num": num,
                    "nom": horse_names.get(num, f"#{num}"),
                    "coteFinale": cote_finale_par_num.get(num),
                    "rangCoteFinale": rang_par_num.get(num),  # 1 = favori officiel de la course
                    "rapportSimpleGagnant": extract_dividende(rapports, num, "GAGNANT"),
                    "rapportSimplePlace": extract_dividende(rapports, num, "PLACE"),
                    "samples": samples,  # [{"t":..., "coteInstant":...}, ...]
                })

            entry = {
                "loggedAt": time.time(),
                "date": date_str,
                "label": label,
                "hippodrome": hippodrome,
                "discipline": discipline,
                "nbPartants": nb_partants,
                "heureDepart": heure_depart,
                "sampleIntervalS": ODDS_TIMESERIES_SAMPLE_INTERVAL_S,
                "windowBeforeS": ODDS_TIMESERIES_WINDOW_BEFORE_S,
                "windowAfterS": ODDS_TIMESERIES_WINDOW_AFTER_S,
                "horses": horses,
                # BUG CORRIGE : contrairement au journal valuebet (qui garde
                # toujours les rapports PMU bruts en secours), ce journal ne
                # conservait QUE le resultat de extract_dividende -- si
                # l'extraction echouait (rapports jamais publies par PMU,
                # enquete des commissaires, format de reponse imprevu...), le
                # rapportSimpleGagnant/rapportSimplePlace restait vide POUR
                # TOUJOURS, sans aucun moyen de retrouver l'info a la main.
                # On garde desormais le JSON brut (ou null si jamais obtenu),
                # expose en secours dans le CSV (colonne rapportsRawJson).
                "rapports": rapports,
            }
            with ODDS_TIMESERIES_LOG_LOCK:
                with open(ODDS_TIMESERIES_LOG_FILE, "a", encoding="utf-8") as f:
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            print(f"[ODDS-TS-LOG] {label} : {len(horses)} cheval(aux), "
                  f"{sum(len(h['samples']) for h in horses)} points au total")
            HEALTH["lookahead_last_write_ok_ts"] = time.time()
            HEALTH["lookahead_last_write_label"] = label
            HEALTH["lookahead_last_write_error"] = None
        except Exception as e:
            print(f"[ODDS-TS-LOG] {label} : ECHEC INATTENDU du worker (rien ecrit) : {e!r}")
            HEALTH["lookahead_last_write_error"] = f"{label} : {e!r}"
            HEALTH["lookahead_last_write_label"] = label

    threading.Thread(target=worker, daemon=True).start()


# ---------------------------------------------------------------------------
# Etat interne du suivi (accede uniquement depuis le thread tracker_loop,
# jamais concurremment -> pas besoin de verrou ici)
# ---------------------------------------------------------------------------
def parse_participants_data(data):
    """Version standalone (stateless) de Tracker.parse_participants,
    dupliquee volontairement pour LookaheadOddsLogger : evite de toucher a
    quoi que ce soit dans la classe Tracker existante (a la demande
    explicite de l'utilisateur de ne pas changer le fonctionnement du bot).
    Lit l'endpoint /participants : chaque partant y expose sa cote Gagnant
    en direct (dernierRapportDirect.rapport)."""
    gagnant_map = {}
    for p in (data.get("participants") or []):
        if p.get("statut") != "PARTANT":
            continue
        direct = p.get("dernierRapportDirect") or {}
        rapport_direct = direct.get("rapport")
        if rapport_direct is None:
            continue
        num = str(p.get("numPmu"))
        gagnant_map[num] = {
            "nom": p.get("nom") or f"#{num}",
            "ratio": rapport_direct,
        }
    return gagnant_map


class LookaheadOddsLogger:
    """Suivi INDEPENDANT et EN PARALLELE du Tracker principal. Seul but :
    alimenter le journal odds_timeseries avec la portion AVANT le depart
    (T-5min) que le Tracker principal rate souvent en pratique -- il ne
    bascule vers la course suivante qu'apres que la precedente devienne
    perimee (RACE_STALE_S = 6min apres son propre depart), donc il arrive
    parfois sur la course suivante alors que celle-ci a deja son propre
    depart officiel passe, ratant toute la fenetre "avant course".

    IMPORTANT : ce mecanisme ne touche a AUCUN etat du Tracker principal
    (cote_t1, valuebet_seen, selected_reunion, l'affichage /api/state,
    etc.). Il lit seulement tracker.all_courses en lecture seule (deja mis
    a jour par le Tracker principal) et fait ses propres requetes HTTP
    independantes, dans son propre thread. Zero impact sur le comportement
    existant du bot -- uniquement une source supplementaire de donnees pour
    le meme journal odds_timeseries.jsonl (via log_odds_timeseries_async,
    la meme fonction que le Tracker principal utilise).

    BUG CORRIGE : la version precedente ne suivait qu'UNE SEULE course a
    la fois (la plus proche dans le temps). Or en France il y a tres
    souvent plusieurs reunions en parallele (2-4 hippodromes en meme
    temps) -- si les fenetres de deux courses se chevauchaient, la
    seconde perdait sa portion "avant depart" (ratee pendant que la
    premiere etait suivie), et une troisieme pouvait meme etre ratee
    entierement si son depart+5min passait avant que ce mecanisme ne se
    libere. Desormais, TOUTES les courses actuellement dans leur fenetre
    [depart-5min, depart+5min] sont suivies EN PARALLELE (une entree par
    course dans self.active), avec une requete HTTP independante pour
    chacune a chaque tour de boucle."""

    def __init__(self, tracker):
        self.tracker = tracker
        # cle (reunion, course) -> {"buffer":..., "horse_names":..., "last_sample_ts":..., "course_info":...}
        self.active = {}
        self.flushed_keys = set()        # eviter de logguer deux fois la meme course

    def flush(self, key):
        state = self.active.pop(key, None)
        if state is None:
            return
        reunion, course = key
        label = f"R{reunion}C{course}"
        # LOG DE DIAGNOSTIC (meme esprit que celui de select_next_course
        # pour valuebet_log) : confirme systematiquement, a chaque flush,
        # si des donnees ont ete accumulees ou non -- pour ne plus jamais
        # se retrouver a devoir deviner si le probleme vient du polling
        # (buffer vide) ou du worker d'ecriture (cf. son propre correctif
        # try/except ci-dessus).
        HEALTH["lookahead_last_flush_ts"] = time.time()
        HEALTH["lookahead_last_flush_label"] = label
        if state["buffer"]:
            nb_chevaux = len(state["buffer"])
            nb_points = sum(len(s) for s in state["buffer"].values())
            print(f"[ODDS-TS-LOG] {label} : flush -> {nb_chevaux} cheval(aux), "
                  f"{nb_points} points accumules, ecriture en tache de fond lancee")
            HEALTH["lookahead_last_flush_chevaux"] = nb_chevaux
            HEALTH["lookahead_last_flush_points"] = nb_points
            date_str = date_pmu(datetime.now(PARIS_TZ))
            log_odds_timeseries_async(date_str, reunion, course, label,
                                       dict(state["buffer"]), dict(state["horse_names"]),
                                       state["course_info"])
        else:
            print(f"[ODDS-TS-LOG] {label} : flush -> buffer vide, rien a ecrire")
            HEALTH["lookahead_last_flush_chevaux"] = 0
            HEALTH["lookahead_last_flush_points"] = 0
        self.flushed_keys.add(key)

    def poll_once(self):
        # BUG CORRIGE : l'ancienne version decidait "flush" uniquement sur la
        # base de la fenetre [depart-5min, depart+5min] recalculee a CHAQUE
        # tour avec l'heure de depart la PLUS RECENTE connue. Or l'heure de
        # depart programmee bouge tres frequemment en cours de journee
        # (retard/avance publie par PMU) : des qu'une course deja suivie
        # voyait son "depart" repousse dans le programme rafraichi (toutes
        # les REPROG_INTERVAL_S=45s), la fenetre se decalait en avant et
        # "now" se retrouvait hors fenetre -> la course etait consideree
        # comme "terminee" et FLUSHEE IMMEDIATEMENT avec les 1-2 points a
        # peine accumules, puis marquee definitivement dans flushed_keys
        # (plus jamais reprise, meme quand "now" rentre a nouveau dans la
        # nouvelle fenetre). C'est exactement le symptome observe : une
        # course avec seulement les toutes premieres colonnes (CoteT-5,
        # CoteT-4min30) remplies puis plus rien.
        #
        # Desormais : une course active n'est flushee QUE quand on est
        # reellement passe sa fenetre en tenant compte de la DERNIERE heure
        # de depart connue (mise a jour a chaque tour), ou quand elle a
        # disparu du programme (annulee, jour suivant...). Si la fenetre se
        # decale en avant, la course reste active (buffer conserve) et on
        # met simplement le sampling en pause jusqu'a ce que "now" soit de
        # nouveau dans la fenetre -- aucune donnee deja accumulee n'est
        # perdue.
        now = time.time()
        all_by_key = {
            (c["numReunion"], c["course"]): c
            for c in self.tracker.all_courses
            if c.get("depart")
        }

        # 1) creation des nouvelles courses qui entrent dans la fenetre, et
        # mise a jour de l'heure de depart (course_info) des courses deja
        # actives -- sans jamais reinitialiser leur buffer deja accumule.
        for key, c in all_by_key.items():
            if key in self.flushed_keys:
                continue
            depart_s = c["depart"] / 1000.0
            in_window = (depart_s - ODDS_TIMESERIES_WINDOW_BEFORE_S) <= now <= (depart_s + ODDS_TIMESERIES_WINDOW_AFTER_S)
            if key not in self.active:
                if in_window:
                    self.active[key] = {
                        "buffer": defaultdict(list),
                        "horse_names": {},
                        "last_sample_ts": 0.0,
                        "course_info": c,
                    }
            else:
                self.active[key]["course_info"] = c

        # 2) flush des courses dont la fenetre est VRAIMENT terminee, sur la
        # base de la derniere heure de depart connue (pas celle d'origine).
        for key, state in list(self.active.items()):
            depart_ms = (state["course_info"] or {}).get("depart")
            if not depart_ms:
                continue
            depart_s = depart_ms / 1000.0
            if now > depart_s + ODDS_TIMESERIES_WINDOW_AFTER_S:
                self.flush(key)

        # 3) flush (best-effort) des courses actives ayant disparu du
        # programme (annulee apres coup, changement de jour PMU...) plutot
        # que de les garder actives indefiniment sans plus jamais recevoir
        # de mise a jour de course_info.
        for key in list(self.active.keys()):
            if key not in all_by_key:
                self.flush(key)

        # 4) poll independant de chaque course active ET actuellement dans
        # sa fenetre (une course dont le depart a ete repousse plus loin
        # reste active mais n'est pas interrogee tant qu'elle n'est pas
        # revenue dans la fenetre, pour ne pas gaspiller d'appels PMU).
        for key, state in list(self.active.items()):
            depart_ms = (state["course_info"] or {}).get("depart")
            if depart_ms:
                depart_s = depart_ms / 1000.0
                if not (depart_s - ODDS_TIMESERIES_WINDOW_BEFORE_S) <= now <= (depart_s + ODDS_TIMESERIES_WINDOW_AFTER_S):
                    continue
            self._poll_one(key, state)

    def _poll_one(self, key, state):
        reunion, course = key
        date_str = date_pmu(datetime.now(PARIS_TZ))
        path = (f"{PMU_CIT_PREFIX}programme/{date_str}/R{reunion}/C{course}"
                f"/participants?specialisation=OFFLINE")
        try:
            data = http_get_json(PMU_CIT_HOST, path)
        except Exception as e:
            print(f"[LOOKAHEAD] R{reunion}C{course} : erreur fetch cotes : {e!r}")
            return

        gagnant_map = parse_participants_data(data)
        if not gagnant_map:
            # Le fetch PMU a REUSSI (pas d'exception, HTTP 200) mais n'a
            # renvoye aucun cheval exploitable (statut != PARTANT ou
            # dernierRapportDirect absent pour tous). C'est exactement le
            # cas qui restait invisible dans l'ancien /api/health : la
            # boucle continue de tourner "sans erreur" indefiniment sans
            # jamais rien accumuler. Trace desormais explicitement.
            HEALTH["lookahead_last_empty_fetch_ts"] = time.time()
            HEALTH["lookahead_consecutive_empty_fetches"] += 1
            return
        HEALTH["lookahead_consecutive_empty_fetches"] = 0
        for num, info in gagnant_map.items():
            state["horse_names"][num] = info["nom"]

        now = time.time()
        if (now - state["last_sample_ts"]) < ODDS_TIMESERIES_SAMPLE_INTERVAL_S:
            return
        state["last_sample_ts"] = now

        appended = 0
        for num, info in gagnant_map.items():
            cote_instant = info.get("ratio")
            if cote_instant is None:
                continue
            state["buffer"][num].append({
                "t": now,
                "coteInstant": cote_instant,
            })
            appended += 1
        if appended:
            HEALTH["lookahead_last_sample_ts"] = now
            HEALTH["lookahead_total_samples"] += appended

    def run_forever(self):
        while True:
            try:
                self.poll_once()
                HEALTH["lookahead_last_ok_ts"] = time.time()
                HEALTH["lookahead_last_error"] = None
            except Exception as e:
                HEALTH["lookahead_last_error"] = repr(e)
                print(f"[LOOKAHEAD] erreur boucle : {e!r}")
            HEALTH["lookahead_active_courses"] = len(self.active)
            time.sleep(2.0)  # frequence plus lache que le Tracker principal (0.5s), pour limiter la charge API additionnelle


# ---------------------------------------------------------------------------
# Etat interne du suivi (accede uniquement depuis le thread tracker_loop,
# jamais concurremment -> pas besoin de verrou ici)
# ---------------------------------------------------------------------------
class Tracker:
    def __init__(self):
        self.all_courses = []
        self.selected_reunion = None
        self.selected_course = None
        self.depart_ts = None
        self.selected_discipline = ''
        self.selected_nb_partants = None
        # Recalcules a chaque selection de course (cf. select_next_course) en
        # fonction de la discipline : Trot (Attele/Monte) vs Plat/Obstacle.
        self.snapshot_lead_s = GALOP_SNAPSHOT_BEFORE_DEPART_S
        self.cote_range_enabled = GALOP_COTE_RANGE_ENABLED
        self.valuebet_eval_after_s = GALOP_VALUEBET_EVAL_AFTER_DEPART_S

        self.favoris_order = []   # ordre de suivi des chevaux (num en string)
        self.horse_names = {}
        self.cote_t1 = {}         # num -> cote Gagnant (%) au moment du snapshot T0 (1ere cote vue pour ce cheval sur cette course)
        self.cote_live_last = {}  # num -> derniere cote Gagnant live connue (mise a jour a chaque poll) ; sert a capturer la "cote finale" au moment de la bascule de course
        self.odds_history = {}    # num -> [(t, prob_lissee_ema), ...]
        self.strategy_odds_history = {}  # num -> [(t, cote_decimale_brute), ...] -- historique DEDIE, longue retention, pour les 2 sections de strategie (cf. STRATEGY_HISTORY_RETENTION_S) ; separe de odds_history ci-dessus, n'affecte rien d'existant
        self.ema_prob = {}        # num -> derniere probabilite implicite lissee (EMA)
        self.ema_last_t = {}      # num -> timestamp du dernier point utilise pour l'EMA
        self.bigmove_seen = {}    # num -> {"delta": ..., "at": ...} une fois le seuil relatif franchi (marquage definitif)
        self.valuebet_seen = {}   # num -> {"pctChute": ..., "at": ...} -- rempli UNE SEULE FOIS, a l'evaluation T+30s (voir valuebet_eval_done), puis fige pour le reste de la course
        self.valuebet_eval_done = False  # True des que l'evaluation unique du signal valuebet (a self.valuebet_eval_after_s, selon la discipline) a eu lieu pour cette course
        self.tracking_started_at = None  # timestamp du debut du suivi de la course en cours (informatif)
        self.snapshot_taken = False      # True des que le snapshot T0 a ete capture (bascule mode warm-up -> mode normal)
        # Pick fige par strategie ("gagnant"/"place") une fois trouve, pour
        # cette course : evite qu'un cheval affiche disparaisse plus tard
        # quand self.strategy_odds_history (retention glissante 6 min, cf.
        # STRATEGY_HISTORY_RETENTION_S) finit par purger le point de DEBUT
        # de fenetre (ex. T-5min pour Trot Gagnant) au fur et a mesure que
        # le suivi avance dans la course -- sans ce gel, compute_strategy_pick
        # se remettait a renvoyer None une fois ce point trop vieux, alors
        # qu'un cheval avait deja ete valide entre-temps.
        self.strategy_picks_frozen = {"gagnant": None, "place": None}
        self.last_reprog = 0.0
        self.last_save = 0.0

    # -- persistance sur disque ------------------------------------------
    def save_snapshot(self):
        """Ecrit l'etat de suivi courant sur disque (ecriture atomique via
        fichier temporaire + renommage, pour ne jamais laisser un fichier
        a moitie ecrit si le process est tue pendant l'ecriture)."""
        try:
            snapshot = {
                "selected_reunion": self.selected_reunion,
                "selected_course": self.selected_course,
                "depart_ts": self.depart_ts,
                "selected_discipline": self.selected_discipline,
                "selected_nb_partants": self.selected_nb_partants,
                "favoris_order": self.favoris_order,
                "horse_names": self.horse_names,
                "cote_t1": self.cote_t1,
                "odds_history": self.odds_history,
                "strategy_odds_history": self.strategy_odds_history,
                "ema_prob": self.ema_prob,
                "ema_last_t": self.ema_last_t,
                "bigmove_seen": self.bigmove_seen,
                "valuebet_seen": self.valuebet_seen,
                "strategy_picks_frozen": self.strategy_picks_frozen,
                "tracking_started_at": self.tracking_started_at,
                "snapshot_taken": self.snapshot_taken,
                "saved_at": time.time(),
            }
            tmp_path = STATE_FILE + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(snapshot, f)
            os.replace(tmp_path, STATE_FILE)
        except Exception as e:
            print(f"[TRACKER] echec sauvegarde etat : {e}")

    def maybe_save_snapshot(self, force=False):
        now = time.time()
        if force or (now - self.last_save >= SAVE_INTERVAL_S):
            self.save_snapshot()
            self.last_save = now

    def load_snapshot(self):
        """Tente de restaurer l'etat sauvegarde au demarrage. Renvoie True si
        une course encore pertinente (pas trop ancienne) a ete restauree."""
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                snap = json.load(f)
        except Exception:
            return False

        depart_ts = snap.get("depart_ts")
        if depart_ts is None:
            return False
        # on ignore un etat sauvegarde trop vieux (course deja bien terminee)
        if time.time() - depart_ts > RACE_STALE_S:
            return False

        self.selected_reunion = snap.get("selected_reunion")
        self.selected_course = snap.get("selected_course")
        self.depart_ts = depart_ts
        self.selected_discipline = snap.get("selected_discipline") or ''
        self.selected_nb_partants = snap.get("selected_nb_partants")
        trot = is_trot_discipline(self.selected_discipline)
        self.snapshot_lead_s = TROT_SNAPSHOT_BEFORE_DEPART_S if trot else GALOP_SNAPSHOT_BEFORE_DEPART_S
        self.cote_range_enabled = TROT_COTE_RANGE_ENABLED if trot else GALOP_COTE_RANGE_ENABLED
        self.valuebet_eval_after_s = TROT_VALUEBET_EVAL_AFTER_DEPART_S if trot else GALOP_VALUEBET_EVAL_AFTER_DEPART_S
        self.favoris_order = snap.get("favoris_order") or []
        self.horse_names = snap.get("horse_names") or {}
        self.cote_t1 = snap.get("cote_t1") or {}
        self.odds_history = snap.get("odds_history") or {}
        self.strategy_odds_history = snap.get("strategy_odds_history") or {}
        self.ema_prob = snap.get("ema_prob") or {}
        self.ema_last_t = snap.get("ema_last_t") or {}
        self.bigmove_seen = snap.get("bigmove_seen") or {}
        self.valuebet_seen = snap.get("valuebet_seen") or {}
        self.strategy_picks_frozen = snap.get("strategy_picks_frozen") or {"gagnant": None, "place": None}
        self.tracking_started_at = snap.get("tracking_started_at")
        # par defaut True (et pas False) pour la compatibilite avec un ancien
        # fichier d'etat sans ce champ : on suppose que le snapshot avait deja
        # ete pris (comportement d'origine) plutot que de relancer un warm-up
        # en pleine course apres un redemarrage
        self.snapshot_taken = snap.get("snapshot_taken", True)
        return self.selected_reunion is not None and self.selected_course is not None

    # -- programme -----------------------------------------------------
    def parse_programme(self, data):
        courses = []
        for ru in (data.get("programme", {}).get("reunions") or []):
            if is_annulee(ru):
                continue
            if HIDE_FOREIGN_RACES and is_etrangere(ru):
                continue
            hippo = ru.get("hippodrome") or {}
            hip = hippo.get("libelleCourt") or hippo.get("libelleLong") or f"R{ru.get('numOfficiel')}"
            num_reunion = ru.get("numOfficiel")
            pays = ru.get("pays") if isinstance(ru.get("pays"), dict) else {}
            pays_code = (pays.get("code") or "").strip().upper()
            pays_label = pays.get("libelleLong") or pays.get("libelleCourt") or pays_code
            etrangere = is_etrangere(ru)
            for co in (ru.get("courses") or []):
                if is_annulee(co):
                    continue
                courses.append({
                    "numReunion": num_reunion,
                    "hip": hip,
                    "paysCode": pays_code,
                    "paysLabel": pays_label,
                    "etrangere": etrangere,
                    "course": co.get("numOrdre"),
                    "depart": co.get("heureDepart"),
                    "libelle": co.get("libelle") or co.get("libelleCourt") or f"Course {co.get('numOrdre')}",
                    "discipline": co.get("discipline") or '',
                    "nbPartants": co.get("nombreDeclaresPartants", co.get("participants")),
                })
        courses.sort(key=lambda c: c["depart"] or 0)
        return courses

    def format_course_label(self, c):
        hhmm = ""
        if c["depart"]:
            hhmm = datetime.fromtimestamp(c["depart"] / 1000, tz=PARIS_TZ).strftime("%H:%M")
        return f"{hhmm} — R{c['numReunion']}C{c['course']} — {c['hip']} — {c['libelle']}"

    def load_programme(self, reset_selection=True):
        today = datetime.now(PARIS_TZ)
        path = f"{PMU_PROG_PREFIX}programme/{date_pmu(today)}?specialisation=OFFLINE"
        try:
            data = http_get_json(PMU_PROG_HOST, path)
            self.all_courses = self.parse_programme(data)
            set_state(statusLine=f"Programme charge ({len(self.all_courses)} courses)")
            if reset_selection:
                self.select_next_course()
            else:
                # une selection restauree depuis le disque est deja en place :
                # on se contente de rafraichir son libelle/heure si elle existe
                # toujours dans le programme du jour (sans effacer l'historique)
                self.handle_programme_refresh()
        except Exception as e:
            set_state(statusLine=f"Erreur programme : {e}")

    # -- journal silencieux des picks de strategie (aucun impact interface/etat) --
    def log_race_strategy_picks_async(self, date_str, reunion, course, label, picks, course_info=None):
        """Lance en tache de fond (thread separe, ne bloque jamais la boucle
        de suivi) la recuperation des rapports definitifs de la course qu'on
        vient de quitter, puis ecrit UNE LIGNE JSON PAR STRATEGIE dans le
        journal (colonne "strategie" a plat cote CSV, cf. build_valuebet_csv).

        picks : liste de tuples (strategie, pick) ou strategie vaut
        "gagnant" ou "place" et pick est le dict renvoye par
        compute_strategy_pick (num/nom/coteDebut/coteFin/pctChute). Ne fait
        rien si la liste est vide (aucune strategie n'a trouve de cheval sur
        cette course -> rien a logger).

        Les rapports definitifs + le classement final ne sont recuperes
        qu'UNE SEULE FOIS pour la course (partages entre les strategies),
        pour ne pas multiplier les appels a l'API PMU.

        course_info (dict issu de self.all_courses, optionnel) fournit le
        pays/hippodrome/discipline pour pouvoir filtrer/regrouper plus tard
        sans devoir re-parser le programme."""
        if not picks:
            return
        hippodrome = (course_info or {}).get("hip")
        pays_code = (course_info or {}).get("paysCode")
        pays_label = (course_info or {}).get("paysLabel")
        etrangere = (course_info or {}).get("etrangere")
        discipline = (course_info or {}).get("discipline")
        discipline_groupe = classify_discipline(discipline)  # PLAT / TROT / HAIE
        nb_partants = (course_info or {}).get("nbPartants")
        depart_ms = (course_info or {}).get("depart")
        heure_depart = (datetime.fromtimestamp(depart_ms / 1000.0, tz=PARIS_TZ).isoformat()
                         if depart_ms else None)

        def worker():
            # BUG CORRIGE : avant, seule l'ecriture finale du fichier etait
            # protegee par un try/except. Si la construction des entrees
            # (extract_classement, json.dumps...) levait une exception
            # inattendue -- par ex. sur un format de payload PMU jamais vu en
            # vrai pour /rapports-definitifs ou /participants post-course --
            # le thread worker() plantait AVANT d'atteindre le bloc d'ecriture,
            # sans AUCUN message dans les logs : le journal restait vide sans
            # la moindre trace de ce qui s'etait passe. Desormais TOUT le corps
            # du worker est protege par un try/except qui logue systematique-
            # ment l'erreur, pour ne plus jamais avoir d'echec 100% silencieux.
            try:
                rapports = None
                rapports_type = None
                last_err = None
                for attempt in range(1, RAPPORTS_FETCH_ATTEMPTS + 1):
                    try:
                        rapports = fetch_rapports_definitifs(date_str, reunion, course)
                        if rapports:
                            rapports_type = "definitifs"
                            break
                    except Exception as e:
                        last_err = e
                    time.sleep(RAPPORTS_FETCH_RETRY_DELAY_S)

                if rapports is None:
                    # repli : les definitifs ne sont toujours pas la apres ~20 min,
                    # on tente les provisoires pour ne pas rentrer bredouille
                    try:
                        rapports = fetch_rapports_provisoires(date_str, reunion, course)
                        if rapports:
                            rapports_type = "provisoires"
                    except Exception as e:
                        last_err = e

                if rapports is None:
                    print(f"[STRATEGY-LOG] {label} : rapports indisponibles apres "
                          f"{RAPPORTS_FETCH_ATTEMPTS} tentatives (+ repli provisoires echoue). "
                          f"Derniere erreur : {last_err!r}")
                else:
                    print(f"[STRATEGY-LOG] {label} : rapports {rapports_type} recuperes avec succes "
                          f"({len(picks)} strategie(s) avec un cheval retenu)")

                # Classement final (best-effort, cf. extract_classement) : recupere
                # une seule fois pour la course, puis assigne a chaque cheval.
                participants_data = None
                try:
                    participants_data = fetch_participants_arrivee(date_str, reunion, course)
                except Exception as e:
                    print(f"[STRATEGY-LOG] {label} : echec recuperation classement final : {e!r}")

                lines = []
                for strategie, pick in picks:
                    cheval = {
                        "num": pick.get("num"),
                        "nom": pick.get("nom"),
                        "coteDebut": pick.get("coteDebut"),
                        "coteFin": pick.get("coteFin"),
                        "pctChute": pick.get("pctChute"),
                        "classementFinal": extract_classement(participants_data, pick.get("num")),
                    }
                    entry = {
                        "loggedAt": time.time(),
                        "date": date_str,
                        "reunion": reunion,
                        "course": course,
                        "label": label,
                        "strategie": strategie,       # "gagnant" ou "place"
                        "hippodrome": hippodrome,
                        "paysCode": pays_code,
                        "paysLabel": pays_label,
                        "etrangere": etrangere,        # True/False/None (None si info indisponible)
                        "discipline": discipline,      # "TROT", "PLAT", "OBSTACLE"... (tel que fourni par l'API PMU)
                        "disciplineGroupe": discipline_groupe,  # PLAT / TROT / HAIE (cf. classify_discipline)
                        "nbPartants": nb_partants,
                        "heureDepart": heure_depart,
                        "cheval": cheval,
                        "rapports": rapports,           # None si toujours indisponible malgre les tentatives
                        "rapportsType": rapports_type,  # "definitifs", "provisoires" ou None
                    }
                    lines.append(json.dumps(entry, ensure_ascii=False))

                with VALUEBET_LOG_LOCK:
                    with open(VALUEBET_LOG_FILE, "a", encoding="utf-8") as f:
                        for line in lines:
                            f.write(line + "\n")
                print(f"[STRATEGY-LOG] {label} : {len(lines)} ligne(s) ecrite(s) dans {VALUEBET_LOG_FILE}")
            except Exception as e:
                print(f"[STRATEGY-LOG] {label} : ECHEC INATTENDU du worker (rien ecrit) : {e!r}")

        threading.Thread(target=worker, daemon=True).start()


    def select_next_course(self):
        now_ms = time.time() * 1000
        upcoming = [c for c in self.all_courses if c["depart"] and c["depart"] >= now_ms]
        chosen = upcoming[0] if upcoming else (self.all_courses[-1] if self.all_courses else None)
        if not chosen:
            set_state(courseInfo="Aucune course disponible.", statusLine="Aucune course disponible pour le moment.")
            return
        # LOG DE DIAGNOSTIC : confirme systematiquement qu'une bascule de
        # course a bien lieu (utile pour verifier que le service ne redemarre
        # pas / ne reste pas bloque avant meme d'atteindre RACE_STALE_S, ce
        # qui empecherait toute ecriture dans le journal valuebet).
        print(f"[TRACKER] select_next_course : bascule de R{self.selected_reunion}C{self.selected_course} "
              f"vers R{chosen['numReunion']}C{chosen['course']} ({chosen.get('libelle', '')})")

        # Juste avant de basculer vers la course suivante (pas avant : la
        # fenetre de chaque strategie doit avoir eu le temps de se
        # terminer), on fige silencieusement le pick final de chaque
        # strategie (Gagnant / Place) sur la course qu'on quitte et on
        # programme la recuperation de ses rapports. self.depart_ts /
        # self.selected_discipline / self.strategy_odds_history pointent
        # encore sur la course qu'on quitte a cet instant precis (pas
        # encore ecrases par la suite de cette fonction).
        if self.selected_reunion is not None and self.selected_course is not None:
            prev_label = f"R{self.selected_reunion}C{self.selected_course}"
            prev_gagnant, prev_place = self.compute_strategy_picks(time.time())
            prev_picks = []
            if prev_gagnant:
                prev_picks.append(("gagnant", prev_gagnant))
            if prev_place:
                prev_picks.append(("place", prev_place))
            # LOG DE DIAGNOSTIC (ajoute pour comprendre pourquoi le CSV
            # valuebet_log.csv restait vide) : indique systematiquement, a
            # CHAQUE bascule de course, si une strategie a trouve un cheval
            # ou non -- avant, en l'absence de pick, rien n'etait logue nulle
            # part et il etait impossible de distinguer "aucun cheval ne
            # correspond au critere" d'un probleme plus profond (switch de
            # course qui n'a pas lieu, worker qui plante silencieusement...).
            if prev_picks:
                print(f"[STRATEGY-LOG] {prev_label} : pick(s) trouve(s) -> "
                      f"{[s for s, _ in prev_picks]}")
                prev_date = date_pmu(datetime.now(PARIS_TZ))
                prev_course_info = next(
                    (c for c in self.all_courses
                     if c["numReunion"] == self.selected_reunion and c["course"] == self.selected_course),
                    None,
                )
                self.log_race_strategy_picks_async(
                    prev_date, self.selected_reunion, self.selected_course, prev_label, prev_picks,
                    prev_course_info,
                )
            else:
                print(f"[STRATEGY-LOG] {prev_label} : aucun cheval ne correspond au critere "
                      f"pour Gagnant ni Place -> rien a logger pour cette course")

        self.selected_reunion = chosen["numReunion"]
        self.selected_course = chosen["course"]
        self.depart_ts = chosen["depart"] / 1000.0
        self.selected_discipline = chosen["discipline"]
        self.selected_nb_partants = chosen["nbPartants"]
        trot = is_trot_discipline(self.selected_discipline)
        self.snapshot_lead_s = TROT_SNAPSHOT_BEFORE_DEPART_S if trot else GALOP_SNAPSHOT_BEFORE_DEPART_S
        self.cote_range_enabled = TROT_COTE_RANGE_ENABLED if trot else GALOP_COTE_RANGE_ENABLED
        self.valuebet_eval_after_s = TROT_VALUEBET_EVAL_AFTER_DEPART_S if trot else GALOP_VALUEBET_EVAL_AFTER_DEPART_S
        set_state(courseInfo=self.format_course_label(chosen), departTs=self.depart_ts)
        self.start_tracking()
        self.maybe_save_snapshot(force=True)

    def start_tracking(self):
        self.favoris_order = []
        self.horse_names = {}
        self.cote_t1 = {}
        self.cote_live_last = {}
        self.ema_prob = {}
        self.ema_last_t = {}
        self.bigmove_seen = {}
        self.valuebet_seen = {}
        self.valuebet_eval_done = False
        self.tracking_started_at = time.time()
        self.snapshot_taken = False
        self.strategy_odds_history = {}
        self.strategy_picks_frozen = {"gagnant": None, "place": None}
        set_state(rows=[], snapLine=STRATEGY_WAIT_MESSAGE,
                  strategyGagnant=None, strategyPlace=None)

    def refresh_programme_background(self):
        today = datetime.now(PARIS_TZ)
        path = f"{PMU_PROG_PREFIX}programme/{date_pmu(today)}?specialisation=OFFLINE"
        try:
            data = http_get_json(PMU_PROG_HOST, path)
            self.all_courses = self.parse_programme(data)
            self.handle_programme_refresh()
        except Exception:
            pass  # echec silencieux : simple tache de fond, on retente au prochain cycle

    def handle_programme_refresh(self):
        now_ms = time.time() * 1000

        if self.selected_reunion is None or self.selected_course is None:
            self.select_next_course()
            return

        if self.depart_ts is not None and (now_ms - self.depart_ts * 1000) > RACE_STALE_S * 1000:
            self.select_next_course()
            return

        current = next((c for c in self.all_courses
                         if c["numReunion"] == self.selected_reunion and c["course"] == self.selected_course), None)
        if current:
            new_depart_ts = current["depart"] / 1000.0
            self.selected_discipline = current["discipline"]
            self.selected_nb_partants = current["nbPartants"]
            trot = is_trot_discipline(self.selected_discipline)
            self.snapshot_lead_s = TROT_SNAPSHOT_BEFORE_DEPART_S if trot else GALOP_SNAPSHOT_BEFORE_DEPART_S
            self.cote_range_enabled = TROT_COTE_RANGE_ENABLED if trot else GALOP_COTE_RANGE_ENABLED
            self.valuebet_eval_after_s = TROT_VALUEBET_EVAL_AFTER_DEPART_S if trot else GALOP_VALUEBET_EVAL_AFTER_DEPART_S
            if self.depart_ts is not None and abs(new_depart_ts - self.depart_ts) > DEPART_CHANGE_THRESHOLD_S:
                was_delayed = new_depart_ts > self.depart_ts
                self.depart_ts = new_depart_ts
                extra = {"snapLine": "⏱ Depart retarde — suivi maintenu…"} if was_delayed else {}
                set_state(courseInfo=self.format_course_label(current), departTs=self.depart_ts, **extra)
            else:
                set_state(courseInfo=self.format_course_label(current))
        # sinon : la course a disparu du programme mais n'est pas encore consideree "stale" -> on continue de la suivre

    # -- cotes -----------------------------------------------------------
    def parse_participants(self, data):
        """Lit l'endpoint /participants : chaque partant y expose directement
        sa cote Gagnant en direct (dernierRapportDirect.rapport).

        Important : "rapport" est la cote DECIMALE classique du PMU
        (ex: 4.5 = "4,5 contre 1"), PAS une probabilite implicite en % comme
        avec l'ancien endpoint /citations. Plus la valeur est BASSE, plus le
        cheval est favori."""
        gagnant_map = {}
        for p in (data.get("participants") or []):
            if p.get("statut") != "PARTANT":
                continue
            direct = p.get("dernierRapportDirect") or {}
            rapport_direct = direct.get("rapport")
            if rapport_direct is None:
                continue
            num = str(p.get("numPmu"))
            gagnant_map[num] = {
                "nom": p.get("nom") or f"#{num}",
                "ratio": rapport_direct,   # cote Gagnant en direct (decimale)
                "favoris": bool(p.get("favoris")),
            }
        return gagnant_map

    def get_value_at(self, num, target_t):
        """Renvoie la probabilite Gagnant (%) lissee (EMA) historique la plus
        proche de l'instant cible (utilise pour comparer avec 'il y a 15s'),
        ou None si on n'a pas encore assez d'historique pour ce cheval (suivi
        demarre depuis moins de 15s)."""
        hist = self.odds_history.get(num)
        if not hist:
            return None
        if hist[0][0] > target_t:
            return None  # pas encore assez d'historique
        best_p = hist[0][1]
        for t, p in hist:
            if t <= target_t:
                best_p = p
            else:
                break
        return best_p

    def update_speed_history(self, gagnant_map):
        """Met a jour, pour chaque cheval, la probabilite implicite lissee
        (EMA) et calcule sa vitesse de variation RELATIVE (en %/min).

        Pourquoi une EMA : la cote brute recue toutes les ~0.5s contient du
        bruit de mesure (micro-arrondis, republications identiques...) qui
        n'a aucune signification de marche. On lisse donc chaque nouvelle
        valeur vers l'ancienne avec un poids qui depend du temps ecoule
        (alpha = 1 - exp(-dt/EMA_SPAN_S)), ce qui revient a une moyenne
        mobile exponentielle a "temps continu" : reactive si les mises a
        jour sont rapprochees, plus lente si elles sont espacees.

        Pourquoi une vitesse en relatif (log) plutot qu'en points absolus :
        un outsider a 2% qui passe a 3% (+1 point, +50% relatif) et un
        favori a 40% qui passe a 41% (+1 point, +2.5% relatif) n'ont pas le
        meme poids informationnel ; le log-delta traite les deux de facon
        symetrique et proportionnelle, ce qui est la pratique standard pour
        comparer des mouvements de probabilite/cote a des niveaux tres
        differents.
        """
        now = time.time()
        speed = {}
        for num, info in gagnant_map.items():
            cote = info["ratio"]  # "ratio" est deja le % Gagnant affiche (probabilite implicite)
            if cote is None or cote <= 0:
                continue

            # -- lissage EMA (ponderee par le temps reellement ecoule) --------
            prev_ema = self.ema_prob.get(num)
            prev_t = self.ema_last_t.get(num)
            if prev_ema is None or prev_t is None:
                ema = cote  # premier point connu pour ce cheval : pas de lissage possible
            else:
                dt = max(now - prev_t, 0.001)
                alpha = 1 - math.exp(-dt / EMA_SPAN_S)
                ema = prev_ema + alpha * (cote - prev_ema)
            self.ema_prob[num] = ema
            self.ema_last_t[num] = now

            hist = self.odds_history.setdefault(num, [])
            hist.append((now, ema))
            while len(hist) > 1 and now - hist[0][0] > HISTORY_RETENTION_S:
                hist.pop(0)

            # -- vitesse relative (log-delta / minute, converti en %/min) -----
            if len(hist) >= 2:
                first_t, first_p = hist[0]
                elapsed_s = now - first_t
                minutes = elapsed_s / 60.0
                if elapsed_s >= SPEED_MIN_WINDOW_S and first_p > 0 and ema > 0:
                    log_rate = (math.log(ema) - math.log(first_p)) / minutes
                    # signe inverse : avec la cote decimale (endpoint participants),
                    # une cote qui BAISSE = cheval plus favori = mouvement "positif"
                    speed[num] = (math.exp(-log_rate) - 1) * 100  # en %/min
        return speed

    def snapshot_phrase(self):
        """Libelle humain du moment de capture du snapshot T1, en fonction de
        self.snapshot_lead_s : "avant le depart" (Galop, lead_s > 0) ou
        "au depart (T0)" (Trot, lead_s == 0)."""
        if self.snapshot_lead_s <= 0:
            return "au depart (T0)"
        lead_label = "5 min" if self.snapshot_lead_s >= 60 else f"{int(self.snapshot_lead_s)}s"
        return f"{lead_label} avant le depart"

    def build_tote_label(self, nb_live=None):
        parts = []
        if self.selected_discipline:
            parts.append(self.selected_discipline)
        nb = nb_live if nb_live is not None else self.selected_nb_partants
        if nb is not None:
            parts.append(f"{nb} partant{'s' if nb > 1 else ''}")
        return " · ".join(parts)

    def build_warmup_rows(self, gagnant_map):
        """Lignes affichees avant la capture du snapshot T1 : tous les
        chevaux avec une cote live DANS [COTE_RANGE_MIN, COTE_RANGE_MAX],
        tries du plus au moins favori (cote decimale croissante), sans
        masquage ni badge (chute/vitesse/valuebet n'ont pas de sens tant
        que T1 n'est pas fige). Filtre d'AFFICHAGE uniquement -- le suivi
        interne (cote_t1, valuebet, logging) reste actif pour TOUS les
        chevaux, filtres ou non de l'affichage."""
        rows = []
        ordered = sorted(gagnant_map.items(), key=lambda kv: kv[1]["ratio"])
        for num, info in ordered:
            if self.cote_range_enabled and not (COTE_RANGE_MIN <= info["ratio"] <= COTE_RANGE_MAX):
                continue
            rows.append({
                "num": num,
                "nom": info["nom"],
                "coteT0": None,
                "coteLive": info["ratio"],
                "pctChute": None,
                "delta15": None,
                "delta120": None,
                "isFavori": bool(info.get("favoris")),
                "isFast": False,
                "isBigMove": False,
                "isValuebet": False,
                "speed": None,
                "hidden": False,
            })
        return rows

    def get_strategy_odds_at(self, num, target_t):
        """Cote decimale BRUTE (pas de lissage EMA -- on veut la cote telle
        quelle, comme demande) la plus proche de target_t, dans l'historique
        dedie self.strategy_odds_history. Renvoie None si aucun point <=
        target_t n'est encore disponible (fenetre pas encore atteinte, ou
        historique pas encore assez profond pour ce cheval)."""
        hist = self.strategy_odds_history.get(num)
        if not hist:
            return None
        if hist[0][0] > target_t:
            return None
        best = None
        for t, c in hist:
            if t <= target_t:
                best = c
            else:
                break
        return best

    def update_strategy_history(self, gagnant_map, now):
        """Alimente self.strategy_odds_history (cote decimale BRUTE) avec
        une retention longue (STRATEGY_HISTORY_RETENTION_S = 6 min) --
        dediee UNIQUEMENT au calcul des 2 sections de strategie Gagnant/
        Place (cf. compute_strategy_picks). N'affecte pas et ne lit pas
        self.odds_history (retention courte 45s, utilise par le
        suivi/vitesse/valuebet existants, inchange)."""
        for num, info in gagnant_map.items():
            cote = info.get("ratio")
            if cote is None or cote <= 0:
                continue
            hist = self.strategy_odds_history.setdefault(num, [])
            hist.append((now, cote))
            while len(hist) > 1 and now - hist[0][0] > STRATEGY_HISTORY_RETENTION_S:
                hist.pop(0)

    def compute_strategy_pick(self, cfg, now):
        """Calcule le cheval retenu pour UNE strategie (Gagnant OU Place) :
        celui qui a la plus grosse chute de cote entre l'instant de DEBUT et
        l'instant de FIN de la fenetre (cfg, relatifs a self.depart_ts),
        parmi les chevaux dont la cote a l'instant de FIN ("cote d'arrivee")
        tombe dans [cote_min, cote_max]. Renvoie None si la fenetre n'est
        pas encore terminee ou si aucun cheval ne correspond au critere."""
        if self.depart_ts is None:
            return None
        start_t = self.depart_ts + cfg["start_offset_s"]
        end_t = self.depart_ts + cfg["end_offset_s"]
        if now < end_t:
            return None  # fenetre pas encore terminee -> pas encore de resultat possible

        best = None
        for num in self.favoris_order:
            cote_debut = self.get_strategy_odds_at(num, start_t)
            cote_fin = self.get_strategy_odds_at(num, end_t)
            if cote_debut is None or cote_fin is None or cote_debut <= 0:
                continue
            if cote_fin < cfg["cote_min"] or cote_fin > cfg["cote_max"]:
                continue
            pct_chute = (cote_debut - cote_fin) / cote_debut * 100
            if pct_chute <= 0:
                continue  # il faut une vraie chute de cote (meme regle que le valuebet existant)
            if best is None or pct_chute > best["pctChute"]:
                best = {
                    "num": num,
                    "nom": self.horse_names.get(num, f"#{num}"),
                    "coteDebut": cote_debut,
                    "coteFin": cote_fin,
                    "pctChute": pct_chute,
                }
        return best

    def compute_strategy_picks(self, now):
        """Calcule les 2 picks (Gagnant, Place) pour la course en cours,
        selon la config de la discipline detectee (cf. STRATEGY_CONFIG,
        classify_discipline). Chaque pick est FIGE des qu'il est trouve une
        premiere fois (cf. self.strategy_picks_frozen) et renvoye tel quel
        pour le reste de la course, meme si self.strategy_odds_history finit
        par purger (retention glissante, STRATEGY_HISTORY_RETENTION_S) le
        point de debut de fenetre necessaire au recalcul -- sans ca, un
        cheval valide pouvait "disparaitre" plus tard alors qu'il avait deja
        rempli le critere. Lecture/ecriture limitees a self.strategy_picks_frozen
        (proprement remis a zero par course dans start_tracking) -- n'affecte
        pas le suivi existant."""
        cfg = STRATEGY_CONFIG.get(classify_discipline(self.selected_discipline))
        if not cfg:
            return None, None

        gagnant = self.strategy_picks_frozen.get("gagnant")
        if gagnant is None:
            gagnant = self.compute_strategy_pick(cfg["gagnant"], now)
            if gagnant is not None:
                self.strategy_picks_frozen["gagnant"] = gagnant

        place = self.strategy_picks_frozen.get("place")
        if place is None:
            place = self.compute_strategy_pick(cfg["place"], now)
            if place is not None:
                self.strategy_picks_frozen["place"] = place

        return gagnant, place

    def handle_odds(self, data):
        gagnant_map = self.parse_participants(data)
        nums = set(gagnant_map.keys())
        if not nums:
            return

        for num in nums:
            self.horse_names[num] = gagnant_map[num]["nom"]
            if num not in self.favoris_order:
                self.favoris_order.append(num)

        now = time.time()
        speed_map = self.update_speed_history(gagnant_map)  # alimente aussi l'historique utilise ci-dessous (deja pendant le warm-up)
        self.update_strategy_history(gagnant_map, now)  # historique dedie (longue retention) pour les 2 sections de strategie -- additif, n'affecte rien ci-dessus

        # -- capture (retardee) du snapshot T1 ---------------------------
        # Le snapshot T1 n'est plus fige des le tout premier poll, mais
        # seulement a self.snapshot_lead_s AVANT l'heure de depart programmee
        # -- 30s avant, pour toutes les disciplines (Trot comme
        # Plat/Obstacle, cf. select_next_course). Avant ce moment, on
        # affiche un mode "warm-up" : tous les chevaux, tries du plus au
        # moins favori, avec un message d'attente indiquant le temps
        # restant avant la capture ; pas de % de chute, pas de masquage,
        # pas de badge (rien de tout ca n'a de sens tant que la reference
        # T1 n'existe pas).
        if not self.snapshot_taken:
            snapshot_ts = (self.depart_ts - self.snapshot_lead_s) if self.depart_ts is not None else None
            snapshot_reached = snapshot_ts is not None and now >= snapshot_ts
            if snapshot_reached:
                for num in nums:
                    self.cote_t1.setdefault(num, gagnant_map[num]["ratio"])
                self.snapshot_taken = True
            else:
                rows = self.build_warmup_rows(gagnant_map)
                snap_msg = STRATEGY_WAIT_MESSAGE
                strategy_gagnant, strategy_place = self.compute_strategy_picks(now)
                set_state(
                    rows=rows,
                    snapLine=snap_msg,
                    toteLabel=self.build_tote_label(len(nums)),
                    statusLine="",
                    updatedAt=time.time(),
                    strategyGagnant=strategy_gagnant,
                    strategyPlace=strategy_place,
                )
                self.maybe_save_snapshot()
                return

        # Cote T1 = snapshot fige au moment de la bascule warm-up -> mode
        # normal ci-dessus. Pour un cheval qui apparaitrait APRES coup (rare :
        # partant declare tardivement), on prend sa toute premiere cote live
        # comme reference, comme avant.
        for num in nums:
            if num not in self.cote_t1:
                self.cote_t1[num] = gagnant_map[num]["ratio"]

        # Pour chaque cheval connu : cote Gagnant actuelle (brute, telle
        # qu'affichee par le PMU), probabilite lissee (EMA) il y a ~15s, et
        # la variation RELATIVE (%) entre les deux (part de marche
        # gagnee/perdue, normalisee pour etre comparable entre favoris et
        # outsiders). Le delta est calcule sur la serie lissee (EMA) pour ne
        # pas reagir a du bruit tick-par-tick ; seule la cote affichee
        # ("coteG") reste la valeur brute instantanee du PMU.
        deltas = {}
        # Ecart T0 -> Live : difference (en points de probabilite implicite, %)
        # entre la cote au moment du snapshot (T0, figee au premier passage du
        # cheval) et la cote actuelle. Positif = la cote du cheval a "chute"
        # (il devient plus favori) ; negatif = sa cote "remonte" (il devient
        # moins favori). C'est desormais le critere principal de tri/masquage.
        ecarts = {}
        for num in self.favoris_order:
            g = gagnant_map.get(num)
            cote_g = g["ratio"] if g else None
            ema_now = self.ema_prob.get(num)
            ema_15 = self.get_value_at(num, now - DELTA_WINDOW_S) if ema_now is not None else None
            delta15 = None
            if ema_now is not None and ema_15 is not None and ema_15 > 0 and ema_now > 0:
                # signe inverse (cote decimale : baisse = favorable, cf. update_speed_history)
                delta15 = (math.exp(-(math.log(ema_now) - math.log(ema_15))) - 1) * 100  # variation relative en %
            deltas[num] = (cote_g, ema_15, delta15)

            cote_t1 = self.cote_t1.get(num)
            cote_live = cote_g
            if cote_live is not None:
                self.cote_live_last[num] = cote_live
            pct_chute = None
            if cote_t1 is not None and cote_live is not None and cote_t1 > 0:
                # % de chute entre T0 et Live : positif = la cote a baisse
                # (cheval devenu plus favori) ; negatif = la cote a monte
                # (cheval devenu moins favori, "remonte").
                pct_chute = (cote_t1 - cote_live) / cote_t1 * 100
            ecarts[num] = (cote_t1, cote_live, pct_chute)

        # VALUEBET : evaluation UNIQUE (pas un recalcul a chaque poll), au
        # moment T+self.valuebet_eval_after_s apres le depart programme --
        # delai qui depend de la discipline (cf. TROT_VALUEBET_EVAL_AFTER_DEPART_S
        # = 30s pour le Trot / GALOP_VALUEBET_EVAL_AFTER_DEPART_S = 120s pour
        # le Galop, fixes lors de la selection de la course). A cet instant
        # precis, on ne marque plus tous les chevaux au-dela d'un seuil -- on
        # choisit UN SEUL cheval : celui qui a subi la plus grosse chute de
        # cote (pctChute, T0 -> Live) parmi les chevaux eligibles (seulement
        # ceux dont la cote live est dans [COTE_RANGE_MIN, COTE_RANGE_MAX]
        # quand self.cote_range_enabled est actif -- Trot uniquement ; en
        # Galop, tous les partants sont eligibles). Il faut que la cote ait
        # effectivement chute (pctChute > 0) pour qu'un signal soit emis --
        # sinon aucun valuebet n'est marque pour cette course. Une fois cette
        # evaluation faite, self.valuebet_seen est FIGE pour le reste de la
        # course (jamais retire, jamais recalcule).
        if self.snapshot_taken and not self.valuebet_eval_done and self.depart_ts is not None:
            eval_ts = self.depart_ts + self.valuebet_eval_after_s
            if now >= eval_ts:
                best_num, best_pc = None, None
                for num, (ct1, clive, pc) in ecarts.items():
                    if pc is None or pc <= 0:
                        continue
                    if self.cote_range_enabled and (clive is None or clive < COTE_RANGE_MIN or clive > COTE_RANGE_MAX):
                        continue
                    if best_pc is None or pc > best_pc:
                        best_num, best_pc = num, pc
                if best_num is not None:
                    self.valuebet_seen[best_num] = {"pctChute": best_pc, "at": now}
                self.valuebet_eval_done = True

        def sort_key(n):
            pc = ecarts[n][2]
            return (pc is None, -(pc if pc is not None else 0))

        # Classement par plus gros % de chute de cote (T0 -> Live, decroissant) :
        # le cheval dont la cote a le plus chute en % (donc devenu le plus
        # favori depuis le snapshot T0) apparait en premier. Seuls les
        # GAINERS_TOP_N premiers restent visibles (les autres sont masques,
        # pas supprimes, donc ils reapparaissent instantanement des qu'ils
        # remontent dans le classement) — et un cheval dont la cote "remonte"
        # (% negatif, il devient moins favori) est TOUJOURS masque, quel que
        # soit son rang.
        display_order = sorted(self.favoris_order, key=sort_key)

        rows = []
        for idx, num in enumerate(display_order):
            g = gagnant_map.get(num)
            cote_g, cote_g15, delta15 = deltas[num]
            cote_t1, cote_live, pct_chute = ecarts[num]
            spd = speed_map.get(num)
            is_fast = spd is not None and abs(spd) >= SPEED_THRESHOLD_PCT_PER_MIN

            # Marquage DEFINITIF : des que la colonne "Δ 15s" (delta15, la
            # variation RELATIVE sur les 15 dernieres secondes) atteint le
            # seuil BIGMOVE_THRESHOLD_PCT (hausse de part de marche
            # uniquement — les baisses ne sont plus signalees), le cheval
            # reste marque pour le reste de la course. Sans ca, la ligne peut
            # redescendre dans le classement / sortir du tableau au cycle
            # suivant et le signal passe inaperçu.
            # L'alerte ne se declenche que dans les BIGMOVE_ALERT_LEAD_S (2 min)
            # avant le depart : avant ca, les mouvements sont frequents mais
            # les enjeux sont trop faibles pour etre significatifs.
            in_alert_window = (
                self.depart_ts is not None
                and (self.depart_ts - now) <= BIGMOVE_ALERT_LEAD_S
            )
            if in_alert_window and delta15 is not None and delta15 >= BIGMOVE_THRESHOLD_PCT:
                prev = self.bigmove_seen.get(num)
                if prev is None or delta15 > prev["delta"]:
                    self.bigmove_seen[num] = {"delta": delta15, "at": now}
            bigmove = self.bigmove_seen.get(num)
            is_bigmove = bigmove is not None

            # remonte = la cote du cheval "remonte" depuis T1 (% de chute
            # negatif, il devient moins favori) -> masque, SAUF si deja
            # marque valuebet (marquage fige a T+30s, cf. plus haut : on le
            # laisse visible quoi qu'il arrive a sa cote ensuite).
            is_remonte = pct_chute is not None and pct_chute < 0

            # VALUEBET : marquage FIGE, decide une seule fois a T+30s (cf.
            # plus haut, avant cette boucle) -- on lit juste ici, on
            # n'ecrit plus rien dans self.valuebet_seen poll apres poll.
            is_valuebet = num in self.valuebet_seen

            # hors-plage = cote Gagnant en DIRECT en dehors de [COTE_RANGE_MIN,
            # COTE_RANGE_MAX] -> masque. Applique pour toutes les disciplines
            # (cf. self.cote_range_enabled). Reevalue a
            # chaque poll sur la cote LIVE (pas T1) : un cheval qui entre
            # dans la plage apparait immediatement, un cheval qui en sort
            # disparait immediatement -- SAUF si marque valuebet : celui-la
            # reste visible meme si sa cote ressort ensuite de la plage
            # (demande explicite).
            is_out_of_range = self.cote_range_enabled and (not is_valuebet) and (
                cote_live is None or cote_live < COTE_RANGE_MIN or cote_live > COTE_RANGE_MAX
            )
            if is_valuebet:
                is_remonte = False  # meme logique : reste visible quoi qu'il arrive

            rows.append({
                "num": num,
                "nom": self.horse_names.get(num, f"#{num}"),
                "coteT0": cote_t1,
                "coteLive": cote_live,
                "pctChute": pct_chute,
                "delta15": delta15,
                "delta120": bigmove["delta"] if bigmove else None,
                "isFavori": bool(g and g.get("favoris")),
                "isFast": is_fast,
                "isBigMove": is_bigmove,
                "isValuebet": is_valuebet,
                "speed": spd,
                # un cheval marque bigmove (definitif) ou valuebet (retire si
                # la cote remonte) reste visible tant qu'il n'est plus dans le
                # top des plus fortes chutes de cote, MAIS un cheval hors-plage
                # [6,20] ou dont la cote remonte restent masques dans tous les cas
                "hidden": is_remonte or ((idx >= GAINERS_TOP_N) and not is_bigmove and not is_valuebet) or is_out_of_range,
            })

        now_str = datetime.now(PARIS_TZ).strftime("%H:%M:%S")
        strategy_gagnant, strategy_place = self.compute_strategy_picks(now)
        set_state(
            rows=rows,
            snapLine=f"📡 Rapports probables mis a jour — {now_str}",
            toteLabel=self.build_tote_label(len(nums)),
            statusLine="",
            updatedAt=time.time(),
            strategyGagnant=strategy_gagnant,
            strategyPlace=strategy_place,
        )
        self.maybe_save_snapshot()

    def poll_once(self):
        if self.selected_reunion is None or self.selected_course is None:
            return
        today = datetime.now(PARIS_TZ)
        path = (f"{PMU_CIT_PREFIX}programme/{date_pmu(today)}/R{self.selected_reunion}/C{self.selected_course}"
                f"/participants?specialisation=OFFLINE")
        try:
            data = http_get_json(PMU_CIT_HOST, path)
            self.handle_odds(data)
            HEALTH["tracker_last_ok_ts"] = time.time()
            HEALTH["tracker_last_error"] = None
        except Exception as e:
            HEALTH["tracker_last_error"] = repr(e)
            set_state(statusLine=f"Erreur cotes : {e}")

    # -- boucle principale -------------------------------------------------
    def run_forever(self):
        restored = self.load_snapshot()
        if restored:
            set_state(
                statusLine="Etat precedent restaure, reprise du suivi…",
                snapLine="🔁 Historique restaure apres redemarrage",
                departTs=self.depart_ts,
            )
            print(f"[TRACKER] etat restaure depuis {STATE_FILE} "
                  f"(R{self.selected_reunion}C{self.selected_course}, "
                  f"{len(self.favoris_order)} chevaux en historique)")
        self.load_programme(reset_selection=not restored)
        self.last_reprog = time.time()
        while True:
            loop_start = time.time()
            try:
                self.poll_once()
            except Exception as e:
                print(f"[TRACKER] erreur inattendue : {e}")

            if time.time() - self.last_reprog >= REPROG_INTERVAL_S:
                self.refresh_programme_background()
                self.last_reprog = time.time()

            elapsed = time.time() - loop_start
            time.sleep(max(0.0, REFRESH_INTERVAL_S - elapsed))


# ---------------------------------------------------------------------------
# Serveur HTTP : sert pmu_bot.html + fichiers statiques, et expose /api/state
# ---------------------------------------------------------------------------
class ThreadingHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class Handler(http.server.SimpleHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        if self.path.startswith("/api/state"):
            self.handle_state()
        elif self.path.startswith("/api/valuebet-log.csv"):
            self.handle_valuebet_csv()
        elif self.path.startswith("/api/valuebet-log/reset"):
            self.handle_valuebet_reset()
        elif self.path.startswith("/api/odds-timeseries.csv"):
            self.handle_odds_timeseries_csv()
        elif self.path.startswith("/api/odds-timeseries/reset"):
            self.handle_odds_timeseries_reset()
        elif self.path.startswith("/api/backup.zip"):
            self.handle_backup()
        elif self.path.startswith("/api/health"):
            self.handle_health()
        elif self.path == "/":
            self.path = "/pmu_bot.html"
            super().do_GET()
        else:
            super().do_GET()

    def do_POST(self):
        if self.path.startswith("/api/restore"):
            self.handle_restore()
        else:
            self.send_response(404)
            self.end_headers()

    def handle_health(self):
        """Diagnostic en direct : depuis combien de temps chaque mecanisme
        (Tracker principal, LookaheadOddsLogger) a-t-il reussi un poll pour
        la derniere fois ? Permet de detecter une panne SILENCIEUSE (le
        processus tourne mais ne collecte plus rien) sans attendre de
        constater le trou le lendemain dans le CSV -- rafraichis cette page
        de temps en temps ; si "secondesDepuisDernierSucces" grimpe sans
        jamais redescendre alors qu'il y a des courses en cours, il y a un
        probleme."""
        now = time.time()
        tracker_ts = HEALTH.get("tracker_last_ok_ts")
        lookahead_ts = HEALTH.get("lookahead_last_ok_ts")
        payload = {
            "trackerPrincipal": {
                "derniersSuccesIl_y_a_s": (round(now - tracker_ts, 1) if tracker_ts else None),
                "derniereErreur": HEALTH.get("tracker_last_error"),
            },
            "lookaheadOddsLogger": {
                "derniersSuccesIl_y_a_s": (round(now - lookahead_ts, 1) if lookahead_ts else None),
                "derniereErreur": HEALTH.get("lookahead_last_error"),
                "coursesSuiviesEnAvanceActuellement": HEALTH.get("lookahead_active_courses", 0),
                # Diagnostic fin (cf. commentaire sur HEALTH) : permet de voir
                # EXACTEMENT a quelle etape ca coince si le CSV ne se remplit
                # plus, sans devoir fouiller les logs Railway.
                "dernierEchantillonAjouteIl_y_a_s": (
                    round(now - HEALTH["lookahead_last_sample_ts"], 1)
                    if HEALTH.get("lookahead_last_sample_ts") else None),
                "totalEchantillonsDepuisDemarrage": HEALTH.get("lookahead_total_samples", 0),
                "dernierFetchVideIl_y_a_s": (
                    round(now - HEALTH["lookahead_last_empty_fetch_ts"], 1)
                    if HEALTH.get("lookahead_last_empty_fetch_ts") else None),
                "fetchsVidesConsecutifs": HEALTH.get("lookahead_consecutive_empty_fetches", 0),
                "dernierFlush": {
                    "il_y_a_s": (round(now - HEALTH["lookahead_last_flush_ts"], 1)
                                 if HEALTH.get("lookahead_last_flush_ts") else None),
                    "label": HEALTH.get("lookahead_last_flush_label"),
                    "chevaux": HEALTH.get("lookahead_last_flush_chevaux"),
                    "points": HEALTH.get("lookahead_last_flush_points"),
                },
                "derniereEcritureReussieIl_y_a_s": (
                    round(now - HEALTH["lookahead_last_write_ok_ts"], 1)
                    if HEALTH.get("lookahead_last_write_ok_ts") else None),
                "derniereEcritureLabel": HEALTH.get("lookahead_last_write_label"),
                "derniereErreurEcriture": HEALTH.get("lookahead_last_write_error"),
            },
        }
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def handle_backup(self):
        """Sauvegarde complete des donnees brutes (les 2 fichiers .jsonl,
        pas les CSV derives) dans un .zip -- a telecharger AVANT toute
        modification/redeploiement du bot, pour pouvoir les restaurer
        ensuite via /api/restore si le systeme de fichiers est reinitialise
        (Railway sans volume persistant efface tout a chaque redeploiement)."""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for path, arcname in [
                (VALUEBET_LOG_FILE, "valuebet_log.jsonl"),
                (ODDS_TIMESERIES_LOG_FILE, "odds_timeseries.jsonl"),
            ]:
                try:
                    with open(path, "rb") as f:
                        zf.writestr(arcname, f.read())
                except FileNotFoundError:
                    zf.writestr(arcname, "")  # fichier pas encore cree -> vide dans le zip, pas d'erreur
        body = buf.getvalue()
        self.send_response(200)
        self.send_header("Content-Type", "application/zip")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Content-Disposition", 'attachment; filename="pmu_bot_backup.zip"')
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def handle_restore(self):
        """Restaure les 2 fichiers .jsonl a partir d'un .zip envoye en POST
        (le meme format que celui genere par /api/backup.zip). ECRASE les
        fichiers actuels -- a utiliser juste apres un redeploiement pour
        recuperer les donnees sauvegardees avant modification du bot."""
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            buf = io.BytesIO(body)
            restored = []
            with zipfile.ZipFile(buf, "r") as zf:
                for arcname, path in [
                    ("valuebet_log.jsonl", VALUEBET_LOG_FILE),
                    ("odds_timeseries.jsonl", ODDS_TIMESERIES_LOG_FILE),
                ]:
                    try:
                        data = zf.read(arcname)
                    except KeyError:
                        continue
                    if not data:
                        continue
                    with open(path, "wb") as f:
                        f.write(data)
                    restored.append(arcname)
            msg = f"Restaure : {', '.join(restored) if restored else '(rien -- zip vide ou fichiers absents du zip)'}"
            resp = msg.encode("utf-8")
            self.send_response(200)
        except Exception as e:
            resp = f"Echec de la restauration : {e}".encode("utf-8")
            self.send_response(500)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(resp)))
        self.end_headers()
        self.wfile.write(resp)

    def handle_valuebet_csv(self):
        body = build_valuebet_csv().encode("utf-8-sig")  # BOM pour un Excel/LibreOffice content
        self.send_response(200)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Content-Disposition", 'attachment; filename="valuebet_log.csv"')
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def handle_odds_timeseries_csv(self):
        body = build_odds_timeseries_csv().encode("utf-8-sig")
        self.send_response(200)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Content-Disposition", 'attachment; filename="odds_timeseries.csv"')
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def handle_odds_timeseries_reset(self):
        query = urllib.parse.urlsplit(self.path).query
        params = urllib.parse.parse_qs(query)
        confirmed = params.get("confirm", [""])[0].lower() in ("oui", "yes", "1", "true")
        if not confirmed:
            body = ("Rien supprime. Ajoute ?confirm=oui a l'URL pour "
                    "confirmer la remise a zero du journal des series temporelles.").encode("utf-8")
            self.send_response(200)
        else:
            try:
                with ODDS_TIMESERIES_LOG_LOCK:
                    open(ODDS_TIMESERIES_LOG_FILE, "w", encoding="utf-8").close()
                body = "Journal des series temporelles remis a zero.".encode("utf-8")
                self.send_response(200)
                print("[ODDS-TS-LOG] journal remis a zero via /api/odds-timeseries/reset")
            except Exception as e:
                body = f"Echec de la remise a zero : {e}".encode("utf-8")
                self.send_response(500)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def handle_valuebet_reset(self):
        # Protection minimale contre un declenchement accidentel (lien
        # partage par erreur, crawler, etc.) : il faut explicitement
        # ?confirm=oui dans l'URL pour que la suppression ait lieu. Un simple
        # GET sur /api/valuebet-log/reset (sans ce parametre) affiche juste
        # un message d'aide, sans rien supprimer.
        query = urllib.parse.urlsplit(self.path).query
        params = urllib.parse.parse_qs(query)
        confirmed = params.get("confirm", [""])[0].lower() in ("oui", "yes", "1", "true")
        if not confirmed:
            body = ("Rien supprime. Ajoute ?confirm=oui a l'URL pour "
                    "confirmer la remise a zero du journal valuebet.").encode("utf-8")
            self.send_response(200)
        else:
            try:
                with VALUEBET_LOG_LOCK:
                    open(VALUEBET_LOG_FILE, "w", encoding="utf-8").close()
                body = "Journal valuebet remis a zero.".encode("utf-8")
                self.send_response(200)
                print("[VALUEBET-LOG] journal remis a zero via /api/valuebet-log/reset")
            except Exception as e:
                body = f"Echec de la remise a zero : {e}".encode("utf-8")
                self.send_response(500)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def handle_state(self):
        body = get_state_json().encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def end_headers(self):
        if not self.path.startswith("/api/"):
            self.send_header("Access-Control-Allow-Origin", "*")
        super().end_headers()

    def log_message(self, format, *args):
        if not self.path.startswith("/api/state"):  # evite de noyer les logs avec le polling frequent
            print(f"[HTTP] {format % args}")


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    tracker = Tracker()
    threading.Thread(target=tracker.run_forever, daemon=True).start()

    # Mecanisme independant, en parallele du Tracker principal, uniquement
    # pour capturer la portion "avant course" (T-5min) du journal
    # odds_timeseries que le Tracker principal rate souvent en pratique.
    # Ne touche a aucun etat/comportement du Tracker principal (cf.
    # LookaheadOddsLogger, docstring).
    lookahead = LookaheadOddsLogger(tracker)
    threading.Thread(target=lookahead.run_forever, daemon=True).start()

    with ThreadingHTTPServer(("0.0.0.0", PORT), Handler) as httpd:
        print(f"Serveur lance sur le port {PORT} (suivi PMU actif en tache de fond)")
        httpd.serve_forever()
