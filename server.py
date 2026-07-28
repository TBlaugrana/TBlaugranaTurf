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
COTE_RANGE_MIN = 0.0
COTE_RANGE_MAX = 20.0

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

# Seuil de declenchement du signal VALUEBET : des que la cote d'un cheval a
# chute (pctChute, colonne "% Chute", ecart T0 -> Live) de 10% ou plus, il
# est marque valuebet. Ce marquage est RETIRE des que pctChute repasse sous
# ce seuil (contrairement a bigmove qui reste definitif), independamment de
# la vitesse de variation ou d'une fenetre de temps avant le depart.
VALUEBET_CHUTE_THRESHOLD_PCT = 10.0

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
# Journal silencieux des signaux valuebet (pour analyse ulterieure : ROI,
# taux de reussite, etc.). N'affecte ni l'interface ni le fonctionnement du
# suivi : c'est un simple fichier ecrit en tache de fond, jamais lu ni
# expose par /api/state. A chaque bascule vers la course suivante, on fige
# la liste des chevaux marques valuebet sur la course qu'on quitte (pas
# avant, pour laisser le marquage se stabiliser jusqu'au dernier moment),
# puis on va chercher en arriere-plan les rapports definitifs PMU de cette
# course pour pouvoir calculer un ROI plus tard.
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
ODDS_TIMESERIES_SAMPLE_INTERVAL_S = 15   # un point toutes les 15s
ODDS_TIMESERIES_WINDOW_BEFORE_S = 5 * 60  # a partir de T-5min (T = heure de depart PROGRAMMEE, cf. self.depart_ts)
ODDS_TIMESERIES_WINDOW_AFTER_S = 5 * 60   # jusqu'a T+5min
ODDS_TIMESERIES_CHUTE_MIN_PCT = 0.0    # on ne garde que les chevaux dont la cote a chute d'au moins ce %...
ODDS_TIMESERIES_CHUTE_MAX_PCT = 200.0  # ...et au plus ce % (filtre large, demande explicite : 0 a 200%) -- NOTE: filtre desactive depuis la fusion avec l'ancien race_snapshot, tout le peloton est echantillonne


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
    """Lit le journal valuebet_log.jsonl (une ligne JSON par course) et le
    met a plat en CSV, une ligne par cheval valuebet. Colonnes best-effort
    pour Gagnant/Place ; la colonne rapportsRawJson garde toujours les
    rapports bruts de la course en secours si l'extraction ci-dessus ne
    trouve rien (format PMU pas garanti identique pour tous les paris)."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "date", "reunion", "course", "label", "hippodrome", "paysCode", "paysLabel", "etrangere", "discipline",
        "nbPartants", "heureDepart", "nbValuebetsSimultanes",
        "loggedAt", "num", "nom", "coteT0", "probaImpliciteT0", "pctChuteAuMarquage", "marqueAt",
        "coteAuMarquage", "probaImpliciteAuMarquage",
        "coteLiveFinale", "probaImpliciteFinale", "pctChuteFinale", "deltaProbaAuMarquage", "deltaProbaFinale",
        "delaiSignalDepartMin", "classementFinal",
        "dividendeGagnant", "dividendePlace", "rapportsType", "rapportsRawJson",
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
                rapports = entry.get("rapports")
                rapports_raw = json.dumps(rapports, ensure_ascii=False) if rapports is not None else ""
                logged_at = entry.get("loggedAt")
                logged_at_str = (datetime.fromtimestamp(logged_at, tz=PARIS_TZ).isoformat()
                                  if logged_at else "")
                for h in (entry.get("valuebetHorses") or []):
                    num = h.get("num")
                    marque_at = h.get("marqueAt")
                    marque_at_str = (datetime.fromtimestamp(marque_at, tz=PARIS_TZ).isoformat()
                                      if marque_at else "")
                    cote_t0 = h.get("coteT0")
                    pct_marquage = h.get("pctChuteAuMarquage")
                    cote_finale = h.get("coteLiveFinale")
                    cote_au_marquage = (cote_t0 * (1 - pct_marquage / 100.0)
                                         if cote_t0 is not None and pct_marquage is not None else None)
                    delta_proba_marquage = delta_proba(cote_t0, cote_au_marquage)
                    delta_proba_finale = delta_proba(cote_t0, cote_finale)
                    writer.writerow([
                        entry.get("date", ""),
                        entry.get("reunion", ""),
                        entry.get("course", ""),
                        entry.get("label", ""),
                        entry.get("hippodrome", ""),
                        entry.get("paysCode", ""),
                        entry.get("paysLabel", ""),
                        entry.get("etrangere", ""),
                        entry.get("discipline", ""),
                        entry.get("nbPartants", ""),
                        entry.get("heureDepart", ""),
                        entry.get("nbValuebetsSimultanes", ""),
                        logged_at_str,
                        num,
                        h.get("nom", ""),
                        cote_t0,
                        proba_implicite(cote_t0),
                        pct_marquage,
                        marque_at_str,
                        cote_au_marquage,
                        proba_implicite(cote_au_marquage),
                        cote_finale,
                        proba_implicite(cote_finale),
                        h.get("pctChuteFinale", ""),
                        delta_proba_marquage,
                        delta_proba_finale,
                        h.get("delaiSignalDepartMin", ""),
                        h.get("classementFinal", ""),
                        extract_dividende(rapports, num, "GAGNANT"),
                        extract_dividende(rapports, num, "PLACE"),
                        entry.get("rapportsType", ""),
                        rapports_raw,
                    ])
    except FileNotFoundError:
        pass  # aucun signal valuebet enregistre pour l'instant -> CSV avec juste l'entete
    return buf.getvalue()


def build_odds_timeseries_csv():
    """Lit le journal odds_timeseries.jsonl (une ligne JSON par course,
    contenant TOUT le peloton suivi avec sa serie de points toutes les
    15s) et le met a plat en CSV, une ligne par (cheval, instant). Fusionne
    l'ancien journal race_snapshot : coteFinale/rangCoteFinale/deltaProba
    sont repetes sur chaque ligne d'un meme cheval pour rester facilement
    filtrables sans avoir a re-agreger la serie temporelle."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "date", "reunion", "course", "label", "hippodrome", "paysCode", "etrangere",
        "discipline", "nbPartants", "heureDepart", "rapportsType",
        "num", "nom", "coteT0", "probaImpliciteT0", "coteFinale", "probaImpliciteFinale",
        "rangCoteFinale", "deltaProbaFinale",
        "classementFinal", "dividendeGagnant", "dividendePlace",
        "t", "secondesDepuisDepart", "coteLiveInstant", "probaImpliciteInstant", "pctChuteInstant", "deltaProbaInstant",
    ])
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
                depart_iso = entry.get("heureDepart")
                depart_ts = None
                if depart_iso:
                    try:
                        depart_ts = datetime.fromisoformat(depart_iso).timestamp()
                    except Exception:
                        depart_ts = None
                for h in (entry.get("horses") or []):
                    for s in (h.get("samples") or []):
                        t = s.get("t")
                        t_str = datetime.fromtimestamp(t, tz=PARIS_TZ).isoformat() if t else ""
                        secs_from_depart = (t - depart_ts) if (t is not None and depart_ts is not None) else ""
                        writer.writerow([
                            entry.get("date", ""),
                            entry.get("reunion", ""),
                            entry.get("course", ""),
                            entry.get("label", ""),
                            entry.get("hippodrome", ""),
                            entry.get("paysCode", ""),
                            entry.get("etrangere", ""),
                            entry.get("discipline", ""),
                            entry.get("nbPartants", ""),
                            depart_iso or "",
                            entry.get("rapportsType", ""),
                            h.get("num", ""),
                            h.get("nom", ""),
                            h.get("coteT0", ""),
                            proba_implicite(h.get("coteT0")),
                            h.get("coteFinale", ""),
                            proba_implicite(h.get("coteFinale")),
                            h.get("rangCoteFinale", ""),
                            delta_proba(h.get("coteT0"), h.get("coteFinale")),
                            h.get("classementFinal", ""),
                            h.get("dividendeGagnant", ""),
                            h.get("dividendePlace", ""),
                            t_str,
                            secs_from_depart,
                            s.get("coteLive", ""),
                            proba_implicite(s.get("coteLive")),
                            s.get("pctChute", ""),
                            delta_proba(s.get("coteT0"), s.get("coteLive")),
                        ])
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
}


def set_state(**kwargs):
    with STATE_LOCK:
        STATE.update(kwargs)


def get_state_json():
    with STATE_LOCK:
        return json.dumps(STATE)


# ---------------------------------------------------------------------------
# Connexion HTTPS persistante vers l'API PMU (reutilisee entre les appels)
# ---------------------------------------------------------------------------
_conns = {}


def http_get_json(host, path):
    conn = _conns.get(host)
    if conn is None:
        conn = http.client.HTTPSConnection(host, timeout=FETCH_TIMEOUT_S)
        _conns[host] = conn
    try:
        conn.request("GET", path, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        resp = conn.getresponse()
        data = resp.read()
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status} sur {host}{path}")
        return json.loads(data)
    except Exception:
        _conns.pop(host, None)  # force une reconnexion propre au prochain appel
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
    """Ecrit en tache de fond le journal des series temporelles de
    cotes (fichier SEPARE, ODDS_TIMESERIES_LOG_FILE) : un point toutes
    les 15s pour chaque cheval en chute (0 a 200%) entre T-5min et
    T+5min (T = heure de depart PROGRAMMEE). Attend aussi le classement
    final (best-effort, cf. extract_classement) avant d'ecrire, comme
    pour le journal valuebet — mais dans un thread separe, pour ne pas
    dependre l'un de l'autre ni bloquer le suivi en direct."""
    if not timeseries_buffer:
        return
    hippodrome = (course_info or {}).get("hip")
    discipline = (course_info or {}).get("discipline")
    nb_partants = (course_info or {}).get("nbPartants")
    pays_code = (course_info or {}).get("paysCode")
    pays_label = (course_info or {}).get("paysLabel")
    etrangere = (course_info or {}).get("etrangere")
    depart_ms = (course_info or {}).get("depart")
    heure_depart = (datetime.fromtimestamp(depart_ms / 1000.0, tz=PARIS_TZ).isoformat()
                     if depart_ms else None)

    def worker():
        participants_data = None
        try:
            for attempt in range(1, RAPPORTS_FETCH_ATTEMPTS + 1):
                participants_data = fetch_participants_arrivee(date_str, reunion, course)
                if participants_data and extract_classement(participants_data, next(iter(timeseries_buffer))):
                    break
                time.sleep(RAPPORTS_FETCH_RETRY_DELAY_S)
        except Exception as e:
            print(f"[ODDS-TS-LOG] {label} : echec recuperation classement : {e!r}")

        # Rapports (dividendes definitifs, avec repli provisoires) : meme
        # logique que le journal valuebet, pour que CHAQUE cheval de ce
        # journal (pas seulement ceux marques valuebet) ait son resultat
        # Gagnant/Place final.
        rapports = None
        rapports_type = None
        try:
            rapports = fetch_rapports_definitifs(date_str, reunion, course)
            if rapports:
                rapports_type = "definitifs"
        except Exception as e:
            print(f"[ODDS-TS-LOG] {label} : echec recuperation rapports definitifs : {e!r}")
        if rapports is None:
            try:
                rapports = fetch_rapports_provisoires(date_str, reunion, course)
                if rapports:
                    rapports_type = "provisoires"
            except Exception as e:
                print(f"[ODDS-TS-LOG] {label} : echec recuperation rapports provisoires : {e!r}")

        # Rang de cote dans le peloton complet (1 = favori officiel de la
        # course = cote finale la plus basse), fusionne ici depuis
        # l'ancien journal race_snapshot : calcule a partir de la
        # derniere cote live connue de chaque cheval (dernier sample).
        cote_finale_par_num = {num: samples[-1]["coteLive"] for num, samples in timeseries_buffer.items() if samples}
        ranked = sorted(cote_finale_par_num.items(), key=lambda kv: kv[1])
        rang_par_num = {num: i + 1 for i, (num, _) in enumerate(ranked)}

        horses = []
        for num, samples in timeseries_buffer.items():
            cote_finale = cote_finale_par_num.get(num)
            cote_t0 = samples[0]["coteT0"] if samples else None
            horses.append({
                "num": num,
                "nom": horse_names.get(num, f"#{num}"),
                "coteT0": cote_t0,
                "coteFinale": cote_finale,
                "rangCoteFinale": rang_par_num.get(num),  # 1 = favori officiel de la course
                "classementFinal": extract_classement(participants_data, num),
                "dividendeGagnant": extract_dividende(rapports, num, "GAGNANT"),
                "dividendePlace": extract_dividende(rapports, num, "PLACE"),
                "samples": samples,  # [{"t":..., "coteT0":..., "coteLive":..., "pctChute":...}, ...]
            })

        entry = {
            "loggedAt": time.time(),
            "date": date_str,
            "reunion": reunion,
            "course": course,
            "label": label,
            "hippodrome": hippodrome,
            "paysCode": pays_code,
            "paysLabel": pays_label,
            "etrangere": etrangere,
            "discipline": discipline,
            "nbPartants": nb_partants,
            "heureDepart": heure_depart,
            "rapportsType": rapports_type,  # "definitifs", "provisoires" ou None
            "sampleIntervalS": ODDS_TIMESERIES_SAMPLE_INTERVAL_S,
            "windowBeforeS": ODDS_TIMESERIES_WINDOW_BEFORE_S,
            "windowAfterS": ODDS_TIMESERIES_WINDOW_AFTER_S,
            "horses": horses,
        }
        try:
            with ODDS_TIMESERIES_LOG_LOCK:
                with open(ODDS_TIMESERIES_LOG_FILE, "a", encoding="utf-8") as f:
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            print(f"[ODDS-TS-LOG] {label} : {len(horses)} cheval(aux), "
                  f"{sum(len(h['samples']) for h in horses)} points au total")
        except Exception as e:
            print(f"[ODDS-TS-LOG] echec ecriture journal : {e}")


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
    (cote_t0, valuebet_seen, selected_reunion, l'affichage /api/state,
    etc.). Il lit seulement tracker.all_courses en lecture seule (deja mis
    a jour par le Tracker principal) et fait ses propres requetes HTTP
    independantes, dans son propre thread. Zero impact sur le comportement
    existant du bot -- uniquement une source supplementaire de donnees pour
    le meme journal odds_timeseries.jsonl (via log_odds_timeseries_async,
    la meme fonction que le Tracker principal utilise)."""

    def __init__(self, tracker):
        self.tracker = tracker
        self.current_key = None          # (reunion, course) actuellement suivie en avance
        self.current_course_info = None
        self.buffer = defaultdict(list)  # num -> [{"t":..., "coteT0":..., "coteLive":..., "pctChute":...}, ...]
        self.horse_names = {}
        self.last_sample_ts = 0.0
        self.flushed_keys = set()        # eviter de logguer deux fois la meme course

    def pick_target_course(self):
        """Choisit, parmi tracker.all_courses, la course la plus proche dans
        le temps dont on est dans la fenetre [depart-5min, depart+5min] et
        qui n'a pas deja ete flushee par ce mecanisme."""
        now = time.time()
        candidates = []
        for c in self.tracker.all_courses:
            depart_ms = c.get("depart")
            if not depart_ms:
                continue
            key = (c["numReunion"], c["course"])
            if key in self.flushed_keys:
                continue
            depart_s = depart_ms / 1000.0
            if (depart_s - ODDS_TIMESERIES_WINDOW_BEFORE_S) <= now <= (depart_s + ODDS_TIMESERIES_WINDOW_AFTER_S):
                candidates.append(c)
        if not candidates:
            return None
        candidates.sort(key=lambda c: c["depart"])
        return candidates[0]

    def flush_current(self):
        if self.current_key is not None and self.buffer:
            reunion, course = self.current_key
            label = f"R{reunion}C{course}"
            date_str = date_pmu(datetime.now(PARIS_TZ))
            log_odds_timeseries_async(date_str, reunion, course, label,
                                       dict(self.buffer), dict(self.horse_names),
                                       self.current_course_info)
            self.flushed_keys.add(self.current_key)
        self.buffer = defaultdict(list)
        self.horse_names = {}

    def poll_once(self):
        target = self.pick_target_course()
        if target is None:
            if self.current_key is not None:
                self.flush_current()
                self.current_key = None
                self.current_course_info = None
            return

        key = (target["numReunion"], target["course"])
        if key != self.current_key:
            self.flush_current()
            self.current_key = key
            self.current_course_info = target
            self.last_sample_ts = 0.0

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
            return
        for num, info in gagnant_map.items():
            self.horse_names[num] = info["nom"]

        now = time.time()
        if (now - self.last_sample_ts) < ODDS_TIMESERIES_SAMPLE_INTERVAL_S:
            return
        self.last_sample_ts = now

        for num, info in gagnant_map.items():
            cote_live = info.get("ratio")
            if cote_live is None:
                continue
            existing = self.buffer.get(num)
            # coteT0 "locale" a ce mini-suivi en avance : la 1ere cote vue
            # pour ce cheval ICI (peut differer legerement du coteT0 du
            # Tracker principal, qui demarre son propre suivi plus tard).
            cote_t0 = existing[0]["coteT0"] if existing else cote_live
            pct_chute = ((cote_t0 - cote_live) / cote_t0 * 100) if cote_t0 else None
            self.buffer[num].append({
                "t": now,
                "coteT0": cote_t0,
                "coteLive": cote_live,
                "pctChute": pct_chute,
            })

    def run_forever(self):
        while True:
            try:
                self.poll_once()
            except Exception as e:
                print(f"[LOOKAHEAD] erreur boucle : {e!r}")
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

        self.favoris_order = []   # ordre de suivi des chevaux (num en string)
        self.horse_names = {}
        self.cote_t0 = {}         # num -> cote Gagnant (%) au moment du snapshot T0 (1ere cote vue pour ce cheval sur cette course)
        self.cote_live_last = {}  # num -> derniere cote Gagnant live connue (mise a jour a chaque poll) ; sert a capturer la "cote finale" au moment de la bascule de course
        self.odds_history = {}    # num -> [(t, prob_lissee_ema), ...]
        self.ema_prob = {}        # num -> derniere probabilite implicite lissee (EMA)
        self.ema_last_t = {}      # num -> timestamp du dernier point utilise pour l'EMA
        self.bigmove_seen = {}    # num -> {"delta": ..., "at": ...} une fois le seuil relatif franchi (marquage definitif)
        self.valuebet_seen = {}   # num -> {"pctChute": ..., "at": ...} une fois le seuil de chute franchi ; retire si la cote remonte ensuite (voir handle_odds)
        self.odds_timeseries_buffer = defaultdict(list)  # num -> [{"t":..., "coteLive":..., "pctChute":...}, ...] echantillonne toutes les 15s pres du depart (journal separe, cf. ODDS_TIMESERIES_*)
        self.last_timeseries_sample_ts = 0.0
        self.tracking_started_at = None  # timestamp du debut du suivi de la course en cours (informatif)
        self.snapshot_taken = False      # True des que le snapshot T0 a ete capture (bascule mode warm-up -> mode normal)
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
                "cote_t0": self.cote_t0,
                "odds_history": self.odds_history,
                "ema_prob": self.ema_prob,
                "ema_last_t": self.ema_last_t,
                "bigmove_seen": self.bigmove_seen,
                "valuebet_seen": self.valuebet_seen,
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
        self.favoris_order = snap.get("favoris_order") or []
        self.horse_names = snap.get("horse_names") or {}
        self.cote_t0 = snap.get("cote_t0") or {}
        self.odds_history = snap.get("odds_history") or {}
        self.ema_prob = snap.get("ema_prob") or {}
        self.ema_last_t = snap.get("ema_last_t") or {}
        self.bigmove_seen = snap.get("bigmove_seen") or {}
        self.valuebet_seen = snap.get("valuebet_seen") or {}
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

    # -- journal silencieux valuebet (aucun impact interface/etat) --------
    def capture_valuebet_snapshot(self):
        """Fige la liste des chevaux actuellement marques valuebet sur la
        course en cours, avec de quoi les identifier et calculer un ROI
        ensuite. Fournit deux photos de la cote de chaque cheval :
        - au moment ou le signal valuebet s'est declenche pour la premiere
          fois (pctChuteAuMarquage, fige des ce moment-la, cf. valuebet_seen) ;
        - juste avant la bascule vers la course suivante, donc proche du
          depart reel (coteLiveFinale / pctChuteFinale), recalculee ici a
          partir de la derniere cote live vue (cote_live_last)."""
        horses = []
        for num, info in self.valuebet_seen.items():
            cote_t0 = self.cote_t0.get(num)
            cote_live_finale = self.cote_live_last.get(num)
            pct_chute_finale = None
            if cote_t0 is not None and cote_live_finale is not None and cote_t0 > 0:
                pct_chute_finale = (cote_t0 - cote_live_finale) / cote_t0 * 100
            marque_at = info.get("at")
            delai_signal_depart_min = None
            if marque_at is not None and self.depart_ts is not None:
                delai_signal_depart_min = (self.depart_ts - marque_at) / 60.0
            horses.append({
                "num": num,
                "nom": self.horse_names.get(num, f"#{num}"),
                "coteT0": cote_t0,
                "pctChuteAuMarquage": info.get("pctChute"),
                "marqueAt": marque_at,
                "coteLiveFinale": cote_live_finale,
                "pctChuteFinale": pct_chute_finale,
                "delaiSignalDepartMin": delai_signal_depart_min,
            })
        return horses

    def log_race_valuebets_async(self, date_str, reunion, course, label, horses, course_info=None):
        """Lance en tache de fond (thread separe, ne bloque jamais la boucle
        de suivi) la recuperation des rapports definitifs de la course
        qu'on vient de quitter, puis ecrit une ligne JSON dans le journal.
        Ne fait rien si aucun cheval n'etait marque valuebet (rien a logger).
        course_info (dict issu de self.all_courses, optionnel) fournit le
        pays/hippodrome pour pouvoir filtrer les courses etrangeres plus
        tard sans devoir re-parser le programme."""
        if not horses:
            return
        hippodrome = (course_info or {}).get("hip")
        pays_code = (course_info or {}).get("paysCode")
        pays_label = (course_info or {}).get("paysLabel")
        etrangere = (course_info or {}).get("etrangere")
        discipline = (course_info or {}).get("discipline")
        nb_partants = (course_info or {}).get("nbPartants")
        depart_ms = (course_info or {}).get("depart")
        heure_depart = (datetime.fromtimestamp(depart_ms / 1000.0, tz=PARIS_TZ).isoformat()
                         if depart_ms else None)
        nb_valuebets_simultanes = len(horses)

        def worker():
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
                print(f"[VALUEBET-LOG] {label} : rapports indisponibles apres "
                      f"{RAPPORTS_FETCH_ATTEMPTS} tentatives (+ repli provisoires echoue). "
                      f"Derniere erreur : {last_err!r}")
            else:
                print(f"[VALUEBET-LOG] {label} : rapports {rapports_type} recuperes avec succes "
                      f"({len(horses)} cheval(aux) valuebet)")

            # Classement final (best-effort, cf. extract_classement) : recupere
            # une seule fois pour la course, puis assigne a chaque cheval.
            participants_data = None
            try:
                participants_data = fetch_participants_arrivee(date_str, reunion, course)
            except Exception as e:
                print(f"[VALUEBET-LOG] {label} : echec recuperation classement final : {e!r}")
            for h in horses:
                h["classementFinal"] = extract_classement(participants_data, h["num"])

            entry = {
                "loggedAt": time.time(),
                "date": date_str,
                "reunion": reunion,
                "course": course,
                "label": label,
                "hippodrome": hippodrome,
                "paysCode": pays_code,
                "paysLabel": pays_label,
                "etrangere": etrangere,  # True/False/None (None si info indisponible)
                "discipline": discipline,  # "TROT", "PLAT", "OBSTACLE"... (tel que fourni par l'API PMU)
                "nbPartants": nb_partants,
                "heureDepart": heure_depart,
                "nbValuebetsSimultanes": nb_valuebets_simultanes,
                "valuebetHorses": horses,
                "rapports": rapports,           # None si toujours indisponible malgre les tentatives
                "rapportsType": rapports_type,  # "definitifs", "provisoires" ou None
            }
            try:
                with VALUEBET_LOG_LOCK:
                    with open(VALUEBET_LOG_FILE, "a", encoding="utf-8") as f:
                        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            except Exception as e:
                print(f"[VALUEBET-LOG] echec ecriture journal : {e}")

        threading.Thread(target=worker, daemon=True).start()


    def select_next_course(self):
        now_ms = time.time() * 1000
        upcoming = [c for c in self.all_courses if c["depart"] and c["depart"] >= now_ms]
        chosen = upcoming[0] if upcoming else (self.all_courses[-1] if self.all_courses else None)
        if not chosen:
            set_state(courseInfo="Aucune course disponible.", statusLine="Aucune course disponible pour le moment.")
            return

        # Juste avant de basculer vers la course suivante (pas avant : le
        # marquage valuebet doit avoir le temps de se stabiliser jusqu'au
        # bout), on fige silencieusement les chevaux valuebet de la course
        # qu'on quitte et on programme la recuperation de ses rapports.
        if self.selected_reunion is not None and self.selected_course is not None:
            prev_horses = self.capture_valuebet_snapshot()
            prev_label = f"R{self.selected_reunion}C{self.selected_course}"
            prev_date = date_pmu(datetime.now(PARIS_TZ))
            prev_course_info = next(
                (c for c in self.all_courses
                 if c["numReunion"] == self.selected_reunion and c["course"] == self.selected_course),
                None,
            )
            self.log_race_valuebets_async(
                prev_date, self.selected_reunion, self.selected_course, prev_label, prev_horses,
                prev_course_info,
            )

            # Meme principe pour le journal des series temporelles de cotes
            # (fichier separe, cf. ODDS_TIMESERIES_*) : on fige le buffer
            # accumule pendant la course qu'on quitte avant qu'il ne soit
            # reinitialise par start_tracking() juste apres. Ce journal
            # couvre desormais TOUT le peloton (fusionne avec l'ancien
            # race_snapshot) et sert donc aussi de groupe de controle.
            prev_timeseries = dict(self.odds_timeseries_buffer)
            prev_horse_names = dict(self.horse_names)
            log_odds_timeseries_async(
                prev_date, self.selected_reunion, self.selected_course, prev_label,
                prev_timeseries, prev_horse_names, prev_course_info,
            )

        self.selected_reunion = chosen["numReunion"]
        self.selected_course = chosen["course"]
        self.depart_ts = chosen["depart"] / 1000.0
        self.selected_discipline = chosen["discipline"]
        self.selected_nb_partants = chosen["nbPartants"]
        set_state(courseInfo=self.format_course_label(chosen), departTs=self.depart_ts)
        self.start_tracking()
        self.maybe_save_snapshot(force=True)

    def start_tracking(self):
        self.favoris_order = []
        self.horse_names = {}
        self.cote_t0 = {}
        self.cote_live_last = {}
        self.ema_prob = {}
        self.ema_last_t = {}
        self.bigmove_seen = {}
        self.valuebet_seen = {}
        self.odds_timeseries_buffer = defaultdict(list)
        self.last_timeseries_sample_ts = 0.0
        self.tracking_started_at = time.time()
        self.snapshot_taken = False
        set_state(rows=[], snapLine="📸 Snapshot T0 au depart de la course — en attente…")

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

    def build_tote_label(self, nb_live=None):
        parts = []
        if self.selected_discipline:
            parts.append(self.selected_discipline)
        nb = nb_live if nb_live is not None else self.selected_nb_partants
        if nb is not None:
            parts.append(f"{nb} partant{'s' if nb > 1 else ''}")
        return " · ".join(parts)

    def build_warmup_rows(self, gagnant_map):
        """Lignes affichees avant la capture du snapshot T0 : tous les
        chevaux avec une cote live, tries du plus au moins favori (cote
        decimale croissante), sans masquage ni badge (chute/vitesse/valuebet
        n'ont pas de sens tant que T0 n'est pas fige)."""
        rows = []
        ordered = sorted(gagnant_map.items(), key=lambda kv: kv[1]["ratio"])
        for num, info in ordered:
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

        # -- capture (retardee) du snapshot T0 ---------------------------
        # Le snapshot T0 n'est plus fige des le tout premier poll, mais
        # seulement a l'heure de depart officielle de la course ("cote de
        # depart"). Avant ce moment, on affiche un mode "warm-up" : tous les
        # chevaux, tries du plus au moins favori, avec un message d'attente
        # indiquant le temps restant avant le depart ; pas de % de chute, pas
        # de masquage, pas de badge (rien de tout ca n'a de sens tant que la
        # reference T0 n'existe pas).
        if not self.snapshot_taken:
            depart_reached = self.depart_ts is not None and now >= self.depart_ts
            if depart_reached:
                for num in nums:
                    self.cote_t0.setdefault(num, gagnant_map[num]["ratio"])
                self.snapshot_taken = True
            else:
                rows = self.build_warmup_rows(gagnant_map)
                if self.depart_ts is not None:
                    remaining_s = max(0, int(self.depart_ts - now))
                    m, s = divmod(remaining_s, 60)
                    remaining_str = f"{m}:{s:02d}"
                    snap_msg = f"📸 Snapshot T0 au depart de la course — dans {remaining_str}"
                else:
                    snap_msg = "📸 Snapshot T0 au depart de la course — en attente de l'heure de depart…"
                set_state(
                    rows=rows,
                    snapLine=snap_msg,
                    toteLabel=self.build_tote_label(len(nums)),
                    statusLine="",
                    updatedAt=time.time(),
                )
                self.maybe_save_snapshot()
                return

        # Cote T0 = snapshot fige au moment de la bascule warm-up -> mode
        # normal ci-dessus. Pour un cheval qui apparaitrait APRES coup (rare :
        # partant declare tardivement), on prend sa toute premiere cote live
        # comme reference, comme avant.
        for num in nums:
            if num not in self.cote_t0:
                self.cote_t0[num] = gagnant_map[num]["ratio"]

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

            cote_t0 = self.cote_t0.get(num)
            cote_live = cote_g
            if cote_live is not None:
                self.cote_live_last[num] = cote_live
            pct_chute = None
            if cote_t0 is not None and cote_live is not None and cote_t0 > 0:
                # % de chute entre T0 et Live : positif = la cote a baisse
                # (cheval devenu plus favori) ; negatif = la cote a monte
                # (cheval devenu moins favori, "remonte").
                pct_chute = (cote_t0 - cote_live) / cote_t0 * 100
            ecarts[num] = (cote_t0, cote_live, pct_chute)

        # --- echantillonnage des series temporelles (journal separe) -------
        # Un point toutes les 15s, uniquement dans la fenetre [depart-5min,
        # depart+5min] (depart = heure PROGRAMMEE, pas reelle, cf. discussion
        # avec l'utilisateur : les courses partent souvent en retard, d'ou la
        # marge de 5min de chaque cote). TOUT le peloton suivi (pas seulement
        # les chevaux en chute) : ce journal sert aussi de groupe de controle
        # (fusionne avec l'ancien race_snapshot, a la demande de l'utilisateur).
        if self.depart_ts is not None:
            in_window = (self.depart_ts - ODDS_TIMESERIES_WINDOW_BEFORE_S) <= now <= (self.depart_ts + ODDS_TIMESERIES_WINDOW_AFTER_S)
            due = (now - self.last_timeseries_sample_ts) >= ODDS_TIMESERIES_SAMPLE_INTERVAL_S
            if in_window and due:
                self.last_timeseries_sample_ts = now
                for num, (ct0, clive, pc) in ecarts.items():
                    if ct0 is not None and clive is not None:
                        self.odds_timeseries_buffer[num].append({
                            "t": now,
                            "coteT0": ct0,
                            "coteLive": clive,
                            "pctChute": pc,
                        })

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
            cote_t0, cote_live, pct_chute = ecarts[num]
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

            # remonte = la cote du cheval "remonte" depuis T0 (% de chute
            # negatif, il devient moins favori) -> TOUJOURS masque, meme s'il
            # est par ailleurs marque bigmove/valuebet (priorite absolue)
            is_remonte = pct_chute is not None and pct_chute < 0

            # VALUEBET : marquage actif tant que la cote d'un cheval a chute
            # (% Chute, ecart T0 -> Live) de VALUEBET_CHUTE_THRESHOLD_PCT (10%)
            # ou plus. Des que pct_chute repasse sous ce seuil (meme encore
            # positif, ex. 12% -> 8%, ou negatif si la cote remonte au-dessus
            # de T0), le marquage valuebet est retire — le cheval perd son
            # statut valuebet. S'il rechute a nouveau a 10%+ par la suite, il
            # peut redevenir valuebet.
            if pct_chute is not None and pct_chute >= VALUEBET_CHUTE_THRESHOLD_PCT:
                prev_vb = self.valuebet_seen.get(num)
                if prev_vb is None:
                    self.valuebet_seen[num] = {"pctChute": pct_chute, "at": now}
            else:
                self.valuebet_seen.pop(num, None)
            is_valuebet = num in self.valuebet_seen

            # hors-plage = cote Gagnant en DIRECT en dehors de [COTE_RANGE_MIN,
            # COTE_RANGE_MAX] -> masque, meme si marque bigmove/valuebet.
            # Reevalue a chaque poll sur la cote LIVE (pas T0) : un cheval qui
            # entre dans la plage apparait immediatement, un cheval qui en
            # sort disparait immediatement.
            is_out_of_range = cote_live is None or cote_live < COTE_RANGE_MIN or cote_live > COTE_RANGE_MAX

            rows.append({
                "num": num,
                "nom": self.horse_names.get(num, f"#{num}"),
                "coteT0": cote_t0,
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
        set_state(
            rows=rows,
            snapLine=f"📡 Rapports probables mis a jour — {now_str}",
            toteLabel=self.build_tote_label(len(nums)),
            statusLine="",
            updatedAt=time.time(),
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
        except Exception as e:
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
        elif self.path == "/":
            self.path = "/pmu_bot.html"
            super().do_GET()
        else:
            super().do_GET()

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
