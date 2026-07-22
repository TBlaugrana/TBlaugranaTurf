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
import http.server
import socketserver
import http.client
import json
import math
import os
import threading
import time
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

# REF_LEAD_S : la "cote de reference" (colonne 2 du tableau) est figee des
# qu'on entre dans cette fenetre avant le depart (3 min). Si le suivi demarre
# alors qu'il reste deja moins de 3 min avant le depart, c'est la toute
# premiere cote observee pour chaque cheval qui sert de reference (le
# meilleur "3 min avant le depart" disponible dans ce cas).
REF_LEAD_S = 180

# Nombre de chevaux affiches dans le tableau : les TOP_N_CHUTE plus grosses
# chutes de cote (entre la reference a 3 min et la cote live), triees par
# ordre de chute decroissant.
TOP_N_CHUTE = 5

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
# Seuil de chute RELATIVE (%) entre la cote de reference (colonne 2, figee a
# ~3 min du depart) et la cote live (colonne 3) au-dela duquel le signal
# VALUEBET se declenche. Marquage DEFINITIF : une fois franchi, le cheval
# reste marque "valuebet" pour le reste de la course.
VALUEBET_THRESHOLD_PCT = 30.0

# on garde en memoire assez d'historique pour satisfaire la fenetre la plus longue
HISTORY_RETENTION_S = max(SPEED_WINDOW_S, DELTA_WINDOW_S)

# Si True, les reunions dont le pays n'est pas la France sont completement
# ignorees des le chargement du programme (elles ne sont jamais candidates
# a la selection automatique et n'apparaissent jamais dans l'etat renvoye
# au bot). Mettre a False pour revenir au comportement d'origine (toutes
# les reunions, France + etranger).
HIDE_FOREIGN_RACES = False

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
    "tableMsg": "",
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
        self.odds_history = {}    # num -> [(t, prob_lissee_ema), ...]
        self.ema_prob = {}        # num -> derniere probabilite implicite lissee (EMA)
        self.ema_last_t = {}      # num -> timestamp du dernier point utilise pour l'EMA
        self.cote_ref = {}        # num -> cote enregistree ~3 min avant le depart (reference pour le calcul de chute, colonne 2 du tableau)
        self.valuebet_seen = {}   # num -> {"ecart": ..., "at": ...} des que le seuil de chute VALUEBET_THRESHOLD_PCT est franchi (marquage definitif)
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
                "odds_history": self.odds_history,
                "ema_prob": self.ema_prob,
                "ema_last_t": self.ema_last_t,
                "cote_ref": self.cote_ref,
                "valuebet_seen": self.valuebet_seen,
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
        self.odds_history = snap.get("odds_history") or {}
        self.ema_prob = snap.get("ema_prob") or {}
        self.ema_last_t = snap.get("ema_last_t") or {}
        self.cote_ref = snap.get("cote_ref") or {}
        self.valuebet_seen = snap.get("valuebet_seen") or {}
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
            for co in (ru.get("courses") or []):
                if is_annulee(co):
                    continue
                courses.append({
                    "numReunion": num_reunion,
                    "hip": hip,
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

    def select_next_course(self):
        now_ms = time.time() * 1000
        upcoming = [c for c in self.all_courses if c["depart"] and c["depart"] >= now_ms]
        chosen = upcoming[0] if upcoming else (self.all_courses[-1] if self.all_courses else None)
        if not chosen:
            set_state(courseInfo="Aucune course disponible.", statusLine="Aucune course disponible pour le moment.")
            return
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
        self.odds_history = {}
        self.ema_prob = {}
        self.ema_last_t = {}
        self.cote_ref = {}
        self.valuebet_seen = {}
        set_state(rows=[], tableMsg="⏳ En attente des 3 dernières minutes avant le départ pour figer la cote de référence (suivi déjà actif en arrière-plan).", snapLine="📸 Rapports probables : en attente")

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
    def parse_citations(self, data):
        gagnant_map, place_map = {}, {}
        for g in (data.get("listeCitations") or []):
            type_pari = g.get("typePari")
            target = place_map if type_pari == "SIMPLE_PLACE" else gagnant_map if type_pari == "SIMPLE_GAGNANT" else None
            if target is None:
                continue
            for p in (g.get("participants") or []):
                if p.get("statut") != "PARTANT":
                    continue
                cits = p.get("citations") or []
                ratio = cits[0].get("ratio") if cits else None
                if ratio is None:
                    continue
                num = str(p.get("numPmu"))
                target[num] = {"nom": p.get("nom") or f"#{num}", "ratio": ratio, "favoris": bool(p.get("favoris"))}
        return gagnant_map, place_map

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
                    speed[num] = (math.exp(log_rate) - 1) * 100  # en %/min
        return speed

    def build_tote_label(self, nb_live=None):
        parts = []
        if self.selected_discipline:
            parts.append(self.selected_discipline)
        nb = nb_live if nb_live is not None else self.selected_nb_partants
        if nb is not None:
            parts.append(f"{nb} partant{'s' if nb > 1 else ''}")
        return " · ".join(parts)

    def handle_odds(self, data):
        gagnant_map, _place_map = self.parse_citations(data)  # place n'est plus utilise
        nums = set(gagnant_map.keys())
        if not nums:
            return

        for num in nums:
            self.horse_names[num] = gagnant_map[num]["nom"]
            if num not in self.favoris_order:
                self.favoris_order.append(num)

        now = time.time()
        self.update_speed_history(gagnant_map)  # alimente l'historique (EMA), conserve pour un usage futur eventuel

        # -- capture de la cote de reference (colonne "Cote -3min") ----------
        # Des qu'on entre dans la fenetre REF_LEAD_S (3 min) avant le depart,
        # on fige la cote actuelle de chaque cheval comme reference. Si le
        # suivi demarre alors qu'il reste deja moins de 3 min avant le
        # depart, la condition est vraie des la premiere cote recue : c'est
        # donc la toute premiere cote observee qui sert de reference (le
        # meilleur "3 min avant le depart" disponible dans ce cas).
        if self.depart_ts is not None and now >= (self.depart_ts - REF_LEAD_S):
            for num in nums:
                if num not in self.cote_ref:
                    self.cote_ref[num] = gagnant_map[num]["ratio"]

        # -- ecart (chute) entre la cote de reference et la cote live -------
        # ecart_pct positif = la cote a baisse depuis la reference (chute).
        candidates = []
        for num in self.favoris_order:
            ref = self.cote_ref.get(num)
            g = gagnant_map.get(num)
            live = g["ratio"] if g else None
            if ref is None or live is None or ref <= 0:
                continue  # pas encore de reference ou pas de cote live -> non classable
            ecart_pct = (ref - live) / ref * 100.0
            candidates.append((num, ref, live, ecart_pct))

        # VALUEBET : marquage DEFINITIF. Des qu'un cheval franchit le seuil de
        # chute relative VALUEBET_THRESHOLD_PCT, il reste marque pour le reste
        # de la course, meme si sa cote remonte ensuite (meme principe que
        # l'ancien "bigmove_seen").
        for num, ref, live, ecart_pct in candidates:
            if ecart_pct >= VALUEBET_THRESHOLD_PCT and num not in self.valuebet_seen:
                self.valuebet_seen[num] = {"ecart": ecart_pct, "at": now}

        # Tri par plus grosse chute decroissante ; seuls les TOP_N_CHUTE
        # premiers restent visibles. Un cheval marque VALUEBET reste affiche
        # meme s'il sort du top (masque, pas supprime, donc il reapparait
        # instantanement s'il remonte dans le classement). Un cheval dont la
        # cote n'a pas (encore) baisse (ecart <= 0) reste masque, sauf s'il
        # est deja marque VALUEBET.
        candidates.sort(key=lambda c: -c[3])

        rows = []
        for idx, (num, ref, live, ecart_pct) in enumerate(candidates):
            is_valuebet = num in self.valuebet_seen
            hidden = (idx >= TOP_N_CHUTE and not is_valuebet) or (ecart_pct <= 0 and not is_valuebet)
            rows.append({
                "num": num,
                "nom": self.horse_names.get(num, f"#{num}"),
                "coteRef": ref,
                "coteLive": live,
                "ecart": ecart_pct,
                "isValuebet": is_valuebet,
                "hidden": hidden,
            })

        # Message explicatif quand le tableau est vide, pour eviter de laisser
        # croire a un bug : soit on n'est pas encore dans la fenetre des 3
        # dernieres minutes avant le depart (cote de reference pas encore
        # figee), soit on y est mais aucun cheval n'a encore vu sa cote chuter.
        if rows:
            table_msg = ""
        elif self.depart_ts is not None and now < (self.depart_ts - REF_LEAD_S):
            table_msg = "⏳ En attente des 3 dernières minutes avant le départ pour figer la cote de référence (suivi déjà actif en arrière-plan)."
        else:
            table_msg = "Aucun cheval n'a vu sa cote chuter pour l'instant."

        now_str = datetime.now(PARIS_TZ).strftime("%H:%M:%S")
        set_state(
            rows=rows,
            tableMsg=table_msg,
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
                f"/citations?paris=SIMPLE_GAGNANT&specialisation=OFFLINE&groupe=true")
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
        elif self.path == "/":
            self.path = "/pmu_bot.html"
            super().do_GET()
        else:
            super().do_GET()

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

    with ThreadingHTTPServer(("0.0.0.0", PORT), Handler) as httpd:
        print(f"Serveur lance sur le port {PORT} (suivi PMU actif en tache de fond)")
        httpd.serve_forever()

