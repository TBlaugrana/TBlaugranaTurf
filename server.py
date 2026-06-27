#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════╗
║         TBLAUGRANA BOT  —  server.py  v12            ║
║  Source : online.turfinfo.api.pmu.fr  (client/7)    ║
╚══════════════════════════════════════════════════════╝
"""

import asyncio
import json
import os
import sys
import time
import threading
import queue as queue_mod
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from datetime import datetime
import ssl

# Render (et la plupart des hébergeurs) capturent stdout via un pipe, pas un
# terminal : Python bascule alors en buffering "par bloc" au lieu de "par
# ligne", et les print() peuvent rester coincés en mémoire sans jamais
# apparaître dans les logs avant que le buffer ne se remplisse (ce qui peut
# ne jamais arriver pour un script qui imprime peu). On force explicitement
# le line-buffering pour que CHAQUE print() apparaisse immédiatement dans
# les logs Render — indispensable pour diagnostiquer quoi que ce soit.
try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass

try:
    import aiohttp
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "aiohttp",
                           "--break-system-packages", "-q"])
    import aiohttp

try:
    import ujson as _json_lib
except ImportError:
    try:
        import subprocess, sys
        subprocess.check_call([sys.executable, "-m", "pip", "install", "ujson",
                               "--break-system-packages", "-q"])
        import ujson as _json_lib
    except Exception:
        import json as _json_lib

# ── Fuseau horaire (Europe/Paris, gère automatiquement CET/CEST) ──────────────
# Render fait tourner le conteneur en UTC par défaut. Sans préciser ce fuseau,
# tous les `datetime.now()` / `datetime.fromtimestamp()` du script (heure de
# départ affichée, "LIVE · HH:MM:SS"...) étaient calculés en UTC — soit 1h
# (heure d'hiver) ou 2h (heure d'été, comme actuellement) de moins que l'heure
# réelle en France. Les timestamps epoch bruts (depart_ts, countdown, etc.)
# n'étaient eux PAS affectés — seul l'AFFICHAGE était décalé.
from zoneinfo import ZoneInfo
try:
    PARIS_TZ = ZoneInfo("Europe/Paris")
except Exception:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "tzdata",
                           "--break-system-packages", "-q"])
    PARIS_TZ = ZoneInfo("Europe/Paris")


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                        ⚙️  PARAMETRES UTILISATEUR                           ║
# ║            Modifiez uniquement cette section selon vos besoins              ║
# ╠══════════════════════════════════════════════════════════════════════════════╣
# ║                                                                              ║
# ║  REFRESH_SEC  : intervalle (en secondes) entre deux appels à l'API PMU      ║
# ║                 Minimum recommandé : 3s  |  Défaut : 5s                     ║
REFRESH_SEC  = 1
# ║                                                                              ║
# ║  DEPART_REFRESH_SEC : intervalle (en secondes) entre deux rechargements     ║
# ║                       de l'heure de départ depuis l'API PMU                 ║
# ║                       Permet de rattraper les retards de départ             ║
# ║                       Défaut : 300s (5 minutes)                             ║
DEPART_REFRESH_SEC = 300
# ║                                                                              ║
# ║  COTE_MIN     : cote minimale surveillée (incluse)                          ║
# ║                 Ex : 1.0 pour surveiller à partir du grand favori           ║
COTE_MIN     = 1.0
# ║                                                                              ║
# ║  COTE_MAX     : cote maximale surveillée (incluse)                          ║
# ║                 Ex : 10.0 pour ignorer les outsiders au-delà de 10          ║
COTE_MAX     = 10.0
# ║                                                                              ║
# ║  DROP_ALERT   : seuil de chute de cote (%) pour déclencher une alerte      ║
# ║                 Ex : 10.0 = alerte si la cote baisse de 10% ou plus         ║
DROP_ALERT   = 30.0
# ║                                                                              ║
# ║  H3_SNAPSHOT_MIN : délai (en minutes) AVANT le départ auquel le bot prend  ║
# ║                    automatiquement un "screen" des cotes de référence.     ║
# ║                    Les chutes affichées dans le classement sont ensuite    ║
# ║                    calculées entre ce screen et la cote actuelle.          ║
# ║                    Défaut : 3 (screen pris à H-3min)                       ║
H3_SNAPSHOT_MIN = 3
# ║                                                                              ║
# ║  AUTO_ADVANCE_MIN_SERVER : délai (en minutes) après le départ d'une course  ║
# ║                    au-delà duquel le SERVEUR bascule lui-même vers la      ║
# ║                    course suivante du programme — sans attendre qu'un      ║
# ║                    navigateur soit ouvert pour le déclencher. C'est ce     ║
# ║                    qui garantit que le screen H-3min de la course suivante ║
# ║                    soit pris à l'heure même si personne ne consulte la     ║
# ║                    page entre deux courses. Garder la même valeur que      ║
# ║                    AUTO_ADVANCE_MIN côté index.html.  Défaut : 5           ║
AUTO_ADVANCE_MIN_SERVER = 5
# ║                                                                              ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
#
# NOTE — Déploiement Render :
#   - Le serveur tourne en continu côté serveur (pas de notion de "fenêtre
#     fermée" : fermer l'onglet ne stoppe rien, le scraping continue tant que
#     le service Render est éveillé).
#   - Render free tier met le service en veille après ~15 min sans requête
#     HTTP entrante. Le scraping s'arrête alors (process éteint), et reprend
#     automatiquement (avec un cold start ~30-50s) à la prochaine visite.
#   - Le PORT est fourni par Render via la variable d'environnement PORT.


# ── Configuration interne (ne pas modifier) ────────────────────────────────────

PORT         = int(os.environ.get("PORT", 8765))
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=4.0, connect=2.0, sock_read=3.5)


BASE_URL        = "https://online.turfinfo.api.pmu.fr/rest/client/7/programme"
BASE_URL_PROG   = "https://online.turfinfo.api.pmu.fr/rest/client/62/programme"
BASE_URL_PARAMS = "?meteo=true&specialisation=OFFLINE"

HTTP_HEADERS = {
    "User-Agent"     : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept"         : "application/json, */*",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "fr-FR,fr;q=0.9",
    "Referer"        : "https://www.pmu.fr/",
    "Cache-Control"  : "no-cache",
}

# ── State partagé ─────────────────────────────────────────────────────────────

STATE = {
    "reunions"         : [],
    "courses"          : [],
    "odds"             : {},        # cotes actuelles  {num: {nom, cote}}
    "odds_prev"        : {},        # cotes du fetch précédent
    "alerts"           : [],        # chevaux en alerte chute (post-départ)
    "dropped_horses"   : [],        # chevaux ayant eu chute ≥10% (mémorisé, list pour JSON)
    "selected_reunion" : None,
    "selected_course"  : None,
    "last_update"      : None,
    "status"           : "idle",
    "error"            : None,
    "refresh_count"        : 0,
    "depart_ts"            : None,
    "depart_str"           : None,
    "countdown_sec"        : None,
    "seq"                  : 0,
    "last_odds_change_ts"  : None,   # epoch ms — dernière fois que les cotes ont bougé
    "h3_snapshot"          : {},      # cotes figées au moment H-3min  {num: {nom, cote}}
    "h3_snapshot_taken"    : False,   # True dès que le screen H-3min a été pris
    "h3_snapshot_ts"       : None,    # epoch ms — instant exact où le screen a été pris
    "h1_snapshot"          : {},      # cotes figées au moment H-1min
    "h1_snapshot_taken"    : False,
    "h1_snapshot_ts"       : None,
    "hd_snapshot"          : {},      # cotes figées au moment du départ (countdown <= 0)
    "hd_snapshot_taken"    : False,
    "hd_snapshot_ts"       : None,
    "history"              : [],      # courses du jour déjà terminées (résumé figé, voir _archive_current_course)
    "history_date"         : None,    # date (ddmmyyyy) à laquelle "history" correspond — sert à purger au changement de jour
}
_state_lock = threading.Lock()

# ── Session aiohttp ────────────────────────────────────────────────────────────

_session: aiohttp.ClientSession | None = None
_session_lock: asyncio.Lock | None = None

async def _get_session() -> aiohttp.ClientSession:
    global _session, _session_lock
    if _session_lock is None:
        _session_lock = asyncio.Lock()
    async with _session_lock:
        if _session is None or _session.closed:
            ssl_ctx = ssl.create_default_context()
            ssl_ctx.check_hostname = True
            ssl_ctx.verify_mode    = ssl.CERT_REQUIRED
            ssl_ctx.options       |= ssl.OP_NO_SSLv2 | ssl.OP_NO_SSLv3
            ssl_ctx.set_ciphers("HIGH:!aNULL:!MD5")

            connector = aiohttp.TCPConnector(
                limit_per_host        = 4,
                ssl                   = ssl_ctx,
                force_close           = False,
                keepalive_timeout     = 60,
                enable_cleanup_closed = True,
                ttl_dns_cache         = 600,
            )
            _session = aiohttp.ClientSession(
                connector      = connector,
                headers        = HTTP_HEADERS,
                timeout        = HTTP_TIMEOUT,
                json_serialize = _json_lib.dumps,
            )
    return _session

# ── Cache de date ─────────────────────────────────────────────────────────────

_today_cache: str = ""
_today_date:  int = 0

def today() -> str:
    global _today_cache, _today_date
    d = datetime.now(PARIS_TZ)
    jd = d.toordinal()
    if jd != _today_date:
        _today_cache = d.strftime("%d%m%Y")
        _today_date  = jd
    return _today_cache

# ── Fetch JSON async ──────────────────────────────────────────────────────────

async def fetch_json_async(url: str) -> dict:
    session = await _get_session()
    async with session.get(url) as resp:
        if resp.status != 200:
            raise Exception(f"HTTP {resp.status}")
        raw = await resp.read()
        return _json_lib.loads(raw)

# Réunions à exclure de l'affichage : Hong Kong (2 hippodromes : Happy Valley,
# Sha Tin) demandé en masquage. Filtrage textuel sur le libellé hippodrome et,
# si présent, sur le code pays — résilient même si l'un des deux champs manque
# ou change de format côté PMU.
HIPPODROMES_MASQUES = ("HONG KONG", "HONG-KONG", "HAPPY VALLEY", "SHA TIN")
PAYS_MASQUES        = ("HK", "HKG")

def _reunion_est_masquee(r: dict) -> bool:
    hippo = r.get("hippodrome") or {}
    lieu  = (hippo.get("libelleLong") or hippo.get("libelleCourt") or "").upper()
    if any(nom in lieu for nom in HIPPODROMES_MASQUES):
        return True
    pays = hippo.get("pays") or {}
    code_pays = (pays.get("code") if isinstance(pays, dict) else str(pays)) or ""
    if code_pays.upper() in PAYS_MASQUES:
        return True
    return False

async def fetch_reunions_async() -> list:
    url       = f"{BASE_URL_PROG}/{today()}{BASE_URL_PARAMS}"
    data      = await fetch_json_async(url)
    out       = []
    programme = data.get("programme") or {}
    for r in programme.get("reunions", []):
        if _reunion_est_masquee(r):
            continue
        # numOfficiel/numExterne ou hippodrome peuvent être présents mais valoir `null`
        # → utiliser "or" plutôt que get(default) pour éviter un crash sur NoneType
        num   = r.get("numOfficiel") or r.get("numExterne") or 0
        hippo = r.get("hippodrome") or {}
        lieu  = hippo.get("libelleLong") or hippo.get("libelleCourt") or "?"
        out.append({"id": str(num), "num": num, "label": f"R{num} — {lieu}"})
    return out

async def fetch_courses_async(r_num: str) -> list:
    url     = f"{BASE_URL_PROG}/{today()}/R{r_num}{BASE_URL_PARAMS}"
    data    = await fetch_json_async(url)
    out     = []
    # "reunion" peut être présent avec une valeur `null` → "or {}" évite le crash
    reunion     = data.get("reunion") or {}
    raw_courses = data.get("courses") or reunion.get("courses") or []
    for c in raw_courses:
        num   = c.get("numOrdre", 0)
        label = c.get("libelle") or f"Course {num}"
        heure = ""
        ts    = c.get("heureDepart")
        if ts:
            try:
                heure = datetime.fromtimestamp(int(ts) / 1000, PARIS_TZ).strftime("%H:%M")
            except Exception:
                pass
        out.append({
            "id": str(num), "num": num,
            "label": f"C{num} — {label}",
            "heure": heure, "heureDepart": ts,
            "partants": c.get("nombreDeclaresPartants"),
        })
    return out

async def fetch_odds_async(r_num: str, c_num: str) -> dict:
    try:
        url  = f"{BASE_URL}/{today()}/R{r_num}/C{c_num}/participants"
        data = await fetch_json_async(url)
        odds = {}
        for p in data.get("participants", []):
            num = str(p.get("numPmu", p.get("numero", "")))
            nom = p.get("nom", f"#{num}")
            crd = (p.get("dernierRapportDirectMini")
                   or p.get("dernierRapportDirect")
                   or {})
            val = crd.get("rapport") if isinstance(crd, dict) else None
            if val is None:
                val = (p.get("coteDirect")
                       or p.get("cote")
                       or p.get("rapportDirect"))
            if num and val:
                try:
                    odds[num] = {"nom": nom.strip(), "cote": float(val)}
                except (ValueError, TypeError):
                    pass
        return odds
    except Exception:
        return {}

# ── SSE clients ───────────────────────────────────────────────────────────────

_sse_clients      = []
_sse_clients_lock = threading.Lock()

def _sse_broadcast(data: str) -> None:
    msg  = f"data: {data}\n\n".encode()
    dead = []
    with _sse_clients_lock:
        for q in _sse_clients:
            try:
                q.put_nowait(msg)
            except Exception:
                dead.append(q)
        for q in dead:
            try:
                _sse_clients.remove(q)
            except ValueError:
                pass

# ── Payload state ─────────────────────────────────────────────────────────────

def _build_state_payload() -> dict:
    odds      = STATE.get("odds", {})
    odds_prev = STATE.get("odds_prev", {})
    dep_ts    = STATE.get("depart_ts")
    now_ms    = int(time.time() * 1000)
    race_started = dep_ts and now_ms >= dep_ts

    rows = []
    for num, info in odds.items():
        cote_now  = info["cote"]
        prev_info = odds_prev.get(num)
        cote_prev = prev_info["cote"] if prev_info else None

        drop_pct = 0.0
        alert    = False
        if cote_prev and cote_prev > 0:
            drop_pct = round((cote_prev - cote_now) / cote_prev * 100, 1)
            if race_started and drop_pct >= DROP_ALERT and COTE_MIN <= cote_now <= COTE_MAX:
                alert = True
            # Mémoriser toute chute ≥10% dans la plage de cotes
            if drop_pct >= DROP_ALERT and COTE_MIN <= cote_now <= COTE_MAX:
                if num not in STATE["dropped_horses"]:
                    STATE["dropped_horses"].append(num)

        # Cotes des snapshots
        h3_snap = STATE.get("h3_snapshot", {}).get(num)
        h1_snap = STATE.get("h1_snapshot", {}).get(num)
        hd_snap = STATE.get("hd_snapshot", {}).get(num)

        # ── Score basé sur les snapshots ──────────────────────────────────────
        # Chaque étape de baisse rapporte des points, pondérés par leur
        # proximité du départ (la dernière minute = signal le plus fort).
        # Score = Δ(H3→H1)×1 + Δ(H1→HD)×2 + Δ(HD→Live)×1.5
        # Une étape manquante (snapshot pas encore pris) est ignorée.
        score = 0.0
        c_h3  = h3_snap["cote"] if h3_snap else None
        c_h1  = h1_snap["cote"] if h1_snap else None
        c_hd  = hd_snap["cote"] if hd_snap else None
        if c_h3 and c_h1 and c_h3 > 0:
            delta = (c_h3 - c_h1) / c_h3 * 100
            score += delta * 1.0
        if c_h1 and c_hd and c_h1 > 0:
            delta = (c_h1 - c_hd) / c_h1 * 100
            score += delta * 2.0
        if c_hd and c_hd > 0:
            delta = (c_hd - cote_now) / c_hd * 100
            score += delta * 1.5
        elif c_h3 and c_h3 > 0 and not c_h1 and not c_hd:
            # Avant H-3min : score partiel H3→Live uniquement (poids réduit)
            delta = (c_h3 - cote_now) / c_h3 * 100
            score += delta * 0.8
        score = round(score, 1)

        rows.append({
            "num"         : num,
            "nom"         : info["nom"],
            "cote_now"    : cote_now,
            "cote_prev"   : cote_prev,
            "drop_pct"    : drop_pct,
            "alert"       : alert,
            "ever_dropped": num in STATE["dropped_horses"],
            "cote_h3"     : c_h3,
            "cote_h1"     : c_h1,
            "cote_hd"     : c_hd,
            "score"       : score,
        })

    # Ne garder dans l'affichage que les chevaux dont la cote ACTUELLE
    # est dans la plage [COTE_MIN, COTE_MAX]. Comme ce filtre se base sur
    # cote_now à chaque rafraîchissement, un cheval qui dépasse COTE_MAX
    # disparaît de la liste, puis y revient automatiquement si sa cote
    # repasse à nouveau dans la plage.
    rows = [r for r in rows if COTE_MIN <= r["cote_now"] <= COTE_MAX]

    # Dès que le snapshot H-3min est pris : tri et masquage basés sur la chute
    # DEPUIS CE SNAPSHOT (cote_h3 → cote_now) — la même référence que celle
    # affichée dans la colonne "VAR." côté client. Important : on n'utilise
    # PAS le "drop_pct" tick-à-tick (qui compare au fetch précédent, donc
    # quasi toujours ≈0 et ne reflète pas la chute réelle depuis le screen).
    h3_taken = STATE.get("h3_snapshot_taken", False)
    if h3_taken:
        for r in rows:
            c_h3 = r.get("cote_h3")
            r["drop_h3_pct"] = (
                round((c_h3 - r["cote_now"]) / c_h3 * 100, 1) if c_h3 else 0.0
            )
        # Masquer les chevaux dont la cote est remontée depuis le snapshot
        # (drop_h3_pct négatif = cote qui monte = cheval moins joué).
        rows = [r for r in rows if r["drop_h3_pct"] >= 0]
        # Trier de la plus grosse chute à la plus petite
        rows.sort(key=lambda x: x["drop_h3_pct"], reverse=True)
    else:
        rows.sort(key=lambda x: x["cote_now"])

    # ── Classement des chutes depuis le screen H-3min ──────────────────────
    # Compare la cote figée au moment du screen (h3_snapshot) à la cote
    # actuelle pour chaque cheval, et trie de la plus grosse chute (% le
    # plus élevé) à la plus petite. Une chute positive = la cote a baissé
    # (cheval davantage joué) ; une valeur négative = la cote est remontée.
    h3_snapshot = STATE.get("h3_snapshot", {})
    h3_rows = []
    if h3_snapshot:
        for num, info in odds.items():
            snap = h3_snapshot.get(num)
            if not snap:
                continue
            cote_h3  = snap["cote"]
            cote_now = info["cote"]
            drop_pct = round((cote_h3 - cote_now) / cote_h3 * 100, 1) if cote_h3 > 0 else 0.0
            h3_rows.append({
                "num"     : num,
                "nom"     : info["nom"],
                "cote_h3" : cote_h3,
                "cote_now": cote_now,
                "drop_pct": drop_pct,
            })
        h3_rows.sort(key=lambda x: x["drop_pct"], reverse=True)

    return {
        "status"              : STATE["status"],
        "last_update"         : STATE["last_update"],
        "refresh_count"       : STATE["refresh_count"],
        "error"               : STATE["error"],
        "rows"                : rows,
        "selected_reunion"    : STATE.get("selected_reunion"),
        "selected_course"     : STATE.get("selected_course"),
        "depart_str"          : STATE.get("depart_str"),
        "depart_ts"           : STATE.get("depart_ts"),
        "countdown_sec"       : STATE.get("countdown_sec"),
        "seq"                 : STATE.get("seq", 0),
        "race_started"        : bool(race_started),
        "last_odds_change_ts" : STATE.get("last_odds_change_ts"),
        "h3_snapshot_taken"   : STATE.get("h3_snapshot_taken", False),
        "h3_snapshot_ts"      : STATE.get("h3_snapshot_ts"),
        "h3_rows"             : h3_rows,
        "h1_snapshot_taken"   : STATE.get("h1_snapshot_taken", False),
        "h1_snapshot_ts"      : STATE.get("h1_snapshot_ts"),
        "hd_snapshot_taken"   : STATE.get("hd_snapshot_taken", False),
        "hd_snapshot_ts"      : STATE.get("hd_snapshot_ts"),
    }

# ── Historique des courses du jour ────────────────────────────────────────────

def _archive_current_course() -> None:
    """Sauvegarde un résumé figé (snapshots H-3/H-1/Départ, cotes finales,
    classement des chutes) de la course actuellement suivie dans
    STATE["history"], AVANT qu'elle soit remplacée par la suivante.

    Appelée à deux endroits : juste avant _apply_selection() (bascule
    automatique serveur) et juste avant la remise à zéro dans le handler
    POST /api/select (changement manuel de course depuis le navigateur).
    Sans cet appel, les snapshots/cotes de la course qui se termine sont
    perdus dès qu'on change de course — c'est ce que cette fonction corrige.

    Ne fait rien si aucune cote n'a jamais été récupérée pour la course en
    cours (rien d'utile à conserver — ex: course jamais réellement suivie).
    """
    r = STATE.get("selected_reunion")
    c = STATE.get("selected_course")
    if not (r and c) or not STATE.get("odds"):
        return

    # Purge automatique au changement de jour calendaire : on ne garde que
    # les courses du jour en cours (le programme PMU change de toute façon
    # chaque jour, ça n'aurait pas de sens de mélanger avec la veille).
    cur_day = today()
    if STATE.get("history_date") != cur_day:
        STATE["history"]      = []
        STATE["history_date"] = cur_day

    try:
        snapshot = _build_state_payload()
        snapshot["archived_at"] = int(time.time() * 1000)
    except Exception as exc:
        print(f"  [HISTORIQUE] Erreur construction snapshot R{r}C{c} : {exc}")
        return

    hist = STATE["history"]
    # Évite un doublon si la fonction est appelée deux fois pour la même
    # course (ex: bascule serveur ET sélection manuelle quasi simultanées) —
    # on remplace alors l'entrée précédente plutôt que d'empiler un doublon.
    if hist and str(hist[-1].get("selected_reunion")) == str(r) and str(hist[-1].get("selected_course")) == str(c):
        hist[-1] = snapshot
    else:
        hist.append(snapshot)

    # Garde-fou mémoire (très large pour une seule journée de courses PMU)
    if len(hist) > 200:
        STATE["history"] = hist[-200:]

    print(f"  [HISTORIQUE] Course R{r}C{c} archivée "
          f"({len(snapshot.get('rows', []))} chevaux) — {len(STATE['history'])} courses en mémoire aujourd'hui")

# ── Boucle scraping async (toutes les 5 secondes) ─────────────────────────────

async def scrape_loop_async() -> None:
    consecutive_errors = 0
    last_r = last_c = None

    while True:
        r = STATE.get("selected_reunion")
        c = STATE.get("selected_course")

        if r and c:
            STATE["status"] = "scraping"
            if r != last_r or c != last_c:
                last_r, last_c = r, c

            try:
                t0     = time.monotonic()
                odds   = await fetch_odds_async(r, c)
                now_ms = int(time.time() * 1000)
                consecutive_errors = 0

                if odds:
                    # Détecter si les cotes ont réellement changé
                    prev_odds = STATE.get("odds", {})
                    odds_changed = (
                        not prev_odds or
                        any(
                            odds.get(k, {}).get("cote") != prev_odds.get(k, {}).get("cote")
                            for k in set(odds) | set(prev_odds)
                        )
                    )

                    # Sauvegarder les cotes précédentes AVANT mise à jour dans STATE
                    prev_snapshot          = STATE.get("odds", {}).copy()
                    STATE["odds_prev"]     = prev_snapshot
                    STATE["odds"]          = odds
                    STATE["last_update"]   = datetime.now(PARIS_TZ).strftime("%H:%M:%S")
                    STATE["refresh_count"] += 1
                    STATE["error"]         = None

                    if odds_changed:
                        STATE["last_odds_change_ts"] = int(time.time() * 1000)

                    dep = STATE.get("depart_ts")
                    STATE["countdown_sec"] = int((dep - now_ms) / 1000) if dep else None

                    # ── Screen automatique des cotes à H-(H3_SNAPSHOT_MIN)min ──
                    if (not STATE.get("h3_snapshot_taken")
                            and STATE["countdown_sec"] is not None
                            and STATE["countdown_sec"] <= H3_SNAPSHOT_MIN * 60):
                        STATE["h3_snapshot"] = {
                            num: {"nom": info["nom"], "cote": info["cote"]}
                            for num, info in odds.items()
                        }
                        STATE["h3_snapshot_taken"] = True
                        STATE["h3_snapshot_ts"]    = now_ms
                        print(f"  [SNAPSHOT] H-3min pris pour R{r}C{c} à "
                              f"{datetime.fromtimestamp(now_ms/1000, PARIS_TZ).strftime('%H:%M:%S')} "
                              f"(countdown={STATE['countdown_sec']}s, {len(odds)} chevaux)")

                    # ── Screen automatique H-1min ──
                    if (not STATE.get("h1_snapshot_taken")
                            and STATE["countdown_sec"] is not None
                            and STATE["countdown_sec"] <= 60):
                        STATE["h1_snapshot"] = {
                            num: {"nom": info["nom"], "cote": info["cote"]}
                            for num, info in odds.items()
                        }
                        STATE["h1_snapshot_taken"] = True
                        STATE["h1_snapshot_ts"]    = now_ms
                        print(f"  [SNAPSHOT] H-1min pris pour R{r}C{c} à "
                              f"{datetime.fromtimestamp(now_ms/1000, PARIS_TZ).strftime('%H:%M:%S')} "
                              f"(countdown={STATE['countdown_sec']}s, {len(odds)} chevaux)")

                    # ── Screen automatique H-départ (1er fetch après countdown <= 0) ──
                    if (not STATE.get("hd_snapshot_taken")
                            and STATE["countdown_sec"] is not None
                            and STATE["countdown_sec"] <= 0):
                        STATE["hd_snapshot"] = {
                            num: {"nom": info["nom"], "cote": info["cote"]}
                            for num, info in odds.items()
                        }
                        STATE["hd_snapshot_taken"] = True
                        STATE["hd_snapshot_ts"]    = now_ms
                        print(f"  [SNAPSHOT] Départ pris pour R{r}C{c} à "
                              f"{datetime.fromtimestamp(now_ms/1000, PARIS_TZ).strftime('%H:%M:%S')} "
                              f"(countdown={STATE['countdown_sec']}s, {len(odds)} chevaux)")

                    STATE["status"] = "live"
                    STATE["seq"]    = STATE.get("seq", 0) + 1
                    # Broadcast à chaque fetch (pas seulement si les cotes ont changé)
                    # → l'interface se met à jour au rythme réel de REFRESH_SEC
                    _sse_broadcast(json.dumps(_build_state_payload(), ensure_ascii=False))
                else:
                    STATE["status"] = "no_data"
                    STATE["error"]  = "Aucune cote (paris pas encore ouverts ?)"
                    _sse_broadcast(json.dumps(_build_state_payload(), ensure_ascii=False))

                # Attendre REFRESH_SEC secondes entre chaque fetch
                elapsed = time.monotonic() - t0
                wait    = max(0.0, REFRESH_SEC - elapsed)
                await asyncio.sleep(wait)

            except Exception as exc:
                consecutive_errors += 1
                STATE["error"]  = str(exc)[:120]
                STATE["status"] = "error"
                await asyncio.sleep(min(2.0 * consecutive_errors, 10.0))
        else:
            last_r = last_c = None
            STATE["status"] = "waiting"
            await asyncio.sleep(0.5)

# ── Rechargement périodique de l'heure de départ (toutes les 5 min) ──────────

async def _depart_refresh_loop() -> None:
    """Recharge l'heure de départ depuis l'API PMU toutes les DEPART_REFRESH_SEC
    secondes. Cela permet de rattraper les retards de départ et de corriger
    l'heure des courses suivantes qui seraient décalées."""
    while True:
        await asyncio.sleep(DEPART_REFRESH_SEC)
        r = STATE.get("selected_reunion")
        c = STATE.get("selected_course")
        if not r or not c:
            continue
        try:
            courses = await fetch_courses_async(r)
            for co in courses:
                if str(co["id"]) == str(c) and co.get("heureDepart"):
                    ts_new = int(co["heureDepart"])
                    ts_old = STATE.get("depart_ts")
                    if ts_new != ts_old:
                        STATE["depart_ts"]  = ts_new
                        STATE["depart_str"] = datetime.fromtimestamp(ts_new / 1000, PARIS_TZ).strftime("%H:%M")
                        print(f"  [DEPART] Heure mise à jour R{r}C{c} : "
                              f"{datetime.fromtimestamp((ts_old or 0)/1000, PARIS_TZ).strftime('%H:%M') if ts_old else '?'} "
                              f"→ {STATE['depart_str']}")
                    else:
                        print(f"  [DEPART] Heure confirmée R{r}C{c} : {STATE['depart_str']} (inchangée)")
                    break
        except Exception as exc:
            print(f"  [DEPART] Erreur rechargement heure de départ : {exc}")


# ── Auto-avance serveur vers la course suivante ──────────────────────────────

def _apply_selection(new_r: str, new_c: str, new_ts: int) -> None:
    """Bascule STATE vers une nouvelle course et réinitialise tous les
    snapshots / compteurs liés à la course précédente."""
    _archive_current_course()
    with _state_lock:
        STATE["selected_reunion"]    = new_r
        STATE["selected_course"]     = new_c
        STATE["odds"]                = {}
        STATE["odds_prev"]           = {}
        STATE["error"]               = None
        STATE["depart_ts"]           = new_ts
        STATE["depart_str"]          = datetime.fromtimestamp(new_ts / 1000, PARIS_TZ).strftime("%H:%M")
        STATE["countdown_sec"]       = None
        STATE["refresh_count"]       = 0
        STATE["dropped_horses"]      = []
        STATE["last_odds_change_ts"] = None
        STATE["h3_snapshot"]         = {}
        STATE["h3_snapshot_taken"]   = False
        STATE["h3_snapshot_ts"]      = None
        STATE["h1_snapshot"]         = {}
        STATE["h1_snapshot_taken"]   = False
        STATE["h1_snapshot_ts"]      = None
        STATE["hd_snapshot"]         = {}
        STATE["hd_snapshot_taken"]   = False
        STATE["hd_snapshot_ts"]      = None


async def _fetch_all_courses_today() -> list:
    """Liste (reunion, course, heureDepart) de TOUTES les courses du jour,
    toutes réunions confondues, triée par heure de départ croissante."""
    reunions    = await fetch_reunions_async()
    all_courses = []
    for rn in reunions:
        cs = await fetch_courses_async(rn["id"])
        for co in cs:
            if co.get("heureDepart"):
                all_courses.append((str(rn["id"]), str(co["id"]), int(co["heureDepart"])))
    all_courses.sort(key=lambda x: x[2])
    return all_courses


async def _auto_advance_loop() -> None:
    """Équivalent côté serveur de l'auto-avance JS (index.html).

    Le JS ne bascule vers la course suivante que si un onglet est ouvert et
    que son setInterval tourne. Si personne ne consulte la page pendant la
    transition entre deux courses, le serveur restait bloqué sur l'ancienne
    course jusqu'à ce qu'un client revienne et appelle /api/select — ce qui
    retardait d'autant le screen H-3min de la course suivante (pris à la
    reconnexion plutôt qu'à l'heure réelle). Cette boucle élimine cette
    dépendance et couvre deux cas :
      1. Démarrage à froid : aucune course sélectionnée (premier lancement
         du process, ou STATE reparti à zéro après un redeploy/crash) → on
         choisit nous-mêmes la course la plus pertinente, sans attendre
         qu'un client se connecte pour le faire.
      2. Transition normale : la course en cours est terminée → bascule vers
         la suivante, soit après la "grâce" habituelle, soit immédiatement
         si attendre ferait manquer le screen H-3 de la suivante (réunions
         aux départs rapprochés).
    """
    CHECK_EVERY_SEC = 15
    while True:
        await asyncio.sleep(CHECK_EVERY_SEC)
        r   = STATE.get("selected_reunion")
        c   = STATE.get("selected_course")
        dep = STATE.get("depart_ts")

        try:
            # ── Cas 1 : rien de sélectionné → choisir directement ──────────
            if not (r and c and dep):
                all_courses = await _fetch_all_courses_today()
                if not all_courses:
                    continue
                now_ms = int(time.time() * 1000)
                cutoff = now_ms - AUTO_ADVANCE_MIN_SERVER * 60 * 1000
                target = next((x for x in all_courses if x[2] > cutoff), all_courses[-1])
                new_r, new_c, new_ts = target
                print(f"  [AUTO-AVANCE] Démarrage à froid (aucune course suivie) "
                      f"→ sélection R{new_r}C{new_c} (départ {datetime.fromtimestamp(new_ts/1000, PARIS_TZ).strftime('%H:%M')})")
                _apply_selection(new_r, new_c, new_ts)
                continue

            # ── Cas 2 : transition normale d'une course à la suivante ──────
            now_ms = int(time.time() * 1000)
            if now_ms < dep:
                continue  # course en cours pas encore partie, rien à faire

            all_courses = await _fetch_all_courses_today()
            nxt = next((x for x in all_courses if x[2] > dep), None)
            if not nxt or (nxt[0], nxt[1]) == (str(r), str(c)):
                continue

            new_r, new_c, new_ts = nxt

            # Deux conditions déclenchent la bascule :
            #  - la course actuelle est terminée depuis AUTO_ADVANCE_MIN_SERVER
            #    min (comportement "normal", identique au JS) ; OU
            #  - la course suivante est tellement proche qu'attendre la grâce
            #    de AUTO_ADVANCE_MIN_SERVER min ferait manquer son screen H-3
            #    (cas de deux réunions dont les départs sont rapprochés).
            grace_elapsed = now_ms >= dep + AUTO_ADVANCE_MIN_SERVER * 60 * 1000
            would_miss_h3 = now_ms >= new_ts - (H3_SNAPSHOT_MIN + 1) * 60 * 1000

            if not (grace_elapsed or would_miss_h3):
                continue

            print(f"  [AUTO-AVANCE] R{r}C{c} terminée → bascule serveur vers R{new_r}C{new_c}"
                  f"{' (urgence H-3 proche)' if would_miss_h3 and not grace_elapsed else ''}")
            _apply_selection(new_r, new_c, new_ts)
        except Exception as exc:
            print(f"  [AUTO-AVANCE] Erreur : {exc}")


# ── Thread dédié à la boucle asyncio ─────────────────────────────────────────

_async_loop: asyncio.AbstractEventLoop | None = None

async def _main_async() -> None:
    await asyncio.gather(scrape_loop_async(), _depart_refresh_loop(), _auto_advance_loop())

def _run_async_loop():
    global _async_loop
    _async_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_async_loop)
    _async_loop.run_until_complete(_main_async())

def run_in_async(coro, timeout: float = 5.0):
    if _async_loop is None:
        return None
    fut = asyncio.run_coroutine_threadsafe(coro, _async_loop)
    return fut.result(timeout=timeout)

# ── HTTP Server multi-thread ──────────────────────────────────────────────────

class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True

class Handler(BaseHTTPRequestHandler):

    def log_message(self, *_): pass

    def send_json(self, data: dict, code: int = 200) -> None:
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type",   "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def send_file(self, path: str, mime: str) -> None:
        try:
            with open(path, "rb") as fh:
                body = fh.read()
            self.send_response(200)
            self.send_header("Content-Type",   mime)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except FileNotFoundError:
            self.send_response(404); self.end_headers()

    def read_body(self) -> dict:
        n = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(n)) if n else {}

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        path = self.path.split("?")[0]

        if path in ("/", "/index.html"):
            self.send_file("index.html", "text/html; charset=utf-8")

        elif path == "/api/reunions":
            try:
                r = run_in_async(fetch_reunions_async(), timeout=5.0)
                STATE["reunions"] = r
                self.send_json({"reunions": r})
            except Exception as exc:
                self.send_json({"reunions": [], "error": str(exc)})

        elif path == "/api/courses":
            # Accepte ?reunion=X pour lire les courses sans modifier selected_reunion
            qs     = self.path.split("?", 1)[1] if "?" in self.path else ""
            params = dict(p.split("=", 1) for p in qs.split("&") if "=" in p)
            r_num  = params.get("reunion") or STATE.get("selected_reunion")
            if not r_num:
                self.send_json({"courses": []}); return
            try:
                c = run_in_async(fetch_courses_async(r_num), timeout=5.0)
                if not params.get("reunion"):   # ne mettre à jour STATE que si pas de param explicite
                    STATE["courses"] = c
                self.send_json({"courses": c})
            except Exception as exc:
                self.send_json({"courses": [], "error": str(exc)})

        elif path == "/api/state":
            self.send_json(_build_state_payload())

        elif path == "/api/history":
            # Historique des courses du jour déjà terminées (snapshots figés,
            # cotes finales, classement des chutes) — voir _archive_current_course().
            # Purgé automatiquement au changement de jour calendaire.
            self.send_json({"history": STATE.get("history", [])})

        elif path == "/api/ping":
            # Endpoint léger pour un service de ping externe (ex: cron-job.org)
            # qui empêche Render de mettre le service en veille. Ne fait
            # aucun calcul lourd, ne touche pas STATE — juste un "je suis vivant".
            self.send_json({"ok": True, "ts": int(time.time() * 1000)})

        elif path == "/api/stream":
            self.send_response(200)
            self.send_header("Content-Type",                "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control",               "no-cache")
            self.send_header("X-Accel-Buffering",           "no")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            client_q = queue_mod.Queue(maxsize=100)
            with _sse_clients_lock:
                _sse_clients.append(client_q)

            try:
                init = json.dumps(_build_state_payload(), ensure_ascii=False)
                self.wfile.write(f"data: {init}\n\n".encode())
                self.wfile.flush()
                while True:
                    try:
                        # Heartbeat court : permet de détecter rapidement la
                        # déconnexion du client (écriture en échec) même
                        # quand le bot est en attente (pas de scraping actif)
                        msg = client_q.get(timeout=2)
                    except queue_mod.Empty:
                        msg = b": heartbeat\n\n"
                    self.wfile.write(msg)
                    self.wfile.flush()
            except Exception:
                pass
            finally:
                with _sse_clients_lock:
                    try:
                        _sse_clients.remove(client_q)
                    except ValueError:
                        pass

        else:
            self.send_response(404); self.end_headers()

    def do_POST(self):
        body = self.read_body()

        if self.path == "/api/select":
            if body.get("reunion"):
                STATE["selected_reunion"] = body["reunion"]
            if body.get("course"):
                c_num = body["course"]
                same_course = (
                    STATE.get("selected_course") is not None
                    and str(STATE.get("selected_course")) == str(c_num)
                    and str(STATE.get("selected_reunion")) == str(body.get("reunion", STATE.get("selected_reunion")))
                )
                if same_course:
                    # Resélection de la course déjà en cours (ex : rafraîchissement
                    # de page) → ne RIEN réinitialiser, en particulier les
                    # snapshots H-3min/H-1min/Départ déjà pris doivent être
                    # conservés tels quels.
                    self.send_json({"ok": True})
                    return
                _archive_current_course()
                STATE["odds"]          = {}
                STATE["odds_prev"]     = {}
                STATE["error"]         = None
                STATE["depart_ts"]     = None
                STATE["depart_str"]    = None
                STATE["countdown_sec"] = None
                STATE["refresh_count"] = 0
                STATE["dropped_horses"] = []
                STATE["last_odds_change_ts"] = None
                STATE["h3_snapshot"]       = {}
                STATE["h3_snapshot_taken"] = False
                STATE["h3_snapshot_ts"]    = None
                STATE["h1_snapshot"]       = {}
                STATE["h1_snapshot_taken"] = False
                STATE["h1_snapshot_ts"]    = None
                STATE["hd_snapshot"]       = {}
                STATE["hd_snapshot_taken"] = False
                STATE["hd_snapshot_ts"]    = None
                STATE["selected_course"] = c_num
                try:
                    courses = run_in_async(fetch_courses_async(STATE["selected_reunion"]))
                    for co in courses:
                        if str(co["id"]) == str(c_num) and co.get("heureDepart"):
                            ts = int(co["heureDepart"])
                            STATE["depart_ts"]  = ts
                            STATE["depart_str"] = datetime.fromtimestamp(ts/1000, PARIS_TZ).strftime("%H:%M")
                            break
                except Exception as exc:
                    print(f"  [WARN] heureDepart: {exc}")
            self.send_json({"ok": True})

        else:
            self.send_response(404); self.end_headers()


# ── Point d'entrée ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print()
    print("╔══════════════════════════════════════════════════════╗")
    print("║         TBLAUGRANA BOT  —  v13 (Render)             ║")
    print("║  Source : online.turfinfo.api.pmu.fr                ║")
    print(f"║  Port       : {PORT}                                    ║")
    print(f"║  Refresh    : toutes les {REFRESH_SEC}s                         ║")
    print(f"║  Départ MAJ : toutes les {DEPART_REFRESH_SEC}s ({DEPART_REFRESH_SEC//60} min)              ║")
    print(f"║  Plage cote : {COTE_MIN:.1f} — {COTE_MAX:.1f}                              ║")
    print(f"║  Alerte     : chute >= {DROP_ALERT}% apres le depart          ║")
    print(f"║  Screen H-3 : pris a H-{H3_SNAPSHOT_MIN}min, classement des chutes    ║")
    print("╚══════════════════════════════════════════════════════╝")
    print()

    t = threading.Thread(target=_run_async_loop, daemon=True)
    t.start()
    time.sleep(0.3)

    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"  Serveur démarré — écoute sur 0.0.0.0:{PORT}")
    print("  (Ctrl+C pour arrêter)\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Arrêt propre.")
