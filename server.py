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
DROP_ALERT   = 10.0
# ║                                                                              ║
# ║  TELEGRAM_ON  : activer (True) ou désactiver (False) les alertes Telegram  ║
TELEGRAM_ON  = os.environ.get("TELEGRAM_ON", "false").lower() in ("1", "true", "yes")
# ║                                                                              ║
# ║  AUTO_CLOSE_ON_EXIT : ferme automatiquement le serveur Python quand la      ║
# ║                       fenêtre de l'appli (Chrome/Edge) est fermée          ║
# ║                       DOIT rester désactivé en hébergement serveur (Railway)║
# ║                       sinon le bot s'arrête dès que l'onglet est fermé.     ║
AUTO_CLOSE_ON_EXIT = os.environ.get("AUTO_CLOSE_ON_EXIT", "false").lower() in ("1", "true", "yes")
# ║                                                                              ║
# ║  AUTO_CLOSE_SEC     : délai (en secondes) sans aucune fenêtre connectée     ║
# ║                       avant la fermeture automatique du serveur             ║
# ║                       (laisse le temps à un simple F5/rechargement)         ║
AUTO_CLOSE_SEC = 2
# ║                                                                              ║
# ╚══════════════════════════════════════════════════════════════════════════════╝


# ── Configuration interne (ne pas modifier) ────────────────────────────────────

PORT         = int(os.environ.get("PORT", 8765))
HOST         = "0.0.0.0"   # 0.0.0.0 = accessible depuis l'extérieur (requis par Railway) ; fonctionne aussi en local
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=4.0, connect=2.0, sock_read=3.5)
PID_FILE     = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tblbot.pid")


def _cleanup_and_exit() -> None:
    """Supprime le fichier PID puis arrête immédiatement le processus."""
    try:
        os.remove(PID_FILE)
    except Exception:
        pass
    os._exit(0)


# IMPORTANT SECURITE : le token et les chat_id ne sont plus écrits en dur ici
# (un repo GitHub, même privé, ne doit jamais contenir un token Telegram en clair).
# Définissez-les comme variables d'environnement :
#   TELEGRAM_TOKEN=xxxxx:yyyyy
#   TELEGRAM_CHAT_IDS=625118343,8288460384
TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_IDS = [c.strip() for c in os.environ.get("TELEGRAM_CHAT_IDS", "").split(",") if c.strip()]
TELEGRAM_API      = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

# Suivi des alertes déjà envoyées pour éviter les doublons (front montant)
# clé = "R{r}C{c}-{num}", valeur = True si alerte active
_tg_alert_state: dict = {}   # état courant  {key: bool}

async def _send_telegram(text: str) -> None:
    """Envoie un message à tous les chat IDs configurés."""
    if not TELEGRAM_ON:
        return
    try:
        session = await _get_session()
        for chat_id in TELEGRAM_CHAT_IDS:
            payload = {
                "chat_id"   : chat_id,
                "text"      : text,
                "parse_mode": "HTML",
            }
            async with session.post(TELEGRAM_API, json=payload) as resp:
                if resp.status not in (200, 201):
                    body = await resp.text()
                    print(f"  [TG] Erreur {resp.status}: {body[:120]}")
    except Exception as exc:
        print(f"  [TG] Exception: {exc}")

async def _check_and_notify(odds: dict, odds_prev: dict,
                             r_num: str, c_num: str) -> None:
    """Détecte les fronts montants d'alerte et envoie les notifications Telegram."""
    dep_ts       = STATE.get("depart_ts")
    now_ms       = int(time.time() * 1000)
    race_started = dep_ts and now_ms >= dep_ts
    if not race_started:
        return

    elapsed_sec = int((now_ms - dep_ts) / 1000) if dep_ts else 0

    for num, info in odds.items():
        key       = f"R{r_num}C{c_num}-{num}"
        cote_now  = info["cote"]
        prev_info = odds_prev.get(num)
        cote_prev = prev_info["cote"] if prev_info else None

        # Ignorer les chevaux hors plage de cotes
        if not (COTE_MIN <= cote_now <= COTE_MAX):
            _tg_alert_state[key] = False
            continue

        is_alert = False
        drop_pct = 0.0
        if cote_prev and cote_prev > 0:
            drop_pct = round((cote_prev - cote_now) / cote_prev * 100, 1)
            if drop_pct >= DROP_ALERT:
                is_alert = True

        was_alert = _tg_alert_state.get(key, False)
        _tg_alert_state[key] = is_alert

        # Front montant uniquement → envoyer la notification
        if is_alert and not was_alert:
            mins = elapsed_sec // 60
            secs = elapsed_sec % 60
            elapsed_str = f"{mins}min {secs}s" if mins else f"{secs}s"
            race_label  = f"R{r_num}C{c_num}"
            nom         = info["nom"]

            text = (
                f"🚨 <b>ALERTE {race_label}</b> 🚨\n"
                f"🐎 {num} — {nom}\n"
                f"{cote_prev:.1f} ➡️ {cote_now:.1f} (–{drop_pct}%)\n"
                f"🏁 {elapsed_str} après départ"
            )
            asyncio.ensure_future(_send_telegram(text))

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
    d = datetime.now()
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

async def fetch_reunions_async() -> list:
    url       = f"{BASE_URL_PROG}/{today()}{BASE_URL_PARAMS}"
    data      = await fetch_json_async(url)
    out       = []
    programme = data.get("programme") or {}
    for r in programme.get("reunions", []):
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
                heure = datetime.fromtimestamp(int(ts) / 1000).strftime("%H:%M")
            except Exception:
                pass

        # Discipline (ATTELE, MONTE, PLAT, HAIES, STEEPLE-CHASE, CROSS...) → libellé FR
        raw_disc  = (c.get("discipline") or c.get("specialite") or "").upper()
        DISC_MAP  = {
            "ATTELE"         : "Attelé",
            "MONTE"          : "Monté",
            "PLAT"           : "Plat",
            "HAIES"          : "Haies",
            "STEEPLE-CHASE"  : "Steeple",
            "STEEPLECHASE"   : "Steeple",
            "CROSS"          : "Cross",
            "TROT"           : "Trot",
        }
        discipline = DISC_MAP.get(raw_disc, raw_disc.capitalize() if raw_disc else "")

        # Nombre de partants déclarés
        partants = (c.get("nombreDeclaresPartants")
                    or c.get("nombreDeclarePartants")
                    or c.get("participants")
                    or 0)
        try:
            partants = int(partants)
        except (ValueError, TypeError):
            partants = 0

        out.append({
            "id": str(num), "num": num,
            "label": f"C{num} — {label}",
            "heure": heure, "heureDepart": ts,
            "discipline": discipline, "partants": partants,
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

# Suivi pour la fermeture automatique : True dès qu'au moins une fenêtre
# s'est connectée une fois, et timestamp depuis lequel plus aucune fenêtre
# n'est connectée (None = au moins une fenêtre connectée actuellement)
_sse_ever_connected = False
_sse_empty_since    = None

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

        rows.append({
            "num"         : num,
            "nom"         : info["nom"],
            "cote_now"    : cote_now,
            "cote_prev"   : cote_prev,
            "drop_pct"    : drop_pct,
            "alert"       : alert,
            "ever_dropped": num in STATE["dropped_horses"],  # list lookup, toujours JSON-safe
        })

    # Ne garder dans l'affichage que les chevaux dont la cote ACTUELLE
    # est dans la plage [COTE_MIN, COTE_MAX]. Comme ce filtre se base sur
    # cote_now à chaque rafraîchissement, un cheval qui dépasse COTE_MAX
    # disparaît de la liste, puis y revient automatiquement si sa cote
    # repasse à nouveau dans la plage.
    rows = [r for r in rows if COTE_MIN <= r["cote_now"] <= COTE_MAX]

    rows.sort(key=lambda x: x["cote_now"])

    return {
        "status"              : STATE["status"],
        "last_update"         : STATE["last_update"],
        "refresh_count"       : STATE["refresh_count"],
        "error"               : STATE["error"],
        "rows"                : rows,
        "depart_str"          : STATE.get("depart_str"),
        "depart_ts"           : STATE.get("depart_ts"),
        "countdown_sec"       : STATE.get("countdown_sec"),
        "seq"                 : STATE.get("seq", 0),
        "race_started"        : bool(race_started),
        "last_odds_change_ts" : STATE.get("last_odds_change_ts"),
        "selected_reunion"    : STATE.get("selected_reunion"),
        "selected_course"     : STATE.get("selected_course"),
    }

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

                # La sélection a pu changer pendant l'appel réseau (l'utilisateur
                # a cliqué sur une autre course). Dans ce cas, ce résultat est
                # obsolète (cotes/noms de chevaux de la course précédente) : on
                # le jette pour éviter d'afficher le mauvais nom de cheval, et
                # on relance aussitôt une requête sur la course réellement
                # sélectionnée.
                if STATE.get("selected_reunion") != r or STATE.get("selected_course") != c:
                    continue

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
                    STATE["last_update"]   = datetime.now().strftime("%H:%M:%S")
                    STATE["refresh_count"] += 1
                    STATE["error"]         = None

                    if odds_changed:
                        STATE["last_odds_change_ts"] = int(time.time() * 1000)
                        # Vérifier les alertes Telegram (front montant uniquement)
                        # On passe prev_snapshot (capturé avant l'écrasement de STATE["odds_prev"])
                        await _check_and_notify(odds, prev_snapshot, r, c)

                    dep = STATE.get("depart_ts")
                    STATE["countdown_sec"] = int((dep - now_ms) / 1000) if dep else None

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
                        STATE["depart_str"] = datetime.fromtimestamp(ts_new / 1000).strftime("%H:%M")
                        print(f"  [DEPART] Heure mise à jour R{r}C{c} : "
                              f"{datetime.fromtimestamp((ts_old or 0)/1000).strftime('%H:%M') if ts_old else '?'} "
                              f"→ {STATE['depart_str']}")
                    else:
                        print(f"  [DEPART] Heure confirmée R{r}C{c} : {STATE['depart_str']} (inchangée)")
                    break
        except Exception as exc:
            print(f"  [DEPART] Erreur rechargement heure de départ : {exc}")


# ── Fermeture automatique quand la fenêtre est fermée ─────────────────────────

async def _auto_close_watcher() -> None:
    """Arrête le serveur si plus aucune fenêtre (SSE) n'est connectée
    depuis AUTO_CLOSE_SEC secondes — détecte la fermeture de l'appli."""
    while True:
        await asyncio.sleep(1.0)
        if not AUTO_CLOSE_ON_EXIT:
            continue
        with _sse_clients_lock:
            ever        = _sse_ever_connected
            empty_since = _sse_empty_since
            has_clients = bool(_sse_clients)
        if ever and not has_clients and empty_since is not None:
            if (time.time() - empty_since) >= AUTO_CLOSE_SEC:
                print(f"  Aucune fenêtre connectée depuis {AUTO_CLOSE_SEC}s — fermeture du serveur.")
                _cleanup_and_exit()

# ── Thread dédié à la boucle asyncio ─────────────────────────────────────────

_async_loop: asyncio.AbstractEventLoop | None = None

async def _main_async() -> None:
    await asyncio.gather(scrape_loop_async(), _auto_close_watcher(), _depart_refresh_loop())

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
                global _sse_ever_connected, _sse_empty_since
                _sse_ever_connected = True
                _sse_empty_since    = None

            try:
                init = json.dumps(_build_state_payload(), ensure_ascii=False)
                self.wfile.write(f"data: {init}\n\n".encode())
                self.wfile.flush()
                while True:
                    try:
                        # Heartbeat court : permet de détecter rapidement la
                        # fermeture de la fenêtre (écriture en échec) même
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
                    if not _sse_clients:
                        _sse_empty_since = time.time()

        else:
            self.send_response(404); self.end_headers()

    def do_POST(self):
        body = self.read_body()

        if self.path == "/api/select":
            if body.get("reunion"):
                STATE["selected_reunion"] = body["reunion"]
            if body.get("course"):
                STATE["odds"]          = {}
                STATE["odds_prev"]     = {}
                STATE["error"]         = None
                STATE["depart_ts"]     = None
                STATE["depart_str"]    = None
                STATE["countdown_sec"] = None
                STATE["refresh_count"] = 0
                STATE["dropped_horses"] = []
                STATE["last_odds_change_ts"] = None
                _tg_alert_state.clear()
                c_num = body["course"]
                STATE["selected_course"] = c_num
                try:
                    courses = run_in_async(fetch_courses_async(STATE["selected_reunion"]))
                    for co in courses:
                        if str(co["id"]) == str(c_num) and co.get("heureDepart"):
                            ts = int(co["heureDepart"])
                            STATE["depart_ts"]  = ts
                            STATE["depart_str"] = datetime.fromtimestamp(ts/1000).strftime("%H:%M")
                            break
                except Exception as exc:
                    print(f"  [WARN] heureDepart: {exc}")
            self.send_json({"ok": True})

        elif self.path == "/api/shutdown":
            self.send_json({"ok": True})
            threading.Thread(target=lambda: (time.sleep(0.3), _cleanup_and_exit()), daemon=True).start()

        else:
            self.send_response(404); self.end_headers()


# ── Point d'entrée ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Écrire le PID dans un fichier pour permettre une fermeture propre via le .bat
    try:
        with open(PID_FILE, "w") as _f:
            _f.write(str(os.getpid()))
    except Exception:
        pass

    import atexit
    @atexit.register
    def _remove_pid():
        try:
            os.remove(PID_FILE)
        except Exception:
            pass

    print()
    tg_status = "✅ activé" if TELEGRAM_ON else "❌ désactivé"
    ac_status = f"✅ activé ({AUTO_CLOSE_SEC}s)" if AUTO_CLOSE_ON_EXIT else "❌ désactivé"
    print("╔══════════════════════════════════════════════════════╗")
    print("║         TBLAUGRANA BOT  —  v12                      ║")
    print("║  Source : online.turfinfo.api.pmu.fr                ║")
    print(f"║  Interface  : http://localhost:{PORT}                  ║")
    print(f"║  Refresh    : toutes les {REFRESH_SEC}s                         ║")
    print(f"║  Départ MAJ : toutes les {DEPART_REFRESH_SEC}s ({DEPART_REFRESH_SEC//60} min)              ║")
    print(f"║  Plage cote : {COTE_MIN:.1f} — {COTE_MAX:.1f}                              ║")
    print(f"║  Alerte     : chute >= {DROP_ALERT}% apres le depart          ║")
    print(f"║  Telegram   : {tg_status}                         ║")
    print(f"║  Auto-close : {ac_status}                       ║")
    print("╚══════════════════════════════════════════════════════╝")
    print()

    t = threading.Thread(target=_run_async_loop, daemon=True)
    t.start()
    time.sleep(0.3)

    # Notification Telegram au démarrage
    now_str = datetime.now().strftime("%H:%M:%S")
    startup_msg = (
        f"✅ <b>TBlaugrana BOT démarré</b>\n"
        f"🕐 {now_str}\n"
        f"🔄 Refresh cotes toutes les {REFRESH_SEC}s\n"
        f"⏱️ Départ rechargé toutes les {DEPART_REFRESH_SEC//60} min\n"
        f"📡 En écoute sur http://localhost:{PORT}"
    )
    run_in_async(_send_telegram(startup_msg))
    print("  Notification Telegram de démarrage envoyée.")

    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"  Serveur démarré — http://{HOST}:{PORT}")
    print("  (Ctrl+C pour arrêter)\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Arrêt propre.")
