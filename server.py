#!/usr/bin/env python3
"""
Serveur pour pmu_bot.html — version Railway.
- Sert le fichier HTML et les fichiers statiques du dossier courant
- Relaie (proxy) les requetes vers /proxy/<cle>/... vers l'hote PMU correspondant,
  afin de contourner le blocage CORS du navigateur.
- Ecoute sur 0.0.0.0:$PORT (Railway impose le port via la variable d'environnement PORT).

Cles de proxy disponibles (PROXY_MAP) :
  /proxy/61/...         -> https://online.turfinfo.api.pmu.fr/rest/client/61/...  (programme du jour)
  /proxy/citations/...  -> https://offline.turfinfo.api.pmu.fr/rest/client/7/...  (rapports probables SG/SP)
"""
import http.server
import socketserver
import http.client
import threading
import os

PORT = int(os.environ.get("PORT", 8000))   # Railway fournit le port via $PORT
HOST = "0.0.0.0"                            # doit ecouter sur toutes les interfaces (pas 127.0.0.1) sur Railway
PROXY_TIMEOUT = 6  # secondes : on abandonne vite une requete lente plutot que de bloquer le poll suivant

# cle_de_route -> (hote_distant, prefixe_chemin_distant)
PROXY_MAP = {
    "61": ("online.turfinfo.api.pmu.fr", "/rest/client/61/"),
    "citations": ("offline.turfinfo.api.pmu.fr", "/rest/client/7/"),
}

# Connexions HTTPS persistantes vers les API PMU (une par thread ET par hote), reutilisees
# entre les appels : evite de renegocier une poignee de main TLS a chaque poll (~100-300ms
# gagnes par requete). Reconnectees automatiquement en cas d'echec.
_thread_local = threading.local()

def _get_pmu_conn(host):
    conns = getattr(_thread_local, "conns", None)
    if conns is None:
        conns = {}
        _thread_local.conns = conns
    conn = conns.get(host)
    if conn is None:
        conn = http.client.HTTPSConnection(host, timeout=PROXY_TIMEOUT)
        conns[host] = conn
    return conn


class ThreadingHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class Handler(http.server.SimpleHTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # garde la connexion navigateur <-> serveur ouverte (keep-alive)

    def do_GET(self):
        # Racine -> redirige directement vers pmu_bot.html pour que le lien Railway
        # affiche la page tout de suite, sans avoir a taper le nom du fichier.
        if self.path == "/":
            self.send_response(302)
            self.send_header("Location", "/pmu_bot.html")
            self.end_headers()
            return
        if self.path.startswith("/proxy/"):
            self.handle_proxy()
        else:
            super().do_GET()

    def handle_proxy(self):
        # /proxy/<cle>/programme/xxx -> https://<hote>/<prefixe>programme/xxx
        rest = self.path[len("/proxy/"):]
        key, _, remainder = rest.partition("/")
        route = PROXY_MAP.get(key)
        if route is None:
            print(f"[PROXY] cle inconnue '{key}' pour {self.path}")
            body = f"Cle de proxy inconnue : {key}".encode("utf-8")
            self.send_response(404)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        host, remote_prefix = route
        target_path = remote_prefix + remainder
        target_url = f"https://{host}{target_path}"
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

        for attempt in (1, 2):  # 1 essai + 1 reessai si la connexion persistante a ete fermee par le serveur distant
            try:
                conn = _get_pmu_conn(host)
                conn.request("GET", target_path, headers=headers)
                resp = conn.getresponse()
                data = resp.read()
                print(f"[PROXY] GET {target_url} -> {resp.status} ({len(data)} octets)")
                self.send_response(resp.status)
                self.send_header("Content-Type", resp.getheader("Content-Type", "application/json"))
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(data)
                return
            except Exception as e:
                print(f"[PROXY] ERREUR sur {target_url} (essai {attempt}) : {e}")
                conns = getattr(_thread_local, "conns", None)
                if conns is not None:
                    conns.pop(host, None)  # force une reconnexion propre
                if attempt == 2:
                    body = f"Erreur proxy: {e}".encode("utf-8")
                    self.send_response(502)
                    self.send_header("Content-Type", "text/plain")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)

    def end_headers(self):
        if not self.path.startswith("/proxy/"):
            self.send_header("Access-Control-Allow-Origin", "*")
        super().end_headers()

    def log_message(self, format, *args):
        # Ne loggue que les requetes non-proxy (le proxy a son propre log détaillé ci-dessus)
        if not self.path.startswith("/proxy/"):
            print(f"[HTTP] {format % args}")


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    with ThreadingHTTPServer((HOST, PORT), Handler) as httpd:
        print(f"Serveur lance sur {HOST}:{PORT} (page: /pmu_bot.html)")
        httpd.serve_forever()
