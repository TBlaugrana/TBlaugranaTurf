#!/usr/bin/env python3
"""
Serveur pour pmu_bot.html — version cloud (Railway).
- Sert pmu_bot.html directement à la racine ("/")
- Sert les fichiers statiques du dossier courant
- Relaie (proxy) les requetes vers /proxy/61/... et /proxy/7/...
  vers https://online.turfinfo.api.pmu.fr/rest/client/61 et /7
  afin de contourner le blocage CORS du navigateur.

Ecoute sur 0.0.0.0 et sur le port fourni par la plateforme (variable
d'environnement PORT), avec un repli sur 8000 en local.
"""
import http.server
import socketserver
import http.client
import threading
import os

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 8000))
PMU_HOST = "online.turfinfo.api.pmu.fr"
PROXY_TIMEOUT = 6  # secondes : on abandonne vite une requete lente plutot que de bloquer le poll suivant

# Connexion HTTPS persistante vers l'API PMU (une par thread), reutilisee entre
# les appels : evite de renegocier une poignee de main TLS a chaque poll (~100-300ms
# gagnes par requete). Reconnectee automatiquement en cas d'echec.
_thread_local = threading.local()

def _get_pmu_conn():
    conn = getattr(_thread_local, "conn", None)
    if conn is None:
        conn = http.client.HTTPSConnection(PMU_HOST, timeout=PROXY_TIMEOUT)
        _thread_local.conn = conn
    return conn


class ThreadingHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class Handler(http.server.SimpleHTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # garde la connexion navigateur <-> serveur ouverte (keep-alive)

    def do_GET(self):
        if self.path.startswith("/proxy/"):
            self.handle_proxy()
            return
        if self.path == "/" or self.path == "":
            # Ouvrir l'URL racine affiche directement le bot (lecture seule, rien a lancer)
            self.path = "/pmu_bot.html"
        super().do_GET()

    def handle_proxy(self):
        # /proxy/61/programme/xxx -> https://online.turfinfo.api.pmu.fr/rest/client/61/programme/xxx
        target_path = "/rest/client/" + self.path[len("/proxy/"):]
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

        for attempt in (1, 2):  # 1 essai + 1 reessai si la connexion persistante a ete fermee par le serveur distant
            try:
                conn = _get_pmu_conn()
                conn.request("GET", target_path, headers=headers)
                resp = conn.getresponse()
                data = resp.read()
                self.send_response(resp.status)
                self.send_header("Content-Type", resp.getheader("Content-Type", "application/json"))
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(data)
                return
            except Exception as e:
                _thread_local.conn = None  # force une reconnexion propre
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
        # Garde les logs Railway lisibles
        pass


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    with ThreadingHTTPServer((HOST, PORT), Handler) as httpd:
        print(f"Serveur lance sur {HOST}:{PORT}")
        httpd.serve_forever()
