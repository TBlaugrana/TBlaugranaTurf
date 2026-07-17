# TBlaugrana BOT

Bot de suivi des cotes PMU en direct, avec alertes de chute de cote.

## Fonctionnement "tout serveur"

Toutes les données (cotes actuelles, cotes précédentes, alertes, % de chute,
heure de départ, compteur de refresh...) sont stockées **uniquement côté
serveur**, dans la variable `STATE` de `server.py`. Le navigateur n'est
qu'un affichage :

- `/api/state` renvoie l'état complet à l'ouverture de la page.
- `/api/stream` (Server-Sent Events) pousse les mises à jour en temps réel.

➡️ Fermer l'onglet, recharger la page ou changer d'appareil ne fait perdre
**aucune** donnée : à la reconnexion, le client redemande simplement l'état
courant au serveur. La seule condition, c'est que **le processus serveur
continue de tourner** — d'où l'intérêt de l'héberger sur Railway plutôt que
de le faire dépendre d'une fenêtre Chrome ouverte sur votre PC.

C'est pour ça que `AUTO_CLOSE_ON_EXIT` (qui coupait le serveur 2s après la
fermeture de la fenêtre) est maintenant **désactivé par défaut** — il ne
doit être réactivé que si vous relancez le bot en local via le `.bat`.

## Déploiement

### 1. GitHub

```bash
cd tblbot
git init
git add .
git commit -m "TBlaugrana BOT - version serveur"
git branch -M main
git remote add origin https://github.com/<votre-utilisateur>/<votre-repo>.git
git push -u origin main
```

⚠️ Le token Telegram n'est **plus** écrit en dur dans `server.py` (il l'était
dans votre version d'origine — ça aurait exposé votre bot Telegram à
n'importe qui consultant le repo). Il se configure maintenant via des
variables d'environnement (voir ci-dessous).

### 2. Railway

1. Sur [railway.app](https://railway.app), **New Project → Deploy from GitHub repo**, choisissez votre repo.
2. Railway détecte automatiquement `requirements.txt` et `Procfile` (Nixpacks) et lance `python server.py`.
3. Railway fournit lui-même la variable `PORT` — c'est déjà géré dans le code.
4. Dans l'onglet **Variables** du service, ajoutez (facultatif, pour Telegram) :
   - `TELEGRAM_ON=true`
   - `TELEGRAM_TOKEN=<votre token>`
   - `TELEGRAM_CHAT_IDS=625118343,8288460384`
5. Une fois déployé, Railway vous donne une URL publique (ex : `https://tblbot-production.up.railway.app`) — c'est cette URL que vous ouvrez dans votre navigateur, plus besoin du `.bat` ni de `localhost`.

### 3. Usage local (optionnel, avec le .bat)

`Lancer_TBL_BOT.bat` fonctionne toujours pour lancer le bot sur votre PC
(utile pour tester). Remplissez les variables Telegram en haut du fichier
si vous voulez les alertes en local aussi.

## Rappel sécurité

Ne partagez jamais votre `TELEGRAM_TOKEN` publiquement. S'il a déjà été
exposé (ce qui est le cas de celui présent dans votre fichier d'origine),
régénérez-le via [@BotFather](https://t.me/BotFather) (`/revoke`) avant de
mettre ce projet sur un repo, même privé.
