# TBlaugranaTurf

Bot de surveillance des cotes PMU en temps réel, avec alertes Telegram sur
chute de cote avant le départ.

## ⚠️ Sécurité — à lire avant de publier sur GitHub

Le fichier original contenait un **token de bot Telegram et des chat IDs en
clair dans le code**. Ils ont été retirés de cette version : le token et les
chat IDs se configurent maintenant uniquement via le panneau **Réglages
(⚙️)** dans l'interface, et sont stockés dans le `localStorage` du
navigateur — jamais dans le code, jamais sur GitHub.

**Si tu as déjà utilisé l'ancien token dans une version publiée ou poussée
sur un repo (même privé), révoque-le et régénère-en un nouveau via
[@BotFather](https://t.me/BotFather) avant de continuer.** Un token visible
dans l'historique git reste récupérable même après suppression du fichier.

## Comment ça fonctionne

- `server.js` sert l'interface (`public/index.html`) et fait office de
  **proxy** vers l'API PMU (`online.turfinfo.api.pmu.fr`).
- Le CORS est une protection imposée par le *navigateur* : une requête
  serveur → serveur n'y est pas soumise. C'est pourquoi le proxy permet de
  se passer du `--disable-web-security` utilisé par l'ancien `.bat` — le
  navigateur ne parle qu'à ton propre serveur, qui relaie ensuite la
  requête vers PMU.
- Les notifications Telegram, elles, continuent de partir directement du
  navigateur vers `api.telegram.org` (l'API Telegram autorise le CORS).

## Déploiement

### 1. Pousser sur GitHub

```bash
cd tblaugranaturf
git init
git add .
git commit -m "Initial commit — TBlaugranaTurf"
git branch -M main
git remote add origin https://github.com/<ton-compte>/<ton-repo>.git
git push -u origin main
```

### 2. Déployer sur Railway

1. Sur [railway.app](https://railway.app), clique **New Project → Deploy
   from GitHub repo** et sélectionne ton repo.
2. Railway détecte automatiquement le `package.json` et lance `npm install`
   puis `npm start`. Aucune variable d'environnement n'est requise (le port
   est fourni automatiquement par Railway via `process.env.PORT`).
3. Une fois le déploiement terminé, va dans **Settings → Networking** et
   clique **Generate Domain** pour obtenir une URL publique
   (`https://....up.railway.app`).
4. Ouvre cette URL : l'appli se charge directement dans le navigateur, sans
   aucune installation côté client.

### 3. Configurer Telegram (optionnel)

Dans l'appli déployée, ouvre **⚙️ Réglages**, renseigne ton **Bot Token**
(obtenu via [@BotFather](https://t.me/BotFather)) et tes **Chat ID(s)**,
clique **🧪 Tester la connexion Telegram**, puis **✓ Enregistrer**. Ces
réglages sont propres à chaque navigateur (stockés en local), donc à refaire
sur chaque appareil utilisé.

## Développement local

```bash
npm install
npm start
```

Puis ouvre `http://localhost:3000`.

## Fichiers du projet

```
tblaugranaturf/
├── server.js          # Serveur Express + proxy API PMU
├── package.json
├── .gitignore
├── README.md
└── public/
    └── index.html     # Interface (ex pmu_bot.html, adaptée)
```

Le lanceur `lancer_pmu_bot.bat` n'est plus nécessaire avec ce déploiement :
il servait uniquement à contourner le CORS en local via un navigateur
modifié, ce que le proxy serveur remplace désormais proprement.
