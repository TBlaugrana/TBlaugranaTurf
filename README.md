# TBlaugranaTurf — déploiement GitHub + Railway

Version web du bot : plus besoin du `.bat` ni de désactiver la sécurité
du navigateur. Un petit serveur Express sert la page et relaie les
appels vers l'API PMU (le proxy tourne côté serveur, donc pas de
blocage CORS).

## Structure

```
.
├── server.js        → serveur Express + proxy API PMU
├── package.json
├── Procfile
└── public/
    └── index.html    → l'app (ex pmu_bot.html)
```

## 1. Mettre le projet sur GitHub

```bash
cd pmu-bot-railway
git init
git add .
git commit -m "Initial commit"
gh repo create tblaugranaturf --private --source=. --push
# ou manuellement : créer un repo vide sur github.com,
# puis `git remote add origin <url>` + `git push -u origin main`
```

## 2. Déployer sur Railway

1. Sur [railway.app](https://railway.app) → **New Project** → **Deploy from GitHub repo**.
2. Sélectionner le repo `tblaugranaturf`.
3. Railway détecte automatiquement Node.js via `package.json` (build avec Nixpacks,
   commande de démarrage `npm start`). Rien à configurer.
4. Une fois le build terminé, Railway génère une URL publique
   (**Settings → Networking → Generate Domain**).
5. Ouvrir cette URL dans n'importe quel navigateur (desktop ou mobile) :
   le bot fonctionne directement, sans manipulation particulière.

## Ce qui a changé par rapport à la version locale

- `PMU_BASE_PROG` / `PMU_BASE_PARTS` pointent maintenant vers `/api/pmu61`
  et `/api/pmu7` (routes internes) au lieu de l'URL PMU directe.
- `server.js` relaie ces requêtes vers `online.turfinfo.api.pmu.fr`,
  y compris la gestion `ETag` / `If-None-Match` (pour garder la détection
  "304 = cotes inchangées" utilisée par le front).
- Le fichier `lancer_pmu_bot.bat` et le profil Chrome dédié ne sont plus
  nécessaires : ils existaient uniquement pour contourner CORS en local.

## Points d'attention

- **Charge sur l'API PMU** : le front lance plusieurs requêtes identiques
  en parallèle par cycle (`raceRequests`, variable `n` selon la proximité
  du départ) pour prendre la plus rapide. C'était pensé pour un usage
  perso en local ; en hébergement partagé, si plusieurs personnes ouvrent
  l'app en même temps, la charge sur l'API PMU se multiplie. Si tu es seul
  utilisateur ça ne change rien, mais si tu comptes partager le lien, vaut
  mieux réduire `raceParallelHot` / `raceParallelUltra` dans `CFG`
  (dans `public/index.html`) pour rester raisonnable.
- **Token Telegram** : il est saisi et stocké côté navigateur (réglages
  de l'app), les notifications partent toujours directement depuis le
  client vers l'API Telegram — le serveur ne le voit pas. Pas de
  changement nécessaire de ce côté.
- Railway attribue le port via la variable d'environnement `PORT` ;
  `server.js` le lit automatiquement (`process.env.PORT`), rien à changer.
