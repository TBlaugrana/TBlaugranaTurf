# TBlaugrana Rang — PMU Bot

Suivi live des cotes PMU avec snapshot pré-course, détection de mouvements
rapides (steam moves) et enchaînement automatique sur la prochaine course.

Ce dossier contient tout ce qu'il faut pour héberger le bot en ligne
(GitHub + Railway) : plus besoin de lancer un serveur local, il suffit
d'ouvrir l'URL fournie par Railway.

## Contenu

- `pmu_bot.html` — l'interface (page unique, HTML/CSS/JS)
- `server.py` — petit serveur Python (stdlib uniquement) qui sert la page
  et fait office de proxy anti-CORS vers l'API PMU
- `Procfile` — indique à Railway comment démarrer le serveur
- `requirements.txt`, `runtime.txt` — config Python pour Railway

## 1. Mettre le projet sur GitHub

```bash
git init
git add .
git commit -m "PMU Bot"
git branch -M main
git remote add origin https://github.com/<ton-compte>/<ton-repo>.git
git push -u origin main
```

## 2. Déployer sur Railway

1. Va sur [railway.app](https://railway.app) et connecte-toi (avec ton compte GitHub).
2. **New Project** → **Deploy from GitHub repo** → sélectionne ce repo.
3. Railway détecte automatiquement `Procfile` + `requirements.txt` et build
   le projet en Python — aucune configuration supplémentaire n'est nécessaire.
4. Une fois le déploiement terminé, va dans l'onglet **Settings** du service
   → **Networking** → **Generate Domain** pour obtenir une URL publique
   (du type `https://ton-projet.up.railway.app`).

## 3. Utilisation

Ouvre simplement l'URL générée par Railway dans ton navigateur — la page
`pmu_bot.html` s'affiche directement à la racine. Il n'y a rien à lancer
ni à installer : c'est un site en ligne, en lecture seule dès l'ouverture.
Le bot charge automatiquement la prochaine course et démarre le suivi tout
seul.

## Notes

- Le serveur écoute sur le port fourni par Railway (variable d'environnement
  `PORT`), avec un repli sur `8000` en local si tu veux tester avant de
  déployer (`python server.py` puis ouvrir `http://localhost:8000`).
- Toute la logique de suivi (cotes, snapshot, retards, changement de
  course) tourne côté navigateur (JavaScript) ; le serveur ne fait que
  relayer les requêtes vers l'API PMU pour contourner le CORS.
