# TBlaugrana Rang — version serveur (GitHub + Railway)

## Ce qui a changé par rapport à la version locale

Avant, c'était ton **navigateur** qui interrogeait directement l'API PMU toutes
les 500 ms tant que l'onglet `pmu_bot.html` restait ouvert. Fermer l'onglet
arrêtait tout et perdait l'historique (cotes, vitesse, top 5...).

Maintenant, c'est **le serveur** (`server.py`) qui fait tout le travail en
continu, dans un thread de fond, **indépendamment du fait qu'un téléphone ou
un navigateur soit connecté ou non** :

- il charge le programme du jour et choisit automatiquement la prochaine course,
- il interroge les rapports probables PMU toutes les ~500 ms,
- il calcule l'écart Gagnant/Placé, la vitesse de variation (⚡), et le top 5,
- il garde tout ça en mémoire et l'expose via `GET /api/state`.

`pmu_bot.html` ne fait plus que lire cet état toutes les ~700 ms et l'afficher.
**Tu peux donc fermer l'onglet sur ton téléphone : le suivi continue côté
serveur, et tu retrouves l'état à jour en rouvrant la page.**

## Déploiement

### 1. GitHub

```bash
git init
git add .
git commit -m "PMU bot — version serveur"
git branch -M main
git remote add origin https://github.com/<ton-compte>/<ton-repo>.git
git push -u origin main
```

### 2. Railway

1. Sur [railway.app](https://railway.app), **New Project → Deploy from GitHub repo**, choisis ce repo.
2. Railway détecte automatiquement Python (grâce à `requirements.txt`) et utilise
   `railway.json` / `Procfile` pour lancer `python server.py`.
3. Railway fournit automatiquement la variable d'environnement `PORT` — le
   serveur l'utilise déjà (`os.environ.get("PORT", 8000)`), rien à configurer.
4. Une fois déployé, Railway te donne une URL du style
   `https://ton-app.up.railway.app`. Ouvre-la (sur ton téléphone ou ailleurs) :
   la page se charge et affiche l'état déjà suivi par le serveur.

Il n'y a **aucune variable d'environnement obligatoire** à définir.

### Notes importantes

- Le serveur poll l'API PMU en continu 24h/24 dès qu'il tourne (dès qu'une
  course est trouvée), pas seulement quand quelqu'un regarde la page — c'est
  ce qui permet de fermer l'onglet sans rien perdre. Si tu veux économiser
  des requêtes PMU hors des heures de courses, il faudra ajouter une logique
  de veille (non incluse ici).
- `lancer_pmu_bot.bat` reste utile uniquement pour un usage **local** sur
  Windows (Firefox + serveur local) ; il n'est pas utilisé sur Railway.
- Le fichier `server.py` ne dépend que de la bibliothèque standard Python
  (aucun `pip install` requis).
