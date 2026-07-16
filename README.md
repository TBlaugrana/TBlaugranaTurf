# TBlaugrana BOT — version Railway (en ligne)

## Ce qui a changé par rapport à la version Render

- **Screen de référence à 0min (départ) au lieu de H-3min** : le "screen" des cotes de référence (colonne **0MIN** dans l'interface, ex-"3MIN") est maintenant pris pile au moment du départ de la course, et non plus 3 minutes avant. Les chutes affichées (colonne "Var.", classement, alertes) sont donc calculées entre la cote au départ et la cote live.
- **Seuil d'alerte abaissé à 15%** (au lieu de 30%) : une alerte se déclenche désormais dès qu'un cheval perd 15% ou plus de cote après le départ, dans la plage de cotes surveillée.
- **Masquage des grosses cotes en live** : les chevaux dont la cote live dépasse **12** sont désormais masqués du tableau principal (au lieu de 10 précédemment sur le filtre serveur / 20 sur le filtre client — les deux sont maintenant alignés sur 12).
- **Hébergement basculé de Render vers Railway** : plus de `render.yaml`, remplacé par `railway.json`. Le code lit toujours le port via la variable d'environnement `PORT`, fournie automatiquement par Railway.

## Ce qui reste identique

- Telegram reste supprimé (pas de token, pas de chat IDs, pas de notifications externes).
- Pas de bouton "Fermer le serveur" ni d'auto-fermeture à la fermeture de l'onglet : le serveur tourne en continu côté Railway, indépendamment de tes connexions.
- **URL d'API relative** dans `index.html` (`const API = ""`) : le front et le serveur sont sur le même domaine Railway.
- La logique de scraping PMU (cotes, détection de chute).
- L'interface (mêmes tableaux, mêmes alertes visuelles dans le panneau).
- Le state est en mémoire — tant que le service Railway ne redémarre pas, tout est conservé.

## Déployer sur Railway

1. Crée un repo GitHub (public ou privé) et mets-y les 4 fichiers : `server.py`, `index.html`, `requirements.txt`, `railway.json`.
2. Va sur [railway.app](https://railway.app), crée un compte.
3. Dashboard → **New Project** → **Deploy from GitHub repo** → connecte ton repo GitHub.
4. Railway détecte `requirements.txt` (Python) et lit `railway.json` pour la commande de démarrage (`python -u server.py`). Aucune configuration manuelle nécessaire.
5. Dans l'onglet **Settings** du service, section **Networking**, clique sur **Generate Domain** pour obtenir une URL publique du type `https://tblaugrana-bot-production.up.railway.app`.
6. Attends la fin du build (généralement moins d'une minute). C'est cette URL que tu ouvres sur ton tel ou ton PC, à la place de `localhost:8765`.

### Variables d'environnement

Railway fournit automatiquement `PORT` — rien à configurer de ton côté. Si tu veux forcer une région ou un plan spécifique, cela se fait depuis les Settings du service dans le dashboard Railway.

## À savoir sur Railway

- **Pas de veille automatique par défaut** : contrairement à Render free, un service Railway déployé sur un plan payant (Hobby, ~5$/mois de crédit inclus au moment de la rédaction) tourne en continu sans s'endormir après 15 minutes d'inactivité — donc pas de "cold start" à chaque réveil. Si tu es sur l'essai gratuit (Trial), des limites de crédit et éventuellement une mise en veille peuvent s'appliquer : vérifie les conditions actuelles sur [railway.app/pricing](https://railway.app/pricing), qui peuvent avoir changé depuis la rédaction de ce README.
- **Facturation à l'usage** : Railway facture au temps de calcul / RAM consommés plutôt que par un "plan gratuit" permanent comme Render. Un bot léger comme celui-ci (scraping toutes les secondes, faible RAM) reste généralement peu coûteux, mais surveille ta consommation dans le dashboard.
- **Stockage** : tout est en mémoire RAM du process, comme avant. Si le service redémarre (déploiement, crash), tout l'état (cotes, screen 0min, course sélectionnée) repart à zéro.

## Sécurité

L'URL Railway sera publique sur Internet. N'importe qui connaissant l'URL peut consulter le bot et changer la course sélectionnée (ça affecterait ce que voient tous les appareils connectés, y compris toi). Si c'est gênant, tu peux ajouter un mot de passe simple dans le code — dis-le-moi si tu veux que je l'ajoute.
