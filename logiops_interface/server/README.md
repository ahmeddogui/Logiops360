# Logiops360 - Microservice Flask (Auth)

Ce microservice fournit les endpoints d'inscription/connexion pour Logiops360 en se connectant à votre base PostgreSQL existante.

## Endpoints
- POST `/api/auth/signup` — crée un utilisateur (autorise le même email avec des profils différents)
- POST `/api/auth/login` — authentifie un utilisateur sur un type_profil donné
- GET `/api/auth/me` — retourne le profil courant (JWT requis)
- GET `/api/health` — check de santé + connectivité DB

## Modèle de données
Table `public.users` (créée automatiquement si absente) :
- id UUID (PK)
- nom text
- email text
- mot_de_passe_hash text
- type_profil text (valeurs: commande | stockage | transport | superviseur)
- date_creation timestamptz (default now())
- contrainte unique: (email, type_profil)

## Configuration
Variables d'environnement (valeurs par défaut entre parenthèses) :
- `DB_USER` (postgres)
- `DB_PASS` (kdh)
- `DB_HOST` (localhost)
- `DB_PORT` (5432)
- `DB_NAME` (logiops)
- `JWT_SECRET_KEY` (change-me-in-prod)
- `CORS_ORIGIN` (http://localhost:5173)

> Note Docker: si vous lancez le microservice en conteneur et que PostgreSQL tourne sur votre machine hôte, utilisez `DB_HOST=host.docker.internal` (Mac/Windows) ou configurez le réseau Docker sur Linux.

## Lancement en local (sans Docker)
```bash
cd server
python -m venv .venv
source .venv/bin/activate # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
export DB_USER=postgres DB_PASS=kdh DB_HOST=localhost DB_PORT=5432 DB_NAME=logiops
export JWT_SECRET_KEY="votre-cle-secrete"
python app.py  # écoute sur http://localhost:8000
```

## Lancement avec Docker
```bash
cd server
# Si DB locale: export DB_HOST=host.docker.internal
docker build -t logiops360-auth:latest .
docker run --rm -p 8000:8000 \
  -e DB_USER=postgres -e DB_PASS=kdh -e DB_HOST=host.docker.internal -e DB_PORT=5432 -e DB_NAME=logiops \
  -e JWT_SECRET_KEY="votre-cle-secrete" \
  logiops360-auth:latest
```

## Exemples de requêtes
Signup
```bash
curl -X POST http://localhost:8000/api/auth/signup \
  -H "Content-Type: application/json" \
  -d '{
    "nom":"Jean Dupont",
    "email":"jean@example.com",
    "password":"MonSecret123",
    "type_profil":"commande"
  }'
```

Login
```bash
curl -X POST http://localhost:8000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "email":"jean@example.com",
    "password":"MonSecret123",
    "type_profil":"commande"
  }'
```

Me
```bash
TOKEN=... # récupéré depuis /login ou /signup
curl http://localhost:8000/api/auth/me -H "Authorization: Bearer $TOKEN"
```

## Intégration Frontend
- Configurez `BASE_URL` côté frontend: `http://localhost:8000`
- À la création de compte (formulaire): POST `/api/auth/signup` avec nom, email, password, type_profil
- À la connexion: POST `/api/auth/login` avec email, password, type_profil
- Stockez le token JWT (localStorage) et envoyez `Authorization: Bearer <token>` pour les routes protégées

Sécurité: ne commitez jamais une vraie `JWT_SECRET_KEY`. Utilisez des variables d'environnement en production.
