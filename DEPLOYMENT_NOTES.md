# Deployment notes

## Recommended setup
- Host the Flask app on Render or Railway.
- Use PostgreSQL in production.
- Store secrets as environment variables.
- Keep the Word template in `template/PUBLISHING_AGREEMENT_CONTRACT.docx` inside the repo.

## Local run
```bash
pip install -r requirements_online.txt
python app_online.py
```

## Render
Build command:
```bash
pip install -r requirements_online.txt
```

Start command:
```bash
gunicorn app_online:app
```

Add these environment variables in Render:
- SECRET_KEY
- DATABASE_URL
- TEAM_USERNAME
- TEAM_PASSWORD
- TEMPLATE_PATH
- OUTPUT_DIR

## Railway
Start command:
```bash
gunicorn app_online:app
```

Add the same environment variables above.

## What this version adds
- Online-ready Flask app
- Login gate for the team
- Writer database
- Writer search/autocomplete
- Auto-save writer info after each contract
- SQLite locally, PostgreSQL-ready in production
