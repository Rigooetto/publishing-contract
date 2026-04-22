# Publishing Agreement App - Online Deployment Package

This package is prepared so you can deploy your Flask contract generator for your team.

## Included
- `app.py` - online-ready Flask app
- `requirements.txt` - Python dependencies
- `Procfile` - production start command
- `.env.example` - environment variable template
- `DEPLOYMENT_NOTES.md` - short deployment notes

## Important
You still need to add your Word contract template to this exact path in the repo:

`template/PUBLISHING_AGREEMENT_CONTRACT.docx`

## What this version supports
- Team login with username/password
- Writer database
- Writer autocomplete
- Auto-save of writer info for future contracts
- SQLite for local testing
- PostgreSQL for production

## Recommended deployment
Use:
- Render for the web app
- PostgreSQL for the database

## Quick local test
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

## Environment variables
Copy `.env.example` and set real values:
- `SECRET_KEY`
- `DATABASE_URL`
- `TEAM_USERNAME`
- `TEAM_PASSWORD`
- `TEMPLATE_PATH`
- `OUTPUT_DIR`

## Production start command
Gunicorn is already included:
```bash
gunicorn app:app
```

## Database
For local testing:
```env
DATABASE_URL=sqlite:///writers.db
```

For production, use your managed PostgreSQL connection string.

## Suggested repo structure
```text
your-repo/
  app.py
  requirements.txt
  Procfile
  .env.example
  README.md
  template/
    PUBLISHING_AGREEMENT_CONTRACT.docx
```
