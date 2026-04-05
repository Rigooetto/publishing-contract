import os

from flask import Flask

from extensions import db, migrate
from config import (
    TEAM_USERNAME, TEAM_PASSWORD,
    GOOGLE_DRIVE_FOLDER_ID, GOOGLE_SERVICE_ACCOUNT_JSON,
    OUTPUT_DIR,
)

# ── App factory ───────────────────────────────────────────────────────────────

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "change-this-secret-key")
app.config["MAX_CONTENT_LENGTH"] = 100 * 1024 * 1024

raw_db_url = os.getenv("DATABASE_URL", "sqlite:///writers.db")
if raw_db_url.startswith("postgres://"):
    raw_db_url = raw_db_url.replace("postgres://", "postgresql://", 1)
if raw_db_url.startswith("postgresql://") and "sslmode=" not in raw_db_url:
    joiner = "&" if "?" in raw_db_url else "?"
    raw_db_url = raw_db_url + joiner + "sslmode=require"

app.config["SQLALCHEMY_DATABASE_URI"] = raw_db_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db.init_app(app)
migrate.init_app(app, db)

# ── Register blueprints ───────────────────────────────────────────────────────

from blueprints.publishing import bp as publishing_bp
from blueprints.releases import bp as releases_bp
from blueprints.api import bp as api_bp

app.register_blueprint(publishing_bp)
app.register_blueprint(releases_bp)
app.register_blueprint(api_bp)

# ── Context processor ─────────────────────────────────────────────────────────

@app.context_processor
def inject_globals():
    return {"team_auth_enabled": bool(TEAM_USERNAME and TEAM_PASSWORD)}

# ── Startup ───────────────────────────────────────────────────────────────────

os.makedirs(OUTPUT_DIR, exist_ok=True)

app.logger.warning("ENV CHECK: folder=%s json=%s", bool(GOOGLE_DRIVE_FOLDER_ID), bool(GOOGLE_SERVICE_ACCOUNT_JSON))
app.logger.warning("JSON LEN: %s", len(GOOGLE_SERVICE_ACCOUNT_JSON or ""))

# ── Import all models so Flask-Migrate can detect them ───────────────────────

import models  # noqa: F401 — ensures all tables are registered with SQLAlchemy metadata

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.getenv("PORT", "5052")))
