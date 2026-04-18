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
_secret_key = os.getenv("SECRET_KEY")
if not _secret_key:
    import secrets
    _secret_key = secrets.token_hex(32)
    import logging
    logging.warning("SECRET_KEY env var not set — using a random key. Sessions will not persist across restarts.")
app.secret_key = _secret_key
app.config["MAX_CONTENT_LENGTH"] = 300 * 1024 * 1024

def _pg_url(url):
    if not url:
        return url
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    if url.startswith("postgresql://") and "sslmode=" not in url:
        joiner = "&" if "?" in url else "?"
        url = url + joiner + "sslmode=require"
    return url

raw_db_url = _pg_url(os.getenv("DATABASE_URL", "sqlite:///writers.db"))
raw_royalties_url = _pg_url((os.getenv("ROYALTIES_DATABASE_URL") or "").strip())

app.config["SQLALCHEMY_DATABASE_URI"] = raw_db_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
if raw_royalties_url:
    app.config["SQLALCHEMY_BINDS"] = {"royalties": raw_royalties_url}

db.init_app(app)
migrate.init_app(app, db)

# ── Register blueprints ───────────────────────────────────────────────────────

from blueprints.publishing import bp as publishing_bp
from blueprints.releases import bp as releases_bp
from blueprints.api import bp as api_bp
from blueprints.artists import bp as artists_bp
from blueprints.catalog_import import bp as catalog_import_bp
from blueprints.reports import bp as reports_bp
from blueprints.audit import bp as audit_bp
from blueprints.mechanical_audit import bp as mechanical_audit_bp
from blueprints.neighboring_rights_audit import bp as neighboring_rights_audit_bp
from blueprints.users import bp as users_bp
from blueprints.title_review import bp as title_review_bp
from blueprints.registration_report import bp as registration_report_bp
from blueprints.streaming_royalties import bp as streaming_royalties_bp

app.register_blueprint(publishing_bp)
app.register_blueprint(releases_bp)
app.register_blueprint(api_bp)
app.register_blueprint(artists_bp)
app.register_blueprint(catalog_import_bp)
app.register_blueprint(reports_bp)
app.register_blueprint(audit_bp)
app.register_blueprint(mechanical_audit_bp)
app.register_blueprint(neighboring_rights_audit_bp)
app.register_blueprint(users_bp)
app.register_blueprint(title_review_bp)
app.register_blueprint(registration_report_bp)
app.register_blueprint(streaming_royalties_bp)

# ── Context processor ─────────────────────────────────────────────────────────

@app.context_processor
def inject_globals():
    from flask import session
    return {
        "team_auth_enabled":  bool(TEAM_USERNAME and TEAM_PASSWORD),
        "current_role":       session.get("role", ""),
        "current_username":   session.get("username", ""),
        "current_user_id":    session.get("user_id"),
    }

# ── Startup ───────────────────────────────────────────────────────────────────

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(os.path.join(os.path.dirname(__file__), "uploads", "streaming_imports"), exist_ok=True)

app.logger.warning("ENV CHECK: folder=%s json=%s", bool(GOOGLE_DRIVE_FOLDER_ID), bool(GOOGLE_SERVICE_ACCOUNT_JSON))
app.logger.warning("JSON LEN: %s", len(GOOGLE_SERVICE_ACCOUNT_JSON or ""))

# ── Import all models so Flask-Migrate can detect them ───────────────────────

import models  # noqa: F401 — ensures all tables are registered with SQLAlchemy metadata

def _run_artist_backfill():
    """Sync ArtistRelease rows from Release.artists JSON. Idempotent — safe to run on every startup."""
    import json
    from models import Release, Artist, ArtistRelease
    try:
        releases = Release.query.all()
        created_artists = 0
        created_links = 0
        for r in releases:
            names = json.loads(r.artists) if r.artists else []
            for name in names:
                name = name.strip()
                if not name:
                    continue
                artist = Artist.query.filter(
                    db.func.lower(Artist.name) == name.lower()
                ).first()
                if not artist:
                    try:
                        artist = Artist(name=name)
                        db.session.add(artist)
                        db.session.flush()
                        created_artists += 1
                    except Exception:
                        db.session.rollback()
                        artist = Artist.query.filter(
                            db.func.lower(Artist.name) == name.lower()
                        ).first()
                        if not artist:
                            continue
                if not artist:
                    continue
                exists = ArtistRelease.query.filter_by(
                    artist_id=artist.id, release_id=r.id
                ).first()
                if not exists:
                    try:
                        db.session.add(ArtistRelease(artist_id=artist.id, release_id=r.id))
                        db.session.flush()
                        created_links += 1
                    except Exception:
                        db.session.rollback()
        db.session.commit()
        app.logger.warning("ARTIST BACKFILL: +%d artists, +%d links", created_artists, created_links)
    except Exception as e:
        db.session.rollback()
        app.logger.error("ARTIST BACKFILL ERROR: %s", e)


@app.cli.command("backfill-artists")
def backfill_artists_cmd():
    """Backfill ArtistRelease rows from existing Release.artists JSON field."""
    _run_artist_backfill()


with app.app_context():
    _run_artist_backfill()
    # Create royalties DB tables if they don't exist yet
    if raw_royalties_url:
        try:
            db.create_all(bind_key='royalties')
            app.logger.warning("Royalties DB tables ensured.")
        except Exception as _e:
            app.logger.warning("Royalties DB create_all failed: %s", _e)
    # Mark any imports that were mid-flight when the server last restarted
    try:
        import datetime as _dt
        from models import StreamingImport as _SI
        stuck = _SI.query.filter_by(status="processing").all()
        for _s in stuck:
            _s.status        = "error"
            _s.error_message = "Processing was interrupted by a server restart. Please re-upload the file."
            _s.finished_at   = _dt.datetime.utcnow()
        if stuck:
            db.session.commit()
            app.logger.warning("Cleaned up %d stuck streaming imports", len(stuck))
    except Exception:
        pass


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.getenv("PORT", "5052")))
