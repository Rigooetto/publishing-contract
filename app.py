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
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {"pool_pre_ping": True, "pool_recycle": 1800, "pool_size": 5, "max_overflow": 10}
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
        # Widen artist_name_map columns if they were created as VARCHAR(255)
        try:
            from sqlalchemy import text as _text
            _roy_engine = db.engines.get('royalties')
            if _roy_engine:
                with _roy_engine.connect() as _c:
                    _c.execute(_text("ALTER TABLE artist_name_map ALTER COLUMN raw_name TYPE TEXT"))
                    _c.execute(_text("ALTER TABLE artist_name_map ALTER COLUMN canonical_name TYPE TEXT"))
                    _c.commit()
        except Exception:
            pass  # already TEXT, or table doesn't exist yet — both fine
        # Create dashboard_cache table
        try:
            from sqlalchemy import text as _text
            _roy_engine = db.engines.get('royalties')
            if _roy_engine:
                with _roy_engine.connect() as _c:
                    _c.execute(_text("""
                        CREATE TABLE IF NOT EXISTS dashboard_cache (
                            cache_key TEXT PRIMARY KEY,
                            data_json TEXT NOT NULL,
                            computed_at TIMESTAMP NOT NULL
                        )
                    """))
                    _c.commit()
        except Exception:
            pass
        # Create royalty_summary table (pre-aggregated for fast dashboard queries)
        try:
            from sqlalchemy import text as _text
            _roy_engine = db.engines.get('royalties')
            if _roy_engine:
                with _roy_engine.connect() as _c:
                    _c.execute(_text("""
                        CREATE TABLE IF NOT EXISTS royalty_summary (
                            reporting_month  DATE          NOT NULL,
                            isrc             TEXT          NOT NULL,
                            artist_name_csv  TEXT,
                            platform         TEXT,
                            country          TEXT,
                            track_title_csv  TEXT,
                            streams          BIGINT        DEFAULT 0,
                            net_revenue      NUMERIC(16,6) DEFAULT 0,
                            PRIMARY KEY (reporting_month, isrc, platform, country)
                        )
                    """))
                    _c.execute(_text("CREATE INDEX IF NOT EXISTS ix_rs_month    ON royalty_summary (reporting_month)"))
                    _c.execute(_text("CREATE INDEX IF NOT EXISTS ix_rs_artist   ON royalty_summary (artist_name_csv)"))
                    _c.execute(_text("CREATE INDEX IF NOT EXISTS ix_rs_platform ON royalty_summary (platform)"))
                    _c.execute(_text("CREATE INDEX IF NOT EXISTS ix_rs_country  ON royalty_summary (country)"))
                    # Trigram index for fast ILIKE artist filtering
                    _c.execute(_text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
                    _c.execute(_text("CREATE INDEX IF NOT EXISTS ix_rs_artist_trgm ON royalty_summary USING gin (artist_name_csv gin_trgm_ops)"))
                    # One-time backfill — skip if table already has data to avoid blocking startup
                    _rs_count = _c.execute(_text("SELECT COUNT(*) FROM royalty_summary")).fetchone()[0]
                    if _rs_count == 0:
                        _c.execute(_text("""
                            INSERT INTO royalty_summary
                                (reporting_month, isrc, artist_name_csv, platform, country,
                                 track_title_csv, streams, net_revenue)
                            SELECT reporting_month, isrc, MAX(artist_name_csv), platform, country,
                                   MAX(track_title_csv), SUM(total_quantity), SUM(total_net_revenue)
                              FROM streaming_royalty
                             GROUP BY reporting_month, isrc, platform, country
                            ON CONFLICT (reporting_month, isrc, platform, country) DO NOTHING
                        """))
                        _c.commit()
                        app.logger.warning("royalty_summary table ensured and backfilled.")
                    else:
                        app.logger.warning("royalty_summary table ensured (%d rows, skipped backfill).", _rs_count)
        except Exception as _rse:
            app.logger.warning("royalty_summary setup failed: %s", _rse)
    # Mark any imports that were mid-flight when the server last restarted
    try:
        import datetime as _dt
        from sqlalchemy import text as _text
        _cleanup_engine = db.engines.get('royalties') or db.engine
        with _cleanup_engine.connect() as _cc:
            result = _cc.execute(_text("""
                UPDATE streaming_import
                   SET status='error',
                       error_message='Processing was interrupted by a server restart. Please re-upload the file.',
                       finished_at=:t
                 WHERE status='processing'
            """), {"t": _dt.datetime.utcnow()})
            _cc.commit()
            app.logger.warning("Cleaned up %d stuck streaming imports", result.rowcount)
    except Exception as _ce:
        app.logger.warning("Stuck import cleanup failed: %s", _ce)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.getenv("PORT", "5052")))
