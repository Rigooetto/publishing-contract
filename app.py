import os

from flask import Flask
from flask_compress import Compress

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
Compress(app)
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
_pool_opts = {"pool_pre_ping": True, "pool_recycle": 1800, "pool_size": 3, "max_overflow": 5}
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = _pool_opts
if raw_royalties_url:
    app.config["SQLALCHEMY_BINDS"] = {
        "royalties": {"url": raw_royalties_url, **_pool_opts}
    }

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
    from sqlalchemy import text as _t2
    try:
        # Skip if schema is mid-migration (royalty_percentage column not yet added)
        with db.engine.connect() as _chk:
            cols = [r[1] for r in _chk.execute(_t2("PRAGMA table_info(artist_release)")).fetchall()] if db.engine.dialect.name == 'sqlite' else [r[0] for r in _chk.execute(_t2("SELECT column_name FROM information_schema.columns WHERE table_name='artist_release'")).fetchall()]
            if 'royalty_percentage' not in cols:
                app.logger.warning("ARTIST BACKFILL: skipped — schema migration pending")
                return
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
                    _c.execute(_text("SET lock_timeout = '3s'"))
                    _c.execute(_text("ALTER TABLE artist_name_map ALTER COLUMN raw_name TYPE TEXT"))
                    _c.execute(_text("ALTER TABLE artist_name_map ALTER COLUMN canonical_name TYPE TEXT"))
                    _c.commit()
        except Exception:
            pass  # already TEXT, or table doesn't exist yet — both fine
        # Add confidence + status columns to artist_name_map (idempotent)
        try:
            from sqlalchemy import text as _text
            _roy_engine = db.engines.get('royalties')
            if _roy_engine:
                with _roy_engine.connect() as _c:
                    _c.execute(_text("SET lock_timeout = '3s'"))
                    _c.execute(_text("ALTER TABLE artist_name_map ADD COLUMN IF NOT EXISTS confidence NUMERIC(4,3)"))
                    _c.execute(_text("ALTER TABLE artist_name_map ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'confirmed'"))
                    _c.commit()
        except Exception:
            pass
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
        # All heavy DDL + recovery runs in a background thread so gunicorn binds immediately.
        # dashboard_cache already created above (needed by requests); everything else is async.
        _startup_app = app
        _startup_engine = db.engines.get('royalties')
        if _startup_engine:
            _startup_url = _startup_engine.url.render_as_string(hide_password=False)

            def _run_startup_bg():
                from sqlalchemy import create_engine as _ce_bg, text as _t_bg
                from sqlalchemy.pool import NullPool
                _eng = _ce_bg(_startup_url, poolclass=NullPool)
                try:
                    # royalty_summary: table + B-tree indexes + optional GIN trigram index.
                    # GIN creation on 6M rows can take 90s on first run — fine in background.
                    with _eng.connect() as _c:
                        _c.execute(_t_bg("""
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
                        _c.execute(_t_bg("CREATE INDEX IF NOT EXISTS ix_rs_month    ON royalty_summary (reporting_month)"))
                        _c.execute(_t_bg("CREATE INDEX IF NOT EXISTS ix_rs_artist   ON royalty_summary (artist_name_csv)"))
                        _c.execute(_t_bg("CREATE INDEX IF NOT EXISTS ix_rs_isrc     ON royalty_summary (isrc)"))
                        _c.execute(_t_bg("CREATE INDEX IF NOT EXISTS ix_rs_platform ON royalty_summary (platform)"))
                        _c.execute(_t_bg("CREATE INDEX IF NOT EXISTS ix_rs_country  ON royalty_summary (country)"))
                        try:
                            _c.execute(_t_bg("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
                            _c.execute(_t_bg("CREATE INDEX IF NOT EXISTS ix_rs_artist_trgm ON royalty_summary USING gin (artist_name_csv gin_trgm_ops)"))
                        except Exception:
                            pass  # trgm index is optional; skip on lock conflict
                        _c.commit()
                        _rs_empty = not _c.execute(_t_bg("SELECT 1 FROM royalty_summary LIMIT 1")).fetchone()
                    _startup_app.logger.warning("royalty_summary DDL complete (empty=%s).", _rs_empty)

                    # artist_royalty_detail: table + indexes
                    with _eng.connect() as _c:
                        _c.execute(_t_bg("""
                            CREATE TABLE IF NOT EXISTS artist_royalty_detail (
                                id              BIGSERIAL PRIMARY KEY,
                                artist_name     TEXT          NOT NULL,
                                reporting_month DATE          NOT NULL,
                                isrc            TEXT          NOT NULL,
                                track_title     TEXT,
                                platform        TEXT,
                                country         TEXT,
                                streams         BIGINT        DEFAULT 0,
                                net_revenue     NUMERIC(16,6) DEFAULT 0
                            )
                        """))
                        _c.execute(_t_bg("""
                            CREATE UNIQUE INDEX IF NOT EXISTS ix_ard_natural
                                ON artist_royalty_detail (artist_name, reporting_month, isrc, platform, country)
                                NULLS NOT DISTINCT
                        """))
                        _c.execute(_t_bg("CREATE INDEX IF NOT EXISTS ix_ard_artist ON artist_royalty_detail (artist_name)"))
                        _c.execute(_t_bg("CREATE INDEX IF NOT EXISTS ix_ard_month  ON artist_royalty_detail (reporting_month)"))
                        _c.commit()
                        _ard_empty = not _c.execute(_t_bg("SELECT 1 FROM artist_royalty_detail LIMIT 1")).fetchone()
                    _startup_app.logger.warning("artist_royalty_detail DDL complete (empty=%s).", _ard_empty)

                    # Recovery sentinel check
                    _already_done = False
                    try:
                        with _eng.connect() as _c:
                            _already_done = bool(_c.execute(_t_bg(
                                "SELECT 1 FROM dashboard_cache WHERE cache_key = '_recovery_v2'"
                            )).fetchone())
                    except Exception:
                        pass

                    if not _already_done:
                        _startup_app.logger.warning("DATA RECOVERY: starting rebuild.")
                        with _eng.connect() as _c:
                            _c.execute(_t_bg("DELETE FROM artist_name_map"))
                            _c.execute(_t_bg("TRUNCATE royalty_summary"))
                            _c.execute(_t_bg("DELETE FROM dashboard_cache WHERE cache_key != '_recovery_v2'"))
                            _c.commit()
                        with _eng.connect() as _c:
                            _c.execute(_t_bg("""
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
                        with _eng.connect() as _c:
                            _c.execute(_t_bg("""
                                INSERT INTO dashboard_cache (cache_key, data_json, computed_at)
                                VALUES ('_recovery_v2', '{}', NOW())
                                ON CONFLICT (cache_key) DO NOTHING
                            """))
                            _c.commit()
                        _ard_empty = True
                        _startup_app.logger.warning("DATA RECOVERY: royalty_summary rebuilt.")
                    else:
                        _startup_app.logger.warning("DATA RECOVERY: sentinel found, skipping rebuild.")

                    if _ard_empty:
                        _startup_app.logger.warning("ARD build: starting.")
                        from blueprints.streaming_royalties import _rebuild_artist_detail as _rad_bg
                        _rad_bg(_eng)
                        _startup_app.logger.warning("ARD build: complete.")
                except Exception as _bg_e:
                    _startup_app.logger.warning("Startup bg thread failed: %s", _bg_e)
                finally:
                    _eng.dispose()

            import threading as _startup_threading
            _startup_threading.Thread(target=_run_startup_bg, daemon=True).start()
            app.logger.warning("Startup: background DDL+recovery thread started, port binding now.")
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
