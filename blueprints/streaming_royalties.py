"""
Streaming Royalties module — ingest Believe monthly CSVs, aggregate by
(isrc, platform, country, sales_type, reporting_month, sales_month), and
serve a Power BI-style dashboard with Label View and Artist View.
"""
import csv
import datetime
import decimal
import difflib
import io
import json
import os
import queue as _queue_mod
import re
import threading
import time as _time
import unicodedata

_dash_cache: dict = {}
_CACHE_TTL = 600  # seconds

_prewarm_status: dict = {"running": False, "done": 0, "total": 0, "current_artist": ""}

from flask import (
    Blueprint, render_template_string, request, redirect, url_for,
    flash, jsonify, current_app, session, Response, stream_with_context,
)
from markupsafe import Markup
from werkzeug.utils import secure_filename

from extensions import db
from utils import auth_required, role_required

_ADMIN_ONLY = {"admin"}

bp = Blueprint("streaming_royalties", __name__)

@bp.record_once
def _startup_prewarm(state):
    """Warm missing cache entries in the background on every deploy/restart."""
    app = state.app
    def _run():
        with app.app_context():
            _prewarm_dashboard_cache()
    threading.Thread(target=_run, daemon=True).start()

_UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "..", "uploads", "streaming_imports")


# ── Background processing ─────────────────────────────────────────────────────

def _parse_date(val):
    """Parse '2026/02/01' or '2026-02-01' to a date, returning None on failure."""
    val = val.strip().strip('"')
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.datetime.strptime(val, fmt).date()
        except ValueError:
            pass
    return None


def _parse_decimal(val):
    """Parse a decimal string tolerating thousand-separator commas (1,234.56)
    and European format (1.234,56).  Returns Decimal(0) for blank/unparseable."""
    val = val.strip().strip('"').replace('\xa0', '').replace(' ', '')
    if not val:
        return decimal.Decimal(0)
    if re.match(r'^-?\d{1,3}(\.\d{3})+(,\d+)?$', val):
        val = val.replace('.', '').replace(',', '.')
    else:
        val = val.replace(',', '')
    try:
        return decimal.Decimal(val)
    except Exception:
        return decimal.Decimal(0)


def _isrc_to_track_map(isrc_set, main_engine=None):
    """Return {isrc: track_id}.  Uses main_engine if provided (background thread),
    otherwise falls back to db.session (request context)."""
    from sqlalchemy import text as _t
    if main_engine is not None:
        with main_engine.connect() as conn:
            rows = conn.execute(
                _t("SELECT isrc, id FROM track WHERE isrc = ANY(:isrcs)"),
                {"isrcs": list(isrc_set)}
            ).fetchall()
        return {r[0]: r[1] for r in rows}
    from models import Track
    from sqlalchemy import select
    rows = db.session.execute(
        select(Track.isrc, Track.id).where(Track.isrc.in_(list(isrc_set)))
    ).fetchall()
    db.session.expunge_all()
    return {r[0]: r[1] for r in rows}


def _save_progress(rec, rows_read, rows_skipped, rows_aggregated, royalties_engine=None,
                   reporting_month=None):
    """Persist import progress.  streaming_import is in the royalties DB."""
    from sqlalchemy import text as _t
    if royalties_engine is not None:
        sql = """
            UPDATE streaming_import
               SET rows_read=:r, rows_skipped=:s, rows_aggregated=:a
                   {rm}
             WHERE id=:id
        """.format(rm=", reporting_month=:rm" if reporting_month else "")
        with royalties_engine.connect() as conn:
            conn.execute(_t(sql), {
                "r": rows_read, "s": rows_skipped, "a": rows_aggregated,
                "id": rec.id,
                **({"rm": reporting_month} if reporting_month else {}),
            })
            conn.commit()
    else:
        rec.rows_read       = rows_read
        rec.rows_skipped    = rows_skipped
        rec.rows_aggregated = rows_aggregated
        if reporting_month:
            rec.reporting_month = reporting_month
        db.session.commit()


def _flush_agg(rec, agg, meta, track_map, rows_aggregated_total, royalties_engine_=None):
    """UPSERT the current agg dict into StreamingRoyalty via the royalties engine directly."""
    from sqlalchemy import text as _t

    now = datetime.datetime.utcnow()
    rows = []
    for key, vals in agg.items():
        isrc, platform, country, sales_type, rep_iso, sal_iso = key
        m = meta[key]
        rows.append((
            rec.id, isrc, platform, country, sales_type,
            datetime.date.fromisoformat(rep_iso),
            datetime.date.fromisoformat(sal_iso),
            m["artist_name_csv"], m["track_title_csv"], m["label_name"],
            m["release_title"], m["upc"], m["streaming_sub_type"],
            m["release_type"], m["currency"],
            vals["qty"], vals["gross"], vals["net"], vals["mech"],
            track_map.get(isrc), now,
        ))

    _engine = royalties_engine_ if royalties_engine_ is not None else _royalties_engine()
    UPSERT = """
        INSERT INTO streaming_royalty (
            import_id, isrc, platform, country, sales_type,
            reporting_month, sales_month,
            artist_name_csv, track_title_csv, label_name, release_title,
            upc, streaming_sub_type, release_type, currency,
            total_quantity, total_gross_revenue, total_net_revenue, total_mechanical_fee,
            track_id, created_at
        ) VALUES (
            :import_id, :isrc, :platform, :country, :sales_type,
            :reporting_month, :sales_month,
            :artist_name_csv, :track_title_csv, :label_name, :release_title,
            :upc, :streaming_sub_type, :release_type, :currency,
            :total_quantity, :total_gross_revenue, :total_net_revenue, :total_mechanical_fee,
            :track_id, :created_at
        )
        ON CONFLICT ON CONSTRAINT uq_streaming_royalty_agg_key DO UPDATE SET
            total_quantity       = streaming_royalty.total_quantity       + EXCLUDED.total_quantity,
            total_gross_revenue  = streaming_royalty.total_gross_revenue  + EXCLUDED.total_gross_revenue,
            total_net_revenue    = streaming_royalty.total_net_revenue    + EXCLUDED.total_net_revenue,
            total_mechanical_fee = streaming_royalty.total_mechanical_fee + EXCLUDED.total_mechanical_fee
    """
    keys = [
        "import_id","isrc","platform","country","sales_type",
        "reporting_month","sales_month",
        "artist_name_csv","track_title_csv","label_name","release_title",
        "upc","streaming_sub_type","release_type","currency",
        "total_quantity","total_gross_revenue","total_net_revenue","total_mechanical_fee",
        "track_id","created_at",
    ]
    with _engine.connect() as conn:
        for i in range(0, len(rows), 500):
            chunk = [dict(zip(keys, r)) for r in rows[i:i + 500]]
            conn.execute(_t(UPSERT), chunk)
        conn.commit()

    return rows_aggregated_total + len(agg)


def _aggregate_and_store(rec, main_engine=None, royalties_engine_=None, progress_callback=None):
    """Stream-parse the CSV and flush aggregated rows to the DB in bounded-memory chunks.
    main_engine / royalties_engine_ are passed by the background thread so it uses
    its own independent connections instead of the shared gunicorn pool."""
    from models import StreamingRoyalty

    agg = {}       # key → dict of accumulated values (cleared every FLUSH_EVERY unique keys)
    meta = {}      # key → snapshot fields (first row wins)
    rows_read = 0
    rows_skipped = 0
    rows_aggregated_total = 0
    FLUSH_EVERY = 10_000  # unique agg keys before flushing to DB to bound memory

    # Detect delimiter from first line (Believe uses ";" but guard against ",")
    with open(rec.file_path, encoding="utf-8-sig", errors="replace") as _peek:
        first_line = _peek.readline()
    delimiter = ";" if first_line.count(";") >= first_line.count(",") else ","

    with open(rec.file_path, encoding="utf-8-sig", errors="replace", newline="") as fh:
        reader = csv.reader(fh, delimiter=delimiter, quotechar='"')
        raw_header = next(reader, None)
        if raw_header is None:
            raise ValueError("CSV file is empty or missing header row")
        header = [h.strip().strip('"') for h in raw_header]

        # Case-insensitive column lookup with common aliases
        _col_aliases = {
            "isrc":                  ["isrc"],
            "platform":              ["platform"],
            "country":               ["country / region", "country/region", "country"],
            "sales_type":            ["sales type", "salestype", "sale type"],
            "reporting_month":       ["reporting month", "reporting_month", "report month"],
            "sales_month":           ["sales month", "sales_month", "sale month"],
            "quantity":              ["quantity", "qty", "units"],
            "gross_revenue":         ["gross revenue", "gross_revenue", "gross"],
            "net_revenue":           ["net revenue", "net_revenue", "net"],
            "mechanical_fee":        ["mechanical fee", "mechanical_fee", "mechanical"],
            "artist_name":           ["artist name", "artist_name", "artist"],
            "track_title":           ["track title", "track_title", "track name", "title"],
            "label_name":            ["label name", "label_name", "label"],
            "release_title":         ["release title", "release_title", "release name", "album"],
            "upc":                   ["upc"],
            "streaming_sub_type":    ["streaming subscription type", "streaming_subscription_type", "subscription type"],
            "release_type":          ["release type", "release_type"],
            "currency":              ["client payment currency", "currency", "payment currency"],
        }
        header_lower = [h.lower() for h in header]
        col = {}
        for field, aliases in _col_aliases.items():
            for alias in aliases:
                if alias in header_lower:
                    col[field] = header_lower.index(alias)
                    break

        required = {"isrc", "platform", "country", "sales_type",
                    "reporting_month", "sales_month", "quantity",
                    "gross_revenue", "net_revenue", "mechanical_fee"}
        missing = required - set(col.keys())
        if missing:
            raise ValueError(
                f"CSV missing required columns: {missing}. "
                f"Headers found: {header[:20]}"
            )

        reporting_month_seen = None

        for raw_row in reader:
            rows_read += 1
            try:
                isrc = raw_row[col["isrc"]].strip().strip('"').upper()
                if not isrc:
                    upc = raw_row[col["upc"]].strip().strip('"') if "upc" in col else ""
                    if upc:
                        isrc = f"UPC:{upc}"
                    else:
                        rows_skipped += 1
                        continue

                rep_month = _parse_date(raw_row[col["reporting_month"]])
                sal_month = _parse_date(raw_row[col["sales_month"]])
                if rep_month is None or sal_month is None:
                    rows_skipped += 1
                    continue

                if reporting_month_seen is None:
                    reporting_month_seen = rep_month

                platform   = raw_row[col["platform"]].strip().strip('"')
                country    = raw_row[col["country"]].strip().strip('"')
                sales_type = raw_row[col["sales_type"]].strip().strip('"')

                qty   = int(_parse_decimal(raw_row[col["quantity"]].strip()))
                gross = _parse_decimal(raw_row[col["gross_revenue"]].strip())
                net   = _parse_decimal(raw_row[col["net_revenue"]].strip())
                mech  = _parse_decimal(raw_row[col["mechanical_fee"]].strip())

                key = (isrc, platform, country, sales_type,
                       rep_month.isoformat(), sal_month.isoformat())

                if key not in agg:
                    agg[key] = {"qty": 0, "gross": decimal.Decimal(0),
                                "net": decimal.Decimal(0), "mech": decimal.Decimal(0)}
                    meta[key] = {
                        "artist_name_csv":    raw_row[col["artist_name"]].strip().strip('"') if "artist_name" in col else "",
                        "track_title_csv":    raw_row[col["track_title"]].strip().strip('"') if "track_title" in col else "",
                        "label_name":         raw_row[col["label_name"]].strip().strip('"') if "label_name" in col else "",
                        "release_title":      raw_row[col["release_title"]].strip().strip('"') if "release_title" in col else "",
                        "upc":                raw_row[col["upc"]].strip().strip('"') if "upc" in col else "",
                        "streaming_sub_type": raw_row[col["streaming_sub_type"]].strip().strip('"') if "streaming_sub_type" in col else "",
                        "release_type":       raw_row[col["release_type"]].strip().strip('"') if "release_type" in col else "",
                        "currency":           raw_row[col["currency"]].strip().strip('"') if "currency" in col else "EUR",
                    }

                agg[key]["qty"]   += qty
                agg[key]["gross"] += gross
                agg[key]["net"]   += net
                agg[key]["mech"]  += mech

            except Exception:
                rows_skipped += 1
                continue

            # Flush and clear agg dict when it reaches FLUSH_EVERY unique keys
            if len(agg) >= FLUSH_EVERY:
                isrc_set = {k[0] for k in agg}
                track_map = _isrc_to_track_map(isrc_set, main_engine)
                rows_aggregated_total = _flush_agg(rec, agg, meta, track_map,
                                                    rows_aggregated_total, royalties_engine_)
                agg.clear()
                meta.clear()
                _save_progress(rec, rows_read, rows_skipped, rows_aggregated_total, royalties_engine_)
                if progress_callback:
                    progress_callback(rows_read, rows_skipped, rows_aggregated_total)

            if rows_read % 25_000 == 0:
                _save_progress(rec, rows_read, rows_skipped, rows_aggregated_total, royalties_engine_)
                if progress_callback:
                    progress_callback(rows_read, rows_skipped, rows_aggregated_total)

    # Final flush for whatever remains in agg
    if agg:
        isrc_set = {k[0] for k in agg}
        track_map = _isrc_to_track_map(isrc_set, main_engine)
        rows_aggregated_total = _flush_agg(rec, agg, meta, track_map,
                                            rows_aggregated_total, royalties_engine_)

    _save_progress(rec, rows_read, rows_skipped, rows_aggregated_total, royalties_engine_,
                   reporting_month=reporting_month_seen)


def _process_import(app, import_id):
    """Background thread: process one StreamingImport record.
    Creates its own NullPool engines so it never contends with gunicorn's connection pool."""
    from sqlalchemy import create_engine, text as _t
    from sqlalchemy.pool import NullPool

    with app.app_context():
        main_url      = app.config.get("SQLALCHEMY_DATABASE_URI", "")
        royalties_url = (app.config.get("SQLALCHEMY_BINDS") or {}).get("royalties", "")

    def _pg(url):
        if url and url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://", 1)
        if url and "sslmode=" not in url and url.startswith("postgresql://"):
            url += ("&" if "?" in url else "?") + "sslmode=require"
        return url

    m_engine = create_engine(_pg(main_url),      poolclass=NullPool) if main_url      else None
    r_engine = create_engine(_pg(royalties_url), poolclass=NullPool) if royalties_url else None

    # streaming_import and streaming_royalty both live in the royalties DB.
    # m_engine is only needed for the track ISRC lookup.
    def _update_status(status, error=None):
        if r_engine is None:
            return
        with r_engine.connect() as conn:
            if error:
                conn.execute(_t("""
                    UPDATE streaming_import
                       SET status=:s, finished_at=:f, error_message=:e
                     WHERE id=:id
                """), {"s": status, "f": datetime.datetime.utcnow(), "e": str(error)[:2000], "id": import_id})
            else:
                conn.execute(_t("""
                    UPDATE streaming_import SET status=:s, finished_at=:f WHERE id=:id
                """), {"s": status, "f": datetime.datetime.utcnow(), "id": import_id})
            conn.commit()

    try:
        # Mark processing + get file_path (all in royalties DB)
        file_path = None
        with r_engine.connect() as conn:
            conn.execute(_t("""
                UPDATE streaming_import SET status='processing', started_at=:t WHERE id=:id
            """), {"t": datetime.datetime.utcnow(), "id": import_id})
            conn.commit()
            row = conn.execute(_t("SELECT file_path FROM streaming_import WHERE id=:id"),
                               {"id": import_id}).fetchone()
            if row:
                file_path = row[0]

        if not file_path:
            _update_status("error", "Import record not found")
            return

        # Build a lightweight rec-like object the helpers can use
        class _Rec:
            id = import_id
            file_path = None

        _rec = _Rec()
        _rec.file_path = file_path

        _aggregate_and_store(_rec, main_engine=m_engine, royalties_engine_=r_engine)
        _update_status("done")
        try:
            os.remove(file_path)
        except OSError:
            pass

    except Exception as e:
        _update_status("error", e)
    finally:
        if m_engine:
            m_engine.dispose()
        if r_engine:
            r_engine.dispose()


def _process_bulk(app, import_ids):
    """Background thread: process a list of imports sequentially."""
    for imp_id in import_ids:
        _process_import(app, imp_id)


def _process_import_sse(import_id, main_url, royalties_url, progress_q):
    """Worker thread for SSE streaming imports. Puts progress dicts in progress_q.
    The SSE route keeps an in-flight HTTP response alive while this runs, so
    gunicorn's graceful shutdown waits for it instead of killing it immediately."""
    from sqlalchemy import create_engine, text as _t
    from sqlalchemy.pool import NullPool

    def _pg(url):
        if url and url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://", 1)
        if url and "sslmode=" not in url and url.startswith("postgresql://"):
            url += ("&" if "?" in url else "?") + "sslmode=require"
        return url

    r_engine = create_engine(_pg(royalties_url), poolclass=NullPool) if royalties_url else None
    m_engine = create_engine(_pg(main_url),      poolclass=NullPool) if main_url      else None

    def _emit(data):
        progress_q.put(data)

    try:
        if r_engine is None:
            _emit({"status": "error", "error_message": "No royalties DB configured"})
            return

        # Atomically claim the import (only this request processes it)
        with r_engine.connect() as conn:
            result = conn.execute(_t("""
                UPDATE streaming_import
                   SET status='processing', started_at=:t
                 WHERE id=:id AND status='pending'
            """), {"t": datetime.datetime.utcnow(), "id": import_id})
            conn.commit()
            if result.rowcount == 0:
                # Already claimed or finished — stream current status
                row = conn.execute(_t(
                    "SELECT status, rows_read, rows_aggregated, rows_skipped, error_message "
                    "FROM streaming_import WHERE id=:id"
                ), {"id": import_id}).fetchone()
                if row:
                    _emit({"status": row[0], "rows_read": row[1] or 0,
                           "rows_aggregated": row[2] or 0, "rows_skipped": row[3] or 0,
                           "error_message": row[4]})
                return
            row = conn.execute(_t("SELECT file_path FROM streaming_import WHERE id=:id"),
                               {"id": import_id}).fetchone()
            file_path = row[0] if row else None

        if not file_path:
            _emit({"status": "error", "error_message": "Import record not found"})
            return

        _emit({"status": "processing", "rows_read": 0, "rows_aggregated": 0, "rows_skipped": 0})

        class _Rec:
            id = import_id

        _rec = _Rec()
        _rec.file_path = file_path

        def _on_progress(rows_read, rows_skipped, rows_aggregated):
            _emit({"status": "processing", "rows_read": rows_read,
                   "rows_aggregated": rows_aggregated, "rows_skipped": rows_skipped})

        _aggregate_and_store(_rec, main_engine=m_engine, royalties_engine_=r_engine,
                             progress_callback=_on_progress)

        # Fetch final counts from DB
        with r_engine.connect() as conn:
            row = conn.execute(_t(
                "SELECT rows_read, rows_aggregated, rows_skipped FROM streaming_import WHERE id=:id"
            ), {"id": import_id}).fetchone()
        _clear_dashboard_cache(r_engine)
        _app = current_app._get_current_object()
        def _run_prewarm():
            with _app.app_context():
                _prewarm_dashboard_cache()
        threading.Thread(target=_run_prewarm, daemon=True).start()
        _emit({"status": "done",
               "rows_read":      row[0] if row else 0,
               "rows_aggregated": row[1] if row else 0,
               "rows_skipped":   row[2] if row else 0})

        try:
            os.remove(file_path)
        except OSError:
            pass

    except Exception as e:
        _emit({"status": "error", "error_message": str(e)[:500]})
        if r_engine:
            try:
                with r_engine.connect() as conn:
                    conn.execute(_t("""
                        UPDATE streaming_import SET status='error', finished_at=:t, error_message=:e
                         WHERE id=:id
                    """), {"t": datetime.datetime.utcnow(), "e": str(e)[:2000], "id": import_id})
                    conn.commit()
            except Exception:
                pass
    finally:
        if r_engine:
            r_engine.dispose()
        if m_engine:
            m_engine.dispose()


# ── Dashboard data helper ─────────────────────────────────────────────────────

_engine_local = threading.local()  # thread-local engine override for pre-warmer isolation

def _royalties_engine():
    """Return the SQLAlchemy engine for the royalties database.
    If the current thread has set _engine_local.override, use that instead
    (pre-warmer uses a NullPool engine so it never corrupts the main pool).
    """
    override = getattr(_engine_local, 'override', None)
    if override is not None:
        return override
    engine = db.engines.get('royalties')
    if engine is None:
        engine = db.engine
    return engine


def _prewarm_dashboard_cache():
    """Background thread: pre-compute every artist × year × quarter × view combo into dashboard_cache."""
    from sqlalchemy import text as _t, create_engine
    from sqlalchemy.pool import NullPool
    # Create a dedicated NullPool engine for this thread so it never shares
    # connections with the main Flask pool — prevents SSL corruption.
    shared_engine = db.engines.get('royalties') or db.engine
    db_url = shared_engine.url.render_as_string(hide_password=False)
    engine = create_engine(db_url, poolclass=NullPool)
    _engine_local.override = engine  # all _royalties_engine() calls in this thread use this
    try:
        with engine.connect() as conn:
            year_rows = conn.execute(_t(
                "SELECT DISTINCT EXTRACT(year FROM reporting_month)::int "
                "FROM royalty_summary WHERE reporting_month IS NOT NULL ORDER BY 1"
            )).fetchall()
            artist_rows = conn.execute(_t(
                "SELECT DISTINCT artist_name_csv FROM royalty_summary "
                "WHERE artist_name_csv IS NOT NULL AND artist_name_csv != '' ORDER BY 1"
            )).fetchall()
        years    = ["all"] + [str(r[0]) for r in year_rows]
        quarters = ["all", "1", "2", "3", "4"]
        split_artists: set = set()
        for r in artist_rows:
            for part in r[0].split(','):
                name = part.strip()
                if name:
                    split_artists.add(name)
        # Skip "all" — those queries scan millions of rows and are cached on first user visit
        artists = sorted(split_artists, key=str.lower)
    except Exception:
        _prewarm_status["running"] = False
        return

    total = len(years) * len(quarters) * len(artists) * 2
    done  = 0
    _prewarm_status.update({"running": True, "done": 0, "total": total, "current_artist": ""})
    for artist in artists:
        _prewarm_status["current_artist"] = artist
        for y in years:
            for qtr in quarters:
                for v in ("label", "artist"):
                    try:
                        _dashboard_data(y, qtr, artist, v)
                    except Exception:
                        pass
                    done += 1
                    _prewarm_status["done"] = done
                    _time.sleep(0.05)  # throttle to avoid SSL connection pool exhaustion
        try:
            current_app.logger.info(
                "Cache pre-warm: '%s' done (%d/%d combos)", artist, done, total
            )
        except Exception:
            pass
    _prewarm_status.update({"running": False, "current_artist": ""})
    _engine_local.override = None
    engine.dispose()


def _clear_dashboard_cache(engine=None):
    """Delete all persistent dashboard cache entries."""
    from sqlalchemy import text
    eng = engine or _royalties_engine()
    try:
        with eng.connect() as conn:
            conn.execute(text("DELETE FROM dashboard_cache"))
            conn.commit()
    except Exception:
        pass
    _dash_cache.clear()


def _dashboard_data(year=None, quarter=None, artist=None, view="label"):
    """Two-layer cache: in-memory (fast) → DB (survives restarts) → compute."""
    from sqlalchemy import text
    mem_key = (year, quarter, artist, view)
    cached = _dash_cache.get(mem_key)
    if cached and (_time.time() - cached["ts"]) < _CACHE_TTL:
        return cached["data"]

    db_key = f"{year}|{quarter}|{artist}|{view}"
    engine = _royalties_engine()
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT data_json FROM dashboard_cache WHERE cache_key = :k"),
                {"k": db_key}
            ).fetchone()
            if row:
                result = json.loads(row[0])
                _dash_cache[mem_key] = {"data": result, "ts": _time.time()}
                return result
    except Exception:
        pass

    result = _compute_dashboard_data(year, quarter, artist, view)

    # Persist to DB cache
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO dashboard_cache (cache_key, data_json, computed_at)
                VALUES (:k, :d, NOW())
                ON CONFLICT (cache_key) DO UPDATE
                    SET data_json = EXCLUDED.data_json, computed_at = EXCLUDED.computed_at
            """), {"k": db_key, "d": json.dumps(result)})
            conn.commit()
    except Exception:
        pass

    _dash_cache[mem_key] = {"data": result, "ts": _time.time()}
    return result


def _compute_dashboard_data(year=None, quarter=None, artist=None, view="label"):
    """Return aggregated dashboard data using SQL GROUP BY — never loads raw rows into Python."""
    from sqlalchemy import text

    # Check whether artist_name_map exists so the LEFT JOIN is safe
    _engine = _royalties_engine()
    try:
        with _engine.connect() as _chk:
            _chk.execute(text("SELECT 1 FROM artist_name_map LIMIT 1"))
        _has_map = True
    except Exception:
        _has_map = False

    if _has_map:
        base_from  = ("royalty_summary sr "
                      "LEFT JOIN artist_name_map anm ON anm.raw_name = sr.artist_name_csv")
        artist_col = "COALESCE(anm.canonical_name, sr.artist_name_csv)"
    else:
        base_from  = "royalty_summary sr"
        artist_col = "sr.artist_name_csv"

    conditions = ["1=1"]
    params = {}
    if year and year != "all":
        year_int = int(year)
        conditions.append("sr.reporting_month >= :year_start AND sr.reporting_month < :year_end")
        params["year_start"] = f"{year_int}-01-01"
        params["year_end"]   = f"{year_int + 1}-01-01"
    if quarter and quarter != "all":
        q_ranges = {"1": (1, 4), "2": (4, 7), "3": (7, 10), "4": (10, 1)}
        q_start_m, q_end_m = q_ranges.get(str(quarter), (1, 1))
        base_year = int(year) if (year and year != "all") else 2000
        q_end_year = base_year + 1 if q_end_m == 1 else base_year
        conditions.append("sr.reporting_month >= :q_start AND sr.reporting_month < :q_end")
        params["q_start"] = f"{base_year}-{q_start_m:02d}-01"
        params["q_end"]   = f"{q_end_year}-{q_end_m:02d}-01"
    if artist and artist != "all":
        escaped = re.escape(artist)
        params["artist_pattern"] = f"(^|,\\s*){escaped}(\\s*,|$)"
        conditions.append(
            f"COALESCE(anm.canonical_name, sr.artist_name_csv) ~* :artist_pattern"
        )
    where = " AND ".join(conditions)

    if view == "artist":
        # Pre-aggregate splits once via CTE — avoids one correlated subquery per result row
        cte = (
            "WITH _splits AS ("
            "SELECT isrc, SUM(percentage)/100.0 AS pct FROM artist_royalty_split GROUP BY isrc"
            ") "
        )
        base_from += " LEFT JOIN _splits _s ON _s.isrc = sr.isrc"
        rev_expr   = "sr.net_revenue * COALESCE(_s.pct, 1.0)"
    else:
        cte      = ""
        rev_expr = "sr.net_revenue"

    def q(sql, p=None):
        with _engine.connect() as conn:
            return conn.execute(text(cte + sql), p or params).fetchall()

    # KPI
    kpi_total = float(q(f"SELECT COALESCE(SUM({rev_expr}), 0) FROM {base_from} WHERE {where}")[0][0])

    # By artist (top 10) — group by canonical collab string so bars show
    # "El Fantasma", "El Fantasma, Los Dos Carnales", etc. and sum to the artist total
    by_artist = q(f"""
        SELECT {artist_col} AS artist, COALESCE(SUM({rev_expr}), 0) AS rev
          FROM {base_from}
         WHERE {where} AND sr.artist_name_csv IS NOT NULL AND sr.artist_name_csv != ''
         GROUP BY {artist_col} ORDER BY rev DESC LIMIT 10
    """)

    # By month (chronological)
    by_month = q(f"""
        SELECT TO_CHAR(sr.reporting_month, 'Mon YYYY') AS mo, sr.reporting_month,
               COALESCE(SUM({rev_expr}), 0) AS rev
          FROM {base_from} WHERE {where}
         GROUP BY sr.reporting_month ORDER BY sr.reporting_month
    """)

    # By platform (top 15)
    by_platform = q(f"""
        SELECT sr.platform, COALESCE(SUM({rev_expr}), 0) AS rev
          FROM {base_from} WHERE {where} AND sr.platform IS NOT NULL
         GROUP BY sr.platform ORDER BY rev DESC LIMIT 15
    """)

    # By country (top 5 + Other)
    by_country_all = q(f"""
        SELECT sr.country, COALESCE(SUM({rev_expr}), 0) AS rev
          FROM {base_from} WHERE {where} AND sr.country IS NOT NULL
         GROUP BY sr.country ORDER BY rev DESC
    """)
    top5 = by_country_all[:5]
    other = sum(float(r[1]) for r in by_country_all[5:])
    country_data = [(r[0], float(r[1])) for r in top5]
    if other > 0:
        country_data.append(("Other", other))

    # Catalog top 50
    catalog_rows = q(f"""
        SELECT sr.isrc,
               MAX(sr.track_title_csv) AS title,
               MAX({artist_col}) AS artist,
               COALESCE(SUM(sr.streams), 0) AS streams,
               COALESCE(SUM({rev_expr}), 0) AS rev
          FROM {base_from} WHERE {where}
         GROUP BY sr.isrc ORDER BY rev DESC LIMIT 50
    """)
    catalog = [{"title": r[1] or r[0], "artist": r[2] or "",
                "streams": int(r[3]), "revenue": float(r[4])}
               for r in catalog_rows]

    # Dropdown options — split collab strings on comma, deduplicate to individual names
    _all_artists_from = ("royalty_summary sr LEFT JOIN artist_name_map anm ON anm.raw_name = sr.artist_name_csv"
                         if _has_map else "royalty_summary sr")
    _all_artists_col  = "COALESCE(anm.canonical_name, sr.artist_name_csv)" if _has_map else "sr.artist_name_csv"
    _raw_strings = [r[0] for r in q(
        f"SELECT DISTINCT {_all_artists_col} FROM {_all_artists_from} "
        "WHERE sr.artist_name_csv IS NOT NULL AND sr.artist_name_csv != ''",
        {},
    )]
    _artist_names: set = set()
    for s in _raw_strings:
        for part in s.split(','):
            name = part.strip()
            if name:
                _artist_names.add(name)
    all_artists = sorted(_artist_names, key=str.lower)
    all_years = [int(r[0]) for r in q(
        "SELECT DISTINCT EXTRACT(year FROM reporting_month) FROM royalty_summary "
        "WHERE reporting_month IS NOT NULL ORDER BY 1 DESC",
        {},
    )]

    return {
        "kpi_total":   kpi_total,
        "by_artist":   [{"name": r[0], "revenue": float(r[1])} for r in by_artist],
        "by_month":    [{"month": r[0], "revenue": float(r[2])} for r in by_month],
        "by_platform": [{"platform": r[0], "revenue": float(r[1])} for r in by_platform],
        "by_country":  [{"country": k, "revenue": v} for k, v in country_data],
        "catalog":     catalog,
        "all_artists": all_artists,
        "all_years":   all_years,
    }


# ── Routes ─────────────────────────────────────────────────────────────────────

@bp.route("/streaming-royalties")
def dashboard():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    year    = request.args.get("year", "all")
    quarter = request.args.get("quarter", "all")
    artist  = request.args.get("artist", "all")
    view    = request.args.get("view", "label")

    data = _dashboard_data(year, quarter, artist, view)
    return render_template_string(
        _DASHBOARD_HTML,
        data=data, year=year, quarter=quarter,
        artist=artist, view=view,
        _sidebar_html=_sb(),
    )


@bp.route("/streaming-royalties/data")
def dashboard_data():
    """JSON endpoint for filter-driven chart updates."""
    if auth_required():
        return jsonify({"error": "auth"}), 401

    year    = request.args.get("year", "all")
    quarter = request.args.get("quarter", "all")
    artist  = request.args.get("artist", "all")
    view    = request.args.get("view", "label")

    data = _dashboard_data(year, quarter, artist, view)
    return jsonify(data)


@bp.route("/streaming-royalties/imports")
def imports_list():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    from models import StreamingImport
    imports = StreamingImport.query.order_by(StreamingImport.uploaded_at.desc()).all()
    return render_template_string(_IMPORTS_HTML, imports=imports, _sidebar_html=_sb())


@bp.route("/streaming-royalties/import", methods=["GET", "POST"])
def import_file():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    if request.method == "GET":
        return render_template_string(_IMPORT_FORM_HTML, _sidebar_html=_sb())

    f = request.files.get("csv_file")
    if not f or not f.filename:
        flash("Please select a CSV file.", "error")
        return redirect(url_for("streaming_royalties.import_file"))

    os.makedirs(_UPLOAD_DIR, exist_ok=True)
    safe_name = secure_filename(f.filename)
    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    dest = os.path.join(_UPLOAD_DIR, f"{ts}_{safe_name}")
    f.save(dest)

    from models import StreamingImport
    rec = StreamingImport(
        original_filename=f.filename,
        file_path=dest,
        status="pending",
        uploaded_by=session.get("username", ""),
    )
    db.session.add(rec)
    db.session.commit()

    return redirect(url_for("streaming_royalties.import_status", import_id=rec.id))


@bp.route("/streaming-royalties/bulk-import", methods=["GET", "POST"])
def bulk_import():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    if request.method == "GET":
        return render_template_string(_BULK_IMPORT_HTML, _sidebar_html=_sb())

    folder = request.form.get("folder_path", "").strip()
    if not folder or not os.path.isdir(folder):
        flash("Folder path not found or not accessible.", "error")
        return redirect(url_for("streaming_royalties.bulk_import"))

    csv_files = sorted(
        f for f in os.listdir(folder)
        if f.lower().endswith(".csv")
    )
    if not csv_files:
        flash("No CSV files found in that folder.", "error")
        return redirect(url_for("streaming_royalties.bulk_import"))

    from models import StreamingImport
    import_ids = []
    for fname in csv_files:
        src = os.path.join(folder, fname)
        os.makedirs(_UPLOAD_DIR, exist_ok=True)
        ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        dest = os.path.join(_UPLOAD_DIR, f"{ts}_{secure_filename(fname)}")
        # copy file to upload dir
        import shutil
        shutil.copy2(src, dest)
        rec = StreamingImport(
            original_filename=fname,
            file_path=dest,
            status="pending",
            uploaded_by=session.get("username", ""),
        )
        db.session.add(rec)
        db.session.flush()
        import_ids.append(rec.id)
    db.session.commit()

    app_obj = current_app._get_current_object()
    threading.Thread(target=_process_bulk, args=(app_obj, import_ids), daemon=True).start()

    flash(f"Queued {len(import_ids)} files for processing.", "success")
    return redirect(url_for("streaming_royalties.imports_list"))


@bp.route("/streaming-royalties/import-status/<int:import_id>")
def import_status(import_id):
    if auth_required():
        return redirect(url_for("publishing.login"))

    from models import StreamingImport
    rec = StreamingImport.query.get_or_404(import_id)
    return render_template_string(_STATUS_HTML, rec=rec, _sidebar_html=_sb())


@bp.route("/streaming-royalties/import-status/<int:import_id>/json")
def import_status_json(import_id):
    if auth_required():
        return jsonify({"error": "auth"}), 401

    from models import StreamingImport
    rec = StreamingImport.query.get_or_404(import_id)
    return jsonify({
        "status":         rec.status,
        "rows_read":      rec.rows_read,
        "rows_aggregated": rec.rows_aggregated,
        "rows_skipped":   rec.rows_skipped,
        "error_message":  rec.error_message,
        "reporting_month": rec.reporting_month.isoformat() if rec.reporting_month else None,
    })


@bp.route("/streaming-royalties/import-stream/<int:import_id>")
def import_stream(import_id):
    """SSE endpoint: claims + processes the import inline, streaming progress to the browser.
    Because this is a long-lived in-flight HTTP request, gunicorn's graceful shutdown
    (triggered by deploys) waits for it to complete rather than killing it immediately."""
    if auth_required():
        return Response("data: {\"status\":\"error\"}\n\n", mimetype="text/event-stream")

    main_url      = current_app.config.get("SQLALCHEMY_DATABASE_URI", "")
    royalties_url = (current_app.config.get("SQLALCHEMY_BINDS") or {}).get("royalties", "")

    q = _queue_mod.Queue()

    t = threading.Thread(
        target=_process_import_sse,
        args=(import_id, main_url, royalties_url, q),
        daemon=True,
    )
    t.start()

    def generate():
        while True:
            try:
                event = q.get(timeout=15)
                yield f"data: {json.dumps(event)}\n\n"
                if event.get("status") in ("done", "error"):
                    break
            except _queue_mod.Empty:
                yield ": keepalive\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@bp.route("/streaming-royalties/import/<int:import_id>/delete", methods=["POST"])
def delete_import(import_id):
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    from models import StreamingImport
    from sqlalchemy import text as _t
    rec = StreamingImport.query.get_or_404(import_id)
    _engine = _royalties_engine()
    with _engine.connect() as _c:
        _c.execute(_t("DELETE FROM streaming_royalty WHERE import_id = :id"), {"id": import_id})
        _c.execute(_t("DELETE FROM streaming_import WHERE id = :id"), {"id": import_id})
        _c.commit()
    flash("Import deleted.", "success")
    return redirect(url_for("streaming_royalties.imports_list"))


@bp.route("/streaming-royalties/purge-all", methods=["POST"])
def purge_all():
    """Delete every streaming_royalty and streaming_import row — full reset."""
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))
    from sqlalchemy import text as _t
    _engine = _royalties_engine()
    with _engine.connect() as _c:
        _c.execute(_t("DELETE FROM streaming_royalty"))
        _c.execute(_t("DELETE FROM streaming_import"))
        _c.execute(_t("DELETE FROM royalty_summary"))
        _c.commit()
    _clear_dashboard_cache(_engine)
    flash("All royalty data purged.", "success")
    return redirect(url_for("streaming_royalties.imports_list"))


@bp.route("/streaming-royalties/cache-status")
def cache_status():
    return jsonify(_prewarm_status)


@bp.route("/streaming-royalties/clear-cache", methods=["POST"])
def clear_cache():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))
    _clear_dashboard_cache()
    flash("Dashboard cache cleared. Charts will recompute on next load.", "success")
    return redirect(url_for("streaming_royalties.imports_list"))


@bp.route("/streaming-royalties/catalog-upload", methods=["GET", "POST"])
def catalog_upload():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    if request.method == "GET":
        return render_template_string(_CATALOG_UPLOAD_HTML, stats=None, _sidebar_html=_sb())

    f = request.files.get("catalog_file")
    if not f or not f.filename:
        flash("Please select an Excel file.", "error")
        return redirect(url_for("streaming_royalties.catalog_upload"))

    try:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(f.read()), data_only=True)
        ws = wb.active
        rows_iter = ws.iter_rows(values_only=True)
        header_row = next(rows_iter, None)
        if header_row is None:
            raise ValueError("Empty file")

        # Normalize header names
        hdr = [str(h).strip() if h else "" for h in header_row]
        col = {name: idx for idx, name in enumerate(hdr)}

        # Find columns (case-insensitive)
        isrc_col = artist_col = pct_col = None
        for k, v in col.items():
            kl = k.lower()
            if "isrc" in kl:
                isrc_col = v
            elif "artist" in kl:
                artist_col = v
            elif "percent" in kl or "%" in kl or "split" in kl or "share" in kl:
                pct_col = v

        if isrc_col is None or artist_col is None or pct_col is None:
            raise ValueError(f"Could not find ISRC, Artist, and Percentage columns. Found: {hdr}")

        from models import ArtistRoyaltySplit, Artist
        rows_loaded = 0
        rows_updated = 0
        rows_skipped = 0
        first_error = None
        artists_matched = set()
        artists_unmatched = set()

        for row in rows_iter:
            try:
                isrc       = str(row[isrc_col]).strip().upper() if row[isrc_col] else ""
                artist_name = str(row[artist_col]).strip() if row[artist_col] else ""
                pct_raw    = row[pct_col]
                if not isrc or not artist_name or pct_raw is None:
                    rows_skipped += 1
                    continue
                pct = decimal.Decimal(str(pct_raw))

                # Try to match artist
                artist_obj = Artist.query.filter(
                    db.func.lower(Artist.name) == artist_name.lower()
                ).first()
                artist_id = artist_obj.id if artist_obj else None
                if artist_id:
                    artists_matched.add(artist_name)
                else:
                    artists_unmatched.add(artist_name)

                # Upsert
                existing = ArtistRoyaltySplit.query.filter_by(
                    isrc=isrc, artist_name=artist_name
                ).first()
                if existing:
                    existing.percentage = pct
                    existing.artist_id  = artist_id
                    rows_updated += 1
                else:
                    db.session.add(ArtistRoyaltySplit(
                        isrc=isrc,
                        artist_name=artist_name,
                        artist_id=artist_id,
                        percentage=pct,
                    ))
                    rows_loaded += 1
            except Exception as row_err:
                rows_skipped += 1
                if first_error is None:
                    first_error = str(row_err)

        db.session.commit()
        stats = {
            "rows_loaded":        rows_loaded,
            "rows_updated":       rows_updated,
            "rows_skipped":       rows_skipped,
            "first_error":        first_error,
            "header_detected":    {"isrc": isrc_col, "artist": artist_col, "pct": pct_col},
            "artists_matched":    sorted(artists_matched),
            "artists_unmatched":  sorted(artists_unmatched),
        }
        return render_template_string(_CATALOG_UPLOAD_HTML, stats=stats, _sidebar_html=_sb())

    except Exception as e:
        flash(f"Error reading catalog: {e}", "error")
        return redirect(url_for("streaming_royalties.catalog_upload"))


# ── Artist Name Consolidation ─────────────────────────────────────────────────

def _norm(s):
    """Strip accents + lowercase + collapse spaces."""
    s = unicodedata.normalize('NFD', s).encode('ascii', 'ignore').decode()
    return ' '.join(s.lower().split())


def _group_by_normalization(names):
    """Group names that are identical after stripping accents and case.
    Names with commas (collaborations) are treated as distinct entries —
    'Artist A' and 'Artist A, Artist B' will never be in the same group.
    Returns list of lists, multi-name groups first.
    """
    from collections import defaultdict
    buckets = defaultdict(list)
    for name in sorted(set(n for n in names if n)):
        buckets[_norm(name)].append(name)
    groups = list(buckets.values())
    groups.sort(key=lambda g: (-len(g), g[0].lower()))
    return groups


def _suggest_canonical(group):
    """Pick the best-looking name from a normalization group.
    Prefer mixed-case (not all-caps) versions; strip accents from the winner.
    """
    # Prefer names that are not all-caps
    mixed = [n for n in group if n != n.upper()]
    candidates = mixed if mixed else group
    # Among candidates, pick the longest (more words = more complete name)
    best = max(candidates, key=len)
    # Strip accents so the canonical is accent-free
    return unicodedata.normalize('NFD', best).encode('ascii', 'ignore').decode()


@bp.route("/streaming-royalties/artist-names", methods=["GET", "POST"])
def artist_names():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    from models import ArtistNameMap
    _engine = _royalties_engine()

    if request.method == "POST":
        count = int(request.form.get("count", 0))
        saved = deleted = 0
        for i in range(count):
            raw = request.form.get(f"raw_{i}", "").strip()
            canonical = request.form.get(f"canonical_{i}", "").strip()
            if not raw:
                continue
            existing = ArtistNameMap.query.filter_by(raw_name=raw).first()
            if not canonical or canonical == raw:
                if existing:
                    db.session.delete(existing)
                    deleted += 1
            else:
                if existing:
                    existing.canonical_name = canonical
                    existing.updated_at = datetime.datetime.utcnow()
                else:
                    db.session.add(ArtistNameMap(raw_name=raw, canonical_name=canonical))
                saved += 1
        db.session.commit()
        flash(f"Saved {saved} mapping(s), removed {deleted} mapping(s).", "success")
        return redirect(url_for("streaming_royalties.artist_names"))

    # GET — load all distinct raw names + existing mappings
    from sqlalchemy import text
    with _engine.connect() as conn:
        raw_names = [r[0] for r in conn.execute(text(
            "SELECT DISTINCT artist_name_csv FROM streaming_royalty "
            "WHERE artist_name_csv IS NOT NULL AND artist_name_csv != '' "
            "ORDER BY artist_name_csv"
        )).fetchall()]

    try:
        existing_maps = {m.raw_name: m.canonical_name for m in ArtistNameMap.query.all()}
    except Exception:
        existing_maps = {}
    groups = _group_by_normalization(raw_names)
    suggestions = {name: _suggest_canonical(g) for g in groups for name in g}

    ordered = []
    for g in groups:
        for name in g:
            ordered.append({"raw": name, "canonical": existing_maps.get(name, ""), "group_size": len(g)})

    return render_template_string(
        _ARTIST_NAMES_HTML,
        ordered=ordered,
        groups=groups,
        existing_maps=existing_maps,
        suggestions=suggestions,
        total=len(raw_names),
        mapped=len(existing_maps),
        _sidebar_html=_sb("streaming_artist_names"),
    )


# ── HTML Templates ─────────────────────────────────────────────────────────────

from ui import _STYLE, _sidebar, _SB_JS  # noqa: E402


def _sb(active="streaming_royalties"):
    """Pre-render the sidebar so its Jinja2 role-guards are evaluated."""
    return render_template_string(_sidebar(active))


def _page(title, active, body):
    return f"""<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title} — AfinArte</title>{_STYLE}</head>
<body><div class="app">
{_sidebar(active)}
<div class="main"><div class="page">
{body}
</div></div></div>{_SB_JS}</body></html>"""


# ── Dashboard ─────────────────────────────────────────────────────────────────

_DASHBOARD_HTML = """<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Streaming Royalties — AfinArte</title>""" + _STYLE + """
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2.2.0/dist/chartjs-plugin-datalabels.min.js"></script>
<style>
.sr-dash{background:#0d1117;min-height:100vh;padding:0 0 60px}
.sr-header{background:#161b27;border-bottom:1px solid rgba(255,255,255,.07);padding:16px 24px;display:flex;align-items:center;gap:16px;flex-wrap:wrap}
.sr-logo{font-size:20px;font-weight:700;color:#fff;letter-spacing:-.03em}
.sr-logo span{color:#6385ff}
.sr-filters{display:flex;gap:10px;flex-wrap:wrap;margin-left:auto;align-items:center}
.sr-filters select{background:#1e2535;border:1px solid rgba(255,255,255,.1);color:#edf0f8;padding:7px 28px 7px 10px;border-radius:7px;font-size:13px;cursor:pointer;appearance:none;-webkit-appearance:none;background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='6'%3E%3Cpath d='M0 0l5 6 5-6z' fill='%238a96b0'/%3E%3C/svg%3E");background-repeat:no-repeat;background-position:right 8px center}
.sr-view-toggle{display:flex;border:1px solid rgba(255,255,255,.1);border-radius:7px;overflow:hidden}
.sr-view-toggle button{background:#1e2535;color:#8a96b0;border:none;padding:7px 14px;font-size:13px;cursor:pointer;transition:background .14s,color .14s}
.sr-view-toggle button.active{background:#6385ff;color:#fff}
.sr-kpi{background:#161b27;border:1px solid rgba(99,133,255,.2);border-radius:12px;padding:20px 28px;min-width:200px;text-align:right}
.sr-kpi-val{font-size:32px;font-weight:700;color:#5eb8ff;letter-spacing:-.03em}
.sr-kpi-lbl{font-size:12px;color:#8a96b0;margin-top:2px}
.sr-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;padding:16px 24px}
.sr-grid-3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:16px;padding:0 24px 16px}
.sr-panel{background:#161b27;border:1px solid rgba(255,255,255,.07);border-radius:12px;padding:18px 20px;overflow:hidden}
.sr-panel-title{font-size:12px;font-weight:600;color:#8a96b0;letter-spacing:.06em;text-transform:uppercase;margin-bottom:14px}
.sr-tbl{width:100%;border-collapse:collapse;font-size:12.5px}
.sr-tbl th{color:#8a96b0;font-weight:600;padding:5px 8px;text-align:left;border-bottom:1px solid rgba(255,255,255,.06)}
.sr-tbl td{padding:5px 8px;color:#edf0f8;border-bottom:1px solid rgba(255,255,255,.04)}
.sr-tbl tr:last-child td{border-bottom:none}
.sr-tbl .num{text-align:right;font-variant-numeric:tabular-nums}
.sr-no-data{color:#4a5470;font-size:13px;text-align:center;padding:30px 0}
</style>
</head><body style="background:#0d1117;margin:0">
<div class="app">""" + "{{ _sidebar_html|safe }}" + """
<div class="main">
<div class="sr-dash">
  <div class="sr-header">
    <div class="sr-logo">AfinArte <span>Music</span> Royalty System</div>
    <div class="sr-filters">
      <select id="selArtist" onchange="applyFilters()">
        <option value="all"{% if artist=='all' %} selected{% endif %}>All Artists</option>
        {% for a in data.all_artists %}
        <option value="{{ a }}"{% if artist==a %} selected{% endif %}>{{ a }}</option>
        {% endfor %}
      </select>
      <select id="selYear" onchange="applyFilters()">
        <option value="all"{% if year=='all' %} selected{% endif %}>All Years</option>
        {% for y in data.all_years %}
        <option value="{{ y }}"{% if year==y|string %} selected{% endif %}>{{ y }}</option>
        {% endfor %}
      </select>
      <select id="selQuarter" onchange="applyFilters()">
        <option value="all"{% if quarter=='all' %} selected{% endif %}>All Quarters</option>
        <option value="1"{% if quarter=='1' %} selected{% endif %}>Qtr 1</option>
        <option value="2"{% if quarter=='2' %} selected{% endif %}>Qtr 2</option>
        <option value="3"{% if quarter=='3' %} selected{% endif %}>Qtr 3</option>
        <option value="4"{% if quarter=='4' %} selected{% endif %}>Qtr 4</option>
      </select>
      <div class="sr-view-toggle">
        <button id="btnLabel" class="{{ 'active' if view=='label' else '' }}" onclick="setView('label')">Label View</button>
        <button id="btnArtist" class="{{ 'active' if view=='artist' else '' }}" onclick="setView('artist')">Artist View</button>
      </div>
    </div>
    <div class="sr-kpi">
      <div class="sr-kpi-val" id="kpiVal">${{ "{:,.2f}".format(data.kpi_total) }}</div>
      <div class="sr-kpi-lbl">Net Revenue</div>
    </div>
  </div>

  <div class="sr-grid">
    <div class="sr-panel">
      <div class="sr-panel-title">Revenue by Artist</div>
      {% if data.by_artist %}
      <div style="position:relative;height:280px"><canvas id="chartArtist"></canvas></div>
      {% else %}<div class="sr-no-data">No data</div>{% endif %}
    </div>
    <div class="sr-panel">
      <div class="sr-panel-title">Revenue by Month</div>
      {% if data.by_month %}
      <div style="position:relative;height:280px"><canvas id="chartMonth"></canvas></div>
      {% else %}<div class="sr-no-data">No data</div>{% endif %}
    </div>
  </div>

  <div class="sr-grid-3">
    <div class="sr-panel" style="grid-column:span 1;max-height:340px;overflow-y:auto">
      <div class="sr-panel-title">Catalog</div>
      {% if data.catalog %}
      <table class="sr-tbl">
        <thead><tr><th>Streams</th><th>Track</th><th class="num">Net Revenue</th></tr></thead>
        <tbody id="catalogBody">
        {% for t in data.catalog %}
        <tr>
          <td class="num">{{ "{:,}".format(t.streams) }}</td>
          <td>{{ t.title[:35] }}{% if t.title|length > 35 %}…{% endif %}</td>
          <td class="num">${{ "{:,.2f}".format(t.revenue) }}</td>
        </tr>
        {% endfor %}
        </tbody>
      </table>
      {% else %}<div class="sr-no-data">No data</div>{% endif %}
    </div>
    <div class="sr-panel">
      <div class="sr-panel-title">Revenue by Country</div>
      {% if data.by_country %}
      <div style="position:relative;height:280px"><canvas id="chartCountry"></canvas></div>
      {% else %}<div class="sr-no-data">No data</div>{% endif %}
    </div>
    <div class="sr-panel">
      <div class="sr-panel-title">Revenue by Platform</div>
      {% if data.by_platform %}
      <div style="position:relative;height:280px"><canvas id="chartPlatform"></canvas></div>
      {% else %}<div class="sr-no-data">No data</div>{% endif %}
    </div>
  </div>
</div>
</div></div>""" + _SB_JS + """
<script>
const BLUE = '#5eb8ff';
const PALETTE = ['#5eb8ff','#6385ff','#34d399','#f59e0b','#ff4f6a','#22d3ee','#a55bff','#fb923c','#84cc16','#e879f9'];
const DL = ChartDataLabels;

function fmtK(n){ n=Number(n); if(n>=1e6) return '$'+(n/1e6).toFixed(1)+'M'; if(n>=1e3) return '$'+(n/1e3).toFixed(0)+'K'; return '$'+n.toFixed(0); }
function fmt2(n){ return '$'+Number(n).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2}); }

let charts = {};
function destroyCharts(){ Object.values(charts).forEach(c=>c.destroy()); charts={}; }

const H = 280;
function mkCanvas(id){
  const el = document.getElementById(id);
  if(!el) return null;
  el.width  = el.parentElement.clientWidth  || 500;
  el.height = H;
  return el;
}

function buildCharts(d){
  destroyCharts();

  const elA = mkCanvas('chartArtist');
  if(elA && d.by_artist?.length){
    charts.artist = new Chart(elA, {
      type:'bar', plugins:[DL],
      data:{
        labels: d.by_artist.map(r=>r.name),
        datasets:[{data:d.by_artist.map(r=>r.revenue), backgroundColor:BLUE, borderRadius:3}]
      },
      options:{
        responsive:false, indexAxis:'y',
        plugins:{
          legend:{display:false},
          tooltip:{callbacks:{label:ctx=>fmt2(ctx.parsed.x)}},
          datalabels:{anchor:'end',align:'end',color:'#8a96b0',font:{size:10},formatter:fmtK,clamp:true}
        },
        scales:{
          x:{grid:{color:'rgba(255,255,255,.05)'},ticks:{color:'#8a96b0',font:{size:10},callback:v=>fmtK(v)},border:{display:false}},
          y:{grid:{display:false},ticks:{color:'#edf0f8',font:{size:11}}}
        },
        layout:{padding:{right:60}}
      }
    });
  }

  const elM = mkCanvas('chartMonth');
  if(elM && d.by_month?.length){
    charts.month = new Chart(elM, {
      type:'bar', plugins:[DL],
      data:{
        labels: d.by_month.map(r=>r.month),
        datasets:[{data:d.by_month.map(r=>r.revenue), backgroundColor:BLUE, borderRadius:3}]
      },
      options:{
        responsive:false,
        plugins:{
          legend:{display:false},
          tooltip:{callbacks:{label:ctx=>fmt2(ctx.parsed.y)}},
          datalabels:{anchor:'end',align:'top',color:'#8a96b0',font:{size:10},formatter:fmtK}
        },
        scales:{
          x:{grid:{display:false},ticks:{color:'#8a96b0',font:{size:11}}},
          y:{grid:{color:'rgba(255,255,255,.05)'},ticks:{color:'#8a96b0',font:{size:10},callback:v=>fmtK(v)},border:{display:false}}
        },
        layout:{padding:{top:24}}
      }
    });
  }

  const elC = mkCanvas('chartCountry');
  if(elC && d.by_country?.length){
    const tot = d.by_country.reduce((s,r)=>s+r.revenue,0);
    charts.country = new Chart(elC, {
      type:'doughnut', plugins:[DL],
      data:{
        labels: d.by_country.map(r=>r.country),
        datasets:[{data:d.by_country.map(r=>r.revenue), backgroundColor:PALETTE, borderWidth:2, borderColor:'#161b27'}]
      },
      options:{
        responsive:false,
        plugins:{
          legend:{display:true,position:'bottom',labels:{color:'#8a96b0',font:{size:11},padding:8,boxWidth:12}},
          tooltip:{callbacks:{label:ctx=>`${ctx.label}: ${fmt2(ctx.parsed)} (${(ctx.parsed/tot*100).toFixed(1)}%)`}},
          datalabels:{color:'#fff',font:{size:10,weight:'bold'},formatter:(v)=>{const p=(v/tot*100);return p>4?p.toFixed(1)+'%':''}}
        }
      }
    });
  }

  const elP = mkCanvas('chartPlatform');
  if(elP && d.by_platform?.length){
    charts.platform = new Chart(elP, {
      type:'bar', plugins:[DL],
      data:{
        labels: d.by_platform.map(r=>r.platform),
        datasets:[{data:d.by_platform.map(r=>r.revenue), backgroundColor:BLUE, borderRadius:3}]
      },
      options:{
        responsive:false,
        plugins:{
          legend:{display:false},
          tooltip:{callbacks:{label:ctx=>fmt2(ctx.parsed.y)}},
          datalabels:{anchor:'end',align:'top',color:'#8a96b0',font:{size:9},rotation:-45,formatter:fmtK}
        },
        scales:{
          x:{grid:{display:false},ticks:{color:'#8a96b0',font:{size:9},maxRotation:45}},
          y:{grid:{color:'rgba(255,255,255,.05)'},ticks:{color:'#8a96b0',font:{size:10},callback:v=>fmtK(v)},border:{display:false}}
        },
        layout:{padding:{top:28}}
      }
    });
  }
}

const initialData = {{ data|tojson }};
let currentView = '{{ view }}';

document.addEventListener('DOMContentLoaded', function(){
  buildCharts(initialData);
});

function setView(v){
  currentView=v;
  document.getElementById('btnLabel').classList.toggle('active',v==='label');
  document.getElementById('btnArtist').classList.toggle('active',v==='artist');
  applyFilters();
}

function applyFilters(){
  const artist=document.getElementById('selArtist').value;
  const year=document.getElementById('selYear').value;
  const quarter=document.getElementById('selQuarter').value;
  fetch(`/streaming-royalties/data?year=${year}&quarter=${quarter}&artist=${encodeURIComponent(artist)}&view=${currentView}`)
    .then(r=>r.json()).then(d=>{
      document.getElementById('kpiVal').textContent='$'+d.kpi_total.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});
      const tbody=document.getElementById('catalogBody');
      if(tbody && d.catalog){
        tbody.innerHTML=d.catalog.map(t=>`<tr>
          <td class="num">${t.streams.toLocaleString()}</td>
          <td>${t.title.substring(0,35)}${t.title.length>35?'…':''}</td>
          <td class="num">$${t.revenue.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})}</td>
        </tr>`).join('');
      }
      buildCharts(d);
    });
}
</script>
</body></html>"""

@bp.app_template_filter("tojson")
def _tojson_filter(value):
    return Markup(json.dumps(value))


# ── Imports list page ─────────────────────────────────────────────────────────

_IMPORTS_HTML = """<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Streaming Imports — AfinArte</title>""" + _STYLE + """
</head><body><div class="app">{{ _sidebar_html|safe }}
<div class="main"><div class="page">
<div class="ph"><div class="ph-left"><h1 class="ph-title">Streaming Imports</h1></div>
<div class="ph-actions">
  <a href="/streaming-royalties/import" class="btn btn-primary">&#8679; Upload CSV</a>
  <a href="/streaming-royalties/bulk-import" class="btn btn-sec">&#8679; Bulk Import</a>
  <a href="/streaming-royalties" class="btn btn-sec">&#128202; Dashboard</a>
</div></div>
{% with messages = get_flashed_messages(with_categories=true) %}
{% if messages %}{% for cat,msg in messages %}<div class="flash {{ cat }}">{{ msg }}</div>{% endfor %}{% endif %}
{% endwith %}
<div class="card" style="margin-top:18px">
{% if imports %}
<div class="tbl-wrap"><table class="tbl">
<thead><tr>
  <th>File</th><th>Reporting Month</th><th>Status</th>
  <th class="num">Rows Read</th><th class="num">Aggregated</th><th class="num">Skipped</th>
  <th>Uploaded</th><th></th>
</tr></thead>
<tbody>
{% for imp in imports %}
<tr>
  <td>{{ imp.original_filename }}</td>
  <td>{{ imp.reporting_month.strftime('%b %Y') if imp.reporting_month else '—' }}</td>
  <td>
    {% if imp.status == 'done' %}<span class="pill ag">Done</span>
    {% elif imp.status == 'processing' %}<span class="pill am">Processing…</span>
    {% elif imp.status == 'error' %}<span class="pill ar" title="{{ imp.error_message }}">Error</span>
    {% else %}<span class="pill">Pending</span>{% endif %}
  </td>
  <td class="num">{{ "{:,}".format(imp.rows_read or 0) }}</td>
  <td class="num">{{ "{:,}".format(imp.rows_aggregated or 0) }}</td>
  <td class="num">{{ "{:,}".format(imp.rows_skipped or 0) }}</td>
  <td>{{ imp.uploaded_at.strftime('%Y-%m-%d %H:%M') if imp.uploaded_at else '—' }}</td>
  <td>
    {% if imp.status in ('pending','processing') %}
    <a href="/streaming-royalties/import-status/{{ imp.id }}" class="btn btn-sec btn-sm">View</a>
    {% endif %}
    <form method="post" action="/streaming-royalties/import/{{ imp.id }}/delete" style="display:inline"
          onsubmit="return confirm('Delete this import and all its royalty rows?')">
      <button class="btn btn-sm" style="color:var(--ar)">Delete</button>
    </form>
  </td>
</tr>
{% endfor %}
</tbody>
</table></div>
{% else %}
<div style="padding:40px;text-align:center;color:var(--t3)">No imports yet. Upload a Believe monthly CSV to get started.</div>
{% endif %}
<div id="cache-status-bar" style="margin-top:20px;padding:12px 16px;border-radius:8px;background:var(--b1);border:1px solid var(--b2);font-size:13px;display:none">
  <div style="display:flex;align-items:center;gap:12px">
    <span id="cache-status-icon" style="font-size:16px">⏳</span>
    <div style="flex:1">
      <div id="cache-status-text" style="font-weight:600;color:var(--t1)">Cache warming…</div>
      <div style="margin-top:6px;height:6px;border-radius:3px;background:var(--b2);overflow:hidden">
        <div id="cache-progress-bar" style="height:100%;border-radius:3px;background:var(--ac,#4f8ef7);width:0%;transition:width 0.4s ease"></div>
      </div>
      <div id="cache-progress-label" style="margin-top:4px;color:var(--t3)">0 / 0 combos</div>
    </div>
  </div>
</div>
<div style="margin-top:24px;padding-top:16px;border-top:1px solid var(--b2);display:flex;gap:12px;align-items:center">
  <form method="post" action="/streaming-royalties/clear-cache">
    <button class="btn btn-sm">Clear Dashboard Cache</button>
  </form>
  <form method="post" action="/streaming-royalties/purge-all"
        onsubmit="return confirm('This will delete ALL royalty rows from the database. Are you sure?')">
    <button class="btn btn-sm" style="color:var(--ar)">Purge All Royalty Data</button>
  </form>
</div>
</div>
</div></div></div>""" + _SB_JS + """
<script>
(function(){
  var bar = document.getElementById('cache-status-bar');
  var txt = document.getElementById('cache-status-text');
  var prg = document.getElementById('cache-progress-bar');
  var lbl = document.getElementById('cache-progress-label');
  var ico = document.getElementById('cache-status-icon');
  var poll;

  function update(){
    fetch('/streaming-royalties/cache-status')
      .then(function(r){ return r.json(); })
      .then(function(d){
        if(d.running){
          bar.style.display = 'block';
          ico.textContent = '⏳';
          var pct = d.total > 0 ? Math.round(d.done / d.total * 100) : 0;
          txt.textContent = 'Cache warming — ' + (d.current_artist || '…');
          prg.style.width = pct + '%';
          lbl.textContent = d.done.toLocaleString() + ' / ' + d.total.toLocaleString() + ' combos (' + pct + '%)';
        } else if(d.total > 0){
          bar.style.display = 'block';
          ico.textContent = '✅';
          txt.textContent = 'Cache ready';
          prg.style.width = '100%';
          lbl.textContent = d.total.toLocaleString() + ' combos cached — all artists load instantly';
          clearInterval(poll);
        } else {
          bar.style.display = 'none';
        }
      })
      .catch(function(){ /* ignore */ });
  }

  update();
  poll = setInterval(update, 4000);
})();
</script>
</body></html>"""

# ── Import form ───────────────────────────────────────────────────────────────

_IMPORT_FORM_HTML = """<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Upload CSV — AfinArte</title>""" + _STYLE + """
</head><body><div class="app">
{{ _sidebar_html|safe }}
<div class="main"><div class="page">
<div class="ph"><div class="ph-left"><h1 class="ph-title">Upload Monthly Report</h1></div>
<div class="ph-actions"><a href="/streaming-royalties/imports" class="btn btn-sec">&#8592; Imports</a></div>
</div>
{% with messages = get_flashed_messages(with_categories=true) %}
{% if messages %}{% for cat,msg in messages %}<div class="flash {{ cat }}">{{ msg }}</div>{% endfor %}{% endif %}
{% endwith %}
<div class="card" style="max-width:560px;margin-top:18px;padding:28px 24px">
<p style="color:var(--t2);font-size:14px;margin-bottom:20px">
  Upload a semicolon-delimited Believe monthly royalty CSV. Files up to 300 MB are supported.
  The file will be processed in the background — you will see live status after uploading.
</p>
<form method="post" enctype="multipart/form-data">
  <div class="form-row">
    <label class="form-label">CSV File</label>
    <input type="file" name="csv_file" accept=".csv" required class="form-input">
  </div>
  <button type="submit" class="btn btn-primary" style="margin-top:18px">&#8679; Upload &amp; Process</button>
</form>
</div>
</div></div></div>""" + _SB_JS + """</body></html>"""

# ── Bulk import form ──────────────────────────────────────────────────────────

_BULK_IMPORT_HTML = """<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Bulk Import — AfinArte</title>""" + _STYLE + """
</head><body><div class="app">
{{ _sidebar_html|safe }}
<div class="main"><div class="page">
<div class="ph"><div class="ph-left"><h1 class="ph-title">Bulk Historical Import</h1></div>
<div class="ph-actions"><a href="/streaming-royalties/imports" class="btn btn-sec">&#8592; Imports</a></div>
</div>
{% with messages = get_flashed_messages(with_categories=true) %}
{% if messages %}{% for cat,msg in messages %}<div class="flash {{ cat }}">{{ msg }}</div>{% endfor %}{% endif %}
{% endwith %}
<div class="card" style="max-width:560px;margin-top:18px;padding:28px 24px">
<p style="color:var(--t2);font-size:14px;margin-bottom:20px">
  Provide the absolute path to a folder containing Believe monthly CSV files.
  All <code>.csv</code> files in that folder will be queued and processed sequentially in the background.
</p>
<form method="post">
  <div class="form-row">
    <label class="form-label">Folder Path on Server</label>
    <input type="text" name="folder_path" placeholder="/path/to/believe_reports" class="form-input" required>
  </div>
  <button type="submit" class="btn btn-primary" style="margin-top:18px">&#9654; Start Bulk Import</button>
</form>
</div>
</div></div></div>""" + _SB_JS + """</body></html>"""

# ── Status page ───────────────────────────────────────────────────────────────

_STATUS_HTML = """<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Import Status — AfinArte</title>""" + _STYLE + """
</head><body><div class="app">
{{ _sidebar_html|safe }}
<div class="main"><div class="page">
<div class="ph"><div class="ph-left"><h1 class="ph-title">Import Status</h1></div>
<div class="ph-actions"><a href="/streaming-royalties/imports" class="btn btn-sec">&#8592; All Imports</a></div>
</div>
<div class="card" style="max-width:520px;margin-top:18px;padding:28px 24px">
  <p style="color:var(--t2);font-size:13px;margin-bottom:6px">File: <strong style="color:var(--t1)">{{ rec.original_filename }}</strong></p>
  <div style="display:flex;align-items:center;gap:12px;margin:18px 0">
    <div id="statusBadge" style="font-size:15px;font-weight:600">
      {% if rec.status == 'done' %}<span style="color:var(--ag)">&#10003; Done</span>
      {% elif rec.status == 'error' %}<span style="color:var(--ar)">&#10007; Error</span>
      {% elif rec.status == 'processing' %}<span style="color:var(--am)">&#9654; Processing…</span>
      {% else %}<span style="color:var(--t2)">&#9679; Pending</span>{% endif %}
    </div>
  </div>
  <div id="statsBox" style="font-size:13px;color:var(--t2);line-height:2">
    Rows read: <strong id="rowsRead" style="color:var(--t1)">{{ "{:,}".format(rec.rows_read or 0) }}</strong><br>
    Aggregated: <strong id="rowsAgg" style="color:var(--t1)">{{ "{:,}".format(rec.rows_aggregated or 0) }}</strong><br>
    Skipped: <strong id="rowsSkip" style="color:var(--t1)">{{ "{:,}".format(rec.rows_skipped or 0) }}</strong>
  </div>
  {% if rec.status == 'error' %}
  <div style="margin-top:14px;background:rgba(255,79,106,.08);border:1px solid rgba(255,79,106,.2);border-radius:8px;padding:12px;font-size:12px;color:var(--ar);word-break:break-all">
    {{ rec.error_message }}
  </div>
  {% endif %}
  {% if rec.status == 'done' %}
  <a href="/streaming-royalties" class="btn btn-primary" style="margin-top:20px;display:inline-block">&#128202; View Dashboard</a>
  {% endif %}
</div>
</div></div></div>""" + _SB_JS + """
<script>
const importId = {{ rec.id }};
const finalStatuses = new Set(['done','error']);
let currentStatus = '{{ rec.status }}';

function applyUpdate(d){
  currentStatus = d.status;
  document.getElementById('rowsRead').textContent = (d.rows_read||0).toLocaleString();
  document.getElementById('rowsAgg').textContent  = (d.rows_aggregated||0).toLocaleString();
  document.getElementById('rowsSkip').textContent = (d.rows_skipped||0).toLocaleString();
  const badge = document.getElementById('statusBadge');
  if(d.status==='done'){
    badge.innerHTML='<span style="color:var(--ag)">&#10003; Done</span>';
    setTimeout(()=>{ location.href='/streaming-royalties'; }, 2000);
  } else if(d.status==='error'){
    badge.innerHTML='<span style="color:var(--ar)">&#10007; Error</span>';
    if(d.error_message){
      let errBox = document.getElementById('errBox');
      if(!errBox){
        errBox=document.createElement('div');
        errBox.id='errBox';
        errBox.style='margin-top:14px;background:rgba(255,79,106,.08);border:1px solid rgba(255,79,106,.2);border-radius:8px;padding:12px;font-size:12px;color:var(--ar);word-break:break-all';
        document.getElementById('statsBox').after(errBox);
      }
      errBox.textContent=d.error_message;
    }
  } else if(d.status==='processing'){
    badge.innerHTML='<span style="color:var(--am)">&#9654; Processing…</span>';
  }
}

// Use SSE for pending/processing imports; fall back to polling if SSE unsupported
if(!finalStatuses.has(currentStatus)){
  if(typeof EventSource !== 'undefined'){
    const es = new EventSource(`/streaming-royalties/import-stream/${importId}`);
    es.onmessage = e => {
      try{
        const d = JSON.parse(e.data);
        applyUpdate(d);
        if(finalStatuses.has(d.status)) es.close();
      }catch(err){}
    };
    es.onerror = () => {
      es.close();
      // Fall back to polling if stream dies
      if(!finalStatuses.has(currentStatus)) setTimeout(poll, 3000);
    };
  } else {
    setTimeout(poll, 2000);
  }
}

function poll(){
  if(finalStatuses.has(currentStatus)) return;
  fetch(`/streaming-royalties/import-status/${importId}/json`)
    .then(r=>r.json())
    .then(d=>{ applyUpdate(d); if(!finalStatuses.has(d.status)) setTimeout(poll, 2000); })
    .catch(()=>setTimeout(poll, 3000));
}
</script>
</body></html>"""

# ── Catalog upload ────────────────────────────────────────────────────────────

_CATALOG_UPLOAD_HTML = """<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Catalog Upload — AfinArte</title>""" + _STYLE + """
</head><body><div class="app">
{{ _sidebar_html|safe }}
<div class="main"><div class="page">
<div class="ph"><div class="ph-left"><h1 class="ph-title">Artist Royalty Catalog</h1></div>
<div class="ph-actions"><a href="/streaming-royalties" class="btn btn-sec">&#128202; Dashboard</a></div>
</div>
{% with messages = get_flashed_messages(with_categories=true) %}
{% if messages %}{% for cat,msg in messages %}<div class="flash {{ cat }}">{{ msg }}</div>{% endfor %}{% endif %}
{% endwith %}
{% if stats %}
<div class="card" style="max-width:560px;margin-top:18px;padding:24px">
  <div style="color:var(--ag);font-weight:600;margin-bottom:12px">&#10003; Catalog loaded successfully</div>
  <div style="font-size:13px;color:var(--t2);line-height:2">
    New rows: <strong style="color:var(--t1)">{{ stats.rows_loaded }}</strong><br>
    Updated rows: <strong style="color:var(--t1)">{{ stats.rows_updated }}</strong><br>
    Skipped rows: <strong style="color:var(--am)">{{ stats.rows_skipped }}</strong><br>
    Artists matched: <strong style="color:var(--ag)">{{ stats.artists_matched|length }}</strong><br>
    Artists unmatched: <strong style="color:var(--am)">{{ stats.artists_unmatched|length }}</strong>
  </div>
  <div style="margin-top:10px;font-size:12px;color:var(--t2)">
    Columns detected → ISRC: col {{ stats.header_detected.isrc }},
    Artist: col {{ stats.header_detected.artist }},
    Percentage: col {{ stats.header_detected.pct }}
  </div>
  {% if stats.first_error %}
  <div style="margin-top:8px;font-size:12px;color:var(--am)">
    First row error: {{ stats.first_error }}
  </div>
  {% endif %}
  {% if stats.artists_unmatched %}
  <div style="margin-top:12px;font-size:12px;color:var(--am)">
    Unmatched (create these artists in the catalog to link): {{ stats.artists_unmatched|join(', ') }}
  </div>
  {% endif %}
  <a href="/streaming-royalties/catalog-upload" class="btn btn-sec" style="margin-top:16px;display:inline-block">Upload Another</a>
</div>
{% else %}
<div class="card" style="max-width:560px;margin-top:18px;padding:28px 24px">
<p style="color:var(--t2);font-size:14px;margin-bottom:20px">
  Upload the artist royalty percentage catalog (.xlsx). Expected columns:
  <strong>ISRC</strong>, <strong>Artist Name</strong>, <strong>Percentage</strong>.
  Column names are flexible — the importer will detect them automatically.
</p>
<form method="post" enctype="multipart/form-data">
  <div class="form-row">
    <label class="form-label">Excel File (.xlsx)</label>
    <input type="file" name="catalog_file" accept=".xlsx" required class="form-input">
  </div>
  <button type="submit" class="btn btn-primary" style="margin-top:18px">&#8679; Upload Catalog</button>
</form>
</div>
{% endif %}
</div></div></div>""" + _SB_JS + """</body></html>"""

# ── Artist Name Consolidation UI ──────────────────────────────────────────────

_ARTIST_NAMES_HTML = """<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Artist Names — AfinArte</title>""" + _STYLE + """
<style>
.an-stats{display:flex;gap:12px;margin-bottom:20px;flex-wrap:wrap}
.an-stat{background:var(--c2);border:1px solid var(--bdr);border-radius:8px;padding:10px 18px;font-size:13px;color:var(--t2)}
.an-stat strong{color:var(--t1);font-size:18px;display:block}
.an-group{background:var(--c2);border:1px solid var(--bdr);border-radius:10px;margin-bottom:10px;overflow:hidden}
.an-group-hd{background:rgba(99,133,255,.08);border-bottom:1px solid var(--bdr);padding:9px 14px;display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.an-badge{background:#6385ff22;color:#6385ff;border:1px solid #6385ff44;border-radius:4px;font-size:11px;padding:2px 7px;font-weight:600;white-space:nowrap}
.an-row{display:grid;grid-template-columns:1fr 1fr auto;gap:8px;align-items:center;padding:7px 14px;border-bottom:1px solid rgba(255,255,255,.04)}
.an-row:last-child{border-bottom:none}
.an-raw{font-size:12.5px;color:var(--t2);font-family:monospace;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.an-clear{background:none;border:none;color:var(--t3);cursor:pointer;padding:2px 8px;font-size:13px;border-radius:4px}
.an-clear:hover{color:var(--err);background:rgba(255,79,106,.1)}
</style>
</head><body><div class="app">{{ _sidebar_html|safe }}
<div class="main"><div class="page">
<div class="ph">
  <div class="ph-left"><h1 class="ph-title">Artist Name Consolidation</h1>
  <div class="ph-sub">Names are grouped when they are identical after removing accents and ignoring case. Comma = collaboration — those are never merged.</div></div>
  <div class="ph-actions"><a href="/streaming-royalties" class="btn btn-sec">&#128202; Dashboard</a></div>
</div>
{% with messages = get_flashed_messages(with_categories=true) %}
{% if messages %}{% for cat,msg in messages %}<div class="flash {{ cat }}">{{ msg }}</div>{% endfor %}{% endif %}
{% endwith %}

<div class="an-stats">
  <div class="an-stat"><strong>{{ total }}</strong>Distinct raw names</div>
  <div class="an-stat"><strong>{{ mapped }}</strong>Mapped</div>
  <div class="an-stat"><strong>{{ total - mapped }}</strong>Unmapped</div>
  <div class="an-stat"><strong>{{ groups|selectattr('length','gt',1)|list|length if false else groups|length }}</strong>Groups</div>
</div>

<div style="display:flex;gap:10px;margin-bottom:14px;flex-wrap:wrap;align-items:center">
  <input class="inp" id="srch" placeholder="&#128269; Search artist name..." style="max-width:340px;flex:1" oninput="doSearch(this.value)">
  <label style="display:flex;align-items:center;gap:6px;font-size:13px;color:var(--t2);cursor:pointer">
    <input type="checkbox" id="chkMulti" onchange="toggleMultiOnly(this.checked)"> Show only groups with variants
  </label>
  <button type="button" class="btn btn-sec btn-sm" onclick="autoApplyAll()">&#9889; Auto-apply all suggestions</button>
</div>

<form method="post">
  <input type="hidden" name="count" value="{{ ordered|length }}">
  {% for item in ordered %}
  <input type="hidden" name="raw_{{ loop.index0 }}" value="{{ item.raw }}">
  {% endfor %}

  {% set flat = namespace(i=0) %}
  {% for group in groups %}
  {% set gi = loop.index0 %}
  {% set sugg = suggestions[group[0]] %}
  <div class="an-group" data-search="{{ group|join('|||')|lower }}" data-multi="{{ 'y' if group|length > 1 else 'n' }}">
    <div class="an-group-hd">
      {% if group|length > 1 %}
      <span class="an-badge">{{ group|length }} variants</span>
      {% else %}
      <span class="an-badge" style="background:#34d39916;color:#34d399;border-color:#34d39940">1 name</span>
      {% endif %}
      <span style="font-size:13px;color:var(--t1);font-weight:500">{{ sugg[:60] }}{% if sugg|length > 60 %}…{% endif %}</span>
      <div style="margin-left:auto;display:flex;gap:8px;align-items:center">
        <input class="inp gc-inp" id="gc_{{ gi }}" value="{{ sugg }}"
               placeholder="Canonical for all {{ group|length }}…"
               style="width:260px;font-size:12px"
               data-suggestion="{{ sugg }}">
        <button type="button" class="btn btn-sm" onclick="applyGroup({{ gi }})">Apply to group</button>
      </div>
    </div>
    {% for name in group %}
    <div class="an-row">
      <span class="an-raw" title="{{ name }}">{{ name }}</span>
      <input class="inp row-can" name="canonical_{{ flat.i }}"
             value="{{ existing_maps.get(name, '') }}"
             placeholder="{{ sugg }}"
             style="font-size:12.5px" data-gi="{{ gi }}" data-suggestion="{{ sugg }}">
      <button type="button" class="an-clear" onclick="this.closest('.an-row').querySelector('.row-can').value=''" title="Clear">&#10005;</button>
    </div>
    {% set flat.i = flat.i + 1 %}
    {% endfor %}
  </div>
  {% endfor %}

  <div style="margin-top:20px;padding-top:16px;border-top:1px solid var(--bdr);display:flex;gap:12px">
    <button type="submit" class="btn btn-primary">&#10003; Save All Mappings</button>
    <a href="/streaming-royalties" class="btn btn-sec">Cancel</a>
  </div>
</form>
</div></div></div>""" + _SB_JS + """
<script>
function applyGroup(gi){
  const val = document.getElementById('gc_'+gi).value.trim();
  if(!val) return;
  document.querySelectorAll('.row-can[data-gi="'+gi+'"]').forEach(el => el.value = val);
}
function autoApplyAll(){
  document.querySelectorAll('.row-can').forEach(el => {
    if(!el.value.trim()) el.value = el.dataset.suggestion || '';
  });
}
function doSearch(q){
  q = q.toLowerCase();
  document.querySelectorAll('.an-group').forEach(g => {
    const matchSearch = !q || g.dataset.search.includes(q);
    const matchMulti  = !document.getElementById('chkMulti').checked || g.dataset.multi === 'y';
    g.style.display = (matchSearch && matchMulti) ? '' : 'none';
  });
}
function toggleMultiOnly(on){
  doSearch(document.getElementById('srch').value);
}
</script>
</body></html>"""
