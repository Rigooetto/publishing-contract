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
_CACHE_TTL = 3600  # seconds — data changes only on import (~monthly)

_splits_cache: dict = {}
_SPLITS_CACHE_TTL = 120  # seconds

_dropdown_cache: dict = {}  # {"raw_strings": [...], "all_years": [...], "ts": float}

# Module-level ISRC→artist_name_csv cache (essentially static: doesn't change between imports)
_isrc_csv_cache: dict = {}  # isrc → artist_name_csv string
_isrc_csv_cache_lock = threading.Lock()

_prewarm_status: dict = {"running": False, "done": 0, "total": 0, "current_artist": ""}
_prewarm_lock = threading.Lock()
_prewarm_counter_lock = threading.Lock()
_ard_rebuild_lock = threading.Lock()  # prevents concurrent ARD/ALD rebuilds
_normalize_lock = threading.Lock()    # prevents concurrent royalty_summary normalization runs

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


def _isrc_to_track_map(isrc_set, main_engine=None, prefetch_all=False):
    """Return {isrc: track_id}.  Pass prefetch_all=True to load the full catalog at once."""
    from sqlalchemy import text as _t
    if main_engine is not None:
        with main_engine.connect() as conn:
            if prefetch_all:
                rows = conn.execute(_t("SELECT isrc, id FROM track WHERE isrc IS NOT NULL")).fetchall()
            else:
                rows = conn.execute(
                    _t("SELECT isrc, id FROM track WHERE isrc = ANY(:isrcs)"),
                    {"isrcs": list(isrc_set)}
                ).fetchall()
        return {r[0]: r[1] for r in rows}
    from models import Track
    from sqlalchemy import select
    q = select(Track.isrc, Track.id).where(Track.isrc.isnot(None)) if prefetch_all else \
        select(Track.isrc, Track.id).where(Track.isrc.in_(list(isrc_set)))
    rows = db.session.execute(q).fetchall()
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
        try:
            with royalties_engine.connect() as conn:
                conn.execute(_t(sql), {
                    "r": rows_read, "s": rows_skipped, "a": rows_aggregated,
                    "id": rec.id,
                    **({"rm": reporting_month} if reporting_month else {}),
                })
                conn.commit()
        except Exception:
            pass  # progress display is best-effort; never block the import
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
        for i in range(0, len(rows), 1000):
            chunk = [dict(zip(keys, r)) for r in rows[i:i + 1000]]
            conn.execute(_t(UPSERT), chunk)
        conn.commit()

    return rows_aggregated_total + len(agg)


def _aggregate_and_store(rec, main_engine=None, royalties_engine_=None, progress_callback=None):
    """Parse CSV with pandas (chunked) and flush aggregated rows to the DB."""
    import pandas as pd

    rows_read = 0
    rows_skipped = 0
    rows_aggregated_total = 0
    reporting_month_seen = None

    # Detect delimiter
    with open(rec.file_path, encoding="utf-8-sig", errors="replace") as _peek:
        first_line = _peek.readline()
    delimiter = ";" if first_line.count(";") >= first_line.count(",") else ","

    # Build alias→canonical lookup
    _alias_map = {}
    for canonical, aliases in {
        "isrc":               ["isrc"],
        "platform":           ["platform"],
        "country":            ["country / region", "country/region", "country"],
        "sales_type":         ["sales type", "salestype", "sale type", "sales_type"],
        "reporting_month":    ["reporting month", "reporting_month", "report month"],
        "sales_month":        ["sales month", "sales_month", "sale month"],
        "quantity":           ["quantity", "qty", "units"],
        "gross_revenue":      ["gross revenue", "gross_revenue", "gross"],
        "net_revenue":        ["net revenue", "net_revenue", "net"],
        "mechanical_fee":     ["mechanical fee", "mechanical_fee", "mechanical"],
        "artist_name":        ["artist name", "artist_name", "artist"],
        "track_title":        ["track title", "track_title", "track name", "title"],
        "label_name":         ["label name", "label_name", "label"],
        "release_title":      ["release title", "release_title", "release name", "album"],
        "upc":                ["upc"],
        "streaming_sub_type": ["streaming subscription type", "streaming_subscription_type", "subscription type"],
        "release_type":       ["release type", "release_type"],
        "currency":           ["client payment currency", "currency", "payment currency"],
    }.items():
        for alias in aliases:
            _alias_map[alias] = canonical

    REQUIRED = {"isrc", "platform", "country", "sales_type",
                "reporting_month", "sales_month", "quantity",
                "gross_revenue", "net_revenue", "mechanical_fee"}

    def _parse_num_series(s):
        s = s.fillna("0").str.strip().str.strip('"') \
              .str.replace('\xa0', '', regex=False).str.replace(' ', '', regex=False)
        eu = s.str.match(r'^-?\d{1,3}(\.\d{3})+(,\d+)?$', na=False)
        s = s.copy()
        s[eu]  = s[eu].str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
        s[~eu] = s[~eu].str.replace(',', '', regex=False)
        return pd.to_numeric(s, errors='coerce').fillna(0.0)

    _engine = royalties_engine_ if royalties_engine_ is not None else _royalties_engine()
    col_rename = None
    now = datetime.datetime.utcnow()

    key_cols  = ["isrc", "platform", "country", "sales_type", "rep_iso", "sal_iso"]
    meta_cols = ["artist_name", "track_title", "label_name", "release_title",
                 "upc", "streaming_sub_type", "release_type", "currency"]
    agg_frames = []
    rows_agg_running = 0

    for chunk in pd.read_csv(
        rec.file_path,
        sep=delimiter,
        encoding="utf-8-sig",
        dtype=str,
        chunksize=100_000,
        on_bad_lines="skip",
    ):
        # Build column rename map from first chunk
        if col_rename is None:
            col_rename = {c: _alias_map[c.strip().lower()]
                          for c in chunk.columns if c.strip().lower() in _alias_map}
            missing = REQUIRED - set(col_rename.values())
            if missing:
                raise ValueError(
                    f"CSV missing required columns: {missing}. "
                    f"Headers found: {list(chunk.columns[:20])}"
                )

        chunk = chunk.rename(columns=col_rename)
        rows_read += len(chunk)

        # Fill missing optional columns with defaults
        for opt, default in [
            ("artist_name",""), ("track_title",""), ("label_name",""),
            ("release_title",""), ("upc",""), ("streaming_sub_type",""),
            ("release_type",""), ("currency","EUR"),
        ]:
            if opt not in chunk.columns:
                chunk[opt] = default

        # Strip whitespace/quotes from all string columns
        for c in chunk.select_dtypes("object").columns:
            chunk[c] = chunk[c].fillna("").str.strip().str.strip('"')

        chunk["isrc"] = chunk["isrc"].str.upper()

        # UPC fallback for empty ISRC
        empty = chunk["isrc"] == ""
        if empty.any():
            chunk.loc[empty, "isrc"] = "UPC:" + chunk.loc[empty, "upc"]

        before = len(chunk)
        chunk = chunk[chunk["isrc"].ne("")]
        rows_skipped += before - len(chunk)

        # Parse dates (two formats)
        for dcol in ("reporting_month", "sales_month"):
            parsed = pd.to_datetime(chunk[dcol], format="%Y/%m/%d", errors="coerce")
            chunk[dcol] = parsed.fillna(
                pd.to_datetime(chunk[dcol], format="%Y-%m-%d", errors="coerce")
            )

        before = len(chunk)
        chunk = chunk.dropna(subset=["reporting_month", "sales_month"])
        rows_skipped += before - len(chunk)

        if chunk.empty:
            _save_progress(rec, rows_read, rows_skipped, rows_aggregated_total, royalties_engine_)
            if progress_callback:
                progress_callback(rows_read, rows_skipped, rows_aggregated_total)
            continue

        if reporting_month_seen is None:
            reporting_month_seen = chunk["reporting_month"].iloc[0].date()

        # Parse numerics with vectorized operations
        chunk["quantity"]       = _parse_num_series(chunk["quantity"]).round(0).astype(int)
        chunk["gross_revenue"]  = _parse_num_series(chunk["gross_revenue"])
        chunk["net_revenue"]    = _parse_num_series(chunk["net_revenue"])
        chunk["mechanical_fee"] = _parse_num_series(chunk["mechanical_fee"])

        chunk["rep_iso"] = chunk["reporting_month"].dt.strftime("%Y-%m-%d")
        chunk["sal_iso"] = chunk["sales_month"].dt.strftime("%Y-%m-%d")

        agg_df = chunk.groupby(key_cols, sort=False).agg(
            quantity=("quantity", "sum"),
            gross_revenue=("gross_revenue", "sum"),
            net_revenue=("net_revenue", "sum"),
            mechanical_fee=("mechanical_fee", "sum"),
            artist_name=("artist_name", "first"),
            track_title=("track_title", "first"),
            label_name=("label_name", "first"),
            release_title=("release_title", "first"),
            upc=("upc", "first"),
            streaming_sub_type=("streaming_sub_type", "first"),
            release_type=("release_type", "first"),
            currency=("currency", "first"),
        ).reset_index()

        agg_frames.append(agg_df)
        rows_agg_running += len(agg_df)
        _save_progress(rec, rows_read, rows_skipped, rows_agg_running, royalties_engine_)
        if progress_callback:
            progress_callback(rows_read, rows_skipped, rows_agg_running)

    # Final cross-chunk aggregation + single bulk write
    if agg_frames:
        import psycopg2.extras as _pg2_extras
        final_df = pd.concat(agg_frames, ignore_index=True)
        final_agg = final_df.groupby(key_cols, sort=False).agg(
            quantity=("quantity", "sum"),
            gross_revenue=("gross_revenue", "sum"),
            net_revenue=("net_revenue", "sum"),
            mechanical_fee=("mechanical_fee", "sum"),
            artist_name=("artist_name", "first"),
            track_title=("track_title", "first"),
            label_name=("label_name", "first"),
            release_title=("release_title", "first"),
            upc=("upc", "first"),
            streaming_sub_type=("streaming_sub_type", "first"),
            release_type=("release_type", "first"),
            currency=("currency", "first"),
        ).reset_index()

        rows_aggregated_total = len(final_agg)

        rows_tuples = [
            (rec.id, r["isrc"], r["platform"], r["country"], r["sales_type"],
             datetime.date.fromisoformat(r["rep_iso"]),
             datetime.date.fromisoformat(r["sal_iso"]),
             r["artist_name"], r["track_title"], r["label_name"], r["release_title"],
             r["upc"], r["streaming_sub_type"], r["release_type"], r["currency"],
             int(r["quantity"]),
             round(float(r["gross_revenue"]), 6),
             round(float(r["net_revenue"]), 6),
             round(float(r["mechanical_fee"]), 6),
             None, now)
            for r in final_agg.to_dict("records")
        ]

        UPSERT_EV = """
            INSERT INTO streaming_royalty (
                import_id, isrc, platform, country, sales_type,
                reporting_month, sales_month,
                artist_name_csv, track_title_csv, label_name, release_title,
                upc, streaming_sub_type, release_type, currency,
                total_quantity, total_gross_revenue, total_net_revenue, total_mechanical_fee,
                track_id, created_at
            ) VALUES %s
            ON CONFLICT ON CONSTRAINT uq_streaming_royalty_agg_key DO UPDATE SET
                total_quantity       = streaming_royalty.total_quantity       + EXCLUDED.total_quantity,
                total_gross_revenue  = streaming_royalty.total_gross_revenue  + EXCLUDED.total_gross_revenue,
                total_net_revenue    = streaming_royalty.total_net_revenue    + EXCLUDED.total_net_revenue,
                total_mechanical_fee = streaming_royalty.total_mechanical_fee + EXCLUDED.total_mechanical_fee
        """
        raw_conn = _engine.raw_connection()
        try:
            with raw_conn.cursor() as cur:
                _pg2_extras.execute_values(cur, UPSERT_EV, rows_tuples, page_size=5000)
            raw_conn.commit()
        finally:
            raw_conn.close()

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
        if isinstance(url, dict):
            url = url.get("url", "")
        url = str(url) if url else ""
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://", 1)
        if url and "sslmode=" not in url and url.startswith("postgresql://"):
            url += ("&" if "?" in url else "?") + "sslmode=require"
        return url

    _db_connect_args = {"connect_timeout": 10}
    m_engine = create_engine(_pg(main_url), poolclass=NullPool,
                             connect_args=_db_connect_args) if main_url else None
    r_engine = create_engine(_pg(royalties_url), poolclass=NullPool,
                             connect_args=_db_connect_args) if royalties_url else None

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
        # Atomically claim the import (only one processor runs at a time)
        file_path = None
        with r_engine.connect() as conn:
            result = conn.execute(_t("""
                UPDATE streaming_import SET status='processing', started_at=:t
                 WHERE id=:id AND status IN ('pending','error')
            """), {"t": datetime.datetime.utcnow(), "id": import_id})
            conn.commit()
            if result.rowcount == 0:
                return  # already processing or done — leave it alone
            row = conn.execute(_t("SELECT file_path FROM streaming_import WHERE id=:id"),
                               {"id": import_id}).fetchone()
            if row:
                file_path = row[0]

        if not file_path:
            _update_status("error", "Import record not found")
            return

        import os as _os
        if not _os.path.exists(file_path):
            _update_status("error", f"File not found: {file_path}")
            return

        # Build a lightweight rec-like object the helpers can use
        class _Rec:
            id = import_id
            file_path = None

        _rec = _Rec()
        _rec.file_path = file_path

        _aggregate_and_store(_rec, main_engine=m_engine, royalties_engine_=r_engine)

        # Sync royalty_summary from this import's streaming_royalty rows
        _imp_months = []
        try:
            with r_engine.connect() as _sc:
                _sc.execute(_t("""
                    INSERT INTO royalty_summary
                        (reporting_month, isrc, artist_name_csv, platform, country,
                         track_title_csv, streams, net_revenue)
                    SELECT reporting_month, isrc, MAX(artist_name_csv), platform, country,
                           MAX(track_title_csv), SUM(total_quantity), SUM(total_net_revenue)
                      FROM streaming_royalty
                     WHERE import_id = :id
                     GROUP BY reporting_month, isrc, platform, country
                    ON CONFLICT (reporting_month, isrc, platform, country) DO UPDATE SET
                        streams         = royalty_summary.streams     + EXCLUDED.streams,
                        net_revenue     = royalty_summary.net_revenue + EXCLUDED.net_revenue,
                        artist_name_csv = EXCLUDED.artist_name_csv,
                        track_title_csv = EXCLUDED.track_title_csv
                """), {"id": import_id})
                _sc.commit()
                _imp_months = [r[0] for r in _sc.execute(_t(
                    "SELECT DISTINCT reporting_month FROM streaming_royalty WHERE import_id = :id"
                ), {"id": import_id}).fetchall()]
        except Exception:
            pass

        # Auto-map new individual artist names
        try:
            with r_engine.connect() as _nc:
                _new_csvs = [r[0] for r in _nc.execute(_t(
                    "SELECT DISTINCT artist_name_csv FROM streaming_royalty "
                    "WHERE import_id = :id AND artist_name_csv IS NOT NULL"
                ), {"id": import_id}).fetchall()]
            with app.app_context():
                _auto_map_individuals(_extract_individuals(_new_csvs))
        except Exception:
            pass

        # Rebuild ARD for affected months only
        try:
            from sqlalchemy import create_engine as _ce_ard
            _ard_eng = _ce_ard(
                r_engine.url.render_as_string(hide_password=False),
                poolclass=NullPool, connect_args={"connect_timeout": 10},
            )
            _rebuild_artist_detail(_ard_eng, months=_imp_months if _imp_months else None)
            _rebuild_artist_label_detail(_ard_eng, months=_imp_months if _imp_months else None)
            _ard_eng.dispose()
        except Exception:
            pass

        # Targeted cache invalidation + full prewarm for affected periods
        # "done" is set only after prewarm so dashboard is warm when user arrives
        try:
            _clear_cache_for_months(r_engine, _imp_months)
            _prewarm_affected_periods(r_engine, _imp_months)
        except Exception:
            pass

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
        if isinstance(url, dict):
            url = url.get("url", "")
        url = str(url) if url else ""
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://", 1)
        if url and "sslmode=" not in url and url.startswith("postgresql://"):
            url += ("&" if "?" in url else "?") + "sslmode=require"
        return url

    _ca = {"connect_timeout": 10}
    r_engine = create_engine(_pg(royalties_url), poolclass=NullPool,
                             connect_args=_ca) if royalties_url else None
    m_engine = create_engine(_pg(main_url), poolclass=NullPool,
                             connect_args=_ca) if main_url else None

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

        # Sync royalty_summary from this import's streaming_royalty rows
        _import_months = []
        _emit({"status": "processing", "message": "Syncing royalty summary..."})
        try:
            with r_engine.connect() as _sc:
                _sc.execute(_t("""
                    INSERT INTO royalty_summary
                        (reporting_month, isrc, artist_name_csv, platform, country,
                         track_title_csv, streams, net_revenue)
                    SELECT reporting_month, isrc, MAX(artist_name_csv), platform, country,
                           MAX(track_title_csv), SUM(total_quantity), SUM(total_net_revenue)
                      FROM streaming_royalty
                     WHERE import_id = :id
                     GROUP BY reporting_month, isrc, platform, country
                    ON CONFLICT (reporting_month, isrc, platform, country) DO UPDATE SET
                        streams         = royalty_summary.streams     + EXCLUDED.streams,
                        net_revenue     = royalty_summary.net_revenue + EXCLUDED.net_revenue,
                        artist_name_csv = EXCLUDED.artist_name_csv,
                        track_title_csv = EXCLUDED.track_title_csv
                """), {"id": import_id})
                _sc.commit()
                _import_months = [r[0] for r in _sc.execute(_t(
                    "SELECT DISTINCT reporting_month FROM streaming_royalty WHERE import_id = :id"
                ), {"id": import_id}).fetchall()]
        except Exception:
            pass

        # Collect new artist names
        try:
            with r_engine.connect() as _nc:
                _new_csvs = [r[0] for r in _nc.execute(_t(
                    "SELECT DISTINCT artist_name_csv FROM streaming_royalty "
                    "WHERE import_id = :id AND artist_name_csv IS NOT NULL"
                ), {"id": import_id}).fetchall()]
            _new_individuals_snap = _extract_individuals(_new_csvs)
        except Exception:
            _new_individuals_snap = set()

        # Auto-map artist names — ARD rebuild depends on canonical mappings
        _emit({"status": "processing", "message": "Mapping artist names..."})
        try:
            _auto_map_individuals(_new_individuals_snap)
        except Exception as _am_e:
            import logging as _lg_am
            _lg_am.getLogger(__name__).warning("Auto-map failed: %s", _am_e)

        # Rebuild ARD for affected months only
        _emit({"status": "processing", "message": "Rebuilding artist revenue cache..."})
        try:
            from sqlalchemy import create_engine as _ce_pi
            from sqlalchemy.pool import NullPool as _NP_pi
            _royalties_url_snap = _royalties_engine().url.render_as_string(hide_password=False)
            _eng_pi = _ce_pi(_royalties_url_snap, poolclass=_NP_pi)
            _rebuild_artist_detail(_eng_pi, months=_import_months if _import_months else None)
            _rebuild_artist_label_detail(_eng_pi, months=_import_months if _import_months else None)
            _eng_pi.dispose()
        except Exception as _ard_pi_e:
            import logging as _lg_pi
            _lg_pi.getLogger(__name__).warning("Post-import ARD rebuild failed: %s", _ard_pi_e)

        # Targeted cache invalidation then synchronous prewarm — "done" only after cache is warm
        _clear_cache_for_months(r_engine, _import_months)
        _emit({"status": "processing", "message": "Warming dashboard cache..."})
        try:
            _prewarm_affected_periods(r_engine, _import_months, emit_fn=_emit)
        except Exception:
            pass

        _emit({"status": "done",
               "rows_read":       row[0] if row else 0,
               "rows_aggregated": row[1] if row else 0,
               "rows_skipped":    row[2] if row else 0})

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
    """Background: pre-compute artist=all for every year×quarter into dashboard_cache.
    Per-artist combos are computed on-demand and cached on first access.
    Historical quarters already in DB cache are served instantly without recomputation.
    """
    from sqlalchemy import text as _t, create_engine
    from sqlalchemy.pool import NullPool
    shared_engine = db.engines.get('royalties') or db.engine
    db_url = shared_engine.url.render_as_string(hide_password=False)
    engine = create_engine(db_url, poolclass=NullPool)
    _engine_local.override = engine
    try:
        with engine.connect() as conn:
            year_rows = conn.execute(_t(
                "SELECT DISTINCT EXTRACT(year FROM reporting_month)::int "
                "FROM royalty_summary WHERE reporting_month IS NOT NULL ORDER BY 1"
            )).fetchall()
        years = [str(r[0]) for r in year_rows]
    except Exception:
        _prewarm_status["running"] = False
        _engine_local.override = None
        engine.dispose()
        return

    # Only warm artist=all combos: specific year×quarter + year totals, both views.
    # Skips year=all (full 6M-row scan) — cached on first user visit.
    combos = []
    for y in years:
        for qtr in ("1", "2", "3", "4"):
            combos.append((y, qtr, "all", "label"))
            combos.append((y, qtr, "all", "artist"))
        combos.append((y, "all", "all", "label"))
        combos.append((y, "all", "all", "artist"))

    # Phase 2: per-artist × all periods × both views
    try:
        with engine.connect() as _ac:
            p2_artist_rows = _ac.execute(_t(
                "SELECT DISTINCT artist_name FROM artist_royalty_detail ORDER BY 1"
            )).fetchall()
        p2_artists = [r[0] for r in p2_artist_rows if r[0]]
    except Exception:
        p2_artists = []

    # Per-artist periods: year×quarter + year totals only.
    # Skip ("all","all") per-artist — slowest combo, cached on first user access instead.
    p2_periods = [(y, q) for y in years for q in ("1", "2", "3", "4", "all")]
    p2_combos_count = len(p2_artists) * len(p2_periods) * 2

    total = len(combos) + p2_combos_count
    done  = 0
    _prewarm_status.update({"running": True, "done": 0, "total": total, "current_artist": "Warming cache…"})

    # Phase 1: artist="all" combos — parallel with ThreadPoolExecutor
    from concurrent.futures import ThreadPoolExecutor as _TPE1, as_completed as _asc1
    p1_combos = [(db_url, y, qtr, artist, view) for (y, qtr, artist, view) in combos]
    with _TPE1(max_workers=4) as _pool1:
        futs1 = [_pool1.submit(_prewarm_worker_fn, c) for c in p1_combos]
        for _ in _asc1(futs1):
            with _prewarm_counter_lock:
                done += 1
                _prewarm_status["done"] = done
    _cleanup_prewarm_engines()

    # Phase 2: per-artist combos — parallel, skip already-cached DB entries
    from concurrent.futures import ThreadPoolExecutor as _TPE2, as_completed as _asc2
    p2_combos = [
        (db_url, y, q, artist, view)
        for artist in p2_artists
        for (y, q) in p2_periods
        for view in ("label", "artist")
    ]
    _prewarm_status["current_artist"] = "Per-artist warming…"
    with _TPE2(max_workers=4) as _pool2:
        futs2 = [_pool2.submit(_prewarm_worker_fn, c) for c in p2_combos]
        for _ in _asc2(futs2):
            with _prewarm_counter_lock:
                done += 1
                _prewarm_status["done"] = done
    _cleanup_prewarm_engines()

    _prewarm_status.update({"running": False, "current_artist": ""})
    _engine_local.override = None
    engine.dispose()


_ARD_INSERT_SQL = """
    INSERT INTO artist_royalty_detail
        (artist_name, reporting_month, isrc, track_title, platform, country, streams, net_revenue)
    SELECT
        COALESCE(m.canonical_name, s.artist_name) AS artist_name,
        rs.reporting_month,
        rs.isrc,
        MAX(rs.track_title_csv)                        AS track_title,
        rs.platform,
        rs.country,
        ROUND(SUM(rs.streams * s.percentage / 100.0))::bigint AS streams,
        SUM(rs.net_revenue  * s.percentage / 100.0)   AS net_revenue
    FROM artist_royalty_split s
    JOIN royalty_summary rs ON rs.isrc = s.isrc
    LEFT JOIN artist_name_map m ON m.raw_name = s.artist_name AND m.status = 'confirmed'
    {where_clause}
    GROUP BY
        COALESCE(m.canonical_name, s.artist_name),
        rs.reporting_month, rs.isrc, rs.platform, rs.country
    ON CONFLICT (artist_name, reporting_month, isrc, platform, country) DO UPDATE
        SET streams=EXCLUDED.streams, net_revenue=EXCLUDED.net_revenue, track_title=EXCLUDED.track_title
"""


_ARD_BULK_SQL = """
    INSERT INTO artist_royalty_detail
        (artist_name, reporting_month, isrc, track_title, platform, country, streams, net_revenue)
    SELECT
        COALESCE(m.canonical_name, s.artist_name)           AS artist_name,
        rs.reporting_month,
        rs.isrc,
        MAX(rs.track_title_csv)                             AS track_title,
        rs.platform,
        rs.country,
        ROUND(SUM(rs.streams     * s.percentage / 100.0))::bigint AS streams,
        SUM(rs.net_revenue       * s.percentage / 100.0)          AS net_revenue
    FROM artist_royalty_split s
    JOIN royalty_summary rs ON rs.isrc = s.isrc AND rs.reporting_month = :month
    LEFT JOIN artist_name_map m ON m.raw_name = s.artist_name AND m.status = 'confirmed'
    GROUP BY COALESCE(m.canonical_name, s.artist_name), rs.reporting_month, rs.isrc, rs.platform, rs.country
"""

_ARD_PARTIAL_SQL = """
    INSERT INTO artist_royalty_detail
        (artist_name, reporting_month, isrc, track_title, platform, country, streams, net_revenue)
    SELECT
        COALESCE(m.canonical_name, s.artist_name)           AS artist_name,
        rs.reporting_month,
        rs.isrc,
        MAX(rs.track_title_csv)                             AS track_title,
        rs.platform,
        rs.country,
        ROUND(SUM(rs.streams     * s.percentage / 100.0))::bigint AS streams,
        SUM(rs.net_revenue       * s.percentage / 100.0)          AS net_revenue
    FROM artist_royalty_split s
    JOIN royalty_summary rs ON rs.isrc = s.isrc
    LEFT JOIN artist_name_map m ON m.raw_name = s.artist_name AND m.status = 'confirmed'
    WHERE COALESCE(m.canonical_name, s.artist_name) = ANY(:canonical_names)
    GROUP BY COALESCE(m.canonical_name, s.artist_name), rs.reporting_month, rs.isrc, rs.platform, rs.country
"""

_ALD_BULK_SQL = """
    INSERT INTO artist_label_detail
        (artist_name, reporting_month, isrc, track_title, platform, country, streams, net_revenue)
    SELECT
        COALESCE(m.canonical_name, TRIM(a.val)) AS artist_name,
        rs.reporting_month,
        rs.isrc,
        MAX(rs.track_title_csv)  AS track_title,
        rs.platform,
        rs.country,
        SUM(rs.streams)          AS streams,
        SUM(rs.net_revenue)      AS net_revenue
    FROM royalty_summary rs
    CROSS JOIN LATERAL unnest(string_to_array(rs.artist_name_csv, ',')) AS a(val)
    LEFT JOIN artist_name_map m
        ON LOWER(TRIM(m.raw_name)) = LOWER(TRIM(a.val))
       AND m.status = 'confirmed'
    WHERE rs.reporting_month = :month
    GROUP BY COALESCE(m.canonical_name, TRIM(a.val)), rs.reporting_month, rs.isrc, rs.platform, rs.country
    ON CONFLICT (artist_name, reporting_month, isrc, platform, country) DO UPDATE SET
        streams     = EXCLUDED.streams,
        net_revenue = EXCLUDED.net_revenue,
        track_title = EXCLUDED.track_title
"""


def _rebuild_artist_detail(engine, artist_names=None, months=None):
    """Pre-aggregate artist_royalty_split × royalty_summary into artist_royalty_detail.
    Full rebuild (all None): DELETE all + INSERT per reporting_month.
    months=[date,...]: INSERT only those months (assumes caller deleted first, or adds on top).
    artist_names=[...]: DELETE those artists + INSERT filtered to those artists (all months).
    Always call from a background NullPool thread — never from a request thread.
    """
    import logging as _lg_ard
    from sqlalchemy import text as _t
    _log = _lg_ard.getLogger(__name__)
    _invalidate_isrc_csv_cache()
    if not _ard_rebuild_lock.acquire(blocking=True, timeout=5):
        _log.warning("_rebuild_artist_detail: another rebuild already running, skipping.")
        return
    try:
        with engine.connect() as conn:
            if months is not None:
                # Partial by month: delete affected rows then reinsert
                for month in months:
                    conn.execute(_t(
                        "DELETE FROM artist_royalty_detail WHERE reporting_month = :m"
                    ), {"m": month})
                    conn.execute(_t(_ARD_BULK_SQL), {"month": month})
                    conn.commit()
                    _log.warning("ARD rebuild: month %s done", month)
            elif artist_names is None:
                all_months = [r[0] for r in conn.execute(_t(
                    "SELECT DISTINCT reporting_month FROM royalty_summary ORDER BY 1"
                )).fetchall()]
                _log.warning("ARD full rebuild: %d months to process", len(all_months))
                conn.execute(_t("DELETE FROM artist_royalty_detail"))
                conn.commit()
                for month in all_months:
                    conn.execute(_t(_ARD_BULK_SQL), {"month": month})
                    conn.commit()
                    _log.warning("ARD full rebuild: month %s done", month)
                _log.warning("ARD full rebuild: complete")
            else:
                names = list(artist_names)
                if not names:
                    return
                conn.execute(_t("DELETE FROM artist_royalty_detail WHERE artist_name = ANY(:n)"), {"n": names})
                conn.execute(_t(_ARD_PARTIAL_SQL), {"canonical_names": names})
                conn.commit()
    except Exception as _e:
        _log.warning("_rebuild_artist_detail failed: %s", _e)
    finally:
        _ard_rebuild_lock.release()


def _normalize_ald_artist_names(engine):
    """Sync ALD artist_name to current canonical names from artist_name_map.
    When a canonical already exists for the same key, delete the stale-casing duplicate.
    Otherwise rename it. Raises on unexpected errors so callers can log accurately.
    """
    import logging as _lg_n
    from sqlalchemy import text as _t
    _log = _lg_n.getLogger(__name__)
    with engine.connect() as conn:
        # Step 1: delete rows whose canonical-cased twin already exists (avoid unique conflict)
        del_result = conn.execute(_t("""
            DELETE FROM artist_label_detail ald
            USING artist_name_map m
            WHERE LOWER(ald.artist_name) = LOWER(m.raw_name)
              AND m.status = 'confirmed'
              AND ald.artist_name != m.canonical_name
              AND EXISTS (
                SELECT 1 FROM artist_label_detail dup
                 WHERE dup.artist_name      = m.canonical_name
                   AND dup.reporting_month  = ald.reporting_month
                   AND dup.isrc             = ald.isrc
                   AND dup.platform         = ald.platform
                   AND dup.country          = ald.country
              )
        """))
        conn.commit()
        # Step 2: rename the remaining stale-casing rows (no duplicate risk now)
        upd_result = conn.execute(_t("""
            UPDATE artist_label_detail ald
               SET artist_name = m.canonical_name
              FROM artist_name_map m
             WHERE LOWER(ald.artist_name) = LOWER(m.raw_name)
               AND m.status = 'confirmed'
               AND ald.artist_name != m.canonical_name
        """))
        conn.commit()
    if del_result.rowcount or upd_result.rowcount:
        _log.warning(
            "ALD normalize: deleted %d duplicates, renamed %d rows to canonical casing",
            del_result.rowcount, upd_result.rowcount
        )


def _rebuild_artist_label_detail(engine, months=None):
    """Pre-aggregate royalty_summary (unnest artist_name_csv) into artist_label_detail.
    Provides a fast indexed path for label-view per-artist dashboard queries,
    replacing the slow ILIKE scan on royalty_summary (6M+ rows).
    months=None: full rebuild. months=[...]: delete+reinsert those months only.
    """
    import logging as _lg_ald
    from sqlalchemy import text as _t
    _log = _lg_ald.getLogger(__name__)
    try:
        with engine.connect() as conn:
            if months is None:
                all_months = [r[0] for r in conn.execute(_t(
                    "SELECT DISTINCT reporting_month FROM royalty_summary ORDER BY 1"
                )).fetchall()]
                conn.execute(_t("DELETE FROM artist_label_detail"))
                conn.commit()
                _log.warning("ALD full rebuild: %d months", len(all_months))
                for month in all_months:
                    conn.execute(_t(_ALD_BULK_SQL), {"month": month})
                    conn.commit()
                    _log.warning("ALD full rebuild: month %s done", month)
                _log.warning("ALD full rebuild: complete")
            else:
                for month in months:
                    conn.execute(_t(
                        "DELETE FROM artist_label_detail WHERE reporting_month = :m"
                    ), {"m": month})
                    conn.execute(_t(_ALD_BULK_SQL), {"month": month})
                    conn.commit()
                    _log.warning("ALD rebuild: month %s done", month)
        # Normalize stored artist names to current canonical names so exact lookups work
        _normalize_ald_artist_names(engine)
    except Exception as _e:
        _log.warning("_rebuild_artist_label_detail failed: %s", _e)


def _get_isrc_csv(engine, isrc_set: set) -> dict:
    """Return {isrc: artist_name_csv} using a module-level cache to avoid repeated royalty_summary scans."""
    from sqlalchemy import text
    missing = isrc_set - _isrc_csv_cache.keys()
    if missing:
        try:
            with engine.connect() as _c:
                rows = _c.execute(
                    text("SELECT DISTINCT ON (isrc) isrc, artist_name_csv "
                         "FROM royalty_summary WHERE isrc = ANY(:isrcs) "
                         "AND artist_name_csv IS NOT NULL AND artist_name_csv != '' "
                         "ORDER BY isrc"),
                    {"isrcs": list(missing)}
                ).fetchall()
            with _isrc_csv_cache_lock:
                for _isrc, _csv in rows:
                    _isrc_csv_cache[_isrc] = _csv
        except Exception:
            pass
    return {i: _isrc_csv_cache[i] for i in isrc_set if i in _isrc_csv_cache}


def _invalidate_isrc_csv_cache():
    """Clear ISRC→CSV cache on import/delete so stale artist names don't persist."""
    with _isrc_csv_cache_lock:
        _isrc_csv_cache.clear()


def _compute_dashboard_data_ard_empty(year, quarter, artist, engine):
    """Return a zero-revenue dashboard for an artist with no splits defined.
    Uses cached dropdown data — no full royalty_summary table scan."""
    _raw_strings, all_years = _get_dropdown_data(engine)
    _dd_name_map = {}
    try:
        from models import ArtistNameMap as _ANM_e
        _dd_name_map = {m.raw_name: m.canonical_name
                        for m in _ANM_e.query.filter_by(status='confirmed').all()}
    except Exception:
        pass
    _artist_names: set = set()
    for _s in _raw_strings:
        for _part in _s.split(','):
            _name = _part.strip()
            if _name:
                _artist_names.add(_dd_name_map.get(_name, _name))
    return {
        "kpi_total":   0.0,
        "by_artist":   [],
        "by_month":    [],
        "by_platform": [],
        "by_country":  [],
        "catalog":     [],
        "all_artists": sorted(_artist_names, key=str.lower),
        "all_years":   all_years,
    }


def _compute_dashboard_data_ard(year, quarter, artist, engine):
    """Fast path: query pre-aggregated artist_royalty_detail instead of royalty_summary.
    Only called when artist != 'all' and ARD has rows for this artist.
    Returns the same dict structure as _compute_dashboard_data().
    """
    from sqlalchemy import text

    conditions = ["ard.artist_name = :artist"]
    params: dict = {"artist": artist}
    if year and year != "all":
        y = int(year)
        conditions.append("ard.reporting_month >= :ys AND ard.reporting_month < :ye")
        params.update({"ys": f"{y}-01-01", "ye": f"{y + 1}-01-01"})
    if quarter and quarter != "all":
        q_ranges = {"1": (1, 4), "2": (4, 7), "3": (7, 10), "4": (10, 1)}
        q_start_m, q_end_m = q_ranges.get(str(quarter), (1, 1))
        if year and year != "all":
            # Specific year+quarter: efficient date-range (index-friendly)
            y = int(year)
            q_end_year = y + 1 if q_end_m == 1 else y
            conditions.append("ard.reporting_month >= :q_start AND ard.reporting_month < :q_end")
            params["q_start"] = f"{y}-{q_start_m:02d}-01"
            params["q_end"]   = f"{q_end_year}-{q_end_m:02d}-01"
        else:
            # year=all + specific quarter: must use EXTRACT(MONTH) across all years
            _q_month_map = {"1": [1, 2, 3], "2": [4, 5, 6], "3": [7, 8, 9], "4": [10, 11, 12]}
            _months = _q_month_map.get(str(quarter), [1, 2, 3])
            _month_phs = ", ".join(f":qm_{i}" for i in range(len(_months)))
            conditions.append(f"EXTRACT(MONTH FROM ard.reporting_month) IN ({_month_phs})")
            for _i, _m in enumerate(_months):
                params[f"qm_{_i}"] = _m
    where = " AND ".join(conditions)

    with engine.connect() as _conn:
        try:
            _conn.execute(text("SET statement_timeout = '60s'"))
        except Exception:
            pass

        def q(sql, p=None):
            return _conn.execute(text(sql), p if p is not None else params).fetchall()

        kpi_total = float(q(
            f"SELECT COALESCE(SUM(ard.net_revenue), 0) FROM artist_royalty_detail ard WHERE {where}"
        )[0][0])

        # Two-step approach: avoid the slow 4-column JOIN on 7M×6M rows.
        # Step 1 — sum ARD revenue per ISRC (fast: ix_ard_artist + date index)
        _isrc_rev_rows = q(f"""
            SELECT ard.isrc, COALESCE(SUM(ard.net_revenue), 0)
              FROM artist_royalty_detail ard WHERE {where}
             GROUP BY ard.isrc
        """)
        _isrc_rev = {r[0]: float(r[1]) for r in _isrc_rev_rows}
        # Step 2 — look up artist_name_csv per ISRC using module-level cache (avoids repeated royalty_summary scans)
        by_artist_rows = []
        if _isrc_rev:
            _isrc_csv_map = _get_isrc_csv(engine, set(_isrc_rev.keys()))
            _csv_buckets: dict = {}
            for _isrc, _csv in _isrc_csv_map.items():
                _csv_buckets[_csv] = _csv_buckets.get(_csv, 0.0) + _isrc_rev.get(_isrc, 0.0)
            by_artist_rows = sorted(_csv_buckets.items(), key=lambda x: x[1], reverse=True)

        by_month_rows = q(f"""
            SELECT TO_CHAR(ard.reporting_month, 'Mon YYYY') AS mo,
                   ard.reporting_month,
                   COALESCE(SUM(ard.net_revenue), 0) AS rev
              FROM artist_royalty_detail ard WHERE {where}
             GROUP BY ard.reporting_month ORDER BY ard.reporting_month
        """)

        by_platform_rows = q(f"""
            SELECT ard.platform, COALESCE(SUM(ard.net_revenue), 0) AS rev
              FROM artist_royalty_detail ard
             WHERE {where} AND ard.platform IS NOT NULL AND ard.platform != ''
             GROUP BY ard.platform ORDER BY rev DESC LIMIT 15
        """)

        by_country_all = q(f"""
            SELECT ard.country, COALESCE(SUM(ard.net_revenue), 0) AS rev
              FROM artist_royalty_detail ard
             WHERE {where} AND ard.country IS NOT NULL AND ard.country != ''
             GROUP BY ard.country ORDER BY rev DESC
        """)
        top5 = by_country_all[:5]
        other = sum(float(r[1]) for r in by_country_all[5:])
        country_data = [(r[0], float(r[1])) for r in top5]
        if other > 0:
            country_data.append(("Other", other))

        catalog_rows = q(f"""
            SELECT ard.isrc,
                   MAX(ard.track_title)              AS title,
                   MAX(ard.artist_name)              AS artist,
                   COALESCE(SUM(ard.streams), 0)     AS streams,
                   COALESCE(SUM(ard.net_revenue), 0) AS rev
              FROM artist_royalty_detail ard
             WHERE {where}
             GROUP BY ard.isrc ORDER BY rev DESC LIMIT 300
        """)
        catalog = [
            {"isrc": r[0], "title": r[1] or r[0], "artist": r[2] or "",
             "streams": int(r[3]), "revenue": float(r[4])}
            for r in catalog_rows
        ]

    _raw_strings, all_years = _get_dropdown_data(engine)

    _dd_name_map = {}
    try:
        from models import ArtistNameMap as _ANM_dd2
        _dd_name_map = {m.raw_name: m.canonical_name
                        for m in _ANM_dd2.query.filter_by(status='confirmed').all()}
    except Exception:
        pass
    _artist_names: set = set()
    for _s in _raw_strings:
        for _part in _s.split(','):
            _name = _part.strip()
            if _name:
                _artist_names.add(_dd_name_map.get(_name, _name))
    all_artists = sorted(_artist_names, key=str.lower)

    if _dd_name_map:
        _norm: dict = {}
        for row in by_artist_rows:
            parts = [p.strip() for p in row[0].split(',') if p.strip()]
            normalized = ', '.join(_dd_name_map.get(p, p) for p in parts)
            _norm[normalized] = _norm.get(normalized, 0.0) + float(row[1])
        by_artist_rows = sorted(_norm.items(), key=lambda x: x[1], reverse=True)

    return {
        "kpi_total":   kpi_total,
        "by_artist":   [{"name": r[0], "revenue": float(r[1])} for r in by_artist_rows],
        "by_month":    [{"month": r[0], "revenue": float(r[2])} for r in by_month_rows],
        "by_platform": [{"platform": r[0], "revenue": float(r[1])} for r in by_platform_rows],
        "by_country":  [{"country": k, "revenue": v} for k, v in country_data],
        "catalog":     catalog,
        "all_artists": all_artists,
        "all_years":   all_years,
        "all_periods": _dropdown_cache.get("all_periods", []),
    }


def _compute_dashboard_data_ald(year, quarter, artist, engine):
    """Fast path for label view per-artist: query artist_label_detail (indexed by artist_name)
    instead of scanning royalty_summary with ILIKE. Same structure as _compute_dashboard_data_ard
    but without percentage weighting — label view shows full track revenue.
    """
    from sqlalchemy import text

    conditions = ["ald.artist_name = :artist"]
    params: dict = {"artist": artist}
    if year and year != "all":
        y = int(year)
        conditions.append("ald.reporting_month >= :ys AND ald.reporting_month < :ye")
        params.update({"ys": f"{y}-01-01", "ye": f"{y + 1}-01-01"})
    if quarter and quarter != "all":
        q_ranges = {"1": (1, 4), "2": (4, 7), "3": (7, 10), "4": (10, 1)}
        q_start_m, q_end_m = q_ranges.get(str(quarter), (1, 1))
        if year and year != "all":
            y = int(year)
            q_end_year = y + 1 if q_end_m == 1 else y
            conditions.append("ald.reporting_month >= :q_start AND ald.reporting_month < :q_end")
            params["q_start"] = f"{y}-{q_start_m:02d}-01"
            params["q_end"]   = f"{q_end_year}-{q_end_m:02d}-01"
        else:
            _q_month_map = {"1": [1, 2, 3], "2": [4, 5, 6], "3": [7, 8, 9], "4": [10, 11, 12]}
            _months = _q_month_map.get(str(quarter), [1, 2, 3])
            _month_phs = ", ".join(f":qm_{i}" for i in range(len(_months)))
            conditions.append(f"EXTRACT(MONTH FROM ald.reporting_month) IN ({_month_phs})")
            for _i, _m in enumerate(_months):
                params[f"qm_{_i}"] = _m
    where = " AND ".join(conditions)

    with engine.connect() as _conn:
        try:
            _conn.execute(text("SET statement_timeout = '60s'"))
        except Exception:
            pass

        def q(sql, p=None):
            return _conn.execute(text(sql), p if p is not None else params).fetchall()

        kpi_total = float(q(
            f"SELECT COALESCE(SUM(ald.net_revenue), 0) FROM artist_label_detail ald WHERE {where}"
        )[0][0])

        _isrc_rev_rows = q(f"""
            SELECT ald.isrc, COALESCE(SUM(ald.net_revenue), 0)
              FROM artist_label_detail ald WHERE {where}
             GROUP BY ald.isrc
        """)
        _isrc_rev = {r[0]: float(r[1]) for r in _isrc_rev_rows}
        by_artist_rows = []
        if _isrc_rev:
            _isrc_csv_map = _get_isrc_csv(engine, set(_isrc_rev.keys()))
            _csv_buckets: dict = {}
            for _isrc, _csv in _isrc_csv_map.items():
                _csv_buckets[_csv] = _csv_buckets.get(_csv, 0.0) + _isrc_rev.get(_isrc, 0.0)
            by_artist_rows = sorted(_csv_buckets.items(), key=lambda x: x[1], reverse=True)

        by_month_rows = q(f"""
            SELECT TO_CHAR(ald.reporting_month, 'Mon YYYY') AS mo,
                   ald.reporting_month,
                   COALESCE(SUM(ald.net_revenue), 0) AS rev
              FROM artist_label_detail ald WHERE {where}
             GROUP BY ald.reporting_month ORDER BY ald.reporting_month
        """)

        by_platform_rows = q(f"""
            SELECT ald.platform, COALESCE(SUM(ald.net_revenue), 0) AS rev
              FROM artist_label_detail ald
             WHERE {where} AND ald.platform IS NOT NULL AND ald.platform != ''
             GROUP BY ald.platform ORDER BY rev DESC LIMIT 15
        """)

        by_country_all = q(f"""
            SELECT ald.country, COALESCE(SUM(ald.net_revenue), 0) AS rev
              FROM artist_label_detail ald
             WHERE {where} AND ald.country IS NOT NULL AND ald.country != ''
             GROUP BY ald.country ORDER BY rev DESC
        """)
        top5 = by_country_all[:5]
        other = sum(float(r[1]) for r in by_country_all[5:])
        country_data = [(r[0], float(r[1])) for r in top5]
        if other > 0:
            country_data.append(("Other", other))

        catalog_rows = q(f"""
            SELECT ald.isrc,
                   MAX(ald.track_title)              AS title,
                   MAX(ald.artist_name)              AS artist,
                   COALESCE(SUM(ald.streams), 0)     AS streams,
                   COALESCE(SUM(ald.net_revenue), 0) AS rev
              FROM artist_label_detail ald
             WHERE {where}
             GROUP BY ald.isrc ORDER BY rev DESC LIMIT 300
        """)
        catalog = [
            {"isrc": r[0], "title": r[1] or r[0], "artist": r[2] or "",
             "streams": int(r[3]), "revenue": float(r[4])}
            for r in catalog_rows
        ]

    _raw_strings, all_years = _get_dropdown_data(engine)
    _dd_name_map = {}
    try:
        from models import ArtistNameMap as _ANM_ald
        _dd_name_map = {m.raw_name: m.canonical_name
                        for m in _ANM_ald.query.filter_by(status='confirmed').all()}
    except Exception:
        pass
    _artist_names_set: set = set()
    for _s in _raw_strings:
        for _part in _s.split(','):
            _name = _part.strip()
            if _name:
                _artist_names_set.add(_dd_name_map.get(_name, _name))
    all_artists = sorted(_artist_names_set, key=str.lower)

    if _dd_name_map:
        _norm: dict = {}
        for row in by_artist_rows:
            parts = [p.strip() for p in row[0].split(',') if p.strip()]
            normalized = ', '.join(_dd_name_map.get(p, p) for p in parts)
            _norm[normalized] = _norm.get(normalized, 0.0) + float(row[1])
        by_artist_rows = sorted(_norm.items(), key=lambda x: x[1], reverse=True)

    return {
        "kpi_total":   kpi_total,
        "by_artist":   [{"name": r[0], "revenue": float(r[1])} for r in by_artist_rows],
        "by_month":    [{"month": r[0], "revenue": float(r[2])} for r in by_month_rows],
        "by_platform": [{"platform": r[0], "revenue": float(r[1])} for r in by_platform_rows],
        "by_country":  [{"country": k, "revenue": v} for k, v in country_data],
        "catalog":     catalog,
        "all_artists": all_artists,
        "all_years":   all_years,
        "all_periods": _dropdown_cache.get("all_periods", []),
    }


def _clear_dashboard_cache(engine=None):
    """Delete all persistent dashboard cache entries."""
    from sqlalchemy import text
    eng = engine or _royalties_engine()
    try:
        with eng.connect() as conn:
            conn.execute(text("DELETE FROM dashboard_cache WHERE cache_key != '_recovery_v2'"))
            conn.commit()
    except Exception:
        pass
    _dash_cache.clear()


def _clear_cache_for_months(engine, reporting_months):
    """Selective cache invalidation: only clear entries covering the given reporting months.
    Historical quarters not in reporting_months are preserved — their data never changes.
    Falls back to full clear if reporting_months is empty.
    """
    from sqlalchemy import text
    if not reporting_months:
        _clear_dashboard_cache(engine)
        return
    affected_years = set()
    affected_yq = set()
    for m in reporting_months:
        y = str(getattr(m, 'year', None) or str(m)[:4])
        month_num = getattr(m, 'month', None) or int(str(m)[5:7])
        q = str((month_num - 1) // 3 + 1)
        affected_years.add(y)
        affected_yq.add((y, q))
    try:
        with engine.connect() as conn:
            conn.execute(text("DELETE FROM dashboard_cache WHERE cache_key LIKE 'all|%'"))
            for y in affected_years:
                conn.execute(text("DELETE FROM dashboard_cache WHERE cache_key LIKE :p"),
                             {"p": f"{y}|all|%"})
            for y, q in affected_yq:
                conn.execute(text("DELETE FROM dashboard_cache WHERE cache_key LIKE :p"),
                             {"p": f"{y}|{q}|%"})
            conn.commit()
    except Exception:
        pass
    for key in list(_dash_cache.keys()):
        if not isinstance(key, tuple) or len(key) < 2:
            continue
        k_year, k_quarter = str(key[0]), str(key[1])
        if k_year == "all":
            _dash_cache.pop(key, None)
        elif k_year in affected_years:
            if k_quarter == "all" or (k_year, k_quarter) in affected_yq:
                _dash_cache.pop(key, None)
    _dropdown_cache.clear()


def _warm_if_missing(engine, year, quarter, artist, view):
    """Compute and store one dashboard combo only if not already in dashboard_cache."""
    from sqlalchemy import text as _t
    db_key = f"{year}|{quarter}|{artist}|{view}"
    try:
        with engine.connect() as _cc:
            if _cc.execute(_t("SELECT 1 FROM dashboard_cache WHERE cache_key=:k"),
                           {"k": db_key}).fetchone():
                return
    except Exception:
        pass
    try:
        _dashboard_data(year, quarter, artist, view)
    except Exception:
        pass


# Shared pool engine for all prewarm workers — pool_size=12 handles 6 workers × 2 nested connects.
# NullPool was creating a new TCP/SSL/auth roundtrip per connect() call (~100-200ms on Render),
# multiplied by ~4 connects per combo = 400-800ms overhead per combo. Shared pool amortizes this.
_prewarm_shared_engine = None
_prewarm_shared_engine_lock = threading.Lock()
import logging as _lg_pw


def _get_prewarm_engine(engine_url):
    """Return the shared prewarm engine, creating it once on first call."""
    global _prewarm_shared_engine
    if _prewarm_shared_engine is None:
        with _prewarm_shared_engine_lock:
            if _prewarm_shared_engine is None:
                from sqlalchemy import create_engine as _ce_t
                _prewarm_shared_engine = _ce_t(
                    engine_url,
                    pool_size=12,
                    max_overflow=4,
                    pool_pre_ping=True,
                    pool_recycle=300,
                    connect_args={"connect_timeout": 10},
                )
                _lg_pw.getLogger(__name__).info("prewarm: shared pool engine created (pool_size=12)")
    return _prewarm_shared_engine


def _cleanup_prewarm_engines():
    """Dispose the shared prewarm engine after the pool shuts down."""
    global _prewarm_shared_engine
    with _prewarm_shared_engine_lock:
        if _prewarm_shared_engine is not None:
            try:
                _prewarm_shared_engine.dispose()
            except Exception:
                pass
            _prewarm_shared_engine = None
    _engine_local.override = None


def _prewarm_worker_fn(args):
    """Thread worker: all workers share one pooled engine — connections reused, no per-connect overhead."""
    engine_url, year, quarter, artist, view = args
    try:
        _eng = _get_prewarm_engine(engine_url)
        _engine_local.override = _eng
        _warm_if_missing(_eng, year, quarter, artist, view)
    except Exception as _pw_exc:
        _lg_pw.getLogger(__name__).warning("prewarm worker error (%s|%s|%s|%s): %s",
                                           year, quarter, artist, view, _pw_exc)


def _prewarm_affected_periods(engine, months, emit_fn=None):
    """Compute and cache all artist × affected-period × both-view combos in parallel.
    Skips ("all","all") per-artist — those are warmed on first access or by startup Phase 2.
    emit_fn(msg) is optional — used by the SSE path to stream progress messages.
    """
    from sqlalchemy import text as _t
    from concurrent.futures import ThreadPoolExecutor as _TPE, as_completed as _asc

    # Derive (year, quarter) pairs for affected months + year totals; skip all|all per-artist
    periods = set()
    for m in months:
        y = str(getattr(m, 'year', None) or str(m)[:4])
        mn = getattr(m, 'month', None) or int(str(m)[5:7])
        q = str((mn - 1) // 3 + 1)
        periods.add((y, q))
        periods.add((y, "all"))
    # Include all|all only for artist="all" (global totals)
    all_time_period = ("all", "all")

    try:
        with engine.connect() as _ac:
            artist_rows = _ac.execute(_t(
                "SELECT DISTINCT artist_name FROM artist_royalty_detail ORDER BY 1"
            )).fetchall()
        artists = [r[0] for r in artist_rows if r[0]]
    except Exception:
        artists = []

    engine_url = engine.url.render_as_string(hide_password=False)
    all_combos = []
    # artist="all" gets all periods including all-time
    for (y, q) in sorted(periods | {all_time_period}):
        all_combos.append((engine_url, y, q, "all", "label"))
        all_combos.append((engine_url, y, q, "all", "artist"))
    # per-artist: only specific year×quarter + year totals (skip all-time — too expensive at import time)
    for (y, q) in sorted(periods):
        for artist in artists:
            all_combos.append((engine_url, y, q, artist, "label"))
            all_combos.append((engine_url, y, q, artist, "artist"))

    total_combos = len(all_combos)
    done = 0
    _prewarm_status.update({"running": True, "done": 0, "total": total_combos, "current_artist": "Warming…"})

    with _TPE(max_workers=4) as _pool:
        futures = [_pool.submit(_prewarm_worker_fn, c) for c in all_combos]
        for fut in _asc(futures):
            with _prewarm_counter_lock:
                done += 1
                _prewarm_status["done"] = done
            if emit_fn and done % 20 == 0:
                try:
                    emit_fn({"status": "processing",
                             "message": f"Warming cache… {done}/{total_combos} combos"})
                except Exception:
                    pass
    _cleanup_prewarm_engines()

    _prewarm_status.update({"running": False, "current_artist": ""})


def _get_dropdown_data(engine):
    """Return (raw_strings, all_years) for dropdown menus, cached in-process.
    The SELECT DISTINCT on 6.3M-row royalty_summary is expensive; cache survives
    across requests and is cleared by _clear_cache_for_months on each import.
    """
    from sqlalchemy import text
    cached = _dropdown_cache.get("ts")
    if cached and _time.time() - cached < _CACHE_TTL:
        return _dropdown_cache["raw_strings"], _dropdown_cache["all_years"]
    with engine.connect() as _c:
        raw_strings = [r[0] for r in _c.execute(text(
            "SELECT DISTINCT artist_name_csv FROM royalty_summary "
            "WHERE artist_name_csv IS NOT NULL AND artist_name_csv != ''"
        )).fetchall()]
        all_years = [int(r[0]) for r in _c.execute(text(
            "SELECT DISTINCT EXTRACT(year FROM reporting_month) FROM royalty_summary "
            "WHERE reporting_month IS NOT NULL ORDER BY 1 DESC"
        )).fetchall()]
        all_periods = [(int(r[0]), int(r[1])) for r in _c.execute(text(
            "SELECT DISTINCT EXTRACT(year FROM reporting_month)::int AS y, "
            "EXTRACT(quarter FROM reporting_month)::int AS q "
            "FROM royalty_summary WHERE reporting_month IS NOT NULL "
            "ORDER BY y DESC, q DESC"
        )).fetchall()]
        latest_month = _c.execute(text(
            "SELECT MAX(reporting_month) FROM royalty_summary WHERE reporting_month IS NOT NULL"
        )).scalar()
        if latest_month:
            _dropdown_cache["latest_year"]    = str(latest_month.year)
            _dropdown_cache["latest_quarter"] = str((latest_month.month - 1) // 3 + 1)
            _dropdown_cache["latest_period"]  = f"{latest_month.year}Q{(latest_month.month-1)//3+1}"
    _dropdown_cache["raw_strings"] = raw_strings
    _dropdown_cache["all_years"]   = all_years
    _dropdown_cache["all_periods"] = all_periods
    _dropdown_cache["ts"] = _time.time()
    return raw_strings, all_years


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

    try:
        result = _compute_dashboard_data(year, quarter, artist, view)
    except Exception as _e:
        import logging as _lg
        _lg.getLogger(__name__).warning("_dashboard_data query failed (%s|%s|%s|%s): %s", year, quarter, artist, view, _e)
        return {"error": "timeout", "kpi_total": 0, "by_artist": [], "by_month": [],
                "by_platform": [], "by_country": [], "catalog": [], "all_artists": [], "all_years": [],
                "all_periods": []}

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

    # royalty_summary stores raw artist names; artist_name_map applied via JOIN at query time
    _engine = _royalties_engine()
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
        # Find raw name aliases that map to this canonical (e.g. "EL FANTASMA" → "El Fantasma")
        try:
            from models import ArtistNameMap as _ANM_filter
            _raw_aliases = [m.raw_name for m in _ANM_filter.query.filter_by(
                canonical_name=artist, status='confirmed').all()
                if m.raw_name != artist]
        except Exception:
            _raw_aliases = []
        if _raw_aliases:
            _alias_clauses = [f"sr.artist_name_csv ILIKE :alias_{i}" for i in range(len(_raw_aliases))]
            conditions.append(f"(sr.artist_name_csv ILIKE :artist_pat OR {' OR '.join(_alias_clauses)})")
            for _i, _alias in enumerate(_raw_aliases):
                params[f"alias_{_i}"] = f"%{_alias}%"
        else:
            conditions.append("sr.artist_name_csv ILIKE :artist_pat")
        params["artist_pat"] = f"%{artist}%"

        # Label view fast path: use artist_label_detail (indexed by artist_name, no ILIKE needed)
        if view == "label":
            try:
                with _engine.connect() as _ald_chk:
                    # Case-insensitive lookup: ALD may store different casing than the URL param
                    _ald_row = _ald_chk.execute(
                        text("SELECT artist_name FROM artist_label_detail "
                             "WHERE LOWER(artist_name) = LOWER(:a) LIMIT 1"),
                        {"a": artist}
                    ).fetchone()
                if _ald_row:
                    # Use the exact casing stored in ALD so downstream WHERE clauses match
                    _ald_artist = _ald_row[0]
                    return _compute_dashboard_data_ald(year, quarter, _ald_artist, _engine)
                else:
                    import logging as _lg_ald
                    _lg_ald.getLogger(__name__).warning(
                        "ALD MISS: no entry for artist=%r — falling back to slow ILIKE. "
                        "Run a manual ARD rebuild to repopulate ALD.", artist
                    )
            except Exception as _ald_exc:
                import logging as _lg_ald2
                _lg_ald2.getLogger(__name__).warning(
                    "ALD check error for artist=%r: %s — falling back to ILIKE", artist, _ald_exc
                )

        # Fast path: use pre-aggregated artist_royalty_detail if available (artist view only)
        if view == "artist":
            try:
                import logging as _lg_fp
                with _engine.connect() as _chk:
                    _ard_row = _chk.execute(
                        text("SELECT artist_name FROM artist_royalty_detail "
                             "WHERE LOWER(artist_name) = LOWER(:a) LIMIT 1"),
                        {"a": artist}
                    ).fetchone()
                    if not _ard_row:
                        _split_check = _chk.execute(
                            text("SELECT 1 FROM artist_royalty_split WHERE artist_name = ANY(:names) LIMIT 1"),
                            {"names": [artist] + _raw_aliases}
                        ).fetchone()
                    else:
                        _split_check = True
                if _ard_row:
                    _ard_artist = _ard_row[0]
                    _lg_fp.getLogger(__name__).warning("ARD fast path: HIT for artist=%r", _ard_artist)
                    return _compute_dashboard_data_ard(year, quarter, _ard_artist, _engine)
                else:
                    _lg_fp.getLogger(__name__).warning("ARD fast path: MISS (no rows) for artist=%r", artist)
                    if not _split_check:
                        _lg_fp.getLogger(__name__).warning("ARD fast path: no splits for artist=%r — returning zeros", artist)
                        return _compute_dashboard_data_ard_empty(year, quarter, artist, _engine)
            except Exception as _fp_exc:
                import logging as _lg_fp2
                _lg_fp2.getLogger(__name__).warning("ARD fast path: ERROR for artist=%r: %s", artist, _fp_exc)
                pass  # fall through to slow CTE path

    where = " AND ".join(conditions)

    if view == "artist":
        if artist and artist != "all":
            # Single-artist view: match split by canonical name OR any raw alias
            _all_split_names = [artist] + _raw_aliases
            _name_ph = ', '.join(f':sname_{i}' for i in range(len(_all_split_names)))
            cte = (
                "WITH _splits AS ("
                "SELECT isrc, percentage/100.0 AS pct FROM artist_royalty_split "
                f"WHERE artist_name IN ({_name_ph})"
                ") "
            )
            for _i, _n in enumerate(_all_split_names):
                params[f'sname_{_i}'] = _n
        else:
            # All-artists view: sum all splits per ISRC (total artist payout)
            cte = (
                "WITH _splits AS ("
                "SELECT isrc, SUM(percentage)/100.0 AS pct FROM artist_royalty_split GROUP BY isrc"
                ") "
            )
        base_from += " LEFT JOIN _splits _s ON _s.isrc = sr.isrc"
        rev_expr   = "sr.net_revenue * COALESCE(_s.pct, 0.0)"
    else:
        cte      = ""
        rev_expr = "sr.net_revenue"

    def q(sql, p=None):
        with _engine.connect() as conn:
            try:
                conn.execute(text("SET statement_timeout = '60s'"))
            except Exception:
                pass
            return conn.execute(text(cte + sql), p or params).fetchall()

    # KPI
    kpi_total = float(q(f"SELECT COALESCE(SUM({rev_expr}), 0) FROM {base_from} WHERE {where}")[0][0])

    # By artist — group by full collab string so "El Fantasma, Los Dos Carnales" is one bar.
    by_artist = q(f"""
        SELECT sr.artist_name_csv AS artist,
               COALESCE(SUM({rev_expr}), 0) AS rev
          FROM {base_from}
         WHERE {where} AND sr.artist_name_csv IS NOT NULL AND sr.artist_name_csv != ''
         GROUP BY sr.artist_name_csv ORDER BY rev DESC
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

    # Full catalog
    catalog_rows = q(f"""
        SELECT sr.isrc,
               MAX(sr.track_title_csv) AS title,
               MAX({artist_col}) AS artist,
               COALESCE(SUM(sr.streams), 0) AS streams,
               COALESCE(SUM({rev_expr}), 0) AS rev
          FROM {base_from} WHERE {where}
         GROUP BY sr.isrc ORDER BY rev DESC
         LIMIT 300
    """)
    catalog = [{"isrc": r[0], "title": r[1] or r[0], "artist": r[2] or "",
                "streams": int(r[3]), "revenue": float(r[4])}
               for r in catalog_rows]

    # Dropdown options — cached to avoid repeated full royalty_summary scans
    _raw_strings, all_years = _get_dropdown_data(_engine)
    _dd_name_map = {}
    try:
        from models import ArtistNameMap as _ANM_dd
        _dd_name_map = {m.raw_name: m.canonical_name
                        for m in _ANM_dd.query.filter_by(status='confirmed').all()}
    except Exception:
        pass
    _artist_names: set = set()
    for s in _raw_strings:
        for part in s.split(','):
            name = part.strip()
            if name:
                _artist_names.add(_dd_name_map.get(name, name))
    all_artists = sorted(_artist_names, key=str.lower)

    # Normalize by_artist: split collab strings → map each name → rejoin → re-aggregate
    if _dd_name_map:
        _norm_buckets: dict = {}
        for row in by_artist:
            parts = [p.strip() for p in row[0].split(',') if p.strip()]
            normalized = ', '.join(_dd_name_map.get(p, p) for p in parts)
            _norm_buckets[normalized] = _norm_buckets.get(normalized, 0.0) + float(row[1])
        by_artist = sorted(_norm_buckets.items(), key=lambda x: x[1], reverse=True)

    return {
        "kpi_total":   kpi_total,
        "by_artist":   [{"name": r[0], "revenue": float(r[1])} for r in by_artist],
        "by_month":    [{"month": r[0], "revenue": float(r[2])} for r in by_month],
        "by_platform": [{"platform": r[0], "revenue": float(r[1])} for r in by_platform],
        "by_country":  [{"country": k, "revenue": v} for k, v in country_data],
        "catalog":     catalog,
        "all_artists": all_artists,
        "all_years":   all_years,
        "all_periods": _dropdown_cache.get("all_periods", []),
    }


# ── Routes ─────────────────────────────────────────────────────────────────────

@bp.route("/streaming-royalties/ard-status")
def ard_status():
    """Quick diagnostic: ARD row counts + split coverage."""
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        from flask import jsonify
        return jsonify({"error": "access restricted"}), 403
    from flask import jsonify
    from sqlalchemy import text
    _engine = _royalties_engine()
    if not _engine:
        return jsonify({"error": "no royalties DB"}), 500
    with _engine.connect() as _c:
        ard_total   = _c.execute(text("SELECT COUNT(*) FROM artist_royalty_detail")).scalar()
        ard_artists = _c.execute(text("SELECT COUNT(DISTINCT artist_name) FROM artist_royalty_detail")).scalar()
        split_total = _c.execute(text("SELECT COUNT(*) FROM artist_royalty_split")).scalar()
        split_isrcs = _c.execute(text("SELECT COUNT(DISTINCT isrc) FROM artist_royalty_split")).scalar()
        rs_total    = _c.execute(text("SELECT COUNT(*) FROM royalty_summary")).scalar()
        sample = [dict(r._mapping) for r in _c.execute(text(
            "SELECT artist_name, COUNT(*) AS rows, SUM(net_revenue) AS revenue "
            "FROM artist_royalty_detail GROUP BY artist_name ORDER BY revenue DESC"
        )).fetchall()]
    return jsonify({
        "ard_total_rows": ard_total,
        "ard_distinct_artists": ard_artists,
        "split_total_rows": split_total,
        "split_distinct_isrcs": split_isrcs,
        "royalty_summary_rows": rs_total,
        "top_artists_by_revenue": sample,
    })


@bp.route("/streaming-royalties/ard-rebuild", methods=["POST"])
def ard_rebuild():
    """Manually trigger a full ARD rebuild in the background."""
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        from flask import jsonify
        return jsonify({"error": "access restricted"}), 403
    from flask import jsonify
    import threading
    from sqlalchemy import create_engine as _ce
    from sqlalchemy.pool import NullPool
    _engine = _royalties_engine()
    if not _engine:
        return jsonify({"error": "no royalties DB"}), 500
    _url = _engine.url.render_as_string(hide_password=False)
    def _bg():
        import logging
        from sqlalchemy import text as _t
        _lg = logging.getLogger(__name__)
        _lg.warning("Manual ARD rebuild: starting")
        _eng = _ce(_url, poolclass=NullPool)
        try:
            with _eng.connect() as _c:
                _missing = [r[0] for r in _c.execute(_t(
                    "SELECT DISTINCT reporting_month FROM royalty_summary "
                    "EXCEPT "
                    "SELECT DISTINCT reporting_month FROM artist_royalty_detail "
                    "ORDER BY 1"
                )).fetchall()]
            if _missing:
                _rebuild_artist_detail(_eng, months=_missing)
            else:
                _rebuild_artist_detail(_eng)
            # ALD: rebuild months missing from artist_label_detail
            with _eng.connect() as _c2:
                _ald_missing = [r[0] for r in _c2.execute(_t(
                    "SELECT DISTINCT reporting_month FROM royalty_summary "
                    "EXCEPT "
                    "SELECT DISTINCT reporting_month FROM artist_label_detail "
                    "ORDER BY 1"
                )).fetchall()]
            if _ald_missing:
                _rebuild_artist_label_detail(_eng, months=_ald_missing)
            else:
                _rebuild_artist_label_detail(_eng)
            _lg.warning("Manual ARD rebuild: complete")
        except Exception as _e:
            _lg.warning("Manual ARD rebuild failed: %s", _e)
        finally:
            _eng.dispose()
    threading.Thread(target=_bg, daemon=True).start()
    flash("Artist cache rebuild started in background. This may take a few minutes.", "success")
    return redirect(url_for("streaming_royalties.imports_list"))


@bp.route("/streaming-royalties/admin/ald-debug")
def ald_debug():
    """Admin: show which artists appear in the dropdown but are missing from ALD."""
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        from flask import jsonify as _jq
        return _jq({"error": "access restricted"}), 403
    from flask import jsonify as _jq
    from sqlalchemy import text as _t
    _engine = _royalties_engine()
    if not _engine:
        return _jq({"error": "no royalties DB"}), 500
    try:
        with _engine.connect() as _c:
            _ald_artists = {r[0] for r in _c.execute(_t(
                "SELECT DISTINCT artist_name FROM artist_label_detail"
            )).fetchall()}
            _ald_count = _c.execute(_t("SELECT COUNT(*) FROM artist_label_detail")).scalar()
        _raw_strings, _ = _get_dropdown_data(_engine)
        try:
            from models import ArtistNameMap as _ANM
            _name_map = {m.raw_name: m.canonical_name for m in _ANM.query.filter_by(status='confirmed').all()}
        except Exception:
            _name_map = {}
        _dd_artists: set = set()
        for _s in _raw_strings:
            for _part in _s.split(','):
                _n = _part.strip()
                if _n:
                    _dd_artists.add(_name_map.get(_n, _n))
        _missing = sorted(_dd_artists - _ald_artists)
        return _jq({
            "ald_row_count": _ald_count,
            "ald_artist_count": len(_ald_artists),
            "dropdown_artist_count": len(_dd_artists),
            "missing_from_ald": _missing,
        })
    except Exception as _e:
        return _jq({"error": str(_e)}), 500


@bp.route("/streaming-royalties")
def dashboard():
    if auth_required():
        return redirect(url_for("publishing.login"))

    _role = session.get("role", "")
    _session_artist = session.get("artist_name", "")

    if _role == "artist":
        if not _session_artist:
            flash("Your account is not linked to an artist. Contact an admin.", "error")
            return redirect(url_for("publishing.login"))
        artist = _session_artist
    else:
        if role_required(_ADMIN_ONLY):
            flash("Access restricted.", "error")
            return redirect(url_for("publishing.works_list"))
        artist = request.args.get("artist", "all")

    period  = request.args.get("period")
    view    = "artist" if _role == "artist" else request.args.get("view", "label")
    is_artist_user = (_role == "artist")

    try:
        _get_dropdown_data(_royalties_engine())
    except Exception:
        pass

    if period is None:
        period = _dropdown_cache.get("latest_period", "all")

    year, quarter = _parse_period(period)

    data = _dashboard_data(year, quarter, artist, view)
    # Inject fresh all_periods so new months appear immediately
    try:
        _get_dropdown_data(_royalties_engine())
        fresh_periods = _dropdown_cache.get("all_periods", [])
        if fresh_periods:
            data = dict(data)
            data["all_periods"] = fresh_periods
    except Exception:
        pass
    if is_artist_user:
        data = dict(data)
        data["all_artists"] = [artist]

    return render_template_string(
        _DASHBOARD_HTML,
        data=data, period=period,
        artist=artist, view=view,
        is_artist_user=is_artist_user,
        _sidebar_html=_sb(),
    )


def _parse_period(period):
    """Convert a period string like '2026Q1' → (year, quarter).
    'all' or None → ('all', 'all').
    """
    if not period or period == "all":
        return "all", "all"
    import re as _re
    m = _re.match(r"^(\d{4})Q([1-4])$", str(period))
    if m:
        return m.group(1), m.group(2)
    return "all", "all"


@bp.route("/streaming-royalties/data")
def dashboard_data():
    """JSON endpoint for filter-driven chart updates."""
    if auth_required():
        return jsonify({"error": "auth"}), 401

    _role = session.get("role", "")
    _session_artist = session.get("artist_name", "")

    period = request.args.get("period", "all")
    view   = "artist" if _role == "artist" else request.args.get("view", "label")

    if _role == "artist":
        artist = _session_artist or "all"
    else:
        if role_required(_ADMIN_ONLY):
            return jsonify({"error": "access denied"}), 403
        artist = request.args.get("artist", "all")

    year, quarter = _parse_period(period)
    data = _dashboard_data(year, quarter, artist, view)
    return jsonify(data)


@bp.route("/streaming-royalties/artist-names.json")
def artist_names_json():
    """Lightweight endpoint: returns canonical artist names for autocomplete."""
    if auth_required():
        return jsonify({"error": "auth"}), 401
    if role_required(_ADMIN_ONLY):
        return jsonify({"error": "access denied"}), 403
    from models import ArtistNameMap as _ANM
    names = sorted({
        m.canonical_name for m in _ANM.query.filter_by(status="confirmed").all()
        if m.canonical_name
    }, key=str.lower)
    return jsonify({"artists": names})


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


@bp.route("/streaming-royalties/upload-chunk", methods=["POST"])
def upload_chunk():
    if auth_required():
        return jsonify({"error": "auth"}), 401
    if role_required(_ADMIN_ONLY):
        return jsonify({"error": "forbidden"}), 403

    upload_id   = request.form.get("upload_id", "").strip()
    chunk_index = request.form.get("chunk_index", "")
    chunk_file  = request.files.get("chunk")

    if not upload_id or not chunk_index.isdigit() or not chunk_file:
        return jsonify({"error": "bad request"}), 400
    if not re.match(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', upload_id):
        return jsonify({"error": "invalid upload_id"}), 400

    chunk_dir = os.path.join(_UPLOAD_DIR, ".chunks", upload_id)
    os.makedirs(chunk_dir, exist_ok=True)
    chunk_file.save(os.path.join(chunk_dir, f"chunk_{int(chunk_index):06d}"))
    return jsonify({"ok": True})


@bp.route("/streaming-royalties/upload-finalize", methods=["POST"])
def upload_finalize():
    if auth_required():
        return jsonify({"error": "auth"}), 401
    if role_required(_ADMIN_ONLY):
        return jsonify({"error": "forbidden"}), 403

    data         = request.get_json(force=True)
    upload_id    = (data.get("upload_id") or "").strip()
    total_chunks = int(data.get("total_chunks") or 0)
    filename     = (data.get("filename") or "upload.csv").strip()

    if not upload_id or not re.match(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', upload_id):
        return jsonify({"error": "invalid upload_id"}), 400
    if total_chunks < 1:
        return jsonify({"error": "total_chunks must be >= 1"}), 400

    chunk_dir = os.path.join(_UPLOAD_DIR, ".chunks", upload_id)
    os.makedirs(_UPLOAD_DIR, exist_ok=True)

    safe_name = secure_filename(filename)
    ts   = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    dest = os.path.join(_UPLOAD_DIR, f"{ts}_{safe_name}")

    try:
        with open(dest, "wb") as out:
            for i in range(total_chunks):
                chunk_path = os.path.join(chunk_dir, f"chunk_{i:06d}")
                with open(chunk_path, "rb") as inp:
                    out.write(inp.read())
    finally:
        import shutil as _shutil
        _shutil.rmtree(chunk_dir, ignore_errors=True)

    from models import StreamingImport
    rec = StreamingImport(
        original_filename=filename,
        file_path=dest,
        status="pending",
        uploaded_by=session.get("username", ""),
    )
    db.session.add(rec)
    db.session.commit()

    app_obj = current_app._get_current_object()
    threading.Thread(target=_process_import, args=(app_obj, rec.id), daemon=True).start()

    return jsonify({"import_id": rec.id})


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


@bp.route("/streaming-royalties/import/<int:import_id>/retry", methods=["POST"])
def retry_import(import_id):
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    from models import StreamingImport
    rec = StreamingImport.query.get_or_404(import_id)
    if rec.status not in ("pending", "error", "processing"):
        flash("Import is already done.", "error")
        return redirect(url_for("streaming_royalties.imports_list"))

    rec.status = "pending"
    rec.error_message = None
    db.session.commit()

    app_obj = current_app._get_current_object()
    threading.Thread(target=_process_import, args=(app_obj, rec.id), daemon=True).start()

    flash("Import queued for processing.", "success")
    return redirect(url_for("streaming_royalties.import_status", import_id=rec.id))


@bp.route("/streaming-royalties/import/<int:import_id>/delete", methods=["POST"])
def delete_import(import_id):
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    from models import StreamingImport
    from sqlalchemy import text as _t
    StreamingImport.query.get_or_404(import_id)
    _engine = _royalties_engine()
    with _engine.connect() as _c:
        # Capture affected months before deleting
        _del_months = [r[0] for r in _c.execute(_t(
            "SELECT DISTINCT reporting_month FROM streaming_royalty WHERE import_id = :id"
        ), {"id": import_id}).fetchall()]

        _c.execute(_t("DELETE FROM streaming_royalty WHERE import_id = :id"), {"id": import_id})
        _c.execute(_t("DELETE FROM streaming_import WHERE id = :id"), {"id": import_id})
        _c.commit()

        # Rebuild royalty_summary for affected months from remaining streaming_royalty data
        for _m in _del_months:
            _c.execute(_t("DELETE FROM royalty_summary WHERE reporting_month = :m"), {"m": _m})
            _c.execute(_t("""
                INSERT INTO royalty_summary
                    (reporting_month, isrc, artist_name_csv, platform, country,
                     track_title_csv, streams, net_revenue)
                SELECT reporting_month, isrc, MAX(artist_name_csv), platform, country,
                       MAX(track_title_csv), SUM(total_quantity), SUM(total_net_revenue)
                  FROM streaming_royalty
                 WHERE reporting_month = :m
                 GROUP BY reporting_month, isrc, platform, country
                ON CONFLICT (reporting_month, isrc, platform, country) DO UPDATE SET
                    streams     = EXCLUDED.streams,
                    net_revenue = EXCLUDED.net_revenue,
                    artist_name_csv = EXCLUDED.artist_name_csv,
                    track_title_csv = EXCLUDED.track_title_csv
            """), {"m": _m})
        _c.commit()

    # Invalidate cache for affected months; rebuild ARD in background
    _clear_cache_for_months(_engine, _del_months)
    if _del_months:
        def _bg_del():
            try:
                from sqlalchemy import create_engine as _ce_del
                from sqlalchemy.pool import NullPool as _NP_del
                _bg_eng = _ce_del(
                    _engine.url.render_as_string(hide_password=False),
                    poolclass=_NP_del, connect_args={"connect_timeout": 10}
                )
                _rebuild_artist_detail(_bg_eng, months=_del_months)
                _rebuild_artist_label_detail(_bg_eng, months=_del_months)
                _prewarm_affected_periods(_bg_eng, _del_months)
                _bg_eng.dispose()
            except Exception:
                pass
        threading.Thread(target=_bg_del, daemon=True).start()

    flash("Import deleted.", "success")
    return redirect(url_for("streaming_royalties.imports_list"))


@bp.route("/streaming-royalties/import/<int:import_id>/backfill-summary", methods=["POST"])
def backfill_summary(import_id):
    """Re-sync royalty_summary + rebuild ARD for an existing done import.
    Needed when an import completed before the royalty_summary sync was added."""
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    from sqlalchemy import text as _t
    from sqlalchemy import create_engine as _ce
    from sqlalchemy.pool import NullPool

    _engine = _royalties_engine()
    try:
        with _engine.connect() as _c:
            _c.execute(_t("""
                INSERT INTO royalty_summary
                    (reporting_month, isrc, artist_name_csv, platform, country,
                     track_title_csv, streams, net_revenue)
                SELECT reporting_month, isrc, MAX(artist_name_csv), platform, country,
                       MAX(track_title_csv), SUM(total_quantity), SUM(total_net_revenue)
                  FROM streaming_royalty
                 WHERE import_id = :id
                 GROUP BY reporting_month, isrc, platform, country
                ON CONFLICT (reporting_month, isrc, platform, country) DO UPDATE SET
                    streams         = royalty_summary.streams     + EXCLUDED.streams,
                    net_revenue     = royalty_summary.net_revenue + EXCLUDED.net_revenue,
                    artist_name_csv = EXCLUDED.artist_name_csv,
                    track_title_csv = EXCLUDED.track_title_csv
            """), {"id": import_id})
            _c.commit()
            _imp_months = [r[0] for r in _c.execute(_t(
                "SELECT DISTINCT reporting_month FROM streaming_royalty WHERE import_id = :id"
            ), {"id": import_id}).fetchall()]

        def _bg_ard():
            import logging as _lg_bf
            try:
                _bg_engine = _ce(
                    _engine.url.render_as_string(hide_password=False),
                    poolclass=NullPool, connect_args={"connect_timeout": 10}
                )
                _rebuild_artist_detail(_bg_engine, months=_imp_months if _imp_months else None)
                _rebuild_artist_label_detail(_bg_engine, months=_imp_months if _imp_months else None)
                _prewarm_affected_periods(_bg_engine, _imp_months)
                _bg_engine.dispose()
            except Exception as _e:
                _lg_bf.getLogger(__name__).warning("backfill_summary ARD rebuild failed: %s", _e)

        threading.Thread(target=_bg_ard, daemon=True).start()
        flash(f"royalty_summary synced for import #{import_id}. ARD rebuild started in background.", "success")
    except Exception as e:
        flash(f"Backfill failed: {e}", "error")

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
    # Lazy-start: kick off pre-warm only once ALD is populated (ALD build runs at startup).
    # If ALD is still empty, skip — startup bg thread will reset total=0 when ready.
    if not _prewarm_status["running"] and _prewarm_status["total"] == 0:
        _ald_ready = False
        try:
            from sqlalchemy import text as _text_cs
            _eng_cs = _royalties_engine()
            with _eng_cs.connect() as _c_cs:
                _ald_ready = bool(_c_cs.execute(
                    _text_cs("SELECT 1 FROM artist_label_detail LIMIT 1")
                ).fetchone())
        except Exception:
            pass
        if _ald_ready and _prewarm_lock.acquire(blocking=False):
            _app = current_app._get_current_object()
            def _run_lazy():
                try:
                    with _app.app_context():
                        _prewarm_dashboard_cache()
                finally:
                    _prewarm_lock.release()
            threading.Thread(target=_run_lazy, daemon=True).start()
    resp = jsonify(_prewarm_status)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Cache-Control"] = "no-store"
    return resp


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

        is_multi = any("artist 1" in h.lower() for h in hdr)

        isrc_col = next((v for k, v in col.items() if "isrc" in k.lower()), None)
        upc_col  = next((v for k, v in col.items() if "upc"  in k.lower()), None)

        from models import Artist, ArtistNameMap as _ANM_upload
        from sqlalchemy import text as _text

        # Pre-load artist name → id map (1 query, avoids per-row lookups)
        artist_map = {a.name.lower(): a.id for a in Artist.query.all()}

        # Pre-load confirmed name mappings so raw names are normalized on upload
        _upload_name_map = {m.raw_name: m.canonical_name
                            for m in _ANM_upload.query.filter_by(status='confirmed').all()}

        rows_skipped = 0
        first_error  = None
        artists_matched   = set()
        artists_unmatched = set()
        pending = []  # list of dicts for bulk insert

        def _collect(isrc, artist_name, pct):
            # Normalize through artist_name_map so "Manue Peña" → "Manuel Peña" on upload
            artist_name = _upload_name_map.get(artist_name.strip(), artist_name.strip())
            artist_id = artist_map.get(artist_name.lower())
            if artist_id:
                artists_matched.add(artist_name)
            else:
                artists_unmatched.add(artist_name)
            pending.append({
                "isrc": isrc, "artist_name": artist_name,
                "artist_id": artist_id, "percentage": float(pct),
            })

        def _resolve_isrc(row):
            isrc = str(row[isrc_col]).strip().upper() if (isrc_col is not None and row[isrc_col]) else ""
            if not isrc:
                upc = str(row[upc_col]).strip() if (upc_col is not None and row[upc_col]) else ""
                if upc:
                    isrc = f"UPC:{upc}"
            return isrc

        if is_multi:
            if isrc_col is None:
                raise ValueError(f"Could not find ISRC column. Found: {hdr}")
            artist_pairs = []
            for i in range(1, 10):
                a_key = next((h for h in hdr if h.lower() in (f"artist {i}", f"artist{i}")), None)
                p_key = next((h for h in hdr if h.lower() in (
                    f"artist {i} %", f"artist{i}%", f"artist {i}%", f"artist{i} %", f"artist{i}%"
                )), None)
                if a_key and p_key:
                    artist_pairs.append((col[a_key], col[p_key]))

            for row in rows_iter:
                try:
                    isrc = _resolve_isrc(row)
                    if not isrc:
                        rows_skipped += 1
                        continue
                    for a_idx, p_idx in artist_pairs:
                        artist_name = str(row[a_idx]).strip() if row[a_idx] else ""
                        pct_raw     = row[p_idx]
                        if not artist_name or pct_raw is None:
                            continue
                        pct = decimal.Decimal(str(pct_raw))
                        if pct < 0:
                            continue
                        if abs(pct) <= 1:
                            pct = pct * 100
                        _collect(isrc, artist_name, pct)
                except Exception as row_err:
                    rows_skipped += 1
                    if first_error is None:
                        first_error = str(row_err)

        else:
            # Simple 3-column format: ISRC | Artist Name | Percentage
            artist_col = pct_col = None
            for k, v in col.items():
                kl = k.lower()
                if "artist" in kl:
                    artist_col = v
                elif "percent" in kl or "%" in kl or "split" in kl or "share" in kl:
                    pct_col = v

            if isrc_col is None or artist_col is None or pct_col is None:
                raise ValueError(f"Could not find ISRC, Artist, and Percentage columns. Found: {hdr}")

            for row in rows_iter:
                try:
                    isrc        = str(row[isrc_col]).strip().upper() if row[isrc_col] else ""
                    artist_name = str(row[artist_col]).strip() if row[artist_col] else ""
                    pct_raw     = row[pct_col]
                    if not isrc or not artist_name or pct_raw is None:
                        rows_skipped += 1
                        continue
                    pct = decimal.Decimal(str(pct_raw))
                    _collect(isrc, artist_name, pct)
                except Exception as row_err:
                    rows_skipped += 1
                    if first_error is None:
                        first_error = str(row_err)

        # Bulk upsert in batches of 500
        roy_engine = db.engines.get('royalties') or db.engine
        rows_loaded = rows_updated = 0
        _upsert_sql = _text("""
            INSERT INTO artist_royalty_split (isrc, artist_name, artist_id, percentage)
            VALUES (:isrc, :artist_name, :artist_id, :percentage)
            ON CONFLICT (isrc, artist_name) DO UPDATE SET
                percentage = EXCLUDED.percentage,
                artist_id  = EXCLUDED.artist_id
        """)
        BATCH = 500
        with roy_engine.connect() as _conn:
            for i in range(0, len(pending), BATCH):
                chunk = pending[i:i + BATCH]
                result = _conn.execute(_upsert_sql, chunk)
                # rowcount == 1 per insert (new) or 1 per update — treat all as loaded
                rows_loaded += len(chunk)
            _conn.commit()
        rows_updated = 0  # ON CONFLICT merges — report total as loaded

        # Register catalog artist names as canonicals, then normalize royalty_summary
        catalog_names = {row["artist_name"] for row in pending if row.get("artist_name")}
        _auto_map_individuals(catalog_names)
        _start_normalize_bg()

        stats = {
            "rows_loaded":        rows_loaded,
            "rows_updated":       rows_updated,
            "rows_skipped":       rows_skipped,
            "first_error":        first_error,
            "header_detected":    {"isrc": isrc_col, "artist": "multi" if is_multi else artist_col, "pct": "multi" if is_multi else pct_col},
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


def _extract_individuals(artist_name_csv_list):
    """Split a list of collaboration CSV strings into a flat set of individual artist names."""
    result = set()
    for csv_str in artist_name_csv_list:
        if csv_str:
            for part in csv_str.split(','):
                part = part.strip()
                if part:
                    result.add(part)
    return result


def _auto_map_individuals(individual_names):
    """Auto-create artist_name_map entries for individual names using fuzzy matching.

    Confidence tiers:
      1.0   — exact match after accent/case normalization (auto-confirmed)
      ≥0.92 — fuzzy match, high confidence (auto-confirmed)
      0.75–0.91 — fuzzy match, needs review (pending_review)
      <0.75 — new canonical (maps to itself, accent-stripped)
    """
    from models import ArtistNameMap
    if not individual_names:
        return

    existing_raw = {m.raw_name for m in ArtistNameMap.query.all()}
    confirmed    = {m.canonical_name for m in ArtistNameMap.query.filter_by(status='confirmed').all()}

    new_entries = []
    for raw in sorted(individual_names):
        if raw in existing_raw:
            continue

        norm_raw = _norm(raw)

        # Tier 1: exact accent/case match against a known canonical
        exact = next((c for c in confirmed if _norm(c) == norm_raw), None)
        if exact:
            new_entries.append(ArtistNameMap(raw_name=raw, canonical_name=exact, confidence=1.0, status='confirmed'))
            existing_raw.add(raw)
            continue

        # Tier 2 & 3: fuzzy match
        if confirmed:
            best_score, best_c = max(
                ((difflib.SequenceMatcher(None, norm_raw, _norm(c)).ratio(), c) for c in confirmed),
                key=lambda x: x[0]
            )
        else:
            best_score, best_c = 0.0, None

        if best_score >= 0.92 and best_c:
            new_entries.append(ArtistNameMap(raw_name=raw, canonical_name=best_c,
                                             confidence=round(best_score, 3), status='confirmed'))
        elif best_score >= 0.75 and best_c:
            new_entries.append(ArtistNameMap(raw_name=raw, canonical_name=best_c,
                                             confidence=round(best_score, 3), status='pending_review'))
        else:
            canonical = _suggest_canonical([raw])
            new_entries.append(ArtistNameMap(raw_name=raw, canonical_name=canonical,
                                             confidence=None, status='confirmed'))
            confirmed.add(canonical)

        existing_raw.add(raw)

    if new_entries:
        try:
            db.session.bulk_save_objects(new_entries)
            db.session.commit()
            current_app.logger.info("_auto_map_individuals: added %d entries", len(new_entries))
        except Exception as e:
            db.session.rollback()
            current_app.logger.warning("_auto_map_individuals error: %s", e)


def _safe_canon(csv_str, name_map):
    """Apply name_map to each individual part of a comma-separated artist string.
    Skips any map entry whose canonical value itself contains a comma (corrupt entry).
    Also deduplicates parts while preserving order.
    """
    parts = [p.strip() for p in csv_str.split(',') if p.strip()]
    mapped = []
    seen = set()
    for p in parts:
        c = name_map.get(p, p)
        if ',' in c:
            c = p  # canonical contains commas → corrupt entry, use original
        if c not in seen:
            seen.add(c)
            mapped.append(c)
    return ', '.join(mapped)


def _normalize_specific_sync(raw_names, name_map):
    """Fast targeted normalization for a small set of just-saved raw names.
    Uses trigram-indexed ILIKE so it avoids scanning all 6M rows.
    Runs synchronously — call before clearing cache so the effect is immediate.
    """
    if not raw_names:
        return
    import logging as _log
    _lg = _log.getLogger(__name__)
    from sqlalchemy import text as _t

    def _canon(csv_str):
        return _safe_canon(csv_str, name_map)

    try:
        engine = _royalties_engine()
        conditions = " OR ".join(f"artist_name_csv ILIKE :p{i}" for i in range(len(raw_names)))
        params = {f"p{i}": f"%{n}%" for i, n in enumerate(raw_names)}
        with engine.connect() as conn:
            affected = [r[0] for r in conn.execute(_t(
                f"SELECT DISTINCT artist_name_csv FROM royalty_summary "
                f"WHERE artist_name_csv IS NOT NULL AND ({conditions})"
            ), params).fetchall()]
            updates = [{"c": _canon(v), "r": v} for v in affected if _canon(v) != v]
            for u in updates:
                conn.execute(_t("UPDATE royalty_summary SET artist_name_csv = :c WHERE artist_name_csv = :r"), u)
            if updates:
                conn.commit()
                _lg.info("_normalize_specific_sync: updated %d rows for %d names", len(updates), len(raw_names))
    except Exception as e:
        _lg.warning("_normalize_specific_sync error: %s", e)


def _normalize_royalty_summary_bg():
    """Background thread: apply confirmed artist_name_map to royalty_summary.artist_name_csv.
    Splits each value on commas, maps each individual part, rejoins.
    Clears dashboard cache when done.
    Must be called from within an app context (or via _start_normalize_bg).
    """
    import logging as _logging
    _log = _logging.getLogger(__name__)
    try:
        from models import ArtistNameMap
        from sqlalchemy import text as _t
        engine = _royalties_engine()

        name_map = {m.raw_name: m.canonical_name
                    for m in ArtistNameMap.query.filter_by(status='confirmed').all()}
        db.session.remove()  # release main DB connection before slow royalties work
        if not name_map:
            _clear_dashboard_cache()
            return

        def _canon(csv_str):
            return _safe_canon(csv_str, name_map)

        with engine.connect() as conn:
            # Use in-memory cache if warm to avoid slow SELECT DISTINCT over 6M rows
            _AN_CACHE_KEY = "artist_names_raw_csvs"
            _cached = _dash_cache.get(_AN_CACHE_KEY)
            if _cached and (_time.time() - _cached["ts"]) < 300:
                rows = [(_v,) for _v in _cached["data"]]
            else:
                rows = conn.execute(_t(
                    "SELECT DISTINCT artist_name_csv FROM royalty_summary "
                    "WHERE artist_name_csv IS NOT NULL AND artist_name_csv != ''"
                )).fetchall()

            # Repair pass: runs once per server session to fix rows corrupted by a prior bug
            # where canonical_name contained commas, causing names to be duplicated on split/join.
            if not _dash_cache.get("_repair_done"):
                def _dedup(csv_str):
                    parts = [p.strip() for p in csv_str.split(',') if p.strip()]
                    seen, out = set(), []
                    for p in parts:
                        if p not in seen:
                            seen.add(p)
                            out.append(p)
                    return ', '.join(out)

                repair_updates = {}
                for (raw,) in rows:
                    fixed = _dedup(raw)
                    if fixed != raw:
                        repair_updates[raw] = fixed
                if repair_updates:
                    for raw, fixed in repair_updates.items():
                        conn.execute(_t(
                            "UPDATE royalty_summary SET artist_name_csv = :c WHERE artist_name_csv = :r"
                        ), {"c": fixed, "r": raw})
                    conn.commit()
                    _log.warning("_normalize_royalty_summary_bg: repaired %d rows with duplicate artist names", len(repair_updates))
                    # Refresh rows to use clean values for the normalization pass below
                    rows = [(repair_updates.get(raw, raw),) for (raw,) in rows]
                _dash_cache["_repair_done"] = True

            updates = []
            for (raw,) in rows:
                canonical = _canon(raw)
                if canonical != raw:
                    updates.append({"c": canonical, "r": raw})

            if updates:
                for u in updates:
                    conn.execute(_t(
                        "UPDATE royalty_summary SET artist_name_csv = :c WHERE artist_name_csv = :r"
                    ), u)
                conn.commit()
                _log.info("_normalize_royalty_summary_bg: updated %d distinct values", len(updates))

        # VACUUM ANALYZE cleans up dead tuples from the UPDATEs above and refreshes
        # planner statistics — critical for GIN trigram index performance.
        # Requires autocommit (cannot run inside a transaction).
        # repair_updates is only defined when the repair block ran; updates is always defined.
        _did_update = bool(locals().get("repair_updates")) or bool(locals().get("updates"))
        if _did_update:
            try:
                with engine.execution_options(isolation_level="AUTOCOMMIT").connect() as _vc:
                    _vc.execute(_t("VACUUM ANALYZE royalty_summary"))
                _log.info("_normalize_royalty_summary_bg: VACUUM ANALYZE complete")
            except Exception as _ve:
                _log.warning("_normalize_royalty_summary_bg: VACUUM ANALYZE failed: %s", _ve)

        _clear_dashboard_cache()
        # Pre-warm the cache so users don't hit cold 5-minute queries after normalization.
        # Use the prewarm lock to prevent a simultaneous prewarm from cache_status route.
        try:
            if _prewarm_lock.acquire(blocking=False):
                try:
                    _prewarm_dashboard_cache()
                finally:
                    _prewarm_lock.release()
            else:
                _log.info("_normalize_royalty_summary_bg: prewarm already running, skipping")
        except Exception as _pw_e:
            _log.warning("_normalize_royalty_summary_bg: prewarm failed: %s", _pw_e)
    except Exception as e:
        _log.warning("_normalize_royalty_summary_bg error: %s", e)


def _start_normalize_bg():
    """Start _normalize_royalty_summary_bg in a daemon thread with a captured app context.
    If a normalization is already in progress, skip — the running one will apply all current mappings.
    """
    if not _normalize_lock.acquire(blocking=False):
        return  # already running; current run will apply the latest mappings
    _app = current_app._get_current_object()
    def _run():
        try:
            with _app.app_context():
                _normalize_royalty_summary_bg()
        finally:
            _normalize_lock.release()
    threading.Thread(target=_run, daemon=True).start()


@bp.route("/streaming-royalties/artist-names/confirm-pending", methods=["POST"])
def artist_names_confirm_pending():
    """Bulk-confirm all pending_review mappings."""
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))
    from models import ArtistNameMap
    pending = ArtistNameMap.query.filter_by(status='pending_review').all()
    for m in pending:
        m.status = 'confirmed'
    db.session.commit()
    _start_normalize_bg()
    flash(f"Confirmed {len(pending)} pending mapping(s). Normalizing data in background.", "success")
    return redirect(url_for("streaming_royalties.artist_names"))


@bp.route("/streaming-royalties/artist-names/rename-canonical", methods=["POST"])
def artist_names_rename_canonical():
    """Rename a canonical artist name everywhere: artist_name_map + artist_royalty_split,
    then rebuild ARD and prewarm dashboard cache in background."""
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    old_name = (request.form.get("old_name") or "").strip()
    new_name = (request.form.get("new_name") or "").strip()
    if not old_name or not new_name or old_name == new_name:
        flash("Provide both old and new names.", "error")
        return redirect(url_for("streaming_royalties.artist_names"))

    from sqlalchemy import text as _t
    eng = _royalties_engine()
    try:
        with eng.connect() as _c:
            # Update all artist_name_map rows where canonical_name = old_name
            r1 = _c.execute(_t(
                "UPDATE artist_name_map SET canonical_name = :new WHERE canonical_name = :old"
            ), {"new": new_name, "old": old_name})
            # Also rename the self-mapping row (raw_name == old_name)
            _c.execute(_t(
                "UPDATE artist_name_map SET raw_name = :new, canonical_name = :new "
                "WHERE raw_name = :old AND canonical_name = :old"
            ), {"new": new_name, "old": old_name})
            # Update artist_royalty_split
            r2 = _c.execute(_t(
                "UPDATE artist_royalty_split SET artist_name = :new WHERE artist_name = :old"
            ), {"new": new_name, "old": old_name})
            _c.commit()
        flash(
            f"Renamed '{old_name}' → '{new_name}': "
            f"{r1.rowcount} map entries, {r2.rowcount} split entries updated. "
            "Rebuilding ARD + warming cache in background…",
            "success",
        )
    except Exception as e:
        flash(f"Error renaming: {e}", "error")
        return redirect(url_for("streaming_royalties.artist_names"))

    _splits_cache.clear()
    _clear_dashboard_cache(eng)

    # Background: rebuild ARD for new name + prewarm
    _ren_url = eng.url.render_as_string(hide_password=False)
    _ren_app = current_app._get_current_object()
    def _run_rename_ard(_url=_ren_url, _app=_ren_app, _new=new_name):
        try:
            from sqlalchemy import create_engine as _ce, text as _t2
            from sqlalchemy.pool import NullPool
            _e = _ce(_url, poolclass=NullPool)
            with _app.app_context():
                _rebuild_artist_detail(_e, artist_names=[_new])
            try:
                with _e.connect() as _cc:
                    _mrows = _cc.execute(_t2(
                        "SELECT DISTINCT reporting_month FROM artist_royalty_detail WHERE artist_name = :n"
                    ), {"n": _new}).fetchall()
                _months = [r[0] for r in _mrows if r[0]]
                if _months:
                    _rebuild_artist_label_detail(_e, months=_months)
                    _prewarm_affected_periods(_e, _months)
            except Exception as _pw_e:
                import logging as _lg_r
                _lg_r.getLogger(__name__).warning("rename prewarm failed: %s", _pw_e)
            _e.dispose()
        except Exception as _e2:
            import logging as _lg_r2
            _lg_r2.getLogger(__name__).warning("rename ARD failed: %s", _e2)
    threading.Thread(target=_run_rename_ard, daemon=True).start()
    return redirect(url_for("streaming_royalties.artist_names"))


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
        try:
            action = request.form.get("action", "save")

            if action == "confirm_single":
                raw = request.form.get("raw", "").strip()
                m = ArtistNameMap.query.filter_by(raw_name=raw).first()
                if m:
                    m.status = 'confirmed'
                    db.session.commit()
                    _start_normalize_bg()
                    flash(f"Confirmed mapping: {raw} → {m.canonical_name}. Normalizing in background.", "success")
                return redirect(url_for("streaming_royalties.artist_names"))

            if action == "reject_single":
                raw = request.form.get("raw", "").strip()
                m = ArtistNameMap.query.filter_by(raw_name=raw).first()
                if m:
                    db.session.delete(m)
                    db.session.commit()
                    flash(f"Rejected mapping for: {raw}", "success")
                return redirect(url_for("streaming_royalties.artist_names"))

            # Default: save manual mappings form — bulk-load existing map to avoid per-row queries
            existing_map = {m.raw_name: m for m in ArtistNameMap.query.all()}
            count = int(request.form.get("count", 0))
            saved = deleted = 0
            saved_raws = []
            for i in range(count):
                raw = request.form.get(f"raw_{i}", "").strip()
                canonical = request.form.get(f"canonical_{i}", "").strip()
                if not raw:
                    continue
                existing = existing_map.get(raw)
                if not canonical:
                    # Empty field = remove mapping
                    if existing:
                        db.session.delete(existing)
                        deleted += 1
                else:
                    # canonical == raw means "confirm this name is already canonical"
                    if existing:
                        existing.canonical_name = canonical
                        existing.status = 'confirmed'
                        existing.updated_at = datetime.datetime.utcnow()
                    else:
                        db.session.add(ArtistNameMap(raw_name=raw, canonical_name=canonical,
                                                      confidence=1.0, status='confirmed'))
                    saved += 1
                    saved_raws.append(raw)
            db.session.commit()
            _clear_dashboard_cache()
            flash(f"Saved {saved} mapping(s), removed {deleted}. Artist name display updated.", "success")
            return redirect(url_for("streaming_royalties.artist_names"))

        except Exception as _post_err:
            db.session.rollback()
            current_app.logger.error("artist_names POST error: %s", _post_err)
            flash("Database connection error — please try again.", "error")
            return redirect(url_for("streaming_royalties.artist_names"))

    # GET — collect all individual names seen in streaming data + map entries
    # Cache the DISTINCT query result — it only changes after a new import
    from sqlalchemy import text
    _AN_CACHE_KEY = "artist_names_raw_csvs"
    _AN_TTL = 300  # 5 minutes
    _cached = _dash_cache.get(_AN_CACHE_KEY)
    if _cached and (_time.time() - _cached["ts"]) < _AN_TTL:
        raw_csvs = _cached["data"]
    else:
        try:
            with _engine.connect() as conn:
                raw_csvs = [r[0] for r in conn.execute(text(
                    "SELECT DISTINCT artist_name_csv FROM royalty_summary "
                    "WHERE artist_name_csv IS NOT NULL AND artist_name_csv != ''"
                )).fetchall()]
            _dash_cache[_AN_CACHE_KEY] = {"data": raw_csvs, "ts": _time.time()}
        except Exception:
            raw_csvs = _cached["data"] if _cached else []

    all_individuals = sorted(_extract_individuals(raw_csvs))

    try:
        all_map_entries = ArtistNameMap.query.order_by(ArtistNameMap.raw_name).all()
        existing_maps   = {m.raw_name: m for m in all_map_entries}
    except Exception:
        all_map_entries = []
        existing_maps   = {}

    # Pending review entries (auto-mapped, awaiting confirmation)
    pending = [m for m in all_map_entries if m.status == 'pending_review']

    # Build display rows for confirmed/manual section — individual names only
    # Group by normalized form so variants are clustered together
    groups = _group_by_normalization(all_individuals)
    suggestions = {name: _suggest_canonical(g) for g in groups for name in g}

    ordered = []
    for g in groups:
        for name in g:
            m = existing_maps.get(name)
            sugg = suggestions.get(name, "")
            # A name with no explicit map entry is "auto" if the suggestion equals itself
            # (already in canonical form, no action needed). Only "unmapped" if it would change.
            if m:
                status = m.status
            elif sugg == name:
                status = "auto"
            else:
                status = "unmapped"
            ordered.append({
                "raw":        name,
                "canonical":  m.canonical_name if m else "",
                "confidence": float(m.confidence) if (m and m.confidence is not None) else None,
                "status":     status,
                "group_size": len(g),
                "suggestion": sugg,
            })

    n_confirmed = sum(1 for m in all_map_entries if m.status == 'confirmed')
    n_auto      = sum(1 for row in ordered if row["status"] == "auto")
    # Only count as unmapped if the name would actually change after normalization
    n_unmapped  = sum(1 for row in ordered if row["status"] == "unmapped")

    return render_template_string(
        _ARTIST_NAMES_HTML,
        ordered=ordered,
        pending=pending,
        total=len(all_individuals),
        n_confirmed=n_confirmed,
        n_auto=n_auto,
        n_pending=len(pending),
        n_unmapped=n_unmapped,
        _sidebar_html=_sb("streaming_artist_names"),
    )


# ── HTML Templates ─────────────────────────────────────────────────────────────

from ui import _STYLE, _sidebar, _SB_JS, _mobile_nav  # noqa: E402


def _sb(active="streaming_royalties"):
    """Pre-render the sidebar so its Jinja2 role-guards are evaluated."""
    return render_template_string(_sidebar(active))


def _page(title, active, body):
    return f"""<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="manifest" href="/static/manifest.json"><link rel="apple-touch-icon" href="/static/labelmind-icon.png"><meta name="apple-mobile-web-app-capable" content="yes"><meta name="apple-mobile-web-app-status-bar-style" content="black-translucent"><meta name="apple-mobile-web-app-title" content="LabelMind"><script src="/static/pwa-nav.js"></script>
<title>{title} — AfinArte</title>{_STYLE}</head>
<body><div class="app">
{_sidebar(active)}
<div class="main"><div class="page">
{body}
</div></div></div>{_SB_JS}</body></html>"""


# ── Dashboard ─────────────────────────────────────────────────────────────────

_DASHBOARD_HTML = """<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="manifest" href="/static/manifest.json"><link rel="apple-touch-icon" href="/static/labelmind-icon.png"><meta name="apple-mobile-web-app-capable" content="yes"><meta name="apple-mobile-web-app-status-bar-style" content="black-translucent"><meta name="apple-mobile-web-app-title" content="LabelMind"><script src="/static/pwa-nav.js"></script>
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
.mobile-nav{display:none}
.mnav-item{display:flex;flex-direction:column;align-items:center;justify-content:center;color:#9ca3af;font-size:11px;text-decoration:none;gap:2px}
.mnav-item span{font-size:20px;line-height:1}
.mnav-item.active{color:#6385ff}
@media(max-width:768px){
  .sb{display:none!important}
  .main{margin-left:0!important}
  .sr-dash{padding-bottom:72px}
  .sr-header{padding:12px 14px;gap:10px}
  .sr-filters{gap:7px}
  .sr-filters select{font-size:12px;padding:6px 22px 6px 8px}
  .sr-grid{grid-template-columns:1fr;padding:10px 12px;gap:10px}
  .sr-grid-3{grid-template-columns:1fr;padding:0 12px 10px;gap:10px}
  .sr-kpi{min-width:unset;width:100%;box-sizing:border-box}
  .mobile-nav{display:flex;position:fixed;bottom:0;left:0;right:0;height:60px;background:#111827;border-top:1px solid rgba(255,255,255,.08);justify-content:space-around;align-items:center;z-index:9999}
}
</style>
</head><body style="background:#0d1117;margin:0">
<div class="app">""" + "{{ _sidebar_html|safe }}" + """
<div class="main">
<div class="sr-dash">
  <div class="sr-header">
    <div class="sr-logo">AfinArte <span>Music</span> Royalty System</div>
    <div class="sr-filters">
      {% if is_artist_user %}
      <span style="font-size:13px;font-weight:600;color:var(--t1);padding:6px 10px;background:var(--s1);border:1px solid var(--b0);border-radius:8px">{{ artist }}</span>
      {% else %}
      <select id="selArtist" onchange="applyFilters()">
        <option value="all"{% if artist=='all' %} selected{% endif %}>All Artists</option>
        {% for a in data.all_artists %}
        <option value="{{ a }}"{% if artist==a %} selected{% endif %}>{{ a }}</option>
        {% endfor %}
      </select>
      {% endif %}
      <select id="selPeriod" onchange="applyFilters()">
        <option value="all"{% if period=='all' %} selected{% endif %}>All Time</option>
        {% for y, q in (data.all_periods or []) %}
        {% set pval = y|string + 'Q' + q|string %}
        <option value="{{ pval }}"{% if period==pval %} selected{% endif %}>{{ y }} Q{{ q }}</option>
        {% endfor %}
      </select>
      {% if not is_artist_user %}
      <div class="sr-view-toggle">
        <button id="btnLabel" class="{{ 'active' if view=='label' else '' }}" onclick="setView('label')">Label View</button>
        <button id="btnArtist" class="{{ 'active' if view=='artist' else '' }}" onclick="setView('artist')">Artist View</button>
      </div>
      {% endif %}
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
      <div id="chartArtistWrap" style="overflow-y:auto;max-height:380px">
        <canvas id="chartArtist"></canvas>
      </div>
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
    <div class="sr-panel" style="grid-column:span 1;max-height:500px;overflow-y:auto">
      <div style="position:sticky;top:0;background:#161b27;z-index:2;padding-bottom:8px">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px">
          <span class="sr-panel-title" style="margin:0">Catalog</span>
          <span id="catalogCount" style="font-size:11px;color:var(--t3)">Top 300</span>
        </div>
        <input id="catalogSearch" type="text" placeholder="Search track or ISRC…"
          style="width:100%;box-sizing:border-box;background:#0d1117;border:1px solid var(--bdr);border-radius:6px;color:#edf0f8;padding:5px 8px;font-size:12px;outline:none"
          oninput="filterCatalog()">
      </div>
      {% if data.catalog %}
      <table class="sr-tbl" style="width:100%">
        <thead style="position:sticky;top:32px;background:#161b27;z-index:1">
          <tr><th class="num">Streams</th><th>Track</th><th class="num">Net Revenue</th></tr>
        </thead>
        <tbody id="catalogBody">
        {% for t in data.catalog %}
        <tr>
          <td class="num">{{ "{:,}".format(t.streams) }}</td>
          <td style="max-width:260px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{{ t.title }}</td>
          <td class="num">${{ "{:,.2f}".format(t.revenue) }}</td>
        </tr>
        {% endfor %}
        </tbody>
        <tfoot style="position:sticky;bottom:0;background:#161b27;border-top:1px solid rgba(255,255,255,.12)">
          <tr>
            <td class="num" id="catalogTotalStreams" style="font-weight:600;color:#edf0f8;padding-top:7px">{{ "{:,}".format(data.catalog|sum(attribute='streams')) }}</td>
            <td style="color:var(--t2);font-size:11px;padding-top:7px">Total</td>
            <td class="num" id="catalogTotalRevenue" style="font-weight:600;color:#edf0f8;padding-top:7px">${{ "{:,.2f}".format(data.catalog|sum(attribute='revenue')) }}</td>
          </tr>
        </tfoot>
      </table>
      <script>_catalogData = {{ data.catalog | tojson }};</script>
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

  const elA = document.getElementById('chartArtist');
  if(elA && d.by_artist?.length){
    const rowH = 26;
    const w = elA.parentElement.clientWidth || 500;
    const h = Math.max(280, d.by_artist.length * rowH);
    elA.width = w; elA.height = h;
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
  const artistEl=document.getElementById('selArtist');
  const artist=artistEl ? artistEl.value : {{ artist|tojson }};
  const period=document.getElementById('selPeriod').value;
  const overlay=document.getElementById('loading-overlay');
  var _loadTimer=setTimeout(function(){ overlay.style.display='flex'; }, 1000);
  fetch(`/streaming-royalties/data?period=${encodeURIComponent(period)}&artist=${encodeURIComponent(artist)}&view=${currentView}`)
    .then(r=>r.json()).then(d=>{
      clearTimeout(_loadTimer);
      overlay.style.display='none';
      document.getElementById('kpiVal').textContent='$'+d.kpi_total.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});
      // Catalog table + totals
      if(d.catalog){
        _catalogData=d.catalog;
        const s=document.getElementById('catalogSearch'); if(s) s.value='';
        filterCatalog();
        const totalStreams=d.catalog.reduce((s,t)=>s+t.streams,0);
        const totalRev=d.catalog.reduce((s,t)=>s+t.revenue,0);
        const ts=document.getElementById('catalogTotalStreams');
        const tr2=document.getElementById('catalogTotalRevenue');
        if(ts) ts.textContent=totalStreams.toLocaleString();
        if(tr2) tr2.textContent='$'+totalRev.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});
      }
      buildCharts(d);
    }).catch(function(){
      clearTimeout(_loadTimer);
      overlay.style.display='none';
    });
}
let _catalogData = [];
function filterCatalog(){
  const q=(document.getElementById('catalogSearch')?.value||'').toLowerCase();
  const rows=q?_catalogData.filter(t=>
    (t.title||'').toLowerCase().includes(q)||
    (t.artist||'').toLowerCase().includes(q)||
    (t.isrc||'').toLowerCase().includes(q)
  ):_catalogData;
  const tbody=document.getElementById('catalogBody');
  if(!tbody) return;
  tbody.innerHTML=rows.map(t=>`<tr>
    <td class="num">${t.streams.toLocaleString()}</td>
    <td style="max-width:260px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${t.isrc||''}">${t.title}</td>
    <td class="num">$${t.revenue.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})}</td>
  </tr>`).join('');
  const cc=document.getElementById('catalogCount');
  if(cc) cc.textContent=q?`${rows.length} / ${_catalogData.length}`:`Top ${_catalogData.length}`;
}
</script>

<div id="loading-overlay" style="display:none;position:fixed;inset:0;z-index:999;background:rgba(10,13,20,.78);backdrop-filter:blur(3px);align-items:center;justify-content:center;flex-direction:column;gap:18px">
  <div style="font-size:52px;animation:spin-gear 1.4s linear infinite">⚙️</div>
  <div style="color:#edf0f8;font-size:15px;font-weight:600;letter-spacing:.4px">Computing dashboard…</div>
  <div style="color:rgba(255,255,255,.45);font-size:12px">Large date ranges take up to 30 s on first load</div>
</div>
<style>@keyframes spin-gear{from{transform:rotate(0deg)}to{transform:rotate(360deg)}}</style>
""" + _mobile_nav() + """
</body></html>"""

@bp.app_template_filter("tojson")
def _tojson_filter(value):
    return Markup(json.dumps(value))


# ── Imports list page ─────────────────────────────────────────────────────────

_IMPORTS_HTML = """<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="manifest" href="/static/manifest.json"><link rel="apple-touch-icon" href="/static/labelmind-icon.png"><meta name="apple-mobile-web-app-capable" content="yes"><meta name="apple-mobile-web-app-status-bar-style" content="black-translucent"><meta name="apple-mobile-web-app-title" content="LabelMind"><script src="/static/pwa-nav.js"></script>
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
  <td style="white-space:nowrap">
    {% if imp.status in ('pending','processing') %}
    <a href="/streaming-royalties/import-status/{{ imp.id }}" class="btn btn-sec btn-sm">View</a>
    {% endif %}
    {% if imp.status in ('pending','error','processing') %}
    <form method="post" action="/streaming-royalties/import/{{ imp.id }}/retry" style="display:inline">
      <button class="btn btn-sm" style="color:var(--a)">&#9654; Retry</button>
    </form>
    {% endif %}
    {% if imp.status == 'done' %}
    <form method="post" action="/streaming-royalties/import/{{ imp.id }}/backfill-summary" style="display:inline"
          onsubmit="return confirm('Re-sync royalty_summary and rebuild dashboard cache for this import?')">
      <button class="btn btn-sm" style="color:var(--ac,#4f8ef7)" title="Sync to royalty_summary and rebuild ARD">Sync&#x2192;Dashboard</button>
    </form>
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
<div style="margin-top:24px;padding-top:16px;border-top:1px solid var(--b2);display:flex;gap:12px;align-items:center;flex-wrap:wrap">
  <form method="post" action="/streaming-royalties/clear-cache">
    <button class="btn btn-sm">Clear Dashboard Cache</button>
  </form>
  <form method="post" action="/streaming-royalties/ard-rebuild"
        onsubmit="return confirm('Rebuild the artist revenue cache (ARD + ALD) from scratch? This runs in the background and may take a few minutes.')">
    <button class="btn btn-sm" style="color:var(--ac,#4f8ef7)">Rebuild Artist Cache (ARD)</button>
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
    fetch(window.location.origin + '/streaming-royalties/cache-status', {credentials: 'omit', mode: 'cors'})
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
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="manifest" href="/static/manifest.json"><link rel="apple-touch-icon" href="/static/labelmind-icon.png"><meta name="apple-mobile-web-app-capable" content="yes"><meta name="apple-mobile-web-app-status-bar-style" content="black-translucent"><meta name="apple-mobile-web-app-title" content="LabelMind"><script src="/static/pwa-nav.js"></script>
<title>Upload CSV — AfinArte</title>""" + _STYLE + """
<style>
#dropZone{border:2px dashed var(--b0);border-radius:10px;padding:36px 24px;text-align:center;cursor:pointer;transition:border-color .2s,background .2s}
#dropZone.drag-over{border-color:var(--a);background:rgba(99,102,241,.06)}
#dropZone.has-file{border-color:var(--ag);background:rgba(52,199,89,.06)}
#progressWrap{display:none;margin-top:20px}
#progressBar{width:100%;height:8px;background:var(--b0);border-radius:4px;overflow:hidden;margin-bottom:8px}
#progressFill{height:100%;width:0%;background:var(--a);border-radius:4px;transition:width .2s}
#progressLabel{font-size:12px;color:var(--t2)}
#uploadBtn{margin-top:18px}
#fileInfo{margin-top:10px;font-size:13px;color:var(--t2)}
</style>
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
  Upload a Believe monthly royalty CSV. Any file size is supported — large files are uploaded in chunks automatically.
</p>
<input type="file" id="fileInput" accept=".csv" style="display:none">
<div id="dropZone">
  <div style="font-size:28px;margin-bottom:8px">&#8679;</div>
  <div style="font-weight:600;color:var(--t1)">Drop CSV here or click to browse</div>
  <div id="fileInfo">No file selected</div>
</div>
<div id="progressWrap">
  <div id="progressBar"><div id="progressFill"></div></div>
  <div id="progressLabel">Preparing…</div>
</div>
<button id="uploadBtn" class="btn btn-primary" disabled>&#8679; Upload &amp; Process</button>
</div>
</div></div></div>""" + _SB_JS + """
<script>
const CHUNK_SIZE = 5 * 1024 * 1024; // 5 MB
const dropZone   = document.getElementById('dropZone');
const fileInput  = document.getElementById('fileInput');
const uploadBtn  = document.getElementById('uploadBtn');
const fileInfo   = document.getElementById('fileInfo');
const progressWrap = document.getElementById('progressWrap');
const progressFill = document.getElementById('progressFill');
const progressLabel = document.getElementById('progressLabel');
let selectedFile = null;

dropZone.addEventListener('click', () => fileInput.click());
dropZone.addEventListener('dragover', e => { e.preventDefault(); dropZone.classList.add('drag-over'); });
dropZone.addEventListener('dragleave', () => dropZone.classList.remove('drag-over'));
dropZone.addEventListener('drop', e => {
  e.preventDefault();
  dropZone.classList.remove('drag-over');
  const f = e.dataTransfer.files[0];
  if (f) setFile(f);
});
fileInput.addEventListener('change', () => { if (fileInput.files[0]) setFile(fileInput.files[0]); });

function setFile(f) {
  if (!f.name.toLowerCase().endsWith('.csv')) {
    alert('Please select a .csv file.'); return;
  }
  selectedFile = f;
  const mb = (f.size / 1024 / 1024).toFixed(1);
  fileInfo.textContent = f.name + '  (' + mb + ' MB)';
  dropZone.classList.add('has-file');
  uploadBtn.disabled = false;
}

uploadBtn.addEventListener('click', async () => {
  if (!selectedFile) return;
  uploadBtn.disabled = true;
  dropZone.style.pointerEvents = 'none';
  progressWrap.style.display = 'block';

  const uploadId    = crypto.randomUUID();
  const totalChunks = Math.ceil(selectedFile.size / CHUNK_SIZE);
  const startTime   = Date.now();

  for (let i = 0; i < totalChunks; i++) {
    const start = i * CHUNK_SIZE;
    const blob  = selectedFile.slice(start, start + CHUNK_SIZE);
    const fd    = new FormData();
    fd.append('upload_id',    uploadId);
    fd.append('chunk_index',  i);
    fd.append('chunk',        blob, selectedFile.name);

    const res = await fetch('/streaming-royalties/upload-chunk', { method: 'POST', body: fd });
    if (!res.ok) {
      progressLabel.textContent = 'Upload failed on chunk ' + i + '. Please try again.';
      progressLabel.style.color = 'var(--ar)';
      uploadBtn.disabled = false;
      dropZone.style.pointerEvents = '';
      return;
    }

    const pct      = Math.round(((i + 1) / totalChunks) * 100);
    const elapsed  = (Date.now() - startTime) / 1000;
    const bytesUp  = (i + 1) * CHUNK_SIZE;
    const speed    = bytesUp / elapsed;
    const remaining = (selectedFile.size - bytesUp) / speed;
    const etaStr   = remaining > 60 ? Math.ceil(remaining / 60) + ' min' : Math.ceil(remaining) + ' s';
    progressFill.style.width  = pct + '%';
    progressLabel.textContent = pct + '% uploaded' + (i + 1 < totalChunks ? ' — ~' + etaStr + ' left' : '');
  }

  progressLabel.textContent = 'Finalizing…';
  const fin = await fetch('/streaming-royalties/upload-finalize', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ upload_id: uploadId, total_chunks: totalChunks, filename: selectedFile.name }),
  });

  if (!fin.ok) {
    progressLabel.textContent = 'Finalize failed. Please try again.';
    progressLabel.style.color = 'var(--ar)';
    uploadBtn.disabled = false;
    dropZone.style.pointerEvents = '';
    return;
  }

  const { import_id } = await fin.json();
  progressLabel.textContent = 'Done! Redirecting…';
  progressFill.style.background = 'var(--ag)';
  location.href = '/streaming-royalties/import-status/' + import_id;
});
</script>
</body></html>"""

# ── Bulk import form ──────────────────────────────────────────────────────────

_BULK_IMPORT_HTML = """<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="manifest" href="/static/manifest.json"><link rel="apple-touch-icon" href="/static/labelmind-icon.png"><meta name="apple-mobile-web-app-capable" content="yes"><meta name="apple-mobile-web-app-status-bar-style" content="black-translucent"><meta name="apple-mobile-web-app-title" content="LabelMind"><script src="/static/pwa-nav.js"></script>
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
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="manifest" href="/static/manifest.json"><link rel="apple-touch-icon" href="/static/labelmind-icon.png"><meta name="apple-mobile-web-app-capable" content="yes"><meta name="apple-mobile-web-app-status-bar-style" content="black-translucent"><meta name="apple-mobile-web-app-title" content="LabelMind"><script src="/static/pwa-nav.js"></script>
<title>Import Status — AfinArte</title>""" + _STYLE + """
<style>
@keyframes scan{0%{background-position:-200% center}100%{background-position:200% center}}
@keyframes spin{to{transform:rotate(360deg)}}
@keyframes numtick{0%{opacity:.3;transform:translateY(-4px)}100%{opacity:1;transform:translateY(0)}}
#scanBar{height:4px;border-radius:2px;margin:16px 0 20px;
  background:linear-gradient(90deg,var(--b0) 20%,var(--a) 50%,var(--b0) 80%);
  background-size:200% 100%;animation:scan 1.5s ease-in-out infinite}
#scanBar.hidden{display:none}
.spinner{display:inline-block;width:14px;height:14px;border:2px solid var(--b0);
  border-top-color:var(--am);border-radius:50%;animation:spin .8s linear infinite;
  vertical-align:middle;margin-right:6px}
.numtick{animation:numtick .2s ease-out}
.stat-row{display:flex;justify-content:space-between;align-items:center;
  padding:8px 0;border-bottom:1px solid var(--b0);font-size:13px}
.stat-row:last-child{border-bottom:none}
.stat-label{color:var(--t2)}
.stat-val{font-weight:600;color:var(--t1);min-width:70px;text-align:right}
</style>
</head><body><div class="app">
{{ _sidebar_html|safe }}
<div class="main"><div class="page">
<div class="ph"><div class="ph-left"><h1 class="ph-title">Import Status</h1></div>
<div class="ph-actions"><a href="/streaming-royalties/imports" class="btn btn-sec">&#8592; All Imports</a></div>
</div>
<div class="card" style="max-width:520px;margin-top:18px;padding:28px 24px">
  <p style="color:var(--t2);font-size:12px;margin-bottom:4px">File</p>
  <p style="color:var(--t1);font-size:14px;font-weight:600;margin-bottom:0;word-break:break-all">{{ rec.original_filename }}</p>

  <div id="scanBar" class="{% if rec.status in ('pending','processing') %}{% else %}hidden{% endif %}"></div>

  <div style="display:flex;align-items:center;gap:10px;margin:{% if rec.status in ('pending','processing') %}0{% else %}20px 0 16px{% endif %} 0 16px">
    <div id="statusBadge" style="font-size:15px;font-weight:600">
      {% if rec.status == 'done' %}<span style="color:var(--ag)">&#10003; Done</span>
      {% elif rec.status == 'error' %}<span style="color:var(--ar)">&#10007; Error</span>
      {% elif rec.status == 'processing' %}<span class="spinner"></span><span style="color:var(--am)">Processing…</span>
      {% else %}<span class="spinner"></span><span style="color:var(--t2)">Pending…</span>{% endif %}
    </div>
  </div>

  <div id="statsBox">
    <div class="stat-row"><span class="stat-label">Rows read</span><span class="stat-val" id="rowsRead">{{ "{:,}".format(rec.rows_read or 0) }}</span></div>
    <div class="stat-row"><span class="stat-label">Aggregated</span><span class="stat-val" id="rowsAgg">{{ "{:,}".format(rec.rows_aggregated or 0) }}</span></div>
    <div class="stat-row"><span class="stat-label">Skipped</span><span class="stat-val" id="rowsSkip">{{ "{:,}".format(rec.rows_skipped or 0) }}</span></div>
  </div>

  <div id="errBox" style="display:none;margin-top:14px;background:rgba(255,79,106,.08);border:1px solid rgba(255,79,106,.2);border-radius:8px;padding:12px;font-size:12px;color:var(--ar);word-break:break-all">
    {% if rec.status == 'error' %}{{ rec.error_message }}{% endif %}
  </div>

  <div id="doneActions" style="{% if rec.status != 'done' %}display:none;{% endif %}margin-top:20px">
    <a href="/streaming-royalties" class="btn btn-primary">&#128202; View Dashboard</a>
  </div>
</div>
</div></div></div>""" + _SB_JS + """
<script>
const importId = {{ rec.id }};
const finalStatuses = new Set(['done','error']);
let currentStatus = '{{ rec.status }}';

function tick(id, val){
  const el = document.getElementById(id);
  const str = (val||0).toLocaleString();
  if(el.textContent === str) return;
  el.textContent = str;
  el.classList.remove('numtick');
  void el.offsetWidth;
  el.classList.add('numtick');
}

function applyUpdate(d){
  currentStatus = d.status;
  tick('rowsRead', d.rows_read);
  tick('rowsAgg',  d.rows_aggregated);
  tick('rowsSkip', d.rows_skipped);
  const badge   = document.getElementById('statusBadge');
  const scanBar = document.getElementById('scanBar');
  if(d.status==='done'){
    scanBar.classList.add('hidden');
    badge.innerHTML='<span style="color:var(--ag)">&#10003; Done</span>';
    document.getElementById('doneActions').style.display='block';
    setTimeout(()=>{ location.href='/streaming-royalties'; }, 2500);
  } else if(d.status==='error'){
    scanBar.classList.add('hidden');
    badge.innerHTML='<span style="color:var(--ar)">&#10007; Error</span>';
    const errBox = document.getElementById('errBox');
    if(d.error_message){ errBox.textContent=d.error_message; }
    errBox.style.display='block';
  } else if(d.status==='processing'){
    scanBar.classList.remove('hidden');
    badge.innerHTML='<span class="spinner"></span><span style="color:var(--am)">Processing\u2026</span>';
  } else {
    scanBar.classList.remove('hidden');
    badge.innerHTML='<span class="spinner"></span><span style="color:var(--t2)">Pending\u2026</span>';
  }
}

if(!finalStatuses.has(currentStatus)){
  setTimeout(poll, 2000);
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
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="manifest" href="/static/manifest.json"><link rel="apple-touch-icon" href="/static/labelmind-icon.png"><meta name="apple-mobile-web-app-capable" content="yes"><meta name="apple-mobile-web-app-status-bar-style" content="black-translucent"><meta name="apple-mobile-web-app-title" content="LabelMind"><script src="/static/pwa-nav.js"></script>
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
  Upload the artist royalty percentage catalog (.xlsx). Two formats accepted:<br>
  <strong>Simple:</strong> ISRC | Artist Name | Percentage<br>
  <strong>Multi-artist catalog:</strong> ISRC (or UPC), Artist 1, Artist 1 %, Artist 2, Artist 2 %, … Artist 9, Artist 9 %<br>
  Percentages can be decimals (0.35) or whole numbers (35) — auto-detected. Column names are flexible.
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
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="manifest" href="/static/manifest.json"><link rel="apple-touch-icon" href="/static/labelmind-icon.png"><meta name="apple-mobile-web-app-capable" content="yes"><meta name="apple-mobile-web-app-status-bar-style" content="black-translucent"><meta name="apple-mobile-web-app-title" content="LabelMind"><script src="/static/pwa-nav.js"></script>
<title>Artist Names — AfinArte</title>""" + _STYLE + """
<style>
.an-stats{display:flex;gap:12px;margin-bottom:20px;flex-wrap:wrap}
.an-stat{background:var(--c2);border:1px solid var(--bdr);border-radius:8px;padding:10px 18px;font-size:13px;color:var(--t2)}
.an-stat strong{color:var(--t1);font-size:18px;display:block}
.an-section-title{font-size:14px;font-weight:600;color:var(--t1);margin:20px 0 10px;display:flex;align-items:center;gap:10px}
.an-group{background:var(--c2);border:1px solid var(--bdr);border-radius:10px;margin-bottom:8px;overflow:hidden}
.an-group-hd{background:rgba(99,133,255,.08);border-bottom:1px solid var(--bdr);padding:9px 14px;display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.an-badge{border-radius:4px;font-size:11px;padding:2px 7px;font-weight:600;white-space:nowrap}
.an-badge-var{background:#6385ff22;color:#6385ff;border:1px solid #6385ff44}
.an-badge-ok{background:#34d39916;color:#34d399;border:1px solid #34d39940}
.an-badge-pend{background:#f59e0b22;color:#f59e0b;border:1px solid #f59e0b44}
.an-badge-conf{background:#22c55e22;color:#22c55e;border:1px solid #22c55e44}
.an-row{display:grid;grid-template-columns:1fr 1fr auto;gap:8px;align-items:center;padding:7px 14px;border-bottom:1px solid rgba(255,255,255,.04)}
.an-row:last-child{border-bottom:none}
.an-raw{font-size:12.5px;color:var(--t2);font-family:monospace;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.an-clear{background:none;border:none;color:var(--t3);cursor:pointer;padding:2px 8px;font-size:13px;border-radius:4px}
.an-clear:hover{color:var(--err);background:rgba(255,79,106,.1)}
.pending-card{background:var(--c2);border:1px solid #f59e0b44;border-radius:10px;margin-bottom:8px;overflow:hidden}
.pending-row{display:grid;grid-template-columns:1fr 1fr auto auto;gap:8px;align-items:center;padding:9px 14px;border-bottom:1px solid rgba(255,255,255,.04)}
.pending-row:last-child{border-bottom:none}
.conf-pct{font-size:11px;color:var(--t3);font-style:italic}
.btn-confirm{background:#22c55e22;color:#22c55e;border:1px solid #22c55e55;border-radius:6px;padding:4px 12px;font-size:12px;cursor:pointer}
.btn-confirm:hover{background:#22c55e44}
.btn-reject{background:rgba(255,79,106,.1);color:var(--err);border:1px solid rgba(255,79,106,.3);border-radius:6px;padding:4px 10px;font-size:12px;cursor:pointer}
.btn-reject:hover{background:rgba(255,79,106,.2)}
</style>
</head><body><div class="app">{{ _sidebar_html|safe }}
<div class="main"><div class="page">
<div class="ph">
  <div class="ph-left"><h1 class="ph-title">Artist Name Consolidation</h1>
  <div class="ph-sub">Individual names are auto-mapped on every import. Case/accent variants are confirmed automatically. Fuzzy matches show here for review.</div></div>
  <div class="ph-actions"><a href="/streaming-royalties" class="btn btn-sec">&#128202; Dashboard</a></div>
</div>
{% with messages = get_flashed_messages(with_categories=true) %}
{% if messages %}{% for cat,msg in messages %}<div class="flash {{ cat }}">{{ msg }}</div>{% endfor %}{% endif %}
{% endwith %}

<div class="card" style="padding:12px 16px;margin-bottom:16px;display:flex;align-items:center;gap:10px;flex-wrap:wrap">
  <span style="font-size:13px;color:var(--t2);white-space:nowrap">&#9998; Rename canonical:</span>
  <form method="post" action="/streaming-royalties/artist-names/rename-canonical" style="margin:0;display:flex;gap:8px;align-items:center;flex:1;flex-wrap:wrap">
    <input class="inp" name="old_name" placeholder="Current name (exact)" style="flex:1;min-width:160px;font-size:13px">
    <span style="color:var(--t3)">&#8594;</span>
    <input class="inp" name="new_name" placeholder="Correct name" style="flex:1;min-width:160px;font-size:13px">
    <button type="submit" class="btn btn-sm" style="white-space:nowrap">Rename + Rebuild</button>
  </form>
</div>

<div class="an-stats">
  <div class="an-stat"><strong>{{ total }}</strong>Individual names</div>
  <div class="an-stat"><strong>{{ n_confirmed }}</strong>Mapped</div>
  <div class="an-stat"><strong style="color:#6385ff">{{ n_auto }}</strong>Already canonical</div>
  <div class="an-stat" style="{{ 'border-color:#f59e0b66' if n_pending else '' }}"><strong style="{{ 'color:#f59e0b' if n_pending else '' }}">{{ n_pending }}</strong>Pending review</div>
  <div class="an-stat" style="{{ 'border-color:#ff4f6a66' if n_unmapped else '' }}"><strong style="{{ 'color:var(--err)' if n_unmapped else '' }}">{{ n_unmapped }}</strong>Needs mapping</div>
</div>

{% if pending %}
<div class="an-section-title">
  <span class="an-badge an-badge-pend">{{ pending|length }} pending review</span>
  Auto-matched by fuzzy similarity — confirm or reject each
  <form method="post" style="margin:0;margin-left:auto">
    <input type="hidden" name="action" value="confirm_all_pending">
    <a href="/streaming-royalties/artist-names/confirm-pending" onclick="return confirm('Confirm all {{ pending|length }} pending mappings?')" class="btn btn-sm" style="background:#22c55e22;color:#22c55e;border:1px solid #22c55e55;text-decoration:none">&#10003; Confirm All ({{ pending|length }})</a>
  </form>
</div>
<div class="pending-card">
  {% for m in pending %}
  <div class="pending-row">
    <span class="an-raw" title="{{ m.raw_name }}">{{ m.raw_name }}</span>
    <span style="font-size:13px;color:var(--t1)">&#8594; {{ m.canonical_name }}
      {% if m.confidence %}<span class="conf-pct">({{ (m.confidence * 100)|int }}% match)</span>{% endif %}
    </span>
    <form method="post" style="margin:0">
      <input type="hidden" name="action" value="confirm_single">
      <input type="hidden" name="raw" value="{{ m.raw_name }}">
      <button type="submit" class="btn-confirm">&#10003; Confirm</button>
    </form>
    <form method="post" style="margin:0">
      <input type="hidden" name="action" value="reject_single">
      <input type="hidden" name="raw" value="{{ m.raw_name }}">
      <button type="submit" class="btn-reject">&#10005;</button>
    </form>
  </div>
  {% endfor %}
</div>
{% endif %}

<div class="an-section-title" style="margin-top:24px">
  All Individual Names
  <div style="margin-left:auto;display:flex;gap:10px;align-items:center">
    <input class="inp" id="srch" placeholder="&#128269; Search..." style="width:280px;font-size:13px" oninput="doSearch(this.value)">
    <label style="display:flex;align-items:center;gap:6px;font-size:13px;color:var(--t2);cursor:pointer;white-space:nowrap">
      <input type="checkbox" id="chkMulti" onchange="doSearch(document.getElementById('srch').value)"> Variants only
    </label>
    <button type="button" class="btn btn-sec btn-sm" onclick="autoApplyAll()">&#9889; Auto-fill suggestions</button>
  </div>
</div>

<form method="post">
  <input type="hidden" name="action" value="save">
  <input type="hidden" name="count" value="{{ ordered|length }}">
  {% for item in ordered %}
  <input type="hidden" name="raw_{{ loop.index0 }}" value="{{ item.raw }}">
  {% endfor %}

  {% set ns = namespace(gi=0, prev_norm='') %}
  {% set groups_seen = namespace(val=[]) %}

  {% for item in ordered %}
  {% set gi = loop.index0 %}
  {% if item.group_size > 1 and item.suggestion not in groups_seen.val %}
    {% if gi > 0 %}</div>{% endif %}
    <div class="an-group" data-search="{{ item.raw|lower }}" data-multi="y">
    <div class="an-group-hd">
      <span class="an-badge an-badge-var">{{ item.group_size }} variants</span>
      <span style="font-size:13px;color:var(--t1);font-weight:500">{{ item.suggestion[:60] }}{% if item.suggestion|length > 60 %}…{% endif %}</span>
      <div style="margin-left:auto;display:flex;gap:8px;align-items:center">
        <input class="inp gc-inp" id="gc_{{ gi }}" value="{{ item.suggestion }}" style="width:240px;font-size:12px" data-suggestion="{{ item.suggestion }}">
        <button type="button" class="btn btn-sm" onclick="applyGroup('{{ item.suggestion }}')">Apply to group</button>
      </div>
    </div>
    {% set groups_seen.val = groups_seen.val + [item.suggestion] %}
  {% elif item.group_size == 1 %}
    {% if gi > 0 and ordered[gi-1].group_size > 1 %}</div>{% endif %}
    <div class="an-group" data-search="{{ item.raw|lower }}" data-multi="n">
  {% endif %}
    <div class="an-row">
      <span class="an-raw" title="{{ item.raw }}">{{ item.raw }}
        {% if item.status == 'confirmed' and item.confidence is not none %}
          <span class="conf-pct" style="color:#22c55e">&#10003; auto-mapped</span>
        {% elif item.status == 'auto' %}
          <span class="conf-pct" style="color:#6385ff">&#10003; canonical</span>
        {% elif item.status == 'unmapped' %}
          <span class="conf-pct" style="color:var(--err)">needs mapping</span>
        {% endif %}
      </span>
      <input class="inp row-can" name="canonical_{{ gi }}"
             value="{{ item.canonical }}"
             placeholder="{{ item.suggestion }}"
             style="font-size:12.5px" data-suggestion="{{ item.suggestion }}" data-gs="{{ item.suggestion }}">
      <button type="button" class="an-clear" onclick="this.closest('.an-row').querySelector('.row-can').value=''" title="Clear">&#10005;</button>
    </div>
  {% if loop.last %}</div>{% endif %}
  {% endfor %}

  <div style="margin-top:20px;padding-top:16px;border-top:1px solid var(--bdr);display:flex;gap:12px">
    <button type="submit" class="btn btn-primary">&#10003; Save Mappings</button>
    <a href="/streaming-royalties" class="btn btn-sec">Cancel</a>
  </div>
</form>
</div></div></div>""" + _SB_JS + """
<script>
function applyGroup(sugg){
  document.querySelectorAll('.row-can[data-gs="'+sugg+'"]').forEach(el => el.value = sugg);
}
function autoApplyAll(){
  document.querySelectorAll('.row-can').forEach(el => {
    if(!el.value.trim()) el.value = el.dataset.suggestion || '';
  });
}
function doSearch(q){
  q = q.toLowerCase();
  const multiOnly = document.getElementById('chkMulti').checked;
  document.querySelectorAll('.an-group').forEach(g => {
    const matchSearch = !q || g.dataset.search.includes(q);
    const matchMulti  = !multiOnly || g.dataset.multi === 'y';
    g.style.display = (matchSearch && matchMulti) ? '' : 'none';
  });
}
</script>
</body></html>"""

# ── Split Gaps UI ─────────────────────────────────────────────────────────────

_SPLIT_GAPS_HTML = """<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="manifest" href="/static/manifest.json"><link rel="apple-touch-icon" href="/static/labelmind-icon.png"><meta name="apple-mobile-web-app-capable" content="yes"><meta name="apple-mobile-web-app-status-bar-style" content="black-translucent"><meta name="apple-mobile-web-app-title" content="LabelMind"><script src="/static/pwa-nav.js"></script>
<title>Split Gaps — AfinArte</title>""" + _STYLE + """
<style>
.gaps-banner{background:var(--c2);border:1px solid rgba(255,79,106,.3);border-radius:10px;padding:16px 20px;margin-bottom:24px;display:flex;gap:32px;align-items:center;flex-wrap:wrap}
.gaps-stat{text-align:center}.gaps-stat strong{display:block;font-size:22px;font-weight:700}
.gaps-stat span{font-size:11px;color:var(--t2);text-transform:uppercase;letter-spacing:.5px}
.gaps-ok{border-color:rgba(34,197,94,.3)}
.sec-hdr{display:flex;align-items:center;gap:12px;margin:24px 0 10px;font-weight:600;font-size:14px;color:var(--t1)}
.sec-badge{font-size:11px;font-weight:600;padding:2px 8px;border-radius:12px;background:rgba(255,79,106,.15);color:var(--err)}
.sec-badge-ok{background:rgba(34,197,94,.15);color:#22c55e}
.gaps-tbl{width:100%;border-collapse:collapse;font-size:13px}
.gaps-tbl th{text-align:left;padding:8px 10px;color:var(--t3);font-size:11px;text-transform:uppercase;letter-spacing:.4px;border-bottom:1px solid var(--bdr)}
.gaps-tbl td{padding:8px 10px;border-bottom:1px solid rgba(255,255,255,.04);vertical-align:middle}
.gaps-tbl tr:last-child td{border-bottom:none}
.arrow{color:var(--t3);margin:0 4px}
.rev-cell{font-weight:600;color:#6385ff;text-align:right}
.inp-sm{background:var(--c1);border:1px solid var(--bdr);border-radius:6px;color:var(--t1);padding:4px 8px;font-size:12px;width:160px}
.inp-pct{width:60px}
.btn-save{background:#22c55e22;color:#22c55e;border:1px solid #22c55e55;border-radius:6px;padding:4px 12px;font-size:12px;cursor:pointer;white-space:nowrap}
.btn-save:hover{background:#22c55e44}
.missing-tag{font-size:10px;font-weight:700;color:var(--err);background:rgba(255,79,106,.12);border-radius:4px;padding:1px 6px}
.sugg-pct{font-size:11px;color:#f59e0b;margin-left:4px}
</style>
</head><body><div class="app">{{ _sidebar_html|safe }}
<div class="main"><div class="page">
<div class="ph"><div class="ph-left">
  <h1 class="ph-title">Split Gaps</h1>
  <div class="ph-sub">ISRCs with missing or mismatched artist royalty splits — fix before artist view is accurate.</div>
</div>
<div class="ph-actions"><a href="/streaming-royalties" class="btn btn-sec">&#128202; Dashboard</a></div>
</div>
{% with messages = get_flashed_messages(with_categories=true) %}
{% if messages %}{% for cat,msg in messages %}<div class="flash {{ cat }}">{{ msg }}</div>{% endfor %}{% endif %}
{% endwith %}

<div class="gaps-banner {{ 'gaps-ok' if not mismatches and not missing else '' }}">
  <div class="gaps-stat"><strong style="color:var(--err)">{{ mismatches|length }}</strong><span>Name Mismatches</span></div>
  <div class="gaps-stat"><strong style="color:var(--err)">{{ missing|length }}</strong><span>ISRCs Missing Splits</span></div>
  <div class="gaps-stat"><strong style="color:#f59e0b">${{ "{:,.0f}".format(total_at_risk) }}</strong><span>Label Rev at Risk</span></div>
  {% if format_mismatch_count %}
  <div class="gaps-stat" title="Splits exist in catalog but ISRC format differs (hyphens). These are now matched automatically.">
    <strong style="color:#f59e0b">{{ format_mismatch_count }}</strong><span>Format Mismatches Fixed</span>
  </div>
  {% endif %}
  {% if not mismatches and not missing %}
  <div style="color:#22c55e;font-weight:600;font-size:14px;margin-left:auto">&#10003; All ISRCs have splits assigned</div>
  {% endif %}
</div>

<!-- Section A: Name mismatches -->
<div class="sec-hdr">
  <span>Section A — Name Mismatches</span>
  <span class="sec-badge {{ 'sec-badge-ok' if not mismatches else '' }}">{{ mismatches|length }}</span>
  {% if mismatches %}
  <form method="post" action="/streaming-royalties/split-gaps/fix-names" style="margin-left:auto">
    <button type="submit" class="btn btn-primary btn-sm" onclick="return confirm('Update {{ mismatches|length }} stored names to their canonical form?')">
      &#10227; Fix All Names
    </button>
  </form>
  {% endif %}
</div>
{% if mismatches %}
<div class="card" style="padding:0;overflow:hidden">
<table class="gaps-tbl">
  <thead><tr>
    <th>ISRC</th><th>Track</th><th>Stored Name</th><th></th><th>Canonical Name</th><th>%</th><th class="rev-cell">Label Rev</th>
  </tr></thead>
  <tbody>
  {% for r in mismatches %}
  <tr>
    <td style="font-family:monospace;font-size:11px;color:var(--t3)">{{ r.isrc }}</td>
    <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{{ r.title or r.isrc }}</td>
    <td style="color:var(--err)">{{ r.stored_name }}</td>
    <td class="arrow">&#8594;</td>
    <td style="color:#22c55e">{{ r.canonical_name }}</td>
    <td>{{ r.percentage }}%</td>
    <td class="rev-cell">${{ "{:,.0f}".format(r.label_rev or 0) }}</td>
  </tr>
  {% endfor %}
  </tbody>
</table>
</div>
{% else %}
<div class="card" style="color:var(--t2);font-size:13px;padding:16px 20px">No name mismatches — all split entries match canonical names.</div>
{% endif %}

<!-- Section B: Missing splits -->
<div class="sec-hdr" style="margin-top:32px">
  <span>Section B — Missing Splits</span>
  <span class="sec-badge {{ 'sec-badge-ok' if not missing else '' }}">{{ missing|length }}</span>
  {% if missing %}
  <button type="button" class="btn btn-primary btn-sm" style="margin-left:auto" onclick="saveAll()">
    &#10003; Save All Pre-filled
  </button>
  {% endif %}
</div>
{% if missing %}
<form id="saveAllForm" method="post" action="/streaming-royalties/split-gaps/save-all">
<div class="card" style="padding:0;overflow:hidden">
<table class="gaps-tbl">
  <thead><tr>
    <th>ISRC</th><th>Track</th><th>Release Date</th><th>Artist &amp; Split %</th><th class="rev-cell">Label Rev</th>
  </tr></thead>
  <tbody>
  {% set row_idx = namespace(n=0) %}
  {% for r in missing %}
    {% for row in r.artist_rows %}
    <tr>
      {% if loop.first %}
      <td rowspan="{{ r.artist_rows|length }}" style="font-family:monospace;font-size:11px;color:var(--t3);vertical-align:top;padding-top:10px">{{ r.isrc }}</td>
      <td rowspan="{{ r.artist_rows|length }}" style="max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;vertical-align:top;padding-top:10px" title="{{ r.title }}">{{ r.title }}</td>
      <td rowspan="{{ r.artist_rows|length }}" style="white-space:nowrap;color:var(--t2);font-size:12px;vertical-align:top;padding-top:10px">{{ r.release_date }}</td>
      {% endif %}
      <td>
        <input type="hidden" name="isrc_{{ row_idx.n }}" value="{{ r.isrc }}">
        <div style="display:flex;gap:6px;align-items:center">
          <input class="inp-sm" name="artist_{{ row_idx.n }}" value="{{ row.artist_name }}">
          <input class="inp-sm inp-pct" name="pct_{{ row_idx.n }}" type="number" min="0" max="100" step="0.01"
                 value="{{ row.pct if row.pct is not none else '' }}" placeholder="%">
          {% if row.source == 'contract' %}<span class="sugg-pct" title="From artist contract">&#9733; contract</span>
          {% elif row.source == 'track' %}<span class="sugg-pct" style="color:#6385ff" title="From track split">&#127925; track</span>
          {% elif row.source == 'release' %}<span class="sugg-pct" style="color:#8a99b3" title="From release (album-level)">&#128209; release</span>{% endif %}
        </div>
      </td>
      {% if loop.first %}
      <td rowspan="{{ r.artist_rows|length }}" class="rev-cell" style="vertical-align:top;padding-top:10px">${{ "{:,.0f}".format(r.label_rev or 0) }}</td>
      {% endif %}
    </tr>
    {% set row_idx.n = row_idx.n + 1 %}
    {% endfor %}
  {% endfor %}
  </tbody>
</table>
</div>
<input type="hidden" name="total_rows" value="{{ row_idx.n }}">
</form>
<script>
function saveAll(){
  var form = document.getElementById('saveAllForm');
  var total = parseInt(form.querySelector('[name=total_rows]').value);
  var skip = 0;
  for(var i=0;i<total;i++){
    var pct = form.querySelector('[name="pct_'+i+'"]');
    if(!pct || !pct.value.trim()) skip++;
  }
  var msg = skip > 0
    ? 'Save all pre-filled rows? ' + skip + ' row(s) with blank % will be skipped.'
    : 'Save all ' + total + ' rows?';
  if(confirm(msg)) form.submit();
}
</script>
{% else %}
<div class="card" style="color:var(--t2);font-size:13px;padding:16px 20px">No missing splits — every ISRC has at least one split entry.</div>
{% endif %}

</div></div></div>""" + _SB_JS + """</body></html>"""


@bp.route("/streaming-royalties/split-gaps")
def split_gaps():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    _cached = _splits_cache.get("data")
    if _cached and (_time.time() - _splits_cache.get("ts", 0)) < _SPLITS_CACHE_TTL:
        return render_template_string(
            _SPLIT_GAPS_HTML,
            mismatches=_cached["mismatches"],
            missing=_cached["missing"],
            total_at_risk=_cached["total_at_risk"],
            format_mismatch_count=_cached["format_mismatch_count"],
            _sidebar_html=_sb("streaming_split_gaps"),
        )

    from sqlalchemy import text as _t
    eng = _royalties_engine()

    # Section A: splits where stored artist_name has a different canonical in artist_name_map
    try:
        with eng.connect() as _c:
            mismatch_rows = _c.execute(_t("""
                SELECT s.isrc,
                       MAX(rs.track_title_csv) AS title,
                       MAX(rs.artist_name_csv) AS royalty_artists,
                       s.artist_name            AS stored_name,
                       m.canonical_name         AS canonical_name,
                       s.percentage,
                       COALESCE(SUM(rs.net_revenue), 0) AS label_rev
                  FROM artist_royalty_split s
                  JOIN artist_name_map m
                    ON m.raw_name = s.artist_name
                   AND m.status = 'confirmed'
                   AND LOWER(TRIM(m.canonical_name)) != LOWER(TRIM(s.artist_name))
                   AND NOT EXISTS (
                       SELECT 1 FROM artist_name_map m2
                        WHERE m2.status = 'confirmed'
                          AND m2.raw_name = m.canonical_name
                          AND LOWER(TRIM(m2.canonical_name)) = LOWER(TRIM(s.artist_name))
                   )
             LEFT JOIN royalty_summary rs ON rs.isrc = s.isrc
                 GROUP BY s.isrc, s.artist_name, m.canonical_name, s.percentage
                 ORDER BY label_rev DESC
            """)).fetchall()
    except Exception:
        mismatch_rows = []

    mismatches = [
        {"isrc": r[0], "title": r[1], "stored_name": r[3],
         "canonical_name": r[4], "percentage": float(r[5]),
         "label_rev": float(r[6])}
        for r in mismatch_rows
    ]

    # Section B: ISRCs in royalty_summary with no artist_royalty_split entry
    # Use REPLACE to normalize hyphens on both sides — catalog and streaming may differ in format
    try:
        with eng.connect() as _c:
            missing_rows = _c.execute(_t("""
                SELECT rs.isrc,
                       MAX(rs.track_title_csv) AS title,
                       MAX(rs.artist_name_csv) AS artists,
                       COALESCE(SUM(rs.net_revenue), 0) AS label_rev
                  FROM royalty_summary rs
                 WHERE NOT EXISTS (
                       SELECT 1 FROM artist_royalty_split s
                        WHERE REPLACE(s.isrc, '-', '') = REPLACE(rs.isrc, '-', '')
                 )
                 GROUP BY rs.isrc
                 ORDER BY label_rev DESC
            """)).fetchall()
            # Count splits that exist in catalog but with mismatched hyphen format
            format_mismatch_count = _c.execute(_t("""
                SELECT COUNT(DISTINCT s.isrc)
                  FROM artist_royalty_split s
                 WHERE NOT EXISTS (
                       SELECT 1 FROM royalty_summary rs WHERE rs.isrc = s.isrc
                 )
                   AND EXISTS (
                       SELECT 1 FROM royalty_summary rs
                        WHERE REPLACE(rs.isrc, '-', '') = REPLACE(s.isrc, '-', '')
                 )
            """)).scalar() or 0
    except Exception:
        missing_rows = []
        format_mismatch_count = 0

    # Pre-load artist_name_map and active contracts
    try:
        from models import ArtistNameMap as _ANM, ArtistContract as _AC, Artist as _Art
        import datetime as _dt
        _name_map = {m.raw_name: m.canonical_name
                     for m in _ANM.query.filter_by(status='confirmed').all()}
        _contracts = _AC.query.filter(
            (_AC.end_date == None) | (_AC.end_date >= _dt.date.today())
        ).order_by(_AC.start_date.desc()).all()
        _artist_pct = {}
        for c in _contracts:
            art = _Art.query.get(c.artist_id)
            if art and art.name.lower() not in _artist_pct:
                _artist_pct[art.name.lower()] = (art.name, float(c.royalty_percentage))
    except Exception:
        _name_map = {}
        _artist_pct = {}

    # Pre-load per-track splits: ArtistTrack first (authoritative), ArtistRelease as fallback.
    # Value tuple: (canonical_name, pct, source) where source is "track" or "release"
    _release_pct = {}
    _release_dates = {}
    try:
        from models import Track as _Track, ArtistRelease as _AR, ArtistTrack as _AT, Artist as _Art2, Release as _Rel
        _missing_isrcs = [r[0] for r in missing_rows]
        if _missing_isrcs:
            _tracks = _Track.query.filter(_Track.isrc.in_(_missing_isrcs)).all()
            for _trk in _tracks:
                rel = _Rel.query.get(_trk.release_id)
                if rel and rel.release_date:
                    _release_dates[_trk.isrc] = rel.release_date
                # 1. ArtistTrack — per-track (most precise)
                _at_rows = _AT.query.filter_by(track_id=_trk.id).all()
                _track_has_splits = False
                for _at in _at_rows:
                    _art2 = _Art2.query.get(_at.artist_id)
                    if _art2:
                        _release_pct.setdefault(_trk.isrc, {})[_art2.name.lower()] = \
                            (_art2.name, float(_at.royalty_percentage), "track")
                        _track_has_splits = True
                # 2. ArtistRelease fallback — only when no per-track rows exist
                if not _track_has_splits:
                    _ar_rows = _AR.query.filter_by(release_id=_trk.release_id).all()
                    for _ar in _ar_rows:
                        _art2 = _Art2.query.get(_ar.artist_id)
                        if _art2 and _ar.royalty_percentage is not None:
                            _release_pct.setdefault(_trk.isrc, {})[_art2.name.lower()] = \
                                (_art2.name, float(_ar.royalty_percentage), "release")
    except Exception:
        _release_pct = {}
        _release_dates = {}

    missing = []
    for r in missing_rows:
        isrc = r[0]
        artists_raw = r[2] or ""
        parts = [p.strip() for p in artists_raw.split(',') if p.strip()]
        is_collab = len(parts) > 1
        release_map = _release_pct.get(isrc, {})

        artist_rows = []
        for part in parts:
            canonical = _name_map.get(part, part)
            rel_match = release_map.get(canonical.lower()) or release_map.get(part.lower())
            if rel_match:
                artist_rows.append({"artist_name": rel_match[0], "pct": rel_match[1], "source": rel_match[2]})
            elif not is_collab:
                contract_match = _artist_pct.get(canonical.lower())
                if contract_match:
                    artist_rows.append({"artist_name": contract_match[0], "pct": contract_match[1], "source": "contract"})
                else:
                    artist_rows.append({"artist_name": canonical, "pct": None, "source": None})
            else:
                # Collab: name pre-filled from canonical, % blank — no contract suggestion
                artist_rows.append({"artist_name": canonical, "pct": None, "source": None})

        _rd = _release_dates.get(isrc)
        missing.append({
            "isrc": isrc,
            "title": r[1] or isrc,
            "label_rev": float(r[3]),
            "release_date": _rd.strftime("%b %d, %Y") if _rd else "—",
            "artist_rows": artist_rows,
        })

    total_at_risk = sum(r["label_rev"] for r in mismatches) + sum(r["label_rev"] for r in missing)

    _splits_cache["data"] = {
        "mismatches": mismatches,
        "missing": missing,
        "total_at_risk": total_at_risk,
        "format_mismatch_count": format_mismatch_count,
    }
    _splits_cache["ts"] = _time.time()

    return render_template_string(
        _SPLIT_GAPS_HTML,
        mismatches=mismatches,
        missing=missing,
        total_at_risk=total_at_risk,
        format_mismatch_count=format_mismatch_count,
        _sidebar_html=_sb("streaming_split_gaps"),
    )


@bp.route("/streaming-royalties/split-gaps/fix-names", methods=["POST"])
def split_gaps_fix_names():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    from sqlalchemy import text as _t
    eng = _royalties_engine()
    try:
        with eng.connect() as _c:
            # If a canonical row already exists for the same ISRC, delete the raw duplicate
            del_result = _c.execute(_t("""
                DELETE FROM artist_royalty_split s
                USING artist_name_map m
                WHERE m.raw_name = s.artist_name
                  AND m.status = 'confirmed'
                  AND LOWER(TRIM(m.canonical_name)) != LOWER(TRIM(s.artist_name))
                  AND EXISTS (
                    SELECT 1 FROM artist_royalty_split ex
                    WHERE ex.isrc = s.isrc
                      AND ex.artist_name = m.canonical_name
                  )
            """))
            # Update remaining raw-name rows (no collision possible now)
            upd_result = _c.execute(_t("""
                UPDATE artist_royalty_split s
                   SET artist_name = m.canonical_name
                  FROM artist_name_map m
                 WHERE m.raw_name = s.artist_name
                   AND m.status = 'confirmed'
                   AND LOWER(TRIM(m.canonical_name)) != LOWER(TRIM(s.artist_name))
            """))
            # Delete reverse mappings that create A→B / B→A cycles
            _c.execute(_t("""
                DELETE FROM artist_name_map m_rev
                USING artist_name_map m_fwd
                WHERE m_fwd.status = 'confirmed'
                  AND LOWER(TRIM(m_fwd.canonical_name)) != LOWER(TRIM(m_fwd.raw_name))
                  AND m_rev.raw_name = m_fwd.canonical_name
                  AND LOWER(TRIM(m_rev.canonical_name)) = LOWER(TRIM(m_fwd.raw_name))
                  AND m_rev.status = 'confirmed'
            """))
            _c.commit()
        _clear_dashboard_cache(eng)
        flash(
            f"Fixed {upd_result.rowcount} split entries "
            f"({del_result.rowcount} duplicates removed).",
            "success",
        )
    except Exception as e:
        flash(f"Error fixing names: {e}", "error")
    _splits_cache.clear()
    return redirect(url_for("streaming_royalties.split_gaps"))


@bp.route("/streaming-royalties/split-gaps/save-all", methods=["POST"])
def split_gaps_save_all():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    from sqlalchemy import text as _t
    from models import Artist as _Art
    eng = _royalties_engine()

    total = int(request.form.get("total_rows", 0))
    saved = skipped = 0
    all_months = []

    try:
        with eng.connect() as _c:
            for i in range(total):
                isrc        = (request.form.get(f"isrc_{i}") or "").strip().upper()
                artist_name = (request.form.get(f"artist_{i}") or "").strip()
                pct_raw     = (request.form.get(f"pct_{i}") or "").strip()
                if not isrc or not artist_name or not pct_raw:
                    skipped += 1
                    continue
                try:
                    percentage = float(pct_raw)
                except ValueError:
                    skipped += 1
                    continue
                if percentage <= 0:
                    skipped += 1
                    continue
                art = _Art.query.filter(db.func.lower(_Art.name) == artist_name.lower()).first()
                _c.execute(_t("""
                    INSERT INTO artist_royalty_split (isrc, artist_name, artist_id, percentage)
                    VALUES (:isrc, :artist_name, :artist_id, :pct)
                    ON CONFLICT (isrc, artist_name) DO UPDATE
                       SET percentage = EXCLUDED.percentage,
                           artist_id  = EXCLUDED.artist_id
                """), {"isrc": isrc, "artist_name": artist_name,
                       "artist_id": art.id if art else None, "pct": percentage})
                saved += 1
            # Collect all affected months in one query after inserts
            if saved:
                isrcs_saved = [
                    (request.form.get(f"isrc_{i}") or "").strip().upper()
                    for i in range(total)
                    if (request.form.get(f"pct_{i}") or "").strip()
                ]
                rows = _c.execute(_t(
                    "SELECT DISTINCT reporting_month FROM royalty_summary WHERE isrc = ANY(:isrcs)"
                ), {"isrcs": isrcs_saved}).fetchall()
                all_months = [r[0] for r in rows]
            _c.commit()
        _clear_cache_for_months(eng, all_months)
        # Write back to ArtistTrack in main DB (for ISRCs that exist as Tracks in LabelMind)
        try:
            from models import Track as _Trk, Artist as _ArtM, ArtistTrack as _ATM
            for i in range(total):
                _isrc  = (request.form.get(f"isrc_{i}") or "").strip().upper()
                _aname = (request.form.get(f"artist_{i}") or "").strip()
                _praw  = (request.form.get(f"pct_{i}") or "").strip()
                if not _isrc or not _aname or not _praw:
                    continue
                try:
                    _pf = float(_praw)
                except ValueError:
                    continue
                if _pf <= 0:
                    continue
                _trk_m = _Trk.query.filter(db.func.lower(_Trk.isrc) == _isrc.lower()).first()
                _art_m = _ArtM.query.filter(db.func.lower(_ArtM.name) == _aname.lower()).first()
                if _trk_m and _art_m:
                    _at_ex = _ATM.query.filter_by(artist_id=_art_m.id, track_id=_trk_m.id).first()
                    if _at_ex:
                        _at_ex.royalty_percentage = _pf
                    else:
                        db.session.add(_ATM(artist_id=_art_m.id, track_id=_trk_m.id, royalty_percentage=_pf))
            db.session.commit()
        except Exception:
            db.session.rollback()
        msg = f"Saved {saved} splits."
        if skipped:
            msg += f" {skipped} skipped (blank %)."
        flash(msg, "success")
    except Exception as e:
        flash(f"Error saving splits: {e}", "error")
    _splits_cache.clear()
    # Rebuild ARD for saved artists in background
    _saved_artists_all = list({
        (request.form.get(f"artist_{i}") or "").strip()
        for i in range(total)
        if (request.form.get(f"pct_{i}") or "").strip()
           and (request.form.get(f"artist_{i}") or "").strip()
    })
    if _saved_artists_all:
        _ard_app_all = current_app._get_current_object()
        _ard_url_all = _royalties_engine().url.render_as_string(hide_password=False)
        def _run_ard_save_all(_names=_saved_artists_all, _url=_ard_url_all, _app2=_ard_app_all):
            try:
                from sqlalchemy import create_engine as _ce_sa, text as _t_sa
                from sqlalchemy.pool import NullPool
                _eng_sa = _ce_sa(_url, poolclass=NullPool)
                with _app2.app_context():
                    try:
                        from models import ArtistNameMap as _ANM_sa
                        _canon = [(_ANM_sa.query.filter_by(raw_name=n, status='confirmed').first() or type('', (), {'canonical_name': n})()).canonical_name for n in _names]
                    except Exception:
                        _canon = _names
                    _rebuild_artist_detail(_eng_sa, artist_names=_canon)
                    # Collect affected months, rebuild ALD, then prewarm dashboard cache
                    try:
                        with _eng_sa.connect() as _cc:
                            _month_rows = _cc.execute(_t_sa(
                                "SELECT DISTINCT reporting_month FROM artist_royalty_detail "
                                "WHERE artist_name = ANY(:n)"
                            ), {"n": _canon}).fetchall()
                        _imp_months = [r[0] for r in _month_rows if r[0]]
                        if _imp_months:
                            _rebuild_artist_label_detail(_eng_sa, months=_imp_months)
                            _prewarm_affected_periods(_eng_sa, _imp_months)
                    except Exception as _pw_e:
                        import logging as _lg_pw
                        _lg_pw.getLogger(__name__).warning("Post-split prewarm failed: %s", _pw_e)
                _eng_sa.dispose()
            except Exception as _e_sa:
                import logging as _lg_sa
                _lg_sa.getLogger(__name__).warning("ARD rebuild (save_all) failed: %s", _e_sa)
        threading.Thread(target=_run_ard_save_all, daemon=True).start()
    return redirect(url_for("streaming_royalties.split_gaps"))


@bp.route("/streaming-royalties/split-gaps/save-split", methods=["POST"])
def split_gaps_save_split():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    isrc        = (request.form.get("isrc") or "").strip().upper()
    artist_name = (request.form.get("artist_name") or "").strip()
    try:
        percentage = float(request.form.get("percentage", 0))
    except ValueError:
        flash("Invalid percentage.", "error")
        return redirect(url_for("streaming_royalties.split_gaps"))

    if not isrc or not artist_name or percentage <= 0:
        flash("ISRC, artist name, and percentage are required.", "error")
        return redirect(url_for("streaming_royalties.split_gaps"))

    from sqlalchemy import text as _t
    from models import Artist as _Art
    eng = _royalties_engine()
    try:
        artist_id = None
        art = _Art.query.filter(db.func.lower(_Art.name) == artist_name.lower()).first()
        if art:
            artist_id = art.id
        with eng.connect() as _c:
            _c.execute(_t("""
                INSERT INTO artist_royalty_split (isrc, artist_name, artist_id, percentage)
                VALUES (:isrc, :artist_name, :artist_id, :pct)
                ON CONFLICT (isrc, artist_name) DO UPDATE
                   SET percentage = EXCLUDED.percentage,
                       artist_id  = EXCLUDED.artist_id
            """), {"isrc": isrc, "artist_name": artist_name,
                   "artist_id": artist_id, "pct": percentage})
            # Get months affected
            months = [row[0] for row in _c.execute(_t(
                "SELECT DISTINCT reporting_month FROM royalty_summary WHERE isrc = :isrc"
            ), {"isrc": isrc}).fetchall()]
            _c.commit()
        _clear_cache_for_months(eng, months)
        # Write back to ArtistTrack in main DB if the ISRC resolves to a Track
        try:
            from models import Track as _Trk, Artist as _ArtM, ArtistTrack as _ATM
            _trk_m = _Trk.query.filter(db.func.lower(_Trk.isrc) == isrc.lower()).first()
            _art_m = _ArtM.query.filter(db.func.lower(_ArtM.name) == artist_name.lower()).first()
            if _trk_m and _art_m:
                _at_ex = _ATM.query.filter_by(artist_id=_art_m.id, track_id=_trk_m.id).first()
                if _at_ex:
                    _at_ex.royalty_percentage = percentage
                else:
                    db.session.add(_ATM(artist_id=_art_m.id, track_id=_trk_m.id, royalty_percentage=percentage))
                db.session.commit()
        except Exception:
            db.session.rollback()
        flash(f"Split saved: {artist_name} @ {percentage}% for {isrc}.", "success")
    except Exception as e:
        flash(f"Error saving split: {e}", "error")
    _splits_cache.clear()
    # Rebuild ARD for this artist in background
    _ard_app_sp = current_app._get_current_object()
    _ard_url_sp = _royalties_engine().url.render_as_string(hide_password=False)
    _ard_name_sp = artist_name
    def _run_ard_save_split(_name=_ard_name_sp, _url=_ard_url_sp, _app3=_ard_app_sp):
        try:
            from sqlalchemy import create_engine as _ce_ss
            from sqlalchemy.pool import NullPool
            _eng_ss = _ce_ss(_url, poolclass=NullPool)
            with _app3.app_context():
                try:
                    from models import ArtistNameMap as _ANM_ss
                    _m_ss = _ANM_ss.query.filter_by(raw_name=_name, status='confirmed').first()
                    _canon_ss = _m_ss.canonical_name if _m_ss else _name
                except Exception:
                    _canon_ss = _name
                _rebuild_artist_detail(_eng_ss, artist_names=[_canon_ss])
                try:
                    from sqlalchemy import text as _t_ss2
                    with _eng_ss.connect() as _cc_ss:
                        _ss_months = [r[0] for r in _cc_ss.execute(_t_ss2(
                            "SELECT DISTINCT reporting_month FROM artist_royalty_detail WHERE artist_name = :n"
                        ), {"n": _canon_ss}).fetchall() if r[0]]
                    if _ss_months:
                        _rebuild_artist_label_detail(_eng_ss, months=_ss_months)
                        _prewarm_affected_periods(_eng_ss, _ss_months)
                except Exception:
                    pass
            _eng_ss.dispose()
        except Exception as _e_ss:
            import logging as _lg_ss
            _lg_ss.getLogger(__name__).warning("ARD rebuild (save_split) failed: %s", _e_ss)
    threading.Thread(target=_run_ard_save_split, daemon=True).start()
    return redirect(url_for("streaming_royalties.split_gaps"))


# ── Artist Audit UI ───────────────────────────────────────────────────────────

_ARTIST_AUDIT_HTML = """<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="manifest" href="/static/manifest.json"><link rel="apple-touch-icon" href="/static/labelmind-icon.png"><meta name="apple-mobile-web-app-capable" content="yes"><meta name="apple-mobile-web-app-status-bar-style" content="black-translucent"><meta name="apple-mobile-web-app-title" content="LabelMind"><script src="/static/pwa-nav.js"></script>
<title>Artist Audit — AfinArte</title>""" + _STYLE + """
<style>
.audit-tbl{width:100%;border-collapse:collapse;font-size:13px}
.audit-tbl th{text-align:left;padding:8px 10px;color:var(--t3);font-size:11px;text-transform:uppercase;letter-spacing:.4px;border-bottom:1px solid var(--bdr);white-space:nowrap}
.audit-tbl td{padding:7px 10px;border-bottom:1px solid rgba(255,255,255,.04);vertical-align:middle}
.audit-tbl tr:last-child td{border-bottom:none}
.num{text-align:right}
.src-ok{font-size:10px;font-weight:700;color:#22c55e;background:rgba(34,197,94,.12);border-radius:4px;padding:1px 6px}
.src-miss{font-size:10px;font-weight:700;color:var(--err);background:rgba(255,79,106,.12);border-radius:4px;padding:1px 6px}
.audit-footer td{font-weight:700;color:var(--t1);border-top:2px solid var(--bdr);padding:10px 10px}
</style>
</head><body><div class="app">{{ _sidebar_html|safe }}
<div class="main"><div class="page">
<div class="ph"><div class="ph-left">
  <h1 class="ph-title">Artist Audit</h1>
  <div class="ph-sub">Per-ISRC revenue breakdown — verify label rev × split % = artist rev.</div>
</div>
<div class="ph-actions">
  <a href="/streaming-royalties/split-gaps" class="btn btn-sec">&#9888; Split Gaps</a>
  <a href="/streaming-royalties" class="btn btn-sec">&#128202; Dashboard</a>
</div>
</div>

<form method="get" style="margin-bottom:20px;display:flex;gap:10px;align-items:center">
  <select name="artist" class="inp" style="width:280px" onchange="this.form.submit()">
    <option value="">— Select artist —</option>
    {% for a in all_artists %}
    <option value="{{ a }}" {{ 'selected' if a == selected_artist else '' }}>{{ a }}</option>
    {% endfor %}
  </select>
  {% if selected_artist %}
  <button type="submit" class="btn btn-primary btn-sm">&#128269; Load</button>
  {% endif %}
</form>

{% if selected_artist and rows %}
<div class="card" style="padding:0;overflow:hidden;overflow-x:auto">
<table class="audit-tbl">
  <thead><tr>
    <th>ISRC</th><th>Track</th><th>Reporting Month</th>
    <th class="num">Label Rev</th><th class="num">Split %</th>
    <th class="num">Artist Rev</th><th>Source</th>
  </tr></thead>
  <tbody>
  {% for r in rows %}
  <tr>
    <td style="font-family:monospace;font-size:11px;color:var(--t3)">{{ r.isrc }}</td>
    <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="{{ r.title }}">{{ r.title or r.isrc }}</td>
    <td style="white-space:nowrap;color:var(--t2)">{{ r.month }}</td>
    <td class="num">${{ "{:,.2f}".format(r.label_rev) }}</td>
    <td class="num">{% if r.split_pct is not none %}{{ "{:.2f}".format(r.split_pct) }}%{% else %}—{% endif %}</td>
    <td class="num" style="{{ 'color:var(--err)' if r.artist_rev == 0 and r.label_rev > 0 else '' }}">${{ "{:,.2f}".format(r.artist_rev) }}</td>
    <td>
      {% if r.source == 'ok' %}<span class="src-ok">&#10003; split</span>
      {% else %}<span class="src-miss">MISSING</span>{% endif %}
    </td>
  </tr>
  {% endfor %}
  </tbody>
  <tfoot class="audit-footer"><tr>
    <td colspan="3">Totals ({{ rows|length }} rows)</td>
    <td class="num">${{ "{:,.2f}".format(rows|sum(attribute='label_rev')) }}</td>
    <td class="num">{{ "{:.1f}".format((rows|sum(attribute='artist_rev') / rows|sum(attribute='label_rev') * 100) if rows|sum(attribute='label_rev') else 0) }}% eff.</td>
    <td class="num">${{ "{:,.2f}".format(rows|sum(attribute='artist_rev')) }}</td>
    <td><span class="src-miss" style="{{ 'display:none' if not rows|selectattr('source','eq','missing')|list else '' }}">
      {{ rows|selectattr('source','eq','missing')|list|length }} missing
    </span></td>
  </tr></tfoot>
</table>
</div>
{% elif selected_artist %}
<div class="card" style="color:var(--t2);font-size:13px;padding:16px 20px">No streaming data found for <strong>{{ selected_artist }}</strong>.</div>
{% endif %}

</div></div></div>""" + _SB_JS + """</body></html>"""


@bp.route("/streaming-royalties/artist-audit")
def artist_audit():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    from sqlalchemy import text as _t
    from models import ArtistNameMap as _ANM
    eng = _royalties_engine()

    # All canonical artist names for the dropdown
    try:
        _name_map = {m.raw_name: m.canonical_name
                     for m in _ANM.query.filter_by(status='confirmed').all()}
        with eng.connect() as _c:
            _raw_strings = [r[0] for r in _c.execute(_t(
                "SELECT DISTINCT artist_name_csv FROM royalty_summary "
                "WHERE artist_name_csv IS NOT NULL AND artist_name_csv != ''"
            )).fetchall()]
        _names: set = set()
        for s in _raw_strings:
            for part in s.split(','):
                name = part.strip()
                if name:
                    _names.add(_name_map.get(name, name))
        all_artists = sorted(_names, key=str.lower)
    except Exception:
        all_artists = []
        _name_map = {}

    selected_artist = request.args.get("artist", "").strip()
    rows = []

    if selected_artist:
        # Raw aliases for this canonical name
        try:
            raw_aliases = [m.raw_name for m in _ANM.query.filter_by(
                canonical_name=selected_artist, status='confirmed').all()]
        except Exception:
            raw_aliases = []
        all_names = [selected_artist] + raw_aliases

        # Split percentages for this artist across all ISRCs
        try:
            _name_ph = ', '.join(f':an_{i}' for i in range(len(all_names)))
            _params: dict = {f'an_{i}': n for i, n in enumerate(all_names)}
            with eng.connect() as _c:
                split_rows = _c.execute(_t(
                    f"SELECT isrc, percentage FROM artist_royalty_split WHERE artist_name IN ({_name_ph})"
                ), _params).fetchall()
            split_by_isrc = {r[0]: float(r[1]) for r in split_rows}
        except Exception:
            split_by_isrc = {}

        # Revenue data: filter royalty_summary rows by artist
        try:
            alias_clauses = ' OR '.join(
                f"artist_name_csv ILIKE :apat_{i}" for i in range(len(all_names))
            )
            _params2: dict = {f'apat_{i}': f'%{n}%' for i, n in enumerate(all_names)}
            with eng.connect() as _c:
                rev_rows = _c.execute(_t(f"""
                    SELECT isrc,
                           MAX(track_title_csv) AS title,
                           reporting_month,
                           COALESCE(SUM(net_revenue), 0) AS label_rev
                      FROM royalty_summary
                     WHERE {alias_clauses}
                     GROUP BY isrc, reporting_month
                     ORDER BY reporting_month DESC, label_rev DESC
                """), _params2).fetchall()
        except Exception:
            rev_rows = []

        for r in rev_rows:
            isrc      = r[0]
            pct       = split_by_isrc.get(isrc)
            label_rev = float(r[3])
            artist_rev = label_rev * (pct / 100.0) if pct is not None else 0.0
            rows.append({
                "isrc":       isrc,
                "title":      r[1] or isrc,
                "month":      r[2].strftime("%b %Y") if r[2] else "",
                "label_rev":  label_rev,
                "split_pct":  pct,
                "artist_rev": artist_rev,
                "source":     "ok" if pct is not None else "missing",
            })

    return render_template_string(
        _ARTIST_AUDIT_HTML,
        all_artists=all_artists,
        selected_artist=selected_artist,
        rows=rows,
        _sidebar_html=_sb("streaming_split_gaps"),
    )
