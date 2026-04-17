"""
Streaming Royalties module — ingest Believe monthly CSVs, aggregate by
(isrc, platform, country, sales_type, reporting_month, sales_month), and
serve a Power BI-style dashboard with Label View and Artist View.
"""
import csv
import datetime
import decimal
import io
import json
import os
import threading

from flask import (
    Blueprint, render_template_string, request, redirect, url_for,
    flash, jsonify, current_app, session,
)
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


def _aggregate_and_store(rec):
    """Stream-parse the CSV and flush aggregated rows to the DB in batches."""
    from models import StreamingImport, StreamingRoyalty, Track

    agg = {}       # key → dict of accumulated values
    meta = {}      # key → snapshot fields (first row wins)
    rows_read = 0
    rows_skipped = 0

    with open(rec.file_path, encoding="utf-8-sig", errors="replace", newline="") as fh:
        reader = csv.reader(fh, delimiter=";", quotechar='"')
        raw_header = next(reader, None)
        if raw_header is None:
            raise ValueError("CSV file is empty or missing header row")
        header = [h.strip().strip('"') for h in raw_header]
        col = {name: idx for idx, name in enumerate(header)}

        required = {"ISRC", "Platform", "Country / Region", "Sales Type",
                    "Reporting month", "Sales Month", "Quantity",
                    "Gross Revenue", "Net Revenue", "Mechanical Fee"}
        missing = required - set(col.keys())
        if missing:
            raise ValueError(f"CSV missing expected columns: {missing}")

        reporting_month_seen = None

        for raw_row in reader:
            rows_read += 1
            try:
                isrc = raw_row[col["ISRC"]].strip().strip('"').upper()
                if not isrc:
                    rows_skipped += 1
                    continue

                rep_month = _parse_date(raw_row[col["Reporting month"]])
                sal_month = _parse_date(raw_row[col["Sales Month"]])
                if rep_month is None or sal_month is None:
                    rows_skipped += 1
                    continue

                if reporting_month_seen is None:
                    reporting_month_seen = rep_month

                platform   = raw_row[col["Platform"]].strip().strip('"')
                country    = raw_row[col["Country / Region"]].strip().strip('"')
                sales_type = raw_row[col["Sales Type"]].strip().strip('"')

                qty   = int(float(raw_row[col["Quantity"]].strip().strip('"') or "0"))
                gross = decimal.Decimal(raw_row[col["Gross Revenue"]].strip().strip('"') or "0")
                net   = decimal.Decimal(raw_row[col["Net Revenue"]].strip().strip('"') or "0")
                mech  = decimal.Decimal(raw_row[col["Mechanical Fee"]].strip().strip('"') or "0")

                key = (isrc, platform, country, sales_type,
                       rep_month.isoformat(), sal_month.isoformat())

                if key not in agg:
                    agg[key] = {"qty": 0, "gross": decimal.Decimal(0),
                                "net": decimal.Decimal(0), "mech": decimal.Decimal(0)}
                    meta[key] = {
                        "artist_name_csv":    raw_row[col["Artist Name"]].strip().strip('"') if "Artist Name" in col else "",
                        "track_title_csv":    raw_row[col["Track title"]].strip().strip('"') if "Track title" in col else "",
                        "label_name":         raw_row[col["Label Name"]].strip().strip('"') if "Label Name" in col else "",
                        "release_title":      raw_row[col["Release title"]].strip().strip('"') if "Release title" in col else "",
                        "upc":                raw_row[col["UPC"]].strip().strip('"') if "UPC" in col else "",
                        "streaming_sub_type": raw_row[col["Streaming Subscription Type"]].strip().strip('"') if "Streaming Subscription Type" in col else "",
                        "release_type":       raw_row[col["Release type"]].strip().strip('"') if "Release type" in col else "",
                        "currency":           raw_row[col["Client Payment Currency"]].strip().strip('"') if "Client Payment Currency" in col else "EUR",
                    }

                agg[key]["qty"]   += qty
                agg[key]["gross"] += gross
                agg[key]["net"]   += net
                agg[key]["mech"]  += mech

            except Exception:
                rows_skipped += 1
                continue

    # Bulk ISRC → track_id lookup
    isrc_set = {k[0] for k in agg}
    track_map = {}
    if isrc_set:
        tracks = Track.query.filter(Track.isrc.in_(isrc_set)).all()
        track_map = {t.isrc: t.id for t in tracks}

    # Flush in batches of 500
    batch = []
    for key, vals in agg.items():
        isrc, platform, country, sales_type, rep_iso, sal_iso = key
        batch.append({
            "import_id":           rec.id,
            "isrc":                isrc,
            "platform":            platform,
            "country":             country,
            "sales_type":          sales_type,
            "reporting_month":     datetime.date.fromisoformat(rep_iso),
            "sales_month":         datetime.date.fromisoformat(sal_iso),
            "artist_name_csv":     meta[key]["artist_name_csv"],
            "track_title_csv":     meta[key]["track_title_csv"],
            "label_name":          meta[key]["label_name"],
            "release_title":       meta[key]["release_title"],
            "upc":                 meta[key]["upc"],
            "streaming_sub_type":  meta[key]["streaming_sub_type"],
            "release_type":        meta[key]["release_type"],
            "currency":            meta[key]["currency"],
            "total_quantity":      vals["qty"],
            "total_gross_revenue": vals["gross"],
            "total_net_revenue":   vals["net"],
            "total_mechanical_fee": vals["mech"],
            "track_id":            track_map.get(isrc),
            "created_at":          datetime.datetime.utcnow(),
        })
        if len(batch) >= 500:
            db.session.bulk_insert_mappings(StreamingRoyalty, batch)
            db.session.commit()
            batch = []

    if batch:
        db.session.bulk_insert_mappings(StreamingRoyalty, batch)
        db.session.commit()

    rec.rows_read       = rows_read
    rec.rows_aggregated = len(agg)
    rec.rows_skipped    = rows_skipped
    if reporting_month_seen:
        rec.reporting_month = reporting_month_seen
    db.session.commit()


def _process_import(app, import_id):
    """Background thread: process one StreamingImport record."""
    with app.app_context():
        from models import StreamingImport
        rec = StreamingImport.query.get(import_id)
        if rec is None:
            return
        rec.status     = "processing"
        rec.started_at = datetime.datetime.utcnow()
        db.session.commit()
        try:
            _aggregate_and_store(rec)
            rec.status      = "done"
            rec.finished_at = datetime.datetime.utcnow()
            db.session.commit()
            try:
                os.remove(rec.file_path)
            except OSError:
                pass
        except Exception as e:
            db.session.rollback()
            rec.status        = "error"
            rec.error_message = str(e)[:2000]
            rec.finished_at   = datetime.datetime.utcnow()
            db.session.commit()


def _process_bulk(app, import_ids):
    """Background thread: process a list of imports sequentially."""
    for imp_id in import_ids:
        _process_import(app, imp_id)


# ── Dashboard data helper ─────────────────────────────────────────────────────

def _dashboard_data(year=None, quarter=None, artist=None, view="label"):
    """Return aggregated dashboard data as a dict.
    view='label'  → use total_net_revenue directly
    view='artist' → multiply by ArtistRoyaltySplit.percentage / 100
    """
    from models import StreamingRoyalty, ArtistRoyaltySplit
    from sqlalchemy import func, case

    q = db.session.query(StreamingRoyalty)

    if year and year != "all":
        q = q.filter(func.extract("year", StreamingRoyalty.reporting_month) == int(year))
    if quarter and quarter != "all":
        month_ranges = {"1": (1, 3), "2": (4, 6), "3": (7, 9), "4": (10, 12)}
        m_start, m_end = month_ranges.get(str(quarter), (1, 12))
        q = q.filter(
            func.extract("month", StreamingRoyalty.reporting_month) >= m_start,
            func.extract("month", StreamingRoyalty.reporting_month) <= m_end,
        )
    if artist and artist != "all":
        q = q.filter(StreamingRoyalty.artist_name_csv == artist)

    rows = q.all()

    # Build split lookup: isrc → percentage (Decimal)
    split_map = {}
    if view == "artist":
        isrc_set = {r.isrc for r in rows}
        if artist and artist != "all":
            splits = ArtistRoyaltySplit.query.filter(
                ArtistRoyaltySplit.isrc.in_(isrc_set),
                db.func.lower(ArtistRoyaltySplit.artist_name) == artist.lower(),
            ).all()
        else:
            splits = ArtistRoyaltySplit.query.filter(
                ArtistRoyaltySplit.isrc.in_(isrc_set)
            ).all()
        for s in splits:
            # If multiple artists per ISRC, sum their percentages for "all artists" view
            split_map[s.isrc] = split_map.get(s.isrc, decimal.Decimal(0)) + s.percentage

    def _rev(row):
        net = row.total_net_revenue or decimal.Decimal(0)
        if view == "artist":
            pct = split_map.get(row.isrc, decimal.Decimal(100))
            return float(net * pct / decimal.Decimal(100))
        return float(net)

    # KPI
    kpi_total = sum(_rev(r) for r in rows)

    # By artist (top 10)
    artist_totals = {}
    for r in rows:
        name = r.artist_name_csv or "Unknown"
        artist_totals[name] = artist_totals.get(name, 0.0) + _rev(r)
    top_artists = sorted(artist_totals.items(), key=lambda x: x[1], reverse=True)[:10]

    # By month
    month_totals = {}
    for r in rows:
        label = r.reporting_month.strftime("%b %Y") if r.reporting_month else "?"
        month_totals[label] = month_totals.get(label, 0.0) + _rev(r)
    # Sort chronologically
    month_sorted = sorted(month_totals.items(), key=lambda x: x[0])

    # By platform
    platform_totals = {}
    for r in rows:
        plat = r.platform or "Unknown"
        platform_totals[plat] = platform_totals.get(plat, 0.0) + _rev(r)
    platform_sorted = sorted(platform_totals.items(), key=lambda x: x[1], reverse=True)

    # By country (top 5 + Other)
    country_totals = {}
    for r in rows:
        ctry = r.country or "Unknown"
        country_totals[ctry] = country_totals.get(ctry, 0.0) + _rev(r)
    top5_countries = sorted(country_totals.items(), key=lambda x: x[1], reverse=True)[:5]
    other_country = sum(v for k, v in country_totals.items()
                        if k not in {c[0] for c in top5_countries})
    country_data = list(top5_countries)
    if other_country > 0:
        country_data.append(("Other", other_country))

    # Catalog table (by track, top 50)
    track_data = {}
    for r in rows:
        key = r.isrc
        if key not in track_data:
            track_data[key] = {"title": r.track_title_csv or r.isrc,
                               "artist": r.artist_name_csv or "",
                               "streams": 0, "revenue": 0.0}
        track_data[key]["streams"] += (r.total_quantity or 0)
        track_data[key]["revenue"] += _rev(r)
    catalog_sorted = sorted(track_data.values(), key=lambda x: x["revenue"], reverse=True)[:50]

    # Available filter options
    all_artists = sorted({r.artist_name_csv for r in
                          db.session.query(StreamingRoyalty.artist_name_csv).distinct().all()
                          if r.artist_name_csv})
    all_years = sorted({r.reporting_month.year for r in
                        db.session.query(StreamingRoyalty.reporting_month).distinct().all()
                        if r.reporting_month}, reverse=True)

    return {
        "kpi_total":      kpi_total,
        "by_artist":      [{"name": k, "revenue": v} for k, v in top_artists],
        "by_month":       [{"month": k, "revenue": v} for k, v in month_sorted],
        "by_platform":    [{"platform": k, "revenue": v} for k, v in platform_sorted],
        "by_country":     [{"country": k, "revenue": v} for k, v in country_data],
        "catalog":        catalog_sorted,
        "all_artists":    all_artists,
        "all_years":      all_years,
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

    app_obj = current_app._get_current_object()
    threading.Thread(target=_process_import, args=(app_obj, rec.id), daemon=True).start()

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


@bp.route("/streaming-royalties/import/<int:import_id>/delete", methods=["POST"])
def delete_import(import_id):
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    from models import StreamingImport, StreamingRoyalty
    rec = StreamingImport.query.get_or_404(import_id)
    StreamingRoyalty.query.filter_by(import_id=import_id).delete()
    db.session.delete(rec)
    db.session.commit()
    flash("Import deleted.", "success")
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
      <canvas id="chartArtist" height="220"></canvas>
      {% else %}<div class="sr-no-data">No data</div>{% endif %}
    </div>
    <div class="sr-panel">
      <div class="sr-panel-title">Revenue by Month</div>
      {% if data.by_month %}
      <canvas id="chartMonth" height="220"></canvas>
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
      <canvas id="chartCountry" height="260"></canvas>
      {% else %}<div class="sr-no-data">No data</div>{% endif %}
    </div>
    <div class="sr-panel">
      <div class="sr-panel-title">Revenue by Platform</div>
      {% if data.by_platform %}
      <canvas id="chartPlatform" height="260"></canvas>
      {% else %}<div class="sr-no-data">No data</div>{% endif %}
    </div>
  </div>
</div>
</div></div>""" + _SB_JS + """
<script>
const PALETTE = ['#5eb8ff','#6385ff','#34d399','#f59e0b','#ff4f6a','#22d3ee','#a55bff','#fb923c','#84cc16','#e879f9'];
const chartOpts = (type, labels, datasets, horizontal) => ({
  type, data:{labels, datasets},
  options:{
    responsive:true, maintainAspectRatio:false,
    indexAxis: horizontal ? 'y' : 'x',
    plugins:{legend:{display:type==='doughnut',position:'bottom',labels:{color:'#8a96b0',font:{size:11}}},
             tooltip:{callbacks:{label:(ctx)=>'$'+ctx.parsed.x?.toLocaleString('en-US',{minimumFractionDigits:2})||'$'+ctx.parsed.toLocaleString('en-US',{minimumFractionDigits:2})}}},
    scales: type==='doughnut' ? {} : {
      x:{grid:{color:'rgba(255,255,255,.05)'},ticks:{color:'#8a96b0',font:{size:10}}},
      y:{grid:{color:'rgba(255,255,255,.05)'},ticks:{color:'#8a96b0',font:{size:10}}}
    }
  }
});

let charts = {};
function destroyCharts(){ Object.values(charts).forEach(c=>c.destroy()); charts={}; }

function buildCharts(d){
  destroyCharts();
  if(d.by_artist?.length){
    const el=document.getElementById('chartArtist');
    if(el) charts.artist=new Chart(el, chartOpts('bar',
      d.by_artist.map(r=>r.name), [{data:d.by_artist.map(r=>r.revenue),backgroundColor:PALETTE[0],borderRadius:4}], true));
  }
  if(d.by_month?.length){
    const el=document.getElementById('chartMonth');
    if(el) charts.month=new Chart(el, chartOpts('bar',
      d.by_month.map(r=>r.month), [{data:d.by_month.map(r=>r.revenue),backgroundColor:PALETTE[1],borderRadius:4}], false));
  }
  if(d.by_country?.length){
    const el=document.getElementById('chartCountry');
    if(el) charts.country=new Chart(el, chartOpts('doughnut',
      d.by_country.map(r=>r.country), [{data:d.by_country.map(r=>r.revenue),backgroundColor:PALETTE}], false));
  }
  if(d.by_platform?.length){
    const el=document.getElementById('chartPlatform');
    if(el) charts.platform=new Chart(el, chartOpts('bar',
      d.by_platform.map(r=>r.platform), [{data:d.by_platform.map(r=>r.revenue),backgroundColor:PALETTE[2],borderRadius:4}], false));
  }
}

// Initial render with server-side data
const initialData = {{ data|tojson }};
buildCharts(initialData);

let currentView = '{{ view }}';

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
  const url=`/streaming-royalties/data?year=${year}&quarter=${quarter}&artist=${encodeURIComponent(artist)}&view=${currentView}`;
  fetch(url).then(r=>r.json()).then(d=>{
    document.getElementById('kpiVal').textContent='$'+d.kpi_total.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});
    // Update catalog table
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

# Patch dashboard to inject sidebar
_orig_dashboard_html = _DASHBOARD_HTML


@bp.app_template_filter("tojson")
def _tojson_filter(value):
    return json.dumps(value)


# ── Imports list page ─────────────────────────────────────────────────────────

_IMPORTS_HTML = """<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Streaming Imports — AfinArte</title>""" + _STYLE + """
</head><body><div class="app">""" + "{% set _s = _sidebar('streaming_royalties') %}" + """
{{ _s | safe }}
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
</div>
</div></div></div>""" + _SB_JS + """</body></html>"""

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

function poll(){
  if(finalStatuses.has(currentStatus)) return;
  fetch(`/streaming-royalties/import-status/${importId}/json`)
    .then(r=>r.json())
    .then(d=>{
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
      } else {
        setTimeout(poll, 2000);
      }
    })
    .catch(()=>setTimeout(poll, 3000));
}

if(!finalStatuses.has(currentStatus)) setTimeout(poll, 2000);
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
