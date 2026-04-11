import csv
import datetime
import io
import json
import os
import re

from flask import Blueprint, render_template_string, request, redirect, url_for, flash

from extensions import db
from models import Work, ProRegistration
from utils import auth_required
from ui import MECHANICAL_AUDIT_HTML

bp = Blueprint("mechanical_audit", __name__)

# ── Paths ─────────────────────────────────────────────────────────────────────

_UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "..", "uploads", "mechanical_catalogs")

_REQUIRED_COLS = {
    "mlc": {"PRIMARY TITLE", "MLC SONG CODE"},
    "mri": {"SONG TITLE", "MRI SONG ID"},
}

_ACCEPTED_EXTS = {".csv", ".xlsx", ".xls"}


# ── File I/O helpers ──────────────────────────────────────────────────────────

def _normalize_headers(row):
    """Strip whitespace and trailing * from every header key."""
    return {re.sub(r"\s*\*+\s*$", "", str(k or "")).strip(): v for k, v in row.items()}


def _rows_from_xlsx(content_bytes):
    import openpyxl
    wb = openpyxl.load_workbook(io.BytesIO(content_bytes))
    ws = wb.active
    raw = list(ws.iter_rows(values_only=True))
    if not raw:
        return []
    headers = [re.sub(r"\s*\*+\s*$", "", str(h or "")).strip() for h in raw[0]]
    return [dict(zip(headers, row)) for row in raw[1:]]


def _rows_from_xls(content_bytes):
    import xlrd
    wb = xlrd.open_workbook(file_contents=content_bytes)
    ws = wb.sheet_by_index(0)
    if ws.nrows == 0:
        return []
    headers = [re.sub(r"\s*\*+\s*$", "", str(ws.cell_value(0, c))).strip()
               for c in range(ws.ncols)]
    return [
        dict(zip(headers, [ws.cell_value(r, c) for c in range(ws.ncols)]))
        for r in range(1, ws.nrows)
    ]


def _rows_from_csv(content_str):
    reader = csv.DictReader(io.StringIO(content_str))
    return [_normalize_headers(row) for row in reader]


def _read_file(content_bytes, filename):
    """Dispatch to the right reader based on file extension."""
    ext = os.path.splitext(filename.lower())[1]
    if ext == ".xlsx":
        return _rows_from_xlsx(content_bytes)
    if ext == ".xls":
        return _rows_from_xls(content_bytes)
    return _rows_from_csv(content_bytes.decode("utf-8-sig", errors="replace"))


# ── Upload metadata ───────────────────────────────────────────────────────────

def _uploaded_path(source):
    """Return path for the saved file (we store the original extension in meta)."""
    meta = _read_meta(source)
    if meta and meta.get("ext"):
        return os.path.join(_UPLOAD_DIR, f"{source}_catalog{meta['ext']}")
    # Fallback: check for any supported extension
    for ext in (".xlsx", ".xls", ".csv"):
        p = os.path.join(_UPLOAD_DIR, f"{source}_catalog{ext}")
        if os.path.exists(p):
            return p
    return ""


def _meta_path(source):
    return os.path.join(_UPLOAD_DIR, f"{source}_meta.json")


def _read_meta(source):
    path = _meta_path(source)
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            pass
    return None


def _write_meta(source, original_name, ext, work_count):
    os.makedirs(_UPLOAD_DIR, exist_ok=True)
    with open(_meta_path(source), "w") as f:
        json.dump({
            "original_name": original_name,
            "ext": ext,
            "uploaded_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
            "work_count": work_count,
        }, f)


# ── CSV parsers ───────────────────────────────────────────────────────────────

def _norm(title):
    t = (title or "").lower().strip()
    t = re.sub(r"[^\w\s]", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def _str(val):
    return str(val).strip() if val is not None else ""


def _parse_mlc():
    """Return {norm_title: {title, mlc_song_code, members_id, iswc, writers, publisher, artist}}"""
    path = _uploaded_path("mlc")
    if not path or not os.path.exists(path):
        return {}
    with open(path, "rb") as f:
        content = f.read()
    rows = _read_file(content, path)

    works = {}
    for row in rows:
        title    = _str(row.get("PRIMARY TITLE", ""))
        mlc_code = _str(row.get("MLC SONG CODE", ""))
        iswc     = _str(row.get("ISWC", ""))
        members_id = _str(row.get("MEMBERS SONG ID", ""))
        w_first  = _str(row.get("WRITER FIRST NAME", ""))
        w_last   = _str(row.get("WRITER LAST NAME", ""))
        publisher = _str(row.get("PUBLISHER NAME", ""))
        artist   = _str(row.get("RECORDING ARTIST NAME", ""))
        if not title:
            continue
        key = _norm(title)
        if key not in works:
            works[key] = dict(title=title, mlc_song_code=mlc_code, members_id=members_id,
                              iswc=iswc, writers=[], publisher=publisher, artist=artist)
        # Update key fields if first row was empty
        if mlc_code and not works[key]["mlc_song_code"]:
            works[key]["mlc_song_code"] = mlc_code
        if iswc and not works[key]["iswc"]:
            works[key]["iswc"] = iswc
        if artist and not works[key]["artist"]:
            works[key]["artist"] = artist
        if publisher and not works[key]["publisher"]:
            works[key]["publisher"] = publisher
        writer_name = " ".join(filter(None, [w_first, w_last]))
        if writer_name and writer_name not in works[key]["writers"]:
            works[key]["writers"].append(writer_name)
    return works


def _parse_mri():
    """Return {norm_title: {title, mri_song_id, publishers_id, iswc, writers, publisher, artist}}"""
    path = _uploaded_path("mri")
    if not path or not os.path.exists(path):
        return {}
    with open(path, "rb") as f:
        content = f.read()
    rows = _read_file(content, path)

    works = {}
    for row in rows:
        title      = _str(row.get("SONG TITLE", ""))
        mri_id     = _str(row.get("MRI SONG ID", ""))
        iswc       = _str(row.get("ISWC", ""))
        pub_id     = _str(row.get("PUBLISHER'S SONG ID", ""))
        c_first    = _str(row.get("COMPOSER FIRST NAME", ""))
        c_last     = _str(row.get("COMPOSER LAST NAME", ""))
        publisher  = _str(row.get("PUBLISHER NAME", ""))
        artist     = _str(row.get("RECORDING ARTIST NAME", ""))
        if not title:
            continue
        key = _norm(title)
        if key not in works:
            works[key] = dict(title=title, mri_song_id=mri_id, publishers_id=pub_id,
                              iswc=iswc, writers=[], publisher=publisher, artist=artist)
        if mri_id and not works[key]["mri_song_id"]:
            works[key]["mri_song_id"] = mri_id
        if iswc and not works[key]["iswc"]:
            works[key]["iswc"] = iswc
        if artist and not works[key]["artist"]:
            works[key]["artist"] = artist
        if publisher and not works[key]["publisher"]:
            works[key]["publisher"] = publisher
        writer_name = " ".join(filter(None, [c_first, c_last]))
        if writer_name and writer_name not in works[key]["writers"]:
            works[key]["writers"].append(writer_name)
    return works


# ── Audit builder ─────────────────────────────────────────────────────────────

def _build_audit():
    mlc  = _parse_mlc()
    mri  = _parse_mri()

    all_works = Work.query.order_by(Work.title).all()
    db_keys   = {_norm(w.title): w for w in all_works}

    matched      = []
    unregistered = []

    for w in all_works:
        key = _norm(w.title)
        m = mlc.get(key)
        r = mri.get(key)

        # Collect ISWC suggestions
        iswcs = set(filter(None, [
            (m or {}).get("iswc"),
            (r or {}).get("iswc"),
        ]))
        iswc_conflict  = len(iswcs) > 1
        suggested_iswc = next(iter(iswcs), "") if not w.iswc else ""

        entry = dict(
            work=w, mlc=m, mri=r,
            suggested_iswc=suggested_iswc,
            iswc_conflict=iswc_conflict,
            iswcs_found=iswcs,
        )
        if m or r:
            matched.append(entry)
        else:
            unregistered.append(entry)

    orphaned = []
    for source, src_dict in [("MLC", mlc), ("MRI", mri)]:
        for key, v in src_dict.items():
            if key not in db_keys:
                orphaned.append(dict(source=source, **v))

    return matched, unregistered, orphaned, mlc, mri


# ── Routes ────────────────────────────────────────────────────────────────────

@bp.route("/mechanical-audit")
def mechanical_audit():
    if auth_required():
        return redirect(url_for("publishing.login"))

    matched, unregistered, orphaned, mlc, mri = _build_audit()

    stats = dict(
        mlc_total=len(mlc),
        mri_total=len(mri),
        db_total=len(matched) + len(unregistered),
        matched=len(matched),
        unregistered=len(unregistered),
        orphaned=len(orphaned),
    )

    upload_meta = {
        "mlc": _read_meta("mlc"),
        "mri": _read_meta("mri"),
    }

    tab = request.args.get("tab", "matched")

    return render_template_string(
        MECHANICAL_AUDIT_HTML,
        matched=matched,
        unregistered=unregistered,
        orphaned=orphaned,
        stats=stats,
        tab=tab,
        upload_meta=upload_meta,
    )


@bp.route("/mechanical-audit/upload", methods=["POST"])
def upload_catalog():
    if auth_required():
        return redirect(url_for("publishing.login"))

    source = request.form.get("source", "").lower().strip()
    file   = request.files.get("file")

    if source not in ("mlc", "mri"):
        flash("Invalid source specified.", "error")
        return redirect(url_for("mechanical_audit.mechanical_audit"))

    if not file or not file.filename:
        flash("No file selected.", "error")
        return redirect(url_for("mechanical_audit.mechanical_audit"))

    ext = os.path.splitext(file.filename.lower())[1]
    if ext not in _ACCEPTED_EXTS:
        flash("Only .csv, .xlsx, or .xls files are accepted.", "error")
        return redirect(url_for("mechanical_audit.mechanical_audit"))

    content_bytes = file.read()

    # Parse and validate
    try:
        rows = _read_file(content_bytes, file.filename)
    except Exception as e:
        flash(f"Could not read file: {e}", "error")
        return redirect(url_for("mechanical_audit.mechanical_audit"))

    if not rows:
        flash("The uploaded file appears to be empty.", "error")
        return redirect(url_for("mechanical_audit.mechanical_audit"))

    # Validate required columns
    actual_cols = set(rows[0].keys())
    required    = _REQUIRED_COLS[source]
    if not required.issubset(actual_cols):
        missing = required - actual_cols
        flash(f"File missing expected columns: {', '.join(missing)}. "
              f"Make sure this is a {source.upper()} catalog export.", "error")
        return redirect(url_for("mechanical_audit.mechanical_audit"))

    # Count unique titles
    title_col   = "PRIMARY TITLE" if source == "mlc" else "SONG TITLE"
    work_count  = len({_str(r.get(title_col, "")) for r in rows
                       if _str(r.get(title_col, ""))})

    # Remove old file(s) for this source
    for old_ext in _ACCEPTED_EXTS:
        old = os.path.join(_UPLOAD_DIR, f"{source}_catalog{old_ext}")
        if os.path.exists(old):
            os.remove(old)

    # Save new file
    os.makedirs(_UPLOAD_DIR, exist_ok=True)
    dest = os.path.join(_UPLOAD_DIR, f"{source}_catalog{ext}")
    with open(dest, "wb") as f_out:
        f_out.write(content_bytes)

    _write_meta(source, file.filename, ext, work_count)
    flash(f"{source.upper()} catalog updated — {work_count} works loaded from {file.filename}.", "success")
    return redirect(url_for("mechanical_audit.mechanical_audit"))


@bp.route("/mechanical-audit/apply", methods=["POST"])
def apply_sync():
    if auth_required():
        return redirect(url_for("publishing.login"))

    matched, _u, _o, _m, _r = _build_audit()

    iswc_updated   = 0
    mri_updated    = 0
    mlc_created    = 0
    skipped_iswc   = 0

    for entry in matched:
        w = entry["work"]
        m = entry["mlc"]
        r = entry["mri"]

        # ── ISWC ─────────────────────────────────────────────────────────────
        if not w.iswc:
            if entry["iswc_conflict"]:
                skipped_iswc += 1
            elif entry["suggested_iswc"]:
                w.iswc = entry["suggested_iswc"]
                iswc_updated += 1

        # ── MRI Song ID → Work.mri_song_id ───────────────────────────────────
        if r and r.get("mri_song_id") and not w.mri_song_id:
            w.mri_song_id = r["mri_song_id"]
            mri_updated += 1

        # ── MLC Song Code → ProRegistration(pro="MLC") ───────────────────────
        if m and m.get("mlc_song_code"):
            exists = ProRegistration.query.filter_by(work_id=w.id, pro="MLC").first()
            if exists:
                if not exists.mlc_song_code:
                    exists.mlc_song_code = m["mlc_song_code"]
                    mlc_created += 1
            else:
                db.session.add(ProRegistration(
                    work_id=w.id,
                    pro="MLC",
                    mlc_song_code=m["mlc_song_code"],
                    registered_at=datetime.date.today(),
                    registered_by="MLC CSV Import",
                    notes="Imported from MLC catalog export",
                ))
                mlc_created += 1

    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        flash(f"Error applying changes: {e}", "error")
        return redirect(url_for("mechanical_audit.mechanical_audit"))

    parts = []
    if iswc_updated:
        parts.append(f"{iswc_updated} ISWC numbers written")
    if mri_updated:
        parts.append(f"{mri_updated} MRI Song IDs written")
    if mlc_created:
        parts.append(f"{mlc_created} MLC Song Codes saved")
    if skipped_iswc:
        parts.append(f"{skipped_iswc} works skipped (ISWC conflict — review manually)")
    flash(", ".join(parts) + "." if parts else "Nothing to update.", "success")
    return redirect(url_for("mechanical_audit.mechanical_audit", tab="matched"))
