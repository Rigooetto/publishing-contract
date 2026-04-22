import csv
import datetime
import glob
import io
import json
import os
import re

from flask import Blueprint, render_template_string, request, redirect, url_for, flash

from extensions import db
from models import Work, ProRegistration, WorkAKA
from utils import auth_required, paginate_list, role_required, FULL_ACCESS_ROLES, normalize_for_match
from ui import MECHANICAL_AUDIT_HTML

bp = Blueprint("mechanical_audit", __name__)

# ── Paths ─────────────────────────────────────────────────────────────────────

_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "..", "template")
_UPLOAD_DIR   = os.path.join(os.path.dirname(__file__), "..", "uploads", "mechanical_catalogs")

# Required columns (uppercased) to validate uploads
_REQUIRED_COLS = {
    "mlc": {"PRIMARY TITLE", "MLC SONG CODE"},
    "mri": {"SONG TITLE", "MRI ID"},
}

_ACCEPTED_EXTS = {".csv", ".xlsx", ".xls"}


def _find_template(pattern):
    """Find the newest file in template dir matching a glob pattern."""
    matches = sorted(glob.glob(os.path.join(_TEMPLATE_DIR, pattern)))
    return matches[-1] if matches else ""


# ── File I/O ──────────────────────────────────────────────────────────────────

def _norm_key(k):
    """Strip trailing * and whitespace, uppercase — for consistent header lookup."""
    return re.sub(r"\s*\*+\s*$", "", str(k or "")).strip().upper()


def _rows_from_xlsx(content_bytes):
    import openpyxl
    wb = openpyxl.load_workbook(io.BytesIO(content_bytes))
    ws = wb.active
    raw = list(ws.iter_rows(values_only=True))
    if not raw:
        return []
    headers = [_norm_key(h) for h in raw[0]]
    return [dict(zip(headers, row)) for row in raw[1:]]


def _rows_from_xls(content_bytes):
    import xlrd
    wb = xlrd.open_workbook(file_contents=content_bytes)
    ws = wb.sheet_by_index(0)
    if ws.nrows == 0:
        return []
    headers = [_norm_key(ws.cell_value(0, c)) for c in range(ws.ncols)]
    return [dict(zip(headers, [ws.cell_value(r, c) for c in range(ws.ncols)]))
            for r in range(1, ws.nrows)]


def _rows_from_csv(content_str):
    reader = csv.DictReader(io.StringIO(content_str))
    return [{_norm_key(k): v for k, v in row.items()} for row in reader]


def _read_file(content_bytes, filename):
    ext = os.path.splitext(filename.lower())[1]
    if ext == ".xlsx":
        return _rows_from_xlsx(content_bytes)
    if ext == ".xls":
        return _rows_from_xls(content_bytes)
    return _rows_from_csv(content_bytes.decode("utf-8-sig", errors="replace"))


# ── Upload metadata ───────────────────────────────────────────────────────────

def _uploaded_path(source):
    meta = _read_meta(source)
    if meta and meta.get("ext"):
        p = os.path.join(_UPLOAD_DIR, f"{source}_catalog{meta['ext']}")
        if os.path.exists(p):
            return p
    for ext in _ACCEPTED_EXTS:
        p = os.path.join(_UPLOAD_DIR, f"{source}_catalog{ext}")
        if os.path.exists(p):
            return p
    return ""


def _active_path(source):
    up = _uploaded_path(source)
    if up:
        return up
    # Fall back to bundled template files
    patterns = {
        "mlc": "mlc_work_report*.csv",
        "mri": "Music Reports*.csv",
    }
    return _find_template(patterns.get(source, ""))


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


# ── Helpers ───────────────────────────────────────────────────────────────────

def _norm(title):
    return normalize_for_match(title)


def _s(val):
    return str(val).strip() if val is not None else ""


def _parse_date(raw, *fmts):
    for fmt in fmts:
        try:
            return datetime.datetime.strptime(raw.strip(), fmt).date()
        except (ValueError, AttributeError):
            pass
    return datetime.date.today()


# ── CSV parsers ───────────────────────────────────────────────────────────────

def _parse_mlc():
    """
    Actual MLC work report export columns (after uppercasing):
    MLC SONG CODE, PRIMARY TITLE, ISWC, MEMBER SONG IDS, ARTISTS,
    PARTY ID, PARTY NAME, PARTY IPI, PARTY ROLE, COLLECTIBLE SHARE, ...
    Multiple rows per work (one per party).
    """
    path = _active_path("mlc")
    if not path or not os.path.exists(path):
        return {}

    with open(path, "rb") as f:
        rows = _read_file(f.read(), path)

    by_code = {}   # keyed by MLC Song Code (most reliable)
    for row in rows:
        title    = _s(row.get("PRIMARY TITLE", ""))
        mlc_code = _s(row.get("MLC SONG CODE", ""))
        iswc     = _s(row.get("ISWC", ""))
        artists  = _s(row.get("ARTISTS", ""))
        party    = _s(row.get("PARTY NAME", ""))
        role     = _s(row.get("PARTY ROLE", "")).lower()
        if not title:
            continue

        key = mlc_code if mlc_code else _norm(title)
        if key not in by_code:
            by_code[key] = dict(title=title, mlc_song_code=mlc_code,
                                iswc=iswc, writers=[], publisher="", artist=artists)
        w = by_code[key]
        if iswc and not w["iswc"]:
            w["iswc"] = iswc
        if artists and not w["artist"]:
            w["artist"] = artists

        is_writer    = "composer" in role or ("writer" in role and "publisher" not in role)
        is_publisher = "publisher" in role or "administrator" in role

        if is_writer and party and party not in w["writers"]:
            w["writers"].append(party)
        elif is_publisher and not w["publisher"]:
            w["publisher"] = party

    # Re-index by normalized title for DB matching
    return {_norm(v["title"]): v for v in by_code.values()}


def _parse_mri():
    """
    Actual Music Reports / Songdex portal export columns (after uppercasing):
    MRI ID, SONG TITLE, PUBLISHER(S), COMPOSER(S), US SHARE, REVISED SHARE,
    NOT OUR SONG, LAST UPDATED
    One row per work.
    """
    path = _active_path("mri")
    if not path or not os.path.exists(path):
        return {}

    with open(path, "rb") as f:
        rows = _read_file(f.read(), path)

    works = {}
    for row in rows:
        title     = _s(row.get("SONG TITLE", ""))
        mri_id    = _s(row.get("MRI ID", ""))
        publisher = _s(row.get("PUBLISHER(S)", ""))
        composer  = _s(row.get("COMPOSER(S)", ""))
        if not title:
            continue
        key = _norm(title)
        if key not in works:
            works[key] = dict(title=title, mri_song_id=mri_id, iswc="",
                              writers=[], publisher=publisher, artist="")
        w = works[key]
        if mri_id and not w["mri_song_id"]:
            w["mri_song_id"] = mri_id
        if composer and composer not in w["writers"]:
            w["writers"].append(composer)
    return works


# ── Audit builder ─────────────────────────────────────────────────────────────

def _build_audit():
    mlc = _parse_mlc()
    mri = _parse_mri()

    all_works = Work.query.order_by(Work.title).all()
    db_keys   = {_norm(w.title): w for w in all_works}

    # Include AKA keys so orphan detection doesn't flag them
    for w in all_works:
        for aka in w.aka_titles:
            if aka.normalized not in db_keys:
                db_keys[aka.normalized] = w

    matched_both = []
    mlc_only     = []
    mri_only     = []
    unregistered = []

    for w in all_works:
        key = _norm(w.title)
        aka_keys = [aka.normalized for aka in w.aka_titles]

        m = mlc.get(key) or next((mlc.get(k) for k in aka_keys if mlc.get(k)), None)
        r = mri.get(key) or next((mri.get(k) for k in aka_keys if mri.get(k)), None)

        iswcs = set(filter(None, [(m or {}).get("iswc"), (r or {}).get("iswc")]))
        iswc_conflict  = len(iswcs) > 1
        suggested_iswc = next(iter(iswcs), "") if not w.iswc else ""

        # Suggest a source title only when it is mixed-case AND has diacritics the
        # DB title is missing — never when the source is just ALL CAPS or same title.
        def _worth_suggesting(src_title, db_title):
            if not src_title or src_title == db_title:
                return False
            if src_title.lower() == db_title.lower():
                return False
            if normalize_for_match(src_title) == src_title.lower():
                return False
            return True

        src_titles = [
            src["title"] for src in [m, r]
            if src and _worth_suggesting(src.get("title", ""), w.title)
        ]
        suggested_title = src_titles[0] if src_titles else ""

        entry = dict(work=w, mlc=m, mri=r,
                     suggested_iswc=suggested_iswc,
                     iswc_conflict=iswc_conflict,
                     iswcs_found=iswcs,
                     suggested_title=suggested_title)
        if m and r:
            matched_both.append(entry)
        elif m:
            mlc_only.append(entry)
        elif r:
            mri_only.append(entry)
        else:
            unregistered.append(entry)

    orphaned = []
    for source, src_dict in [("MLC", mlc), ("MRI", mri)]:
        for key, v in src_dict.items():
            if key not in db_keys:
                orphaned.append(dict(source=source, **v))

    return matched_both, mlc_only, mri_only, unregistered, orphaned, mlc, mri


# ── Routes ────────────────────────────────────────────────────────────────────

@bp.route("/mechanical-audit")
def mechanical_audit():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(FULL_ACCESS_ROLES):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    matched_both, mlc_only, mri_only, unregistered, orphaned, mlc, mri = _build_audit()

    stats = dict(
        mlc_total=len(mlc),
        mri_total=len(mri),
        db_total=len(matched_both) + len(mlc_only) + len(mri_only) + len(unregistered),
        matched_both=len(matched_both),
        mlc_only=len(mlc_only),
        mri_only=len(mri_only),
        unregistered=len(unregistered),
        orphaned=len(orphaned),
    )

    upload_meta = {"mlc": _read_meta("mlc"), "mri": _read_meta("mri")}
    tab  = request.args.get("tab", "matched_both")
    page = request.args.get("page", 1, type=int)

    if tab == "matched_both":
        pagination = paginate_list(matched_both, page)
        matched_both = pagination.items
    elif tab == "mlc_only":
        pagination = paginate_list(mlc_only, page)
        mlc_only = pagination.items
    elif tab == "mri_only":
        pagination = paginate_list(mri_only, page)
        mri_only = pagination.items
    elif tab == "unregistered":
        pagination = paginate_list(unregistered, page)
        unregistered = pagination.items
    else:
        pagination = paginate_list(orphaned, page)
        orphaned = pagination.items

    return render_template_string(
        MECHANICAL_AUDIT_HTML,
        matched_both=matched_both, mlc_only=mlc_only, mri_only=mri_only,
        unregistered=unregistered, orphaned=orphaned,
        stats=stats, tab=tab, upload_meta=upload_meta, pagination=pagination,
    )


@bp.route("/mechanical-audit/upload", methods=["POST"])
def upload_catalog():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(FULL_ACCESS_ROLES):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    source = request.form.get("source", "").lower().strip()
    file   = request.files.get("file")

    if source not in ("mlc", "mri"):
        flash("Invalid source.", "error")
        return redirect(url_for("mechanical_audit.mechanical_audit"))
    if not file or not file.filename:
        flash("No file selected.", "error")
        return redirect(url_for("mechanical_audit.mechanical_audit"))

    ext = os.path.splitext(file.filename.lower())[1]
    if ext not in _ACCEPTED_EXTS:
        flash("Only .csv, .xlsx, or .xls files are accepted.", "error")
        return redirect(url_for("mechanical_audit.mechanical_audit"))

    content_bytes = file.read()
    try:
        rows = _read_file(content_bytes, file.filename)
    except Exception as e:
        flash(f"Could not read file: {e}", "error")
        return redirect(url_for("mechanical_audit.mechanical_audit"))

    if not rows:
        flash("The uploaded file appears to be empty.", "error")
        return redirect(url_for("mechanical_audit.mechanical_audit"))

    actual_cols = set(rows[0].keys())
    required    = _REQUIRED_COLS[source]
    if not required.issubset(actual_cols):
        missing = required - actual_cols
        flash(f"File missing expected columns: {', '.join(missing)}. "
              f"Make sure this is a {source.upper()} catalog export.", "error")
        return redirect(url_for("mechanical_audit.mechanical_audit"))

    title_col  = "PRIMARY TITLE" if source == "mlc" else "SONG TITLE"
    work_count = len({_s(r.get(title_col, "")) for r in rows
                      if _s(r.get(title_col, ""))})

    # Remove old files for this source
    for old_ext in _ACCEPTED_EXTS:
        old = os.path.join(_UPLOAD_DIR, f"{source}_catalog{old_ext}")
        if os.path.exists(old):
            os.remove(old)

    os.makedirs(_UPLOAD_DIR, exist_ok=True)
    with open(os.path.join(_UPLOAD_DIR, f"{source}_catalog{ext}"), "wb") as f_out:
        f_out.write(content_bytes)

    _write_meta(source, file.filename, ext, work_count)
    flash(f"{source.upper()} catalog updated — {work_count} works loaded.", "success")
    return redirect(url_for("mechanical_audit.mechanical_audit"))


@bp.route("/mechanical-audit/apply", methods=["POST"])
def apply_sync():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(FULL_ACCESS_ROLES):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    matched_both, mlc_only, mri_only, _u, _o, _m, _r = _build_audit()
    all_matched = matched_both + mlc_only + mri_only

    iswc_updated = mri_updated = mlc_created = skipped_iswc = confirmed_count = 0

    for entry in all_matched:
        w = entry["work"]
        m = entry["mlc"]
        r = entry["mri"]

        # Auto-confirm registration status
        if w.registration_status != "confirmed":
            w.registration_status = "confirmed"
            confirmed_count += 1

        if not w.iswc:
            if entry["iswc_conflict"]:
                skipped_iswc += 1
            elif entry["suggested_iswc"]:
                w.iswc = entry["suggested_iswc"]
                iswc_updated += 1

        if r and r.get("mri_song_id") and not w.mri_song_id:
            w.mri_song_id = r["mri_song_id"]
            mri_updated += 1

        if m and m.get("mlc_song_code"):
            exists = ProRegistration.query.filter_by(work_id=w.id, pro="MLC").first()
            if exists:
                if not exists.mlc_song_code:
                    exists.mlc_song_code = m["mlc_song_code"]
                    mlc_created += 1
            else:
                db.session.add(ProRegistration(
                    work_id=w.id, pro="MLC",
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
        flash(f"Error: {e}", "error")
        return redirect(url_for("mechanical_audit.mechanical_audit"))

    parts = []
    if iswc_updated:    parts.append(f"{iswc_updated} ISWC numbers written")
    if mri_updated:     parts.append(f"{mri_updated} MRI Song IDs written")
    if mlc_created:     parts.append(f"{mlc_created} MLC Song Codes saved")
    if confirmed_count: parts.append(f"{confirmed_count} works marked confirmed")
    if skipped_iswc:    parts.append(f"{skipped_iswc} works skipped (ISWC conflict)")
    flash(", ".join(parts) + "." if parts else "Nothing to update.", "success")
    return redirect(url_for("mechanical_audit.mechanical_audit", tab="matched_both"))


@bp.route("/mechanical-audit/apply-title", methods=["POST"])
def apply_title():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(FULL_ACCESS_ROLES):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    work_id   = request.form.get("work_id", type=int)
    new_title = (request.form.get("new_title") or "").strip()
    if not work_id or not new_title:
        flash("Invalid request.", "error")
        return redirect(url_for("mechanical_audit.mechanical_audit", tab="matched_both"))

    work = Work.query.get_or_404(work_id)
    old_title = work.title
    work.title = new_title
    work.normalized_title = _norm(new_title)
    try:
        db.session.commit()
        flash(f'Title updated: "{old_title}" → "{new_title}".', "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Error: {e}", "error")
    return redirect(url_for("mechanical_audit.mechanical_audit", tab="matched_both"))
