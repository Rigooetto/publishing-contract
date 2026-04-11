import csv
import datetime
import glob
import io
import json
import os
import re

from flask import Blueprint, render_template_string, request, redirect, url_for, flash

from models import Track, Release
from utils import auth_required, paginate_list, role_required, FULL_ACCESS_ROLES, normalize_for_match
from ui import NEIGHBORING_RIGHTS_AUDIT_HTML

bp = Blueprint("neighboring_rights_audit", __name__)

# ── Paths ─────────────────────────────────────────────────────────────────────

_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "..", "template")
_UPLOAD_DIR   = os.path.join(os.path.dirname(__file__), "..", "uploads", "neighboring_rights_catalogs")

_ACCEPTED_EXTS = {".csv", ".xlsx", ".xls"}

_REQUIRED_COLS = {"ARTIST", "TRACK TITLE", "ISRC"}


def _find_template(pattern):
    matches = sorted(glob.glob(os.path.join(_TEMPLATE_DIR, pattern)))
    return matches[-1] if matches else ""


# ── File I/O ──────────────────────────────────────────────────────────────────

def _norm_key(k):
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

def _uploaded_path():
    meta = _read_meta()
    if meta and meta.get("ext"):
        p = os.path.join(_UPLOAD_DIR, f"soundexchange_catalog{meta['ext']}")
        if os.path.exists(p):
            return p
    for ext in _ACCEPTED_EXTS:
        p = os.path.join(_UPLOAD_DIR, f"soundexchange_catalog{ext}")
        if os.path.exists(p):
            return p
    return ""


def _active_path():
    up = _uploaded_path()
    if up:
        return up
    return _find_template("SoundExchange*.csv")


def _meta_path():
    return os.path.join(_UPLOAD_DIR, "soundexchange_meta.json")


def _read_meta():
    path = _meta_path()
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            pass
    return None


def _write_meta(original_name, ext, track_count):
    os.makedirs(_UPLOAD_DIR, exist_ok=True)
    with open(_meta_path(), "w") as f:
        json.dump({
            "original_name": original_name,
            "ext": ext,
            "uploaded_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
            "track_count": track_count,
        }, f)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _norm(title):
    return normalize_for_match(title)


def _s(val):
    return str(val).strip() if val is not None else ""


def _artist_names(track):
    """Return a comma-separated artist string from Track.artists JSON field."""
    try:
        names = json.loads(track.artists or "[]")
        return ", ".join(n for n in names if n)
    except Exception:
        return track.artists or ""


# ── SoundExchange parser ──────────────────────────────────────────────────────

def _parse_sx():
    """
    SoundExchange export columns (uppercased after BOM strip):
    ARTIST, TRACK TITLE, ISRC, SXID, EFFECTIVE %, HOLD, REGISTRANT, PAYEE ID#, ASSOCIATION TYPE
    One row per registered recording.
    Returns {isrc: entry} — keyed by ISRC for reliable matching.
    Also builds a norm-title fallback index.
    """
    path = _active_path()
    if not path or not os.path.exists(path):
        return {}, {}

    with open(path, "rb") as f:
        rows = _read_file(f.read(), path)

    by_isrc  = {}
    by_title = {}

    for row in rows:
        artist     = _s(row.get("ARTIST", ""))
        title      = _s(row.get("TRACK TITLE", ""))
        isrc       = _s(row.get("ISRC", ""))
        sxid       = _s(row.get("SXID", ""))
        eff_pct    = _s(row.get("EFFECTIVE %", ""))
        hold       = _s(row.get("HOLD", ""))
        registrant = _s(row.get("REGISTRANT", ""))
        assoc_type = _s(row.get("ASSOCIATION TYPE", ""))

        if not title:
            continue

        entry = dict(title=title, artist=artist, isrc=isrc, sxid=sxid,
                     effective_pct=eff_pct, hold=hold,
                     registrant=registrant, assoc_type=assoc_type)

        if isrc:
            by_isrc[isrc] = entry
        by_title[_norm(title)] = entry

    return by_isrc, by_title


# ── Audit builder ─────────────────────────────────────────────────────────────

def _build_audit():
    sx_by_isrc, sx_by_title = _parse_sx()

    # All tracks with their release loaded
    all_tracks = (Track.query
                  .join(Release, Release.id == Track.release_id)
                  .order_by(Release.title, Track.track_number)
                  .all())

    matched      = []
    unregistered = []
    matched_isrcs  = set()
    matched_titles = set()

    for t in all_tracks:
        sx = None
        # 1. Match by ISRC (most reliable)
        if t.isrc:
            sx = sx_by_isrc.get(t.isrc.strip())
        # 2. Fall back to normalized title
        if sx is None:
            sx = sx_by_title.get(_norm(t.primary_title))

        entry = dict(track=t, release=t.release, sx=sx,
                     artist_names=_artist_names(t))

        if sx:
            matched.append(entry)
            if sx.get("isrc"):
                matched_isrcs.add(sx["isrc"])
            matched_titles.add(_norm(sx["title"]))
        else:
            unregistered.append(entry)

    # SoundExchange entries with no DB match
    orphaned = []
    for isrc, v in sx_by_isrc.items():
        if isrc not in matched_isrcs and _norm(v["title"]) not in matched_titles:
            orphaned.append(v)
    # Also catch title-only matches that weren't ISRC-matched
    for norm_title, v in sx_by_title.items():
        if (not v.get("isrc") and norm_title not in matched_titles
                and v not in orphaned):
            orphaned.append(v)

    sx_total = len(sx_by_isrc) or len(sx_by_title)

    return matched, unregistered, orphaned, sx_total


# ── Routes ────────────────────────────────────────────────────────────────────

@bp.route("/neighboring-rights-audit")
def neighboring_rights_audit():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(FULL_ACCESS_ROLES):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    matched, unregistered, orphaned, sx_total = _build_audit()

    stats = dict(
        sx_total=sx_total,
        db_total=len(matched) + len(unregistered),
        matched=len(matched),
        unregistered=len(unregistered),
        orphaned=len(orphaned),
    )

    upload_meta = _read_meta()
    tab  = request.args.get("tab", "matched")
    page = request.args.get("page", 1, type=int)

    if tab == "matched":
        pagination = paginate_list(matched, page)
        matched = pagination.items
    elif tab == "unregistered":
        pagination = paginate_list(unregistered, page)
        unregistered = pagination.items
    else:
        pagination = paginate_list(orphaned, page)
        orphaned = pagination.items

    return render_template_string(
        NEIGHBORING_RIGHTS_AUDIT_HTML,
        matched=matched,
        unregistered=unregistered,
        orphaned=orphaned,
        stats=stats,
        tab=tab,
        upload_meta=upload_meta,
        pagination=pagination,
    )


@bp.route("/neighboring-rights-audit/upload", methods=["POST"])
def upload_catalog():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(FULL_ACCESS_ROLES):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    file = request.files.get("file")

    if not file or not file.filename:
        flash("No file selected.", "error")
        return redirect(url_for("neighboring_rights_audit.neighboring_rights_audit"))

    ext = os.path.splitext(file.filename.lower())[1]
    if ext not in _ACCEPTED_EXTS:
        flash("Only .csv, .xlsx, or .xls files are accepted.", "error")
        return redirect(url_for("neighboring_rights_audit.neighboring_rights_audit"))

    content_bytes = file.read()
    try:
        rows = _read_file(content_bytes, file.filename)
    except Exception as e:
        flash(f"Could not read file: {e}", "error")
        return redirect(url_for("neighboring_rights_audit.neighboring_rights_audit"))

    if not rows:
        flash("The file appears to be empty.", "error")
        return redirect(url_for("neighboring_rights_audit.neighboring_rights_audit"))

    actual_cols = set(rows[0].keys())
    if not _REQUIRED_COLS.issubset(actual_cols):
        missing = _REQUIRED_COLS - actual_cols
        flash(f"File missing expected columns: {', '.join(missing)}. "
              "Make sure this is a SoundExchange Rights Owner catalog export.", "error")
        return redirect(url_for("neighboring_rights_audit.neighboring_rights_audit"))

    track_count = len({_s(r.get("ISRC", "")) or _s(r.get("TRACK TITLE", ""))
                       for r in rows if _s(r.get("TRACK TITLE", ""))})

    # Remove old files
    for old_ext in _ACCEPTED_EXTS:
        old = os.path.join(_UPLOAD_DIR, f"soundexchange_catalog{old_ext}")
        if os.path.exists(old):
            os.remove(old)

    os.makedirs(_UPLOAD_DIR, exist_ok=True)
    with open(os.path.join(_UPLOAD_DIR, f"soundexchange_catalog{ext}"), "wb") as f_out:
        f_out.write(content_bytes)

    _write_meta(file.filename, ext, track_count)
    flash(f"SoundExchange catalog updated — {track_count} tracks loaded.", "success")
    return redirect(url_for("neighboring_rights_audit.neighboring_rights_audit"))
