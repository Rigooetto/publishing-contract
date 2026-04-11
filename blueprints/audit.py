import csv
import datetime
import json
import os
import re

from flask import Blueprint, render_template_string, request, redirect, url_for, flash
from werkzeug.utils import secure_filename

from extensions import db
from models import Work, ProRegistration
from utils import auth_required
from ui import PRO_AUDIT_HTML

bp = Blueprint("audit", __name__)

# ── Paths ─────────────────────────────────────────────────────────────────────

_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "..", "template")
_UPLOAD_DIR   = os.path.join(os.path.dirname(__file__), "..", "uploads", "pro_catalogs")

# Bundled fallback files (committed to repo)
_FALLBACK = {
    "ascap":  os.path.join(_TEMPLATE_DIR, "ASCAPWorksCatalog (1).csv"),
    "bmi":    os.path.join(_TEMPLATE_DIR, "BMICatalogExport_1771278.csv"),
    "sesac":  os.path.join(_TEMPLATE_DIR, "SESACSong_Catalog_SESAC_LLC (1).csv"),
}

# Expected header columns to validate uploads
_REQUIRED_COLS = {
    "ascap":  {"Work Title", "ASCAP Work ID", "ISWC Number"},
    "bmi":    {"Title", "TitleNumber", "ISWCNumber"},
    "sesac":  {"Song Title", "Song #", "ISWC #"},
}


def _uploaded_csv(pro):
    """Return path to the uploaded CSV for `pro`, or '' if none exists."""
    return os.path.join(_UPLOAD_DIR, f"{pro}_catalog.csv")


def _active_csv(pro):
    """Return the active CSV path: uploaded file if present, otherwise bundled fallback."""
    up = _uploaded_csv(pro)
    if os.path.exists(up):
        return up
    return _FALLBACK.get(pro, "")


def _meta_path(pro):
    return os.path.join(_UPLOAD_DIR, f"{pro}_meta.json")


def _read_meta(pro):
    path = _meta_path(pro)
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            pass
    return None


def _write_meta(pro, original_name, work_count):
    os.makedirs(_UPLOAD_DIR, exist_ok=True)
    with open(_meta_path(pro), "w") as f:
        json.dump({
            "original_name": original_name,
            "uploaded_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
            "work_count": work_count,
        }, f)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _norm(title):
    t = (title or "").lower().strip()
    t = re.sub(r"[^\w\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _parse_date(raw, *fmts):
    for fmt in fmts:
        try:
            return datetime.datetime.strptime(raw.strip(), fmt).date()
        except (ValueError, AttributeError):
            pass
    return datetime.date.today()


def _parse_ascap():
    works = {}
    path = _active_csv("ascap")
    if not path or not os.path.exists(path):
        return works
    with open(path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            title   = row.get("Work Title", "").strip()
            work_id = row.get("ASCAP Work ID", "").strip()
            iswc    = row.get("ISWC Number", "").strip().strip('"')
            reg_date   = row.get("Registration Date", "").strip()
            reg_status = row.get("Registration Status", "").strip()
            name    = row.get("Interested Parties", "").strip()
            role    = row.get("Role", "").strip().upper()
            if not title or not work_id:
                continue
            key = _norm(title)
            if key not in works:
                works[key] = dict(title=title, work_id=work_id, iswc=iswc,
                                  reg_date=reg_date, reg_status=reg_status,
                                  writers=[], publisher="")
            if role in ("C", "A", "CA", "AR"):
                if name and name not in works[key]["writers"]:
                    works[key]["writers"].append(name)
            elif role in ("E", "AM") and not works[key]["publisher"]:
                works[key]["publisher"] = name
    return works


def _parse_bmi():
    works = {}
    path = _active_csv("bmi")
    if not path or not os.path.exists(path):
        return works
    with open(path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            title   = row.get("Title", "").strip()
            work_id = row.get("TitleNumber", "").strip()
            iswc    = row.get("ISWCNumber", "").strip()
            reg_date   = row.get("RegistrationDate", "").strip()
            reg_status = row.get("SongviewStatus", "").strip()
            name    = row.get("Participant", "").strip()
            kind    = row.get("WtrPubIndicator", "").strip().upper()
            if not title or not work_id:
                continue
            key = _norm(title)
            if key not in works:
                works[key] = dict(title=title, work_id=work_id, iswc=iswc,
                                  reg_date=reg_date, reg_status=reg_status,
                                  writers=[], publisher="")
            if kind == "W":
                if name and name not in works[key]["writers"]:
                    works[key]["writers"].append(name)
            elif kind == "P" and not works[key]["publisher"]:
                works[key]["publisher"] = name
    return works


def _parse_sesac():
    works = {}
    path = _active_csv("sesac")
    if not path or not os.path.exists(path):
        return works
    with open(path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            title   = row.get("Song Title", "").strip()
            work_id = row.get("Song #", "").strip()
            iswc    = row.get("ISWC #", "").strip()
            reg_date = row.get("Reg. Date", "").strip()
            name     = row.get("Name", "").strip()
            kind     = row.get("Publisher/Writer", "").strip().upper()
            if not title or not work_id:
                continue
            key = _norm(title)
            if key not in works:
                works[key] = dict(title=title, work_id=work_id, iswc=iswc,
                                  reg_date=reg_date, reg_status="",
                                  writers=[], publisher="")
            if kind == "W":
                if name and name not in works[key]["writers"]:
                    works[key]["writers"].append(name)
            elif kind == "P" and not works[key]["publisher"]:
                works[key]["publisher"] = name
    return works


def _build_audit():
    ascap  = _parse_ascap()
    bmi    = _parse_bmi()
    sesac  = _parse_sesac()

    all_works = Work.query.order_by(Work.title).all()
    db_keys   = {_norm(w.title): w for w in all_works}

    matched      = []
    unregistered = []

    for w in all_works:
        key = _norm(w.title)
        a = ascap.get(key)
        b = bmi.get(key)
        s = sesac.get(key)

        iswcs_found = set(filter(None, [
            (a or {}).get("iswc"),
            (b or {}).get("iswc"),
            (s or {}).get("iswc"),
        ]))
        iswc_conflict  = len(iswcs_found) > 1
        suggested_iswc = next(iter(iswcs_found), "") if not w.iswc else ""

        entry = dict(
            work=w, ascap=a, bmi=b, sesac=s,
            suggested_iswc=suggested_iswc,
            iswc_conflict=iswc_conflict,
            iswcs_found=iswcs_found,
        )
        if a or b or s:
            matched.append(entry)
        else:
            unregistered.append(entry)

    orphaned = []
    for pro_name, src_dict in [("ASCAP", ascap), ("BMI", bmi), ("SESAC", sesac)]:
        for key, v in src_dict.items():
            if key not in db_keys:
                orphaned.append(dict(pro=pro_name, **v))

    return matched, unregistered, orphaned, ascap, bmi, sesac


# ── Routes ────────────────────────────────────────────────────────────────────

@bp.route("/pro-audit")
def pro_audit():
    if auth_required():
        return redirect(url_for("publishing.login"))

    matched, unregistered, orphaned, ascap, bmi, sesac = _build_audit()

    stats = dict(
        ascap_total=len(ascap),
        bmi_total=len(bmi),
        sesac_total=len(sesac),
        db_total=len(matched) + len(unregistered),
        matched=len(matched),
        unregistered=len(unregistered),
        orphaned=len(orphaned),
    )

    upload_meta = {
        "ascap": _read_meta("ascap"),
        "bmi":   _read_meta("bmi"),
        "sesac": _read_meta("sesac"),
    }

    tab = request.args.get("tab", "matched")

    return render_template_string(
        PRO_AUDIT_HTML,
        matched=matched,
        unregistered=unregistered,
        orphaned=orphaned,
        stats=stats,
        tab=tab,
        upload_meta=upload_meta,
    )


@bp.route("/pro-audit/upload", methods=["POST"])
def upload_catalog():
    if auth_required():
        return redirect(url_for("publishing.login"))

    pro  = request.form.get("pro", "").lower().strip()
    file = request.files.get("file")

    if pro not in ("ascap", "bmi", "sesac"):
        flash("Invalid PRO specified.", "error")
        return redirect(url_for("audit.pro_audit"))

    if not file or not file.filename:
        flash("No file selected.", "error")
        return redirect(url_for("audit.pro_audit"))

    if not file.filename.lower().endswith(".csv"):
        flash("Only .csv files are accepted.", "error")
        return redirect(url_for("audit.pro_audit"))

    # Read content and validate headers
    content = file.read().decode("utf-8-sig", errors="replace")
    lines   = content.splitlines()
    if not lines:
        flash("The uploaded file is empty.", "error")
        return redirect(url_for("audit.pro_audit"))

    reader   = csv.DictReader(lines)
    fieldnames = set(reader.fieldnames or [])
    required   = _REQUIRED_COLS[pro]
    if not required.issubset(fieldnames):
        missing = required - fieldnames
        flash(f"File does not look like a {pro.upper()} export. Missing columns: {', '.join(missing)}", "error")
        return redirect(url_for("audit.pro_audit"))

    # Count unique works and save file
    rows = list(reader)
    work_count = len({r.get(list(required)[0], "") for r in rows if r.get(list(required)[0], "").strip()})

    os.makedirs(_UPLOAD_DIR, exist_ok=True)
    dest = _uploaded_csv(pro)
    with open(dest, "w", encoding="utf-8") as f:
        f.write(content)

    _write_meta(pro, secure_filename(file.filename), work_count)
    flash(f"{pro.upper()} catalog updated — {work_count} works loaded from {file.filename}.", "success")
    return redirect(url_for("audit.pro_audit"))


@bp.route("/pro-audit/apply", methods=["POST"])
def apply_iswc():
    if auth_required():
        return redirect(url_for("publishing.login"))

    matched, _unregistered, _orphaned, _a, _b, _s = _build_audit()

    iswc_updated = 0
    reg_created  = 0
    skipped_iswc = 0

    for entry in matched:
        w = entry["work"]

        if not w.iswc:
            if entry["iswc_conflict"]:
                skipped_iswc += 1
            elif entry["suggested_iswc"]:
                w.iswc = entry["suggested_iswc"]
                iswc_updated += 1

        for pro_name, src in [("ASCAP", entry["ascap"]),
                               ("BMI",   entry["bmi"]),
                               ("SESAC", entry["sesac"])]:
            if not src:
                continue
            exists = ProRegistration.query.filter_by(work_id=w.id, pro=pro_name).first()
            if exists:
                continue
            raw_date = src.get("reg_date", "")
            reg_date = _parse_date(raw_date, "%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y")
            db.session.add(ProRegistration(
                work_id=w.id,
                pro=pro_name,
                pro_work_number=src.get("work_id", ""),
                registered_at=reg_date,
                registered_by="PRO CSV Import",
                notes=f"Imported from {pro_name} catalog export",
            ))
            reg_created += 1

    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        flash(f"Error applying changes: {e}", "error")
        return redirect(url_for("audit.pro_audit"))

    parts = []
    if iswc_updated:
        parts.append(f"{iswc_updated} ISWC numbers written")
    if reg_created:
        parts.append(f"{reg_created} PRO registration records created")
    if skipped_iswc:
        parts.append(f"{skipped_iswc} works skipped (ISWC conflict — review manually)")
    flash(", ".join(parts) + "." if parts else "Nothing to update.", "success")
    return redirect(url_for("audit.pro_audit", tab="matched"))
