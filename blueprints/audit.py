import csv
import datetime
import os
import re

from flask import Blueprint, render_template_string, request, redirect, url_for, flash

from extensions import db
from models import Work, ProRegistration
from utils import auth_required
from ui import PRO_AUDIT_HTML

bp = Blueprint("audit", __name__)

# ── CSV file paths ────────────────────────────────────────────────────────────

_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "..", "template")
ASCAP_CSV  = os.path.join(_TEMPLATE_DIR, "ASCAPWorksCatalog (1).csv")
BMI_CSV    = os.path.join(_TEMPLATE_DIR, "BMICatalogExport_1771278.csv")
SESAC_CSV  = os.path.join(_TEMPLATE_DIR, "SESACSong_Catalog_SESAC_LLC (1).csv")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _norm(title):
    """Lowercase, strip punctuation, collapse whitespace for fuzzy matching."""
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
    """Return {norm_title: {title, work_id, iswc, reg_date, reg_status, writers, publisher}}."""
    works = {}
    if not os.path.exists(ASCAP_CSV):
        return works
    with open(ASCAP_CSV, encoding="utf-8-sig") as f:
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
    if not os.path.exists(BMI_CSV):
        return works
    with open(BMI_CSV, encoding="utf-8-sig") as f:
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
    if not os.path.exists(SESAC_CSV):
        return works
    with open(SESAC_CSV, encoding="utf-8-sig") as f:
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
    """Match DB works against all three PRO CSVs. Returns (matched, unregistered, orphans)."""
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
        iswc_conflict   = len(iswcs_found) > 1
        suggested_iswc  = next(iter(iswcs_found), "") if not w.iswc else ""

        entry = dict(
            work=w,
            ascap=a,
            bmi=b,
            sesac=s,
            suggested_iswc=suggested_iswc,
            iswc_conflict=iswc_conflict,
            iswcs_found=iswcs_found,
        )

        if a or b or s:
            matched.append(entry)
        else:
            unregistered.append(entry)

    # Works in PRO CSVs with no match in LabelMind DB
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
        db_total=matched + unregistered,   # will be overridden below
        matched=len(matched),
        unregistered=len(unregistered),
        orphaned=len(orphaned),
    )
    stats["db_total"] = len(matched) + len(unregistered)

    tab = request.args.get("tab", "matched")

    return render_template_string(
        PRO_AUDIT_HTML,
        matched=matched,
        unregistered=unregistered,
        orphaned=orphaned,
        stats=stats,
        tab=tab,
    )


@bp.route("/pro-audit/apply", methods=["POST"])
def apply_iswc():
    if auth_required():
        return redirect(url_for("publishing.login"))
    """
    For every DB work matched in a PRO CSV:
      1. Write ISWC if the work has none and there is no conflict.
      2. Create a ProRegistration record if one doesn't exist for that PRO.
    """
    matched, _unregistered, _orphaned, _a, _b, _s = _build_audit()

    iswc_updated = 0
    reg_created  = 0
    skipped_iswc = 0

    for entry in matched:
        w = entry["work"]

        # ── ISWC update ───────────────────────────────────────────────────────
        if not w.iswc:
            if entry["iswc_conflict"]:
                skipped_iswc += 1          # conflicting values — don't auto-pick
            elif entry["suggested_iswc"]:
                w.iswc = entry["suggested_iswc"]
                iswc_updated += 1

        # ── ProRegistration records ───────────────────────────────────────────
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
            pr = ProRegistration(
                work_id=w.id,
                pro=pro_name,
                pro_work_number=src.get("work_id", ""),
                registered_at=reg_date,
                registered_by="PRO CSV Import",
                notes=f"Imported from {pro_name} catalog export",
            )
            db.session.add(pr)
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
