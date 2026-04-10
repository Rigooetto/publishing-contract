import datetime

from flask import Blueprint, request, jsonify, current_app
from sqlalchemy import func, or_

from extensions import db
import json as _json

from models import Writer, Work, WorkWriter, Release, Track
from utils import auth_required, default_publisher_for_pro, default_publisher_ipi_for_pro, build_full_name, normalize_title, build_session_name
from ui import WRITER_MODAL_HTML

bp = Blueprint("api", __name__)


@bp.route("/writers/search")
def search_writers():
    if auth_required():
        return jsonify([])
    q = (request.args.get("q") or "").strip()
    if len(q) < 2:
        return jsonify([])
    like_q = "%" + q.lower() + "%"
    writers = (
        Writer.query
        .filter(or_(
            func.lower(Writer.full_name).like(like_q),
            func.lower(Writer.first_name).like(like_q),
            func.lower(Writer.middle_name).like(like_q),
            func.lower(Writer.last_names).like(like_q),
            func.lower(Writer.writer_aka).like(like_q),
            func.lower(Writer.ipi).like(like_q),
        ))
        .order_by(Writer.full_name.asc())
        .limit(8)
        .all()
    )
    return jsonify([
        {
            "id": w.id,
            "first_name": w.first_name,
            "middle_name": w.middle_name,
            "last_names": w.last_names,
            "full_name": w.full_name,
            "writer_aka": w.writer_aka,
            "ipi": w.ipi or "",
            "email": w.email or "",
            "phone_number": w.phone_number or "",
            "pro": w.pro,
            "address": w.address,
            "city": w.city,
            "state": w.state,
            "zip_code": w.zip_code,
            "has_master_contract": w.has_master_contract,
            "default_publisher": w.default_publisher or default_publisher_for_pro(w.pro),
            "default_publisher_ipi": w.default_publisher_ipi or default_publisher_ipi_for_pro(w.pro),
        }
        for w in writers
    ])


@bp.route("/writers/<int:writer_id>/json")
def writer_json(writer_id):
    if auth_required():
        return jsonify({}), 401
    w = Writer.query.get_or_404(writer_id)
    return jsonify({
        "id": w.id,
        "first_name": w.first_name,
        "middle_name": w.middle_name,
        "last_names": w.last_names,
        "full_name": w.full_name,
        "writer_aka": w.writer_aka,
        "ipi": w.ipi or "",
        "email": w.email or "",
        "phone_number": w.phone_number or "",
        "pro": w.pro or "",
        "address": w.address or "",
        "city": w.city or "",
        "state": w.state or "",
        "zip_code": w.zip_code or "",
        "has_master_contract": w.has_master_contract,
        "default_publisher": w.default_publisher or default_publisher_for_pro(w.pro),
        "default_publisher_ipi": w.default_publisher_ipi or default_publisher_ipi_for_pro(w.pro),
    })


@bp.route("/writers/<int:writer_id>/modal")
def writer_modal(writer_id):
    if auth_required():
        return ""
    from flask import render_template_string
    writer = Writer.query.get_or_404(writer_id)
    return render_template_string(
        WRITER_MODAL_HTML,
        writer=writer,
        default_publisher_for_pro=default_publisher_for_pro,
        default_publisher_ipi_for_pro=default_publisher_ipi_for_pro,
    )


@bp.route("/writers/<int:writer_id>/modal-save", methods=["POST"])
def writer_modal_save(writer_id):
    if auth_required():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    writer = Writer.query.get_or_404(writer_id)

    first_name = (request.form.get("first_name") or "").strip()
    middle_name = (request.form.get("middle_name") or "").strip()
    last_names = (request.form.get("last_names") or "").strip()
    full_name = build_full_name(first_name, middle_name, last_names)
    writer_aka = (request.form.get("writer_aka") or "").strip()
    email = (request.form.get("email") or "").strip()
    phone_number = (request.form.get("phone_number") or "").strip()
    ipi = (request.form.get("ipi") or "").strip()
    pro = (request.form.get("pro") or "").strip()
    default_publisher = (request.form.get("default_publisher") or "").strip()
    default_publisher_ipi = (request.form.get("default_publisher_ipi") or "").strip()
    address = (request.form.get("address") or "").strip()
    city = (request.form.get("city") or "").strip()
    state = (request.form.get("state") or "").strip()
    zip_code = (request.form.get("zip_code") or "").strip()
    has_master_contract = request.form.get("has_master_contract") == "1"

    if not first_name or not last_names:
        return jsonify({"ok": False, "error": "First and last name are required."})
    if not email or "@" not in email:
        return jsonify({"ok": False, "error": "Valid email is required."})
    if not ipi:
        return jsonify({"ok": False, "error": "IPI is required."})
    if not pro:
        return jsonify({"ok": False, "error": "PRO is required."})

    existing_ipi = Writer.query.filter(
        func.lower(Writer.ipi) == ipi.lower(),
        Writer.id != writer.id
    ).first()
    if existing_ipi:
        return jsonify({"ok": False, "error": "That IPI already belongs to " + existing_ipi.full_name})

    existing_name = Writer.query.filter(
        func.lower(Writer.full_name) == full_name.lower(),
        Writer.id != writer.id
    ).first()
    if existing_name:
        return jsonify({"ok": False, "error": "That full name already exists for another writer."})

    writer.first_name = first_name
    writer.middle_name = middle_name
    writer.last_names = last_names
    writer.full_name = full_name
    writer.writer_aka = writer_aka
    writer.email = email
    writer.phone_number = phone_number
    writer.ipi = ipi
    writer.pro = pro
    writer.default_publisher = default_publisher
    writer.default_publisher_ipi = default_publisher_ipi
    writer.address = address
    writer.city = city
    writer.state = state
    writer.zip_code = zip_code
    writer.has_master_contract = has_master_contract

    WorkWriter.query.filter(
        WorkWriter.writer_id == writer.id
    ).update({
        WorkWriter.publisher: default_publisher,
        WorkWriter.publisher_ipi: default_publisher_ipi
    })
    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        current_app.logger.error("writer_modal_save error: %s", e)
        return jsonify({"ok": False, "error": "An error occurred while saving. Please try again."})

    return jsonify({
        "ok": True,
        "writer": {
            "id": writer.id,
            "full_name": writer.full_name,
            "ipi": writer.ipi or "",
            "pro": writer.pro or "",
            "email": writer.email or "",
            "default_publisher": writer.default_publisher or "",
            "default_publisher_ipi": writer.default_publisher_ipi or "",
        }
    })


@bp.route("/works/search")
def works_search():
    if auth_required():
        return jsonify([])
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])
    works = Work.query.filter(Work.title.ilike(f"%{q}%")).limit(10).all()
    return jsonify([{
        "id": w.id,
        "title": w.title,
        "writers": ", ".join(ww.writer.full_name for ww in w.work_writers if ww.writer)
    } for w in works])


@bp.route("/artists/search")
def artists_search():
    if auth_required():
        return jsonify([])
    q = (request.args.get("q") or "").strip()
    if len(q) < 2:
        return jsonify([])
    # Collect unique artist names from both Release.artists and Track.artists
    like_q = f"%{q.lower()}%"
    seen = set()
    results = []
    for row in Release.query.filter(Release.artists.ilike(like_q)).limit(50).all():
        for name in (_json.loads(row.artists) if row.artists else []):
            if q.lower() in name.lower() and name not in seen:
                seen.add(name)
                results.append(name)
    for row in Track.query.filter(Track.artists.ilike(like_q)).limit(50).all():
        for name in (_json.loads(row.artists) if row.artists else []):
            if q.lower() in name.lower() and name not in seen:
                seen.add(name)
                results.append(name)
    results.sort(key=lambda s: s.lower())
    return jsonify(results[:10])


@bp.route("/tracks/search")
def tracks_search():
    if auth_required():
        return jsonify([])
    q = (request.args.get("q") or "").strip()
    if len(q) < 2:
        return jsonify([])
    tracks = (
        Track.query
        .filter(Track.primary_title.ilike(f"%{q}%"))
        .order_by(Track.primary_title.asc())
        .limit(10).all()
    )
    return jsonify([{
        "primary_title":      t.primary_title,
        "recording_title":    t.recording_title or "",
        "aka_title":          t.aka_title or "",
        "aka_type_code":      t.aka_type_code or "",
        "duration":           t.duration or "",
        "isrc":               t.isrc or "",
        "genre":              t.genre or "",
        "producer":           t.producer or "",
        "recording_engineer": t.recording_engineer or "",
        "executive_producer": t.executive_producer or "",
        "track_label":        t.track_label or "",
        "track_p_line":       t.track_p_line or "",
        "artists":            _json.loads(t.artists) if t.artists else [],
    } for t in tracks])


@bp.route("/works/quick-create", methods=["POST"])
def work_quick_create():
    """Create a Work (with writers) from the Release form quick-create modal."""
    if auth_required():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    from models import GenerationBatch
    from utils import parse_float, find_existing_writer, build_full_name as _bfn
    from config import DEFAULT_PUBLISHER_ADDRESS, DEFAULT_PUBLISHER_CITY, DEFAULT_PUBLISHER_STATE, DEFAULT_PUBLISHER_ZIP

    f = request.form
    title = f.get("title", "").strip()
    session_name = f.get("session_name", "").strip()
    contract_date_str = f.get("contract_date", "").strip()
    existing_batch_id = f.get("existing_batch_id", "").strip()

    if not title:
        return jsonify({"ok": False, "error": "Work title is required."})

    contract_date = None
    if contract_date_str:
        try:
            contract_date = datetime.datetime.strptime(contract_date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify({"ok": False, "error": "Invalid contract date."})

    # --- Session ---
    batch = None
    if existing_batch_id:
        try:
            batch = GenerationBatch.query.get(int(existing_batch_id))
        except (ValueError, TypeError):
            return jsonify({"ok": False, "error": "Invalid session ID."})
        if not batch:
            return jsonify({"ok": False, "error": "Session not found."})
        if not contract_date:
            contract_date = batch.contract_date
    elif session_name:
        batch = GenerationBatch(
            session_name=build_session_name(session_name),
            contract_date=contract_date or datetime.date.today(),
            created_by="",
            status="draft",
        )
        db.session.add(batch)
        db.session.flush()

    # --- Work ---
    work = Work(
        title=title,
        normalized_title=normalize_title(title),
        batch_id=batch.id if batch else None,
        contract_date=contract_date,
    )
    db.session.add(work)
    db.session.flush()

    # --- Writers ---
    writer_ids     = f.getlist("writer_id")
    first_names    = f.getlist("writer_first_name")
    middle_names   = f.getlist("writer_middle_name")
    last_names_l   = f.getlist("writer_last_names")
    writer_akas    = f.getlist("writer_aka")
    ipis           = f.getlist("writer_ipi")
    emails         = f.getlist("writer_email")
    phones         = f.getlist("writer_phone_number")
    pros           = f.getlist("writer_pro")
    percentages    = f.getlist("writer_percentage")
    publishers     = f.getlist("writer_publisher")
    publisher_ipis = f.getlist("publisher_ipi")
    pub_addresses  = f.getlist("publisher_address")
    pub_cities     = f.getlist("publisher_city")
    pub_states     = f.getlist("publisher_state")
    pub_zips       = f.getlist("publisher_zip_code")
    addresses      = f.getlist("writer_address")
    cities         = f.getlist("writer_city")
    states         = f.getlist("writer_state")
    zip_codes      = f.getlist("writer_zip_code")

    # Validate split total before touching the DB
    total_split = 0.0
    for i, fn in enumerate(first_names):
        fn = fn.strip()
        ln = (last_names_l[i] if i < len(last_names_l) else "").strip()
        if not fn or not ln:
            continue
        try:
            total_split += float((percentages[i] if i < len(percentages) else "0") or 0)
        except ValueError:
            pass
    if first_names and abs(total_split - 100.0) >= 0.001:
        db.session.rollback()
        return jsonify({"ok": False, "error": f"Writer splits must total 100%. Current total: {round(total_split, 2)}%"})

    try:
        writer_names_saved = []
        for i, fn in enumerate(first_names):
            fn = fn.strip()
            ln = (last_names_l[i] if i < len(last_names_l) else "").strip()
            if not fn or not ln:
                continue
            mn = (middle_names[i] if i < len(middle_names) else "").strip()
            full_name = _bfn(fn, mn, ln)
            ipi  = (ipis[i] if i < len(ipis) else "").strip()
            pro  = (pros[i] if i < len(pros) else "").strip()
            pct  = parse_float(percentages[i] if i < len(percentages) else "0")

            writer = find_existing_writer(writer_ids[i] if i < len(writer_ids) else "")
            # IPI match takes priority; only fall back to name if no IPI match found
            if not writer and ipi:
                writer = Writer.query.filter(func.lower(Writer.ipi) == ipi.lower()).first()
            if not writer and full_name:
                writer = Writer.query.filter(func.lower(Writer.full_name) == full_name.lower()).first()

            if writer:
                writer.first_name  = fn or writer.first_name
                writer.middle_name = mn or writer.middle_name
                writer.last_names  = ln or writer.last_names
                # Only update full_name if it doesn't conflict with another writer
                if full_name and full_name.lower() != writer.full_name.lower():
                    conflict = Writer.query.filter(
                        func.lower(Writer.full_name) == full_name.lower(),
                        Writer.id != writer.id
                    ).first()
                    if not conflict:
                        writer.full_name = full_name
                writer.ipi   = ipi or writer.ipi
                writer.email = (emails[i] if i < len(emails) else "").strip() or writer.email
                writer.pro   = pro or writer.pro
            else:
                writer = Writer(
                    first_name=fn, middle_name=mn, last_names=ln, full_name=full_name,
                    writer_aka=(writer_akas[i] if i < len(writer_akas) else "").strip(),
                    ipi=ipi or None,
                    email=(emails[i] if i < len(emails) else "").strip(),
                    phone_number=(phones[i] if i < len(phones) else "").strip(),
                    pro=pro,
                    address=(addresses[i] if i < len(addresses) else "").strip(),
                    city=(cities[i] if i < len(cities) else "").strip(),
                    state=(states[i] if i < len(states) else "").strip(),
                    zip_code=(zip_codes[i] if i < len(zip_codes) else "").strip(),
                    has_master_contract=False,
                )
                db.session.add(writer)
                db.session.flush()

            ww = WorkWriter(
                work_id=work.id,
                writer_id=writer.id,
                writer_percentage=pct,
                publisher=(publishers[i] if i < len(publishers) else "").strip(),
                publisher_ipi=(publisher_ipis[i] if i < len(publisher_ipis) else "").strip(),
                publisher_address=(pub_addresses[i] if i < len(pub_addresses) else DEFAULT_PUBLISHER_ADDRESS).strip(),
                publisher_city=(pub_cities[i] if i < len(pub_cities) else DEFAULT_PUBLISHER_CITY).strip(),
                publisher_state=(pub_states[i] if i < len(pub_states) else DEFAULT_PUBLISHER_STATE).strip(),
                publisher_zip_code=(pub_zips[i] if i < len(pub_zips) else DEFAULT_PUBLISHER_ZIP).strip(),
            )
            db.session.add(ww)
            writer_names_saved.append(full_name)

        db.session.commit()
    except Exception as e:
        db.session.rollback()
        current_app.logger.error("work_quick_create error: %s", e)
        return jsonify({"ok": False, "error": "An error occurred while saving. Please try again."})

    batch_url = f"/batches/{batch.id}" if batch else None
    return jsonify({
        "ok": True,
        "work": {
            "id": work.id,
            "title": work.title,
            "writers": ", ".join(writer_names_saved),
        },
        "batch_id": batch.id if batch else None,
        "batch_url": batch_url,
    })
