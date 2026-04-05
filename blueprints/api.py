from flask import Blueprint, request, jsonify
from sqlalchemy import func, or_

from extensions import db
from models import Writer, Work, WorkWriter
from utils import auth_required, default_publisher_for_pro, default_publisher_ipi_for_pro, build_full_name
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
    db.session.commit()

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
        "writers": ", ".join(ww.writer.full_name for ww in w.work_writers)
    } for w in works])
