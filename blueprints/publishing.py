import io
import os
import re
import json
import uuid
import zipfile
import csv
import datetime
import base64
import xml.etree.ElementTree as ET
import traceback

from flask import (
    Blueprint, request, redirect, url_for, session, flash,
    render_template_string, send_file, jsonify, current_app, make_response
)
from sqlalchemy import func, or_
from sqlalchemy.orm import joinedload
from babel.dates import format_date
from docusign_esign import (
    EnvelopesApi, EnvelopeDefinition, Document as DocusignDocument,
    Signer, SignHere, Tabs, Recipients
)

from extensions import db
from models import (
    Camp, GenerationBatch, Writer, Work, WorkWriter,
    ContractDocument
)
from utils import (
    auth_required, slugify, parse_float, build_full_name,
    normalize_text, normalize_title,
    build_writer_identity_from_row, build_writer_identity_from_workwriter,
    build_session_name, default_publisher_for_pro, default_publisher_ipi_for_pro,
    get_drive_service, get_docusign_api_client, upload_bytes_to_drive,
    find_existing_writer, render_docx_template,
    collect_form_context, collect_submitted_form_data,
    get_batch_writer_summary, get_writer_directory_rows,
)
from ui import (
    LOGIN_HTML, FORM_HTML, DUPLICATE_WARNING_HTML,
    WORKS_LIST_HTML, BATCHES_LIST_HTML, BATCH_DETAIL_HTML,
    WORK_DETAIL_HTML, WORK_EDIT_HTML,
    WRITERS_LIST_HTML, WRITER_DETAIL_HTML, WRITER_EDIT_HTML,
    ADMIN_HTML, IMPORT_PREVIEW_HTML,
)
from config import (
    TEAM_USERNAME, TEAM_PASSWORD,
    DOCUSIGN_ACCOUNT_ID, DOCUSIGN_BASE_PATH,
    DOCUSIGN_INTEGRATION_KEY, DOCUSIGN_USER_ID, DOCUSIGN_PRIVATE_KEY,
    GOOGLE_DRIVE_FOLDER_ID, GOOGLE_SERVICE_ACCOUNT_JSON,
    FULL_CONTRACT_TEMPLATE, SCHEDULE_1_TEMPLATE, OUTPUT_DIR,
    DEFAULT_PUBLISHER_ADDRESS, DEFAULT_PUBLISHER_CITY,
    DEFAULT_PUBLISHER_STATE, DEFAULT_PUBLISHER_ZIP,
)

bp = Blueprint("publishing", __name__)

# Temporary server-side store for catalog import previews (keyed by UUID token)
_import_preview_store: dict = {}

@bp.route("/login", methods=["GET", "POST"])
def login():
    if not (TEAM_USERNAME and TEAM_PASSWORD):
        return redirect(url_for(".formulario"))
    if request.method == "POST":
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        if username == TEAM_USERNAME and password == TEAM_PASSWORD:
            session["logged_in"] = True
            return redirect(url_for(".formulario"))
        flash("Incorrect username or password.")
    return render_template_string(LOGIN_HTML)


@bp.route("/logout")
def logout():
    session.clear()
    return redirect(url_for(".login"))


@bp.route("/", methods=["GET", "POST"])
def formulario():
    if auth_required():
        return redirect(url_for(".login"))

    if request.method == "POST":
        work_title = (request.form.get("work_title") or "").strip()
        contract_date_str = (request.form.get("contract_date") or "").strip()

        normalized_title = normalize_title(work_title)


        if not work_title:
            flash("Work title is required.")
            return render_template_string(
            FORM_HTML,
            **collect_form_context(),
            **collect_submitted_form_data()
        )


        if not contract_date_str:
            flash("Contract date is required.")
            return render_template_string(
            FORM_HTML,
            **collect_form_context(),
            **collect_submitted_form_data()
        )

        try:
            contract_date = datetime.datetime.strptime(contract_date_str, "%Y-%m-%d").date()
        except ValueError:
            flash("Please enter a valid contract date.")
            return render_template_string(
            FORM_HTML,
            **collect_form_context(),
            **collect_submitted_form_data()
        )

        writer_ids = request.form.getlist("writer_id")
        first_names = request.form.getlist("writer_first_name")
        middle_names = request.form.getlist("writer_middle_name")
        last_names_list = request.form.getlist("writer_last_names")
        writer_akas = request.form.getlist("writer_aka")
        ipis = request.form.getlist("writer_ipi")
        emails = request.form.getlist("writer_email")
        phones = request.form.getlist("writer_phone_number")
        pros = request.form.getlist("writer_pro")
        percentages = request.form.getlist("writer_percentage")
        publishers = request.form.getlist("writer_publisher")
        publisher_ipis = request.form.getlist("publisher_ipi")
        publisher_addresses = request.form.getlist("publisher_address")
        publisher_cities = request.form.getlist("publisher_city")
        publisher_states = request.form.getlist("publisher_state")
        publisher_zips = request.form.getlist("publisher_zip_code")
        addresses = request.form.getlist("writer_address")
        cities = request.form.getlist("writer_city")
        states = request.form.getlist("writer_state")
        zip_codes = request.form.getlist("writer_zip_code")
        
        writer_rows = []
        total_split = 0.0
        for pct in percentages:
            try:
                total_split += float((pct or "0").strip() or 0)
            except ValueError:
                flash("Invalid writer split value.")
                return render_template_string(
                    FORM_HTML,
                    **collect_form_context(),
                    **collect_submitted_form_data()
                )

        if abs(total_split - 100.0) >= 0.001:
            flash("Total writer split must equal 100%. Current total: " + str(round(total_split, 2)) + "%")
            return render_template_string(
                FORM_HTML,
                **collect_form_context(),
                **collect_submitted_form_data()
             )
        

        for i in range(len(first_names)):
            first_name = (first_names[i] or "").strip()
            middle_name = (middle_names[i] or "").strip()
            last_names = (last_names_list[i] or "").strip()
            full_name = build_full_name(first_name, middle_name, last_names)
            if not full_name:
                continue
            split_value = parse_float(percentages[i] if i < len(percentages) else "0")
            if split_value <= 0:
                flash("Writer '" + full_name + "' must have a split greater than 0.")
                return render_template_string(
            FORM_HTML,
            **collect_form_context(),
            **collect_submitted_form_data()
        )
            
            writer_rows.append({
                "selected_writer_id": writer_ids[i] if i < len(writer_ids) else "",
                "first_name": first_name,
                "middle_name": middle_name,
                "last_names": last_names,
                "full_name": full_name,
                "writer_aka": (writer_akas[i] or "").strip(),
                "ipi": (ipis[i] or "").strip(),
                "email": (emails[i] or "").strip(),
                "phone_number": (phones[i] if i < len(phones) else "").strip(),
                "pro": (pros[i] or "").strip(),
                "writer_percentage": split_value,
                "publisher": (publishers[i] or "").strip(),
                "publisher_ipi": (publisher_ipis[i] or "").strip(),
                "publisher_address": (publisher_addresses[i] or DEFAULT_PUBLISHER_ADDRESS).strip(),
                "publisher_city": (publisher_cities[i] or DEFAULT_PUBLISHER_CITY).strip(),
                "publisher_state": (publisher_states[i] or DEFAULT_PUBLISHER_STATE).strip(),
                "publisher_zip_code": (publisher_zips[i] or DEFAULT_PUBLISHER_ZIP).strip(),
                "address": (addresses[i] or "").strip(),
                "city": (cities[i] or "").strip(),
                "state": (states[i] or "").strip(),
                "zip_code": (zip_codes[i] or "").strip(),
            })

        if not writer_rows:
            flash("Add at least one writer.")
            return render_template_string(
            FORM_HTML,
            **collect_form_context(),
            **collect_submitted_form_data()
        )


        for row in writer_rows:
            if not row["first_name"] or not row["last_names"]:
                flash("Each writer must have first and last name.")
                return render_template_string(
                    FORM_HTML,
                    **collect_form_context(),
                    **collect_submitted_form_data()
                )

            if not row["email"]:
                flash("Each writer must have an email.")
                return render_template_string(
                    FORM_HTML,
                    **collect_form_context(),
                    **collect_submitted_form_data()
                )

            if "@" not in row["email"]:
                flash("Invalid email format for " + row["full_name"])
                return render_template_string(
                    FORM_HTML,
                    **collect_form_context(),
                    **collect_submitted_form_data()
                )

            if not row["ipi"]:
                flash("Each writer must have an IPI number.")
                return render_template_string(
                    FORM_HTML,
                    **collect_form_context(),
                    **collect_submitted_form_data()
                )

            if not row["pro"]:
                flash("Each writer must have a PRO selected.")
                return render_template_string(
                    FORM_HTML,
                    **collect_form_context(),
                    **collect_submitted_form_data()
                )

            if not row["address"] or not row["city"] or not row["state"] or not row["zip_code"]:
                flash("Complete address required for " + row["full_name"])
                return render_template_string(
                    FORM_HTML,
                    **collect_form_context(),
                    **collect_submitted_form_data()
                )

       
            
        seen_writer_ids = set()
        seen_ipis = set()
        seen_names = set()

        for row in writer_rows:
            selected_writer_id = (row["selected_writer_id"] or "").strip()
            ipi = (row["ipi"] or "").strip()
            normalized_name = normalize_text(row["full_name"])
            if selected_writer_id:
                if selected_writer_id in seen_writer_ids:
                    flash("Duplicate writer selected in this work: " + row["full_name"])
                    return render_template_string(
                    FORM_HTML,
                    **collect_form_context(),
                    **collect_submitted_form_data()
                )
                seen_writer_ids.add(selected_writer_id)
            if ipi:
                ipi_key = ipi.lower()
                if ipi_key in seen_ipis:
                    flash("Duplicate IPI in this work: " + ipi)
                    return render_template_string(
                    FORM_HTML,
                    **collect_form_context(),
                    **collect_submitted_form_data()
                )
                seen_ipis.add(ipi_key)
            else:
                if normalized_name in seen_names:
                    flash("Duplicate writer name in this work: " + row["full_name"])
                    return render_template_string(
                    FORM_HTML,
                    **collect_form_context(),
                    **collect_submitted_form_data()
                )
                seen_names.add(normalized_name)

        for row in writer_rows:
            if row["ipi"]:
                existing_ipi_writer = Writer.query.filter(func.lower(Writer.ipi) == row["ipi"].lower()).first()
                if existing_ipi_writer:
                    selected_id = (row["selected_writer_id"] or "").strip()
                    if not selected_id or str(existing_ipi_writer.id) != selected_id:
                        flash("IPI " + row["ipi"] + " already belongs to " + existing_ipi_writer.full_name + ". Please select the existing writer.")
                        return render_template_string(
                    FORM_HTML,
                    **collect_form_context(),
                    **collect_submitted_form_data()
                )

        warnings = []
        for row in writer_rows:
            if not row["ipi"]:
                existing_name_writer = Writer.query.filter(
                    func.lower(Writer.full_name) == normalize_text(row["full_name"])
                ).first()
                if existing_name_writer:
                    warnings.append("Writer '" + row["full_name"] + "' already exists in the system without using an IPI match.")

        writer_identity_set = sorted([build_writer_identity_from_row(row) for row in writer_rows])
        
        possible_duplicates = []

        if request.form.get("return_to_form"):
            return render_template_string(
                FORM_HTML,
                **collect_form_context(),
                **collect_submitted_form_data()
            )

        existing_works = Work.query.filter_by(normalized_title=normalized_title).all()
        for existing_work in existing_works:
            existing_identities = sorted([
                 build_writer_identity_from_workwriter(ww) for ww in existing_work.work_writers
            ])
            if existing_identities == writer_identity_set:
                batch = GenerationBatch.query.get(existing_work.batch_id) if existing_work.batch_id else None
                possible_duplicates.append({
                    "title": existing_work.title,
                    "camp_name": batch.session_name if batch else "",
                    "created_at": existing_work.created_at.strftime("%Y-%m-%d"),
                    "work_id": existing_work.id,
                    "batch_id": existing_work.batch_id,
                })

        force_create = request.form.get("force_create") == "1"

        if possible_duplicates and not force_create:
            form_data = {}
            for key in request.form.keys():
                values = request.form.getlist(key)
                form_data[key] = values if len(values) > 1 else values[0]

            return render_template_string(
                DUPLICATE_WARNING_HTML,
                duplicates=possible_duplicates,
                form_data=form_data,
            )

        for warning in warnings:
            flash(warning)

        existing_batch_id = (request.form.get("existing_batch_id") or "").strip()
        new_session_name = (request.form.get("new_session_name") or "").strip()

        if existing_batch_id:
            batch = GenerationBatch.query.get(int(existing_batch_id))
            if not batch:
                flash("Selected session was not found.")
                return render_template_string(
                FORM_HTML,
                **collect_form_context(),
                **collect_submitted_form_data()
            )

            contract_date = batch.contract_date

        else:
            if not new_session_name:
                flash("Please enter a new session name or select an existing session.")
                return render_template_string(
                FORM_HTML,
                **collect_form_context(),
                **collect_submitted_form_data()
                )

            batch = GenerationBatch(
                session_name=build_session_name(new_session_name),
                contract_date=contract_date,
                created_by="",
                status="draft",
            )
            db.session.add(batch)
            db.session.flush()

        work = Work(
            title=work_title,
            normalized_title=normalized_title,
            batch_id=batch.id,
            contract_date=contract_date,
        )
        db.session.add(work)
        db.session.flush()

        for row in writer_rows:
            writer = find_existing_writer(row["selected_writer_id"])

            if not writer and row["ipi"]:
                writer = Writer.query.filter(
                    func.lower(Writer.ipi) == row["ipi"].lower()
                ).first()

            if not writer and row["full_name"]:
                writer = Writer.query.filter(
                    func.lower(Writer.full_name) == row["full_name"].lower()
                ).first()

            if writer:
                writer.first_name = row["first_name"] or writer.first_name
                writer.middle_name = row["middle_name"] or writer.middle_name
                writer.last_names = row["last_names"] or writer.last_names
                # Only update full_name if it doesn't conflict with another writer
                new_full_name = row["full_name"]
                if new_full_name and new_full_name.lower() != writer.full_name.lower():
                    conflict = Writer.query.filter(
                        func.lower(Writer.full_name) == new_full_name.lower(),
                        Writer.id != writer.id
                    ).first()
                    if not conflict:
                        writer.full_name = new_full_name
                writer.writer_aka = row["writer_aka"] or writer.writer_aka
                writer.ipi = row["ipi"] or writer.ipi
                writer.email = row["email"] or writer.email
                writer.phone_number = row["phone_number"] or writer.phone_number
                writer.pro = row["pro"] or writer.pro
                writer.address = row["address"] or writer.address
                writer.city = row["city"] or writer.city
                writer.state = row["state"] or writer.state
                writer.zip_code = row["zip_code"] or writer.zip_code
            else:
                writer = Writer(
                    first_name=row["first_name"],
                    middle_name=row["middle_name"],
                    last_names=row["last_names"],
                    full_name=row["full_name"],
                    writer_aka=row["writer_aka"],
                    ipi=row["ipi"] or None,
                    email=row["email"],
                    phone_number=row["phone_number"],
                    pro=row["pro"],
                    address=row["address"],
                    city=row["city"],
                    state=row["state"],
                    zip_code=row["zip_code"],
                    has_master_contract=False,
                )
                db.session.add(writer)
                db.session.flush()

            work_writer = WorkWriter(
                work_id=work.id,
                writer_id=writer.id,
                writer_percentage=row["writer_percentage"],
                publisher=row["publisher"],
                publisher_ipi=row["publisher_ipi"],
                publisher_address=row["publisher_address"],
                publisher_city=row["publisher_city"],
                publisher_state=row["publisher_state"],
                publisher_zip_code=row["publisher_zip_code"],
            )
            db.session.add(work_writer)

        db.session.commit()

        is_modal = request.form.get("_modal") == "1"
        if is_modal:
            writers_str = ", ".join(
                ww.writer.full_name for ww in work.work_writers
            )
            batch_url = url_for(".batch_detail", batch_id=batch.id)
            script = (
                "<script>"
                "window.parent.onWorkCreated("
                f"{work.id}, {work.title!r}, {writers_str!r}, {batch_url!r}"
                ");"
                "</script>"
            )
            return make_response(script)

        return redirect(url_for(".batch_detail", batch_id=batch.id))

    is_modal = request.args.get("modal") == "1"
    prefill_title = request.args.get("work_title", "")
    return render_template_string(
        FORM_HTML,
        **collect_form_context(),
        work_title_value=prefill_title,
        contract_date_value="",
        new_session_name_value="",
        submitted_writers=[],
        is_modal=is_modal,
    )



@bp.route("/works")
def works_list():
    if auth_required():
        return redirect(url_for(".login"))

    q = (request.args.get("q") or "").strip()
    sort = (request.args.get("sort") or "newest").strip()
    page = max(1, int(request.args.get("page") or 1))
    per_page = 50

    query = Work.query.options(
        joinedload(Work.work_writers).joinedload(WorkWriter.writer),
        joinedload(Work.contract_documents),
        joinedload(Work.batch),
    )

    if q:
        like_q = "%" + q.lower() + "%"
        query = (
            query
            .outerjoin(WorkWriter, WorkWriter.work_id == Work.id)
            .outerjoin(Writer, Writer.id == WorkWriter.writer_id)
            .filter(
                or_(
                    func.lower(Work.title).like(like_q),
                    func.lower(Writer.full_name).like(like_q),
                    func.lower(Writer.ipi).like(like_q)
                )
            )
            .distinct()
        )

    if sort == "oldest":
        query = query.order_by(Work.created_at.asc())
    elif sort == "title_asc":
        query = query.order_by(func.lower(Work.title).asc())
    elif sort == "title_desc":
        query = query.order_by(func.lower(Work.title).desc())
    else:
        query = query.order_by(Work.created_at.desc())

    pagination = query.paginate(page=page, per_page=per_page, error_out=False)

    return render_template_string(
        WORKS_LIST_HTML,
        works=pagination.items,
        q=q,
        sort=sort,
        pagination=pagination,
    )


@bp.route("/batches")
def batches_list():
    if auth_required():
        return redirect(url_for(".login"))

    q    = (request.args.get("q") or "").strip()
    sort = (request.args.get("sort") or "newest").strip()
    page = max(1, int(request.args.get("page") or 1))
    per_page = 50

    query = GenerationBatch.query

    if q:
        like_q = f"%{q.lower()}%"
        query = (
            query
            .outerjoin(Work, Work.batch_id == GenerationBatch.id)
            .outerjoin(WorkWriter, WorkWriter.work_id == Work.id)
            .outerjoin(Writer, Writer.id == WorkWriter.writer_id)
            .filter(or_(
                func.lower(GenerationBatch.session_name).like(like_q),
                func.lower(Work.title).like(like_q),
                func.lower(Writer.full_name).like(like_q),
            ))
            .distinct()
        )

    if sort == "oldest":
        query = query.order_by(GenerationBatch.created_at.asc())
    elif sort == "title_asc":
        query = query.order_by(func.lower(GenerationBatch.session_name).asc())
    elif sort == "title_desc":
        query = query.order_by(func.lower(GenerationBatch.session_name).desc())
    else:
        query = query.order_by(GenerationBatch.created_at.desc())

    pagination = query.paginate(page=page, per_page=per_page, error_out=False)

    batch_ids = [b.id for b in pagination.items]
    work_counts = dict(
        db.session.query(Work.batch_id, func.count(Work.id))
        .filter(Work.batch_id.in_(batch_ids))
        .group_by(Work.batch_id)
        .all()
    ) if batch_ids else {}
    raw_works = (
        Work.query
        .options(joinedload(Work.work_writers))
        .filter(Work.batch_id.in_(batch_ids))
        .order_by(Work.title.asc())
        .all()
    ) if batch_ids else []
    session_works = {}
    for w in raw_works:
        session_works.setdefault(w.batch_id, []).append(w)
    return render_template_string(BATCHES_LIST_HTML, batches=pagination.items, pagination=pagination, work_counts=work_counts, session_works=session_works, q=q, sort=sort)


@bp.route("/batches/<int:batch_id>")
def batch_detail(batch_id):
    if auth_required():
        return redirect(url_for(".login"))
    batch = GenerationBatch.query.get_or_404(batch_id)
    works = Work.query.filter_by(batch_id=batch.id).order_by(Work.created_at.asc()).all()
    documents = ContractDocument.query.filter_by(batch_id=batch.id).order_by(ContractDocument.generated_at.desc()).all()
    writer_summary = get_batch_writer_summary(batch.id)
    return render_template_string(BATCH_DETAIL_HTML, batch=batch, works=works, documents=documents, writer_summary=writer_summary)


@bp.route("/batches/<int:batch_id>/status-json")
def batch_status_json(batch_id):
    if auth_required():
        return jsonify({"error": "unauthorized"}), 401
    batch = GenerationBatch.query.get_or_404(batch_id)
    documents = ContractDocument.query.filter_by(batch_id=batch.id).order_by(ContractDocument.generated_at.asc()).all()
    return jsonify({
        "batch_id": batch.id,
        "status": batch.status,
        "documents": [
            {
                "id": doc.id,
                "writer_name_snapshot": doc.writer_name_snapshot,
                "document_type": doc.document_type,
                "file_name": doc.file_name,
                "generated_at": doc.generated_at.strftime("%b %d %H:%M") if doc.generated_at else "",
                "drive_web_view_link": doc.drive_web_view_link,
                "docusign_status": doc.docusign_status,
                "status": doc.status,
                "signed_pdf_drive_web_view_link": getattr(doc, "signed_pdf_drive_web_view_link", None),
                "signed_web_view_link": getattr(doc, "signed_web_view_link", None),
                "certificate_drive_web_view_link": getattr(doc, "certificate_drive_web_view_link", None),
            }
            for doc in documents
        ],
    })


@bp.route("/batches/<int:batch_id>/generate", methods=["POST"])
def generate_batch_documents(batch_id):
    if auth_required():
        return redirect(url_for(".login"))

    batch = GenerationBatch.query.get_or_404(batch_id)
    work_writers = (
        WorkWriter.query.join(Work)
        .filter(Work.batch_id == batch.id)
        .order_by(Work.id.asc(), WorkWriter.id.asc())
        .all()
    )

    if not work_writers:
        flash("No works found in this session.")
        return redirect(url_for(".batch_detail", batch_id=batch.id))

    grouped = {}
    for ww in work_writers:
        if ww.writer_id not in grouped:
            grouped[ww.writer_id] = {"writer": ww.writer, "rows": []}
        grouped[ww.writer_id]["rows"].append(ww)

    existing_docs = ContractDocument.query.filter_by(batch_id=batch.id).all()
    for doc in existing_docs:
        db.session.delete(doc)
    db.session.flush()

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for writer_id, item in grouped.items():
            writer = item["writer"]
            rows = item["rows"]

            works_for_table = []
            for ww in rows:
                works_for_table.append({
                    "work_title": ww.work.title,
                    "writer_name": writer.full_name,
                    "writer_percentage": str(round(ww.writer_percentage, 2)) + "%",
                    "publisher": ww.publisher or "",
                })

            first_work = rows[0].work
            first_work_writer = rows[0]

            if writer.has_master_contract:
                document_type = "schedule_1"
                template_path = SCHEDULE_1_TEMPLATE
                prefix = "S1"
            else:
                document_type = "full_contract"
                template_path = FULL_CONTRACT_TEMPLATE
                prefix = "FULL"

            contract_date = batch.contract_date
            day = contract_date.day
            suffix = "th" if 11 <= day <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")

            data = {
                "Date": contract_date.strftime("%B") + " " + str(day) + suffix + ", " + str(contract_date.year),
                "Fecha": format_date(contract_date, format="d 'de' MMMM 'del' y", locale="es"),
                "WriterName": writer.full_name,
                "WriterFirstName": writer.first_name,
                "WriterMiddleName": writer.middle_name,
                "WriterLastNames": writer.last_names,
                "WriterAKA": writer.writer_aka or "",
                "WriterIPI": writer.ipi or "",
                "WriterAddress": writer.address or "",
                "WriterCity": writer.city or "",
                "WriterState": writer.state or "",
                "WriterZipCode": writer.zip_code or "",
                "PRO": writer.pro or "",
                "PublisherName": first_work_writer.publisher or "",
                "PublisherIPI": first_work_writer.publisher_ipi or "",
                "PublisherAddress": first_work_writer.publisher_address or "",
                "PublisherCity": first_work_writer.publisher_city or "",
                "PublisherState": first_work_writer.publisher_state or "",
                "PublisherZipCode": first_work_writer.publisher_zip_code or "",
                "WorkTitle": first_work.title,
            }

            file_buffer = render_docx_template(template_path, data, works_for_table=works_for_table)
            file_bytes = file_buffer.getvalue()

            batch_label = batch.session_name if batch.session_name else "batch_" + str(batch.id)
            file_name = prefix + "_" + slugify(writer.full_name) + "_" + slugify(batch_label) + "_" + batch.contract_date.isoformat() + ".docx"

            zip_file.writestr(file_name, file_bytes)

            drive_info = {"file_id": None, "web_view_link": None}
            if GOOGLE_DRIVE_FOLDER_ID and GOOGLE_SERVICE_ACCOUNT_JSON:
                try:
                    drive_info = upload_bytes_to_drive(
                        file_name=file_name,
                        file_bytes=file_bytes,
                        parent_folder_id=GOOGLE_DRIVE_FOLDER_ID,
                        mime_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    )
                    current_app.logger.warning("DRIVE SUCCESS: %s", drive_info)
                except Exception as e:
                    current_app.logger.error("DRIVE FAILURE: %s", e)
                    traceback.print_exc()
                    flash("Drive upload failed for " + file_name + ": " + str(e))
            else:
                flash("Drive upload skipped: missing GOOGLE_DRIVE_FOLDER_ID or GOOGLE_SERVICE_ACCOUNT_JSON")

            doc_record = ContractDocument(
                batch_id=batch.id,
                work_id=first_work.id,
                writer_id=writer.id,
                document_type=document_type,
                file_name=file_name,
                writer_name_snapshot=writer.full_name,
                work_title_snapshot=", ".join(sorted({ww.work.title for ww in rows})),
                drive_file_id=drive_info["file_id"],
                drive_web_view_link=drive_info["web_view_link"],
                status="generated",
            )
            db.session.add(doc_record)

            if document_type == "full_contract":
                writer.has_master_contract = True

    batch.status = "docs_generated"
    db.session.commit()

    zip_buffer.seek(0)
    return send_file(
        zip_buffer,
        as_attachment=True,
        download_name="session_" + str(batch.id) + "_documents.zip",
        mimetype="application/zip",
    )


@bp.route("/docusign/webhook", methods=["POST"])
def docusign_webhook():
    raw_data = request.data
    current_app.logger.warning("RAW BODY: %s", raw_data)
    try:
        root = ET.fromstring(raw_data)
        envelope_id = None
        raw_status = ""
        for elem in root.iter():
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            text = (elem.text or "").strip()
            if tag == "EnvelopeID" and text:
                envelope_id = text
            if tag == "Status" and text:
                raw_status = text.lower()

        if not envelope_id:
            return "ok", 200

        document = ContractDocument.query.filter_by(docusign_envelope_id=envelope_id).first()
        if not document:
            return "ok", 200

        if "completed" in raw_status:
            normalized_status = "completed"
        elif "delivered" in raw_status:
            normalized_status = "delivered"
        elif "sent" in raw_status:
            normalized_status = "sent"
        elif "declined" in raw_status:
            normalized_status = "declined"
        elif "voided" in raw_status:
            normalized_status = "voided"
        else:
            normalized_status = raw_status or document.docusign_status or "sent"

        document.docusign_status = normalized_status

        if normalized_status == "completed":
            api_client = get_docusign_api_client()
            envelopes_api = EnvelopesApi(api_client)

            signed_bytes = envelopes_api.get_document(
                account_id=DOCUSIGN_ACCOUNT_ID,
                envelope_id=document.docusign_envelope_id,
                document_id="combined",
            )
            signed_file_name = document.file_name.rsplit(".", 1)[0] + "_SIGNED.pdf"
            signed_drive_info = upload_bytes_to_drive(
                file_name=signed_file_name,
                file_bytes=signed_bytes,
                parent_folder_id=GOOGLE_DRIVE_FOLDER_ID,
                mime_type="application/pdf",
            )
            document.signed_pdf_drive_file_id = signed_drive_info.get("file_id")
            document.signed_pdf_drive_web_view_link = signed_drive_info.get("web_view_link")

            certificate_bytes = envelopes_api.get_document(
                account_id=DOCUSIGN_ACCOUNT_ID,
                envelope_id=document.docusign_envelope_id,
                document_id="certificate",
            )
            certificate_file_name = document.file_name.rsplit(".", 1)[0] + "_CERTIFICATE.pdf"
            certificate_drive_info = upload_bytes_to_drive(
                file_name=certificate_file_name,
                file_bytes=certificate_bytes,
                parent_folder_id=GOOGLE_DRIVE_FOLDER_ID,
                mime_type="application/pdf",
            )
            document.certificate_drive_file_id = certificate_drive_info.get("file_id")
            document.certificate_drive_web_view_link = certificate_drive_info.get("web_view_link")
            document.completed_at = datetime.datetime.utcnow()
            document.status = "signed"

        db.session.commit()
    except Exception as e:
        db.session.rollback()
        current_app.logger.error("DocuSign webhook failed: %s", e)
    return "ok", 200


@bp.route("/documents/<int:document_id>/send-docusign", methods=["POST"])
def send_document_docusign(document_id):
    document = None
    try:
        if auth_required():
            return redirect(url_for(".login"))

        document = ContractDocument.query.get(document_id)
        if not document:
            flash("This document no longer exists. Please refresh the session page.")
            return redirect(request.referrer or url_for(".batches_list"))

        writer = Writer.query.get(document.writer_id)

        if not writer:
            flash("Writer not found.")
            return redirect(url_for(".batch_detail", batch_id=document.batch_id))
        if not getattr(writer, "email", None):
            flash("Writer email is required before sending to DocuSign.")
            return redirect(url_for(".batch_detail", batch_id=document.batch_id))
        if not document.drive_file_id:
            flash("Generated document file is missing.")
            return redirect(url_for(".batch_detail", batch_id=document.batch_id))
        if not DOCUSIGN_ACCOUNT_ID or not DOCUSIGN_INTEGRATION_KEY or not DOCUSIGN_USER_ID or not DOCUSIGN_PRIVATE_KEY:
            flash("DocuSign environment variables are not fully configured.")
            return redirect(url_for(".batch_detail", batch_id=document.batch_id))

        service = get_drive_service()
        file_bytes = service.files().get_media(fileId=document.drive_file_id, supportsAllDrives=True).execute()

        api_client = get_docusign_api_client()
        envelopes_api = EnvelopesApi(api_client)

        doc_b64 = base64.b64encode(file_bytes).decode("ascii")
        ds_document = DocusignDocument(
            document_base64=doc_b64,
            name=document.file_name,
            file_extension="docx",
            document_id="1",
        )

        signer = Signer(
            email=writer.email,
            name=writer.full_name,
            recipient_id="1",
            routing_order="1",
        )
        sign_here = SignHere(
            anchor_string="[[DS_SIGN_HERE]]",
            anchor_units="pixels",
            anchor_x_offset="0",
            anchor_y_offset="0",
        )
        signer.tabs = Tabs(sign_here_tabs=[sign_here])

        webhook_url = request.url_root.rstrip("/") + url_for(".docusign_webhook")
        event_notification = {
            "url": webhook_url,
            "loggingEnabled": "true",
            "requireAcknowledgment": "true",
            "includeEnvelopeVoidReason": "true",
            "includeTimeZone": "true",
            "includeSenderAccountAsCustomField": "true",
            "envelopeEvents": [
                {"envelopeEventStatusCode": "sent"},
                {"envelopeEventStatusCode": "delivered"},
                {"envelopeEventStatusCode": "completed"},
                {"envelopeEventStatusCode": "declined"},
                {"envelopeEventStatusCode": "voided"},
            ],
        }

        envelope_definition = EnvelopeDefinition(
            email_subject="Please sign: " + document.file_name,
            documents=[ds_document],
            recipients=Recipients(signers=[signer]),
            status="sent",
            event_notification=event_notification,
        )

        result = envelopes_api.create_envelope(
            account_id=DOCUSIGN_ACCOUNT_ID,
            envelope_definition=envelope_definition,
        )

        document.docusign_envelope_id = result.envelope_id
        document.docusign_status = "sent"
        document.sent_for_signature_at = datetime.datetime.utcnow()
        db.session.commit()

        flash("Sent to DocuSign successfully.")
        return redirect(url_for(".batch_detail", batch_id=document.batch_id))

    except Exception as e:
        db.session.rollback()
        current_app.logger.error("DOCUSIGN SEND ERROR: %s", e)
        current_app.logger.error(traceback.format_exc())
        flash("DocuSign send failed: " + str(e))
        if document:
            return redirect(url_for(".batch_detail", batch_id=document.batch_id))
        return redirect(request.referrer or url_for(".batches_list"))


@bp.route("/documents/<int:document_id>/upload-signed", methods=["POST"])
def upload_signed_document(document_id):
    if auth_required():
        return redirect(url_for(".login"))

    document = ContractDocument.query.get_or_404(document_id)
    uploaded_file = request.files.get("signed_file")

    if not uploaded_file or not uploaded_file.filename:
        flash("Please choose a signed file to upload.")
        return redirect(url_for(".batch_detail", batch_id=document.batch_id))
    if not GOOGLE_DRIVE_FOLDER_ID or not GOOGLE_SERVICE_ACCOUNT_JSON:
        flash("Google Drive is not configured yet.")
        return redirect(url_for(".batch_detail", batch_id=document.batch_id))

    file_bytes = uploaded_file.read()
    file_name = uploaded_file.filename
    mime_type = uploaded_file.mimetype or "application/octet-stream"

    try:
        drive_info = upload_bytes_to_drive(
            file_name=file_name,
            file_bytes=file_bytes,
            parent_folder_id=GOOGLE_DRIVE_FOLDER_ID,
            mime_type=mime_type,
        )
    except Exception as e:
        flash("Signed upload failed: " + str(e))
        return redirect(url_for(".batch_detail", batch_id=document.batch_id))

    document.signed_file_name = file_name
    document.signed_drive_file_id = drive_info["file_id"]
    document.signed_web_view_link = drive_info["web_view_link"]
    document.signed_uploaded_at = datetime.datetime.utcnow()
    document.status = "signed_uploaded"

    batch_docs = ContractDocument.query.filter_by(batch_id=document.batch_id).all()
    if batch_docs and all(doc.status == "signed_uploaded" for doc in batch_docs):
        document.batch.status = "signed_complete"
    else:
        document.batch.status = "signed_partial"

    db.session.commit()
    flash("Signed file uploaded successfully.")
    return redirect(url_for(".batch_detail", batch_id=document.batch_id))

@bp.route("/writers")
def writers_list():
    if auth_required():
        return redirect(url_for(".login"))

    q = (request.args.get("q") or "").strip()
    page = max(1, int(request.args.get("page") or 1))
    writers, pagination = get_writer_directory_rows(q, page=page)
    return render_template_string(
        WRITERS_LIST_HTML,
        writers=writers,
        q=q,
        pagination=pagination,
    )


@bp.route("/writers/<int:writer_id>")
def writer_detail(writer_id):
    if auth_required():
        return redirect(url_for(".login"))

    writer = Writer.query.get_or_404(writer_id)
    work_writers = (
        WorkWriter.query
        .filter_by(writer_id=writer.id)
        .join(Work)
        .order_by(Work.created_at.desc())
        .all()
    )

    return render_template_string(
        WRITER_DETAIL_HTML,
        writer=writer,
        work_writers=work_writers,
    )

@bp.route("/writers/<int:writer_id>/edit", methods=["GET", "POST"])
def writer_edit(writer_id):
    if auth_required():
        return redirect(url_for(".login"))

    writer = Writer.query.get_or_404(writer_id)

    if request.method == "POST":
        first_name = (request.form.get("first_name") or "").strip()
        middle_name = (request.form.get("middle_name") or "").strip()
        last_names = (request.form.get("last_names") or "").strip()
        full_name = build_full_name(first_name, middle_name, last_names)
        email = (request.form.get("email") or "").strip()
        phone_number = (request.form.get("phone_number") or "").strip()
        ipi = (request.form.get("ipi") or "").strip()
        pro = (request.form.get("pro") or "").strip()
        default_publisher = (request.form.get("default_publisher") or "").strip()
        default_publisher_ipi = (request.form.get("default_publisher_ipi") or "").strip()
        writer_aka = (request.form.get("writer_aka") or "").strip()
        address = (request.form.get("address") or "").strip()
        city = (request.form.get("city") or "").strip()
        state = (request.form.get("state") or "").strip()
        zip_code = (request.form.get("zip_code") or "").strip()
        has_master_contract = request.form.get("has_master_contract") == "1"

        if not first_name or not last_names:
            flash("First and last name are required.")
            return render_template_string(
            WRITER_EDIT_HTML,
            writer=writer,
            default_publisher_for_pro=default_publisher_for_pro,
            default_publisher_ipi_for_pro=default_publisher_ipi_for_pro,
        )

        if not email or "@" not in email:
            flash("A valid email is required.")
            return render_template_string(
            WRITER_EDIT_HTML,
            writer=writer,
            default_publisher_for_pro=default_publisher_for_pro,
            default_publisher_ipi_for_pro=default_publisher_ipi_for_pro,
        )

        if not ipi:
            flash("IPI is required.")
            return render_template_string(
            WRITER_EDIT_HTML,
            writer=writer,
            default_publisher_for_pro=default_publisher_for_pro,
            default_publisher_ipi_for_pro=default_publisher_ipi_for_pro,
        )

        if not pro:
            flash("PRO is required.")
            return render_template_string(
            WRITER_EDIT_HTML,
            writer=writer,
            default_publisher_for_pro=default_publisher_for_pro,
            default_publisher_ipi_for_pro=default_publisher_ipi_for_pro,
        )

        if not address or not city or not state or not zip_code:
            flash("Complete address is required.")
            return render_template_string(
            WRITER_EDIT_HTML,
            writer=writer,
            default_publisher_for_pro=default_publisher_for_pro,
            default_publisher_ipi_for_pro=default_publisher_ipi_for_pro,
        )

        existing_ipi = Writer.query.filter(
            func.lower(Writer.ipi) == ipi.lower(),
            Writer.id != writer.id
        ).first()
        if existing_ipi:
            flash("That IPI already belongs to " + existing_ipi.full_name)
            return render_template_string(
            WRITER_EDIT_HTML,
            writer=writer,
            default_publisher_for_pro=default_publisher_for_pro,
            default_publisher_ipi_for_pro=default_publisher_ipi_for_pro,
        )

        existing_name = Writer.query.filter(
            func.lower(Writer.full_name) == full_name.lower(),
            Writer.id != writer.id
        ).first()
        if existing_name:
            flash("That full name already exists for another writer.")
            return render_template_string(
            WRITER_EDIT_HTML,
            writer=writer,
            default_publisher_for_pro=default_publisher_for_pro,
            default_publisher_ipi_for_pro=default_publisher_ipi_for_pro,
        )

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

        db.session.commit()
        flash("Writer updated successfully.")
        return redirect(url_for(".writer_detail", writer_id=writer.id))

    return render_template_string(
            WRITER_EDIT_HTML,
            writer=writer,
            default_publisher_for_pro=default_publisher_for_pro,
            default_publisher_ipi_for_pro=default_publisher_ipi_for_pro,
        )

@bp.route("/works/<int:work_id>/edit", methods=["GET", "POST"])
def work_edit(work_id):
    if auth_required():
        return redirect(url_for(".login"))

    work = Work.query.get_or_404(work_id)
    batches = GenerationBatch.query.order_by(GenerationBatch.created_at.desc()).all()

    if request.method == "POST":
        title = (request.form.get("title") or "").strip()
        contract_date_str = (request.form.get("contract_date") or "").strip()
        batch_id = (request.form.get("batch_id") or "").strip()

        work_writer_ids = request.form.getlist("work_writer_id")
        existing_writer_ids = request.form.getlist("existing_writer_id")
        writer_percentages = request.form.getlist("writer_percentage")
        publishers = request.form.getlist("publisher")
        publisher_ipis = request.form.getlist("publisher_ipi")

        if not title:
            flash("Work title is required.")
            return render_template_string(WORK_EDIT_HTML, work=work, batches=batches)

        if not contract_date_str:
            flash("Contract date is required.")
            return render_template_string(WORK_EDIT_HTML, work=work, batches=batches)

        try:
            contract_date = datetime.datetime.strptime(contract_date_str, "%Y-%m-%d").date()
        except ValueError:
            flash("Please enter a valid contract date.")
            return render_template_string(WORK_EDIT_HTML, work=work, batches=batches)

        # Build submitted writer identities from posted rows
        submitted_writer_ids = []
        total_split = 0.0

        for i in range(len(existing_writer_ids)):
            writer_id = (existing_writer_ids[i] or "").strip()
            pct = (writer_percentages[i] or "0").strip()

            if not writer_id:
                flash("Each work-writer row must have a selected writer.")
                return render_template_string(WORK_EDIT_HTML, work=work, batches=batches)

            if writer_id in submitted_writer_ids:
                flash("Duplicate writer selected in this work.")
                return render_template_string(WORK_EDIT_HTML, work=work, batches=batches)

            submitted_writer_ids.append(writer_id)

            try:
                total_split += float(pct or 0)
            except ValueError:
                flash("Invalid writer split value.")
                return render_template_string(WORK_EDIT_HTML, work=work, batches=batches)

        if not submitted_writer_ids:
            flash("At least one writer is required.")
            return render_template_string(WORK_EDIT_HTML, work=work, batches=batches)

        if abs(total_split - 100.0) >= 0.001:
            flash("Total writer split must equal 100%. Current total: " + str(round(total_split, 2)) + "%")
            return render_template_string(WORK_EDIT_HTML, work=work, batches=batches)

        normalized_title = normalize_title(title)

        submitted_identities = sorted(["id:" + wid for wid in submitted_writer_ids])

        duplicate_query = Work.query.filter(
            Work.id != work.id,
            Work.normalized_title == normalized_title
        ).all()

        for existing_work in duplicate_query:
            existing_identities = sorted([
                build_writer_identity_from_workwriter(ww) for ww in existing_work.work_writers
            ])
            if existing_identities == submitted_identities:
                flash("Another work with this same title and writer set already exists.")
                return render_template_string(WORK_EDIT_HTML, work=work, batches=batches)

        work.title = title
        work.normalized_title = normalized_title
        work.contract_date = contract_date

        if batch_id:
            batch = GenerationBatch.query.get(int(batch_id))
            if not batch:
                flash("Selected session was not found.")
                return render_template_string(WORK_EDIT_HTML, work=work, batches=batches)
            work.batch_id = batch.id
        else:
            work.batch_id = None

        # Remove rows no longer submitted
        existing_links = {str(ww.id): ww for ww in work.work_writers}
        submitted_existing_link_ids = set([wid for wid in work_writer_ids if wid.strip()])

        for existing_link_id, ww in existing_links.items():
            if existing_link_id not in submitted_existing_link_ids:
                db.session.delete(ww)

        # Update existing or create new rows
        for i in range(len(existing_writer_ids)):
            ww_id = (work_writer_ids[i] or "").strip()
            writer_id = (existing_writer_ids[i] or "").strip()

            writer = Writer.query.get(int(writer_id)) if writer_id else None
            if not writer:
                flash("Selected writer was not found.")
                return render_template_string(WORK_EDIT_HTML, work=work, batches=batches)

            pct = float((writer_percentages[i] or "0").strip() or 0)
            publisher = (publishers[i] or "").strip()
            publisher_ipi = (publisher_ipis[i] or "").strip()

            if ww_id:
                ww = WorkWriter.query.get(int(ww_id))
                if not ww or ww.work_id != work.id:
                    continue
                ww.writer_id = writer.id
                ww.writer_percentage = pct
                ww.publisher = publisher
                ww.publisher_ipi = publisher_ipi
            else:
                ww = WorkWriter(
                    work_id=work.id,
                    writer_id=writer.id,
                    writer_percentage=pct,
                    publisher=publisher,
                    publisher_ipi=publisher_ipi,
                    publisher_address=DEFAULT_PUBLISHER_ADDRESS,
                    publisher_city=DEFAULT_PUBLISHER_CITY,
                    publisher_state=DEFAULT_PUBLISHER_STATE,
                    publisher_zip_code=DEFAULT_PUBLISHER_ZIP,
                )
                db.session.add(ww)

        db.session.commit()
        flash("Work updated successfully.")
        return redirect(url_for(".work_detail", work_id=work.id))

    return render_template_string(WORK_EDIT_HTML, work=work, batches=batches)

@bp.route("/admin")
def admin_panel():
    if auth_required():
        return redirect(url_for(".login"))
    return render_template_string(ADMIN_HTML)


@bp.route("/admin/merge-writers", methods=["POST"])
def merge_writers():
    if auth_required():
        return redirect(url_for(".login"))

    primary_id   = request.form.get("primary_writer_id", type=int)
    duplicate_id = request.form.get("duplicate_writer_id", type=int)

    if not primary_id or not duplicate_id or primary_id == duplicate_id:
        flash("Please select two different writers.")
        return redirect(url_for(".admin_panel"))

    primary   = Writer.query.get(primary_id)
    duplicate = Writer.query.get(duplicate_id)

    if not primary or not duplicate:
        flash("One or both writers not found.")
        return redirect(url_for(".admin_panel"))

    primary_pk   = primary.id
    duplicate_pk = duplicate.id

    # Find work_ids the primary already owns — those duplicate links must be deleted
    primary_work_ids = {
        ww.work_id for ww in WorkWriter.query.filter_by(writer_id=primary_pk).all()
    }

    # Delete conflicting WorkWriter rows (primary already covers that work)
    if primary_work_ids:
        WorkWriter.query.filter(
            WorkWriter.writer_id == duplicate_pk,
            WorkWriter.work_id.in_(primary_work_ids)
        ).delete(synchronize_session="fetch")

    # Repoint remaining WorkWriter rows directly — no ORM attribute assignment
    WorkWriter.query.filter_by(writer_id=duplicate_pk).update(
        {"writer_id": primary_pk}, synchronize_session="fetch"
    )

    # Repoint ContractDocument rows
    ContractDocument.query.filter_by(writer_id=duplicate_pk).update(
        {"writer_id": primary_pk}, synchronize_session="fetch"
    )

    # Push all FK changes to DB before the writer row is deleted
    db.session.flush()

    # Fill any missing fields on primary from duplicate
    for field in ("ipi", "email", "phone_number", "pro", "address",
                  "city", "state", "zip_code", "default_publisher",
                  "default_publisher_ipi", "first_name", "middle_name", "last_names"):
        if not getattr(primary, field) and getattr(duplicate, field):
            setattr(primary, field, getattr(duplicate, field))

    dup_name = duplicate.full_name
    db.session.delete(duplicate)
    db.session.commit()

    flash(f"Merged '{dup_name}' into '{primary.full_name}' successfully.")
    return redirect(url_for(".admin_panel"))

@bp.route("/works/<int:work_id>")
def work_detail(work_id):
    if auth_required():
        return redirect(url_for(".login"))
    work = Work.query.get_or_404(work_id)
    documents = (
        ContractDocument.query
        .filter_by(work_id=work.id)
        .order_by(ContractDocument.generated_at.desc())
        .all()
    )
    return render_template_string(
    WORK_DETAIL_HTML,
    work=work,
    documents=documents
)


@bp.route("/admin/import-catalog", methods=["GET", "POST"])
def admin_import_catalog():
    if auth_required():
        return redirect(url_for(".login"))

    if request.method == "POST":
        file = request.files.get("file")

        if not file:
            return "No file uploaded", 400

        import csv, io

        stream = io.StringIO(file.stream.read().decode("utf-8"))
        reader = csv.DictReader(stream)

        for row in reader:
            # --- CREATE / FIND WORK ---
            title = row["work_title"]
            contract_date = datetime.datetime.strptime(row["contract_date"], "%Y-%m-%d").date()

            work = Work.query.filter_by(title=title).first()
            if not work:
                work = Work(
                    title=title,
                    normalized_title=normalize_title(title),
                    contract_date=contract_date
                )
                db.session.add(work)
                db.session.flush()

            # --- CREATE / FIND WRITER ---
            writer = Writer.query.filter_by(ipi=row["ipi"]).first()
            if not writer:
                writer = Writer(
                    first_name=row["first_name"],
                    middle_name=row["middle_name"],
                    last_names=row["last_names"],
                    full_name=row["writer_full_name"],
                    ipi=row["ipi"],
                    pro=row["pro"],
                    email=row["email"],
                    default_publisher=row["publisher"],
                    default_publisher_ipi=row["publisher_ipi"]
                )
                db.session.add(writer)
                db.session.flush()

            # --- CREATE WORK WRITER ---
            ww = WorkWriter(
                work_id=work.id,
                writer_id=writer.id,
                writer_percentage=float(row["writer_percentage"]),
                publisher=row["publisher"],
                publisher_ipi=row["publisher_ipi"]
            )
            db.session.add(ww)

        db.session.commit()

        return "Import successful ✅"

    return """
    <h2>Import Catalog CSV</h2>
    <form method="post" enctype="multipart/form-data">
        <input type="file" name="file" accept=".csv">
        <button type="submit">Upload</button>
    </form>
    """
@bp.route("/works/<int:work_id>/delete", methods=["POST"])
def work_delete(work_id):
    if auth_required():
        return redirect(url_for(".login"))

    work = Work.query.get_or_404(work_id)

    # delete related documents first
    ContractDocument.query.filter(
        ContractDocument.work_id == work.id
    ).delete()

    # delete related writer links
    WorkWriter.query.filter(
        WorkWriter.work_id == work.id
    ).delete()

    work_title = work.title
    db.session.delete(work)
    db.session.commit()

    flash(f'Work "{work_title}" deleted successfully.')
    return redirect(url_for(".works_list"))

@bp.route("/admin/import-catalog/preview", methods=["POST"])
def import_catalog_preview():
    if auth_required():
        return redirect(url_for(".login"))

    file = request.files.get("catalog_file")
    if not file or not file.filename:
        flash("Please choose a CSV file.")
        return redirect(url_for(".admin_panel"))

    if not file.filename.lower().endswith(".csv"):
        flash("Only CSV files are allowed.")
        return redirect(url_for(".admin_panel"))

    try:
        content = file.read().decode("utf-8-sig")
    except Exception:
        flash("Could not read the CSV file. Please save it as UTF-8 CSV.")
        return redirect(url_for(".admin_panel"))

    reader = csv.DictReader(io.StringIO(content))

    required_columns = [
        "work_title",
        "contract_date",
        "session_name",
        "writer_full_name",
        "ipi",
        "pro",
        "writer_percentage",
        "publisher",
        "publisher_ipi",
    ]

    missing = [c for c in required_columns if c not in (reader.fieldnames or [])]
    if missing:
        flash("Missing required CSV columns: " + ", ".join(missing))
        return redirect(url_for(".admin_panel"))

    preview_rows = []

    for row_num, row in enumerate(reader, start=2):
        work_title = (row.get("work_title") or "").strip()
        contract_date_str = (row.get("contract_date") or "").strip()
        session_name = (row.get("session_name") or "").strip()
        writer_full_name = (row.get("writer_full_name") or "").strip()
        first_name = (row.get("first_name") or "").strip()
        middle_name = (row.get("middle_name") or "").strip()
        last_names = (row.get("last_names") or "").strip()
        writer_aka = (row.get("writer_aka") or "").strip()
        ipi = (row.get("ipi") or "").strip()
        email = (row.get("email") or "").strip()
        phone_number = (row.get("phone_number") or "").strip()
        pro = (row.get("pro") or "").strip()
        writer_percentage = (row.get("writer_percentage") or "").strip()
        publisher = (row.get("publisher") or "").strip()
        publisher_ipi = (row.get("publisher_ipi") or "").strip()
        address = (row.get("address") or "").strip()
        city = (row.get("city") or "").strip()
        state = (row.get("state") or "").strip()
        zip_code = (row.get("zip_code") or "").strip()

        error = ""

        if not work_title or not contract_date_str or not writer_full_name:
            error = "Missing work_title, contract_date, or writer_full_name."

        if not error:
            try:
                datetime.datetime.strptime(contract_date_str, "%Y-%m-%d").date()
            except ValueError:
                error = "Invalid contract_date. Use YYYY-MM-DD."

        if not error:
            try:
                float(writer_percentage or 0)
            except ValueError:
                error = "Invalid writer_percentage."

        preview_rows.append({
            "row_num": row_num,
            "work_title": work_title,
            "contract_date": contract_date_str,
            "session_name": session_name,
            "writer_full_name": writer_full_name,
            "first_name": first_name,
            "middle_name": middle_name,
            "last_names": last_names,
            "writer_aka": writer_aka,
            "ipi": ipi,
            "email": email,
            "phone_number": phone_number,
            "pro": pro,
            "writer_percentage": writer_percentage,
            "publisher": publisher,
            "publisher_ipi": publisher_ipi,
            "address": address,
            "city": city,
            "state": state,
            "zip_code": zip_code,
            "error": error,
        })

    valid_count = len([r for r in preview_rows if not r["error"]])
    error_count = len([r for r in preview_rows if r["error"]])

    import_token = str(uuid.uuid4())
    _import_preview_store[import_token] = preview_rows

    return render_template_string(
        IMPORT_PREVIEW_HTML,
        rows=preview_rows,
        valid_count=valid_count,
        error_count=error_count,
        import_token=import_token
    )

@bp.route("/admin/import-catalog/confirm", methods=["POST"])
def import_catalog_confirm():
    if auth_required():
        return redirect(url_for(".login"))

    import_token = request.form.get("import_token") or ""
    rows = _import_preview_store.pop(import_token, None)

    if rows is None:
        flash("Import session expired or invalid. Please re-upload your CSV.")
        return redirect(url_for(".admin_panel"))

    writers_created = 0
    writers_reused = 0
    sessions_created = 0
    sessions_reused = 0
    works_created = 0
    works_reused = 0
    links_created = 0
    rows_skipped = 0

    work_cache = {}
    session_cache = {}
    writer_cache = {}

    for row in rows:
        if row.get("error"):
            rows_skipped += 1
            continue

        work_title = row["work_title"]
        contract_date = datetime.datetime.strptime(row["contract_date"], "%Y-%m-%d").date()
        session_name = row["session_name"]
        writer_full_name = row["writer_full_name"]
        first_name = row["first_name"]
        middle_name = row["middle_name"]
        last_names = row["last_names"]
        writer_aka = row["writer_aka"]
        ipi = row["ipi"]
        email = row["email"]
        phone_number = row["phone_number"]
        pro = row["pro"]
        writer_percentage = float(row["writer_percentage"] or 0)
        publisher = row["publisher"]
        publisher_ipi = row["publisher_ipi"]
        address = row["address"]
        city = row["city"]
        state = row["state"]
        zip_code = row["zip_code"]

        if not first_name and not last_names and writer_full_name:
            name_parts = writer_full_name.split()
            if len(name_parts) == 1:
                first_name = name_parts[0]
                last_names = ""
            elif len(name_parts) >= 2:
                first_name = name_parts[0]
                last_names = " ".join(name_parts[1:])

        full_name = build_full_name(first_name, middle_name, last_names) or writer_full_name

        writer_key = ("ipi", ipi.lower()) if ipi else ("name", full_name.lower())

        if writer_key in writer_cache:
            writer = writer_cache[writer_key]
            writers_reused += 1
        else:
            writer = None
            if ipi:
                writer = Writer.query.filter(func.lower(Writer.ipi) == ipi.lower()).first()
            if not writer and full_name:
                writer = Writer.query.filter(func.lower(Writer.full_name) == full_name.lower()).first()

            if writer:
                writers_reused += 1
            else:
                writer = Writer(
                    first_name=first_name,
                    middle_name=middle_name,
                    last_names=last_names,
                    full_name=full_name,
                    writer_aka=writer_aka,
                    ipi=ipi or None,
                    email=email,
                    phone_number=phone_number,
                    pro=pro,
                    default_publisher=publisher,
                    default_publisher_ipi=publisher_ipi,
                    address=address,
                    city=city,
                    state=state,
                    zip_code=zip_code,
                    has_master_contract=False,
                )
                db.session.add(writer)
                db.session.flush()
                writers_created += 1

            writer.first_name = writer.first_name or first_name
            writer.middle_name = writer.middle_name or middle_name
            writer.last_names = writer.last_names or last_names
            writer.full_name = writer.full_name or full_name
            writer.writer_aka = writer.writer_aka or writer_aka
            writer.ipi = writer.ipi or (ipi or None)
            writer.email = writer.email or email
            writer.phone_number = writer.phone_number or phone_number
            writer.pro = writer.pro or pro
            writer.default_publisher = writer.default_publisher or publisher
            writer.default_publisher_ipi = writer.default_publisher_ipi or publisher_ipi
            writer.address = writer.address or address
            writer.city = writer.city or city
            writer.state = writer.state or state
            writer.zip_code = writer.zip_code or zip_code

            writer_cache[writer_key] = writer

        session_key = (session_name.lower(), contract_date.isoformat())

        if session_key in session_cache:
            batch = session_cache[session_key]
            sessions_reused += 1
        else:
            batch = GenerationBatch.query.filter(
                func.lower(GenerationBatch.session_name) == session_name.lower(),
                GenerationBatch.contract_date == contract_date
            ).first()

            if batch:
                sessions_reused += 1
            else:
                batch = GenerationBatch(
                    session_name=session_name,
                    contract_date=contract_date,
                    created_by="CSV Import",
                    status="imported",
                )
                db.session.add(batch)
                db.session.flush()
                sessions_created += 1

            session_cache[session_key] = batch

        normalized_title = normalize_title(work_title)
        work_key = (normalized_title, batch.id)

        if work_key in work_cache:
            work = work_cache[work_key]
            works_reused += 1
        else:
            work = Work.query.filter_by(
                normalized_title=normalized_title,
                batch_id=batch.id
            ).first()

            if work:
                works_reused += 1
            else:
                work = Work(
                    title=work_title,
                    normalized_title=normalized_title,
                    batch_id=batch.id,
                    contract_date=contract_date,
                )
                db.session.add(work)
                db.session.flush()
                works_created += 1

            work_cache[work_key] = work

        existing_link = WorkWriter.query.filter_by(
            work_id=work.id,
            writer_id=writer.id
        ).first()

        if not existing_link:
            link = WorkWriter(
                work_id=work.id,
                writer_id=writer.id,
                writer_percentage=writer_percentage,
                publisher=publisher,
                publisher_ipi=publisher_ipi,
                publisher_address=DEFAULT_PUBLISHER_ADDRESS,
                publisher_city=DEFAULT_PUBLISHER_CITY,
                publisher_state=DEFAULT_PUBLISHER_STATE,
                publisher_zip_code=DEFAULT_PUBLISHER_ZIP,
            )
            db.session.add(link)
            links_created += 1
        else:
            existing_link.writer_percentage = writer_percentage
            existing_link.publisher = publisher
            existing_link.publisher_ipi = publisher_ipi

    db.session.commit()

    flash(
        "Import complete. "
        f"Writers created: {writers_created}, reused: {writers_reused}. "
        f"Sessions created: {sessions_created}, reused: {sessions_reused}. "
        f"Works created: {works_created}, reused: {works_reused}. "
        f"Links created: {links_created}. "
        f"Rows skipped: {rows_skipped}."
    )

    return redirect(url_for(".admin_panel"))


@bp.route("/test")
def test():
    return "App is working"




@bp.route("/debug/works")
def debug_works():
    try:
        works = Work.query.order_by(Work.id.desc()).limit(10).all()
        return jsonify([
            {
                "id": w.id,
                "title": w.title,
                "batch_id": w.batch_id,
                "contract_date": str(w.contract_date) if w.contract_date else None
            }
            for w in works
        ])
    except Exception as e:
        return "DEBUG ERROR: " + str(e), 500

