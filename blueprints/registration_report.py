import csv
import io
import datetime

from flask import Blueprint, render_template_string, request, redirect, url_for, flash, make_response

from extensions import db
from models import Work, WorkWriter
from utils import auth_required, role_required, FULL_ACCESS_ROLES
from ui import REGISTRATION_REPORT_HTML

bp = Blueprint("registration_report", __name__)


def _works_by_status(status):
    return (
        Work.query
        .filter_by(registration_status=status)
        .order_by(Work.title)
        .all()
    )


def _build_report():
    new_works       = _works_by_status("new")
    submitted_works = _works_by_status("submitted")
    confirmed_works = _works_by_status("confirmed")
    return new_works, submitted_works, confirmed_works


# ── Routes ────────────────────────────────────────────────────────────────────

@bp.route("/registration-report")
def registration_report():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(FULL_ACCESS_ROLES):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    new_works, submitted_works, confirmed_works = _build_report()
    tab = request.args.get("tab", "new")

    stats = dict(
        new=len(new_works),
        submitted=len(submitted_works),
        confirmed=len(confirmed_works),
        total=len(new_works) + len(submitted_works) + len(confirmed_works),
    )

    return render_template_string(
        REGISTRATION_REPORT_HTML,
        new_works=new_works,
        submitted_works=submitted_works,
        confirmed_works=confirmed_works,
        stats=stats,
        tab=tab,
    )


@bp.route("/registration-report/mark-submitted", methods=["POST"])
def mark_submitted():
    """Mark selected (or all new) works as submitted."""
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(FULL_ACCESS_ROLES):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    work_ids = request.form.getlist("work_ids", type=int)
    if not work_ids:
        flash("No works selected.", "error")
        return redirect(url_for("registration_report.registration_report", tab="new"))

    updated = (
        Work.query
        .filter(Work.id.in_(work_ids), Work.registration_status == "new")
        .all()
    )
    for w in updated:
        w.registration_status = "submitted"

    try:
        db.session.commit()
        flash(f"{len(updated)} work(s) marked as submitted.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Error: {e}", "error")

    return redirect(url_for("registration_report.registration_report", tab="new"))


@bp.route("/registration-report/mark-new", methods=["POST"])
def mark_new():
    """Revert submitted works back to new (e.g. if a submission was recalled)."""
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(FULL_ACCESS_ROLES):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    work_ids = request.form.getlist("work_ids", type=int)
    if not work_ids:
        flash("No works selected.", "error")
        return redirect(url_for("registration_report.registration_report", tab="submitted"))

    updated = (
        Work.query
        .filter(Work.id.in_(work_ids), Work.registration_status == "submitted")
        .all()
    )
    for w in updated:
        w.registration_status = "new"

    try:
        db.session.commit()
        flash(f"{len(updated)} work(s) moved back to New.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Error: {e}", "error")

    return redirect(url_for("registration_report.registration_report", tab="submitted"))


@bp.route("/registration-report/export-csv")
def export_csv():
    """Export new or submitted works as CSV for sending to PRO/MLC."""
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(FULL_ACCESS_ROLES):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    status = request.args.get("status", "new")
    if status not in ("new", "submitted"):
        status = "new"

    works = _works_by_status(status)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Work Title", "ISWC", "Contract Date",
        "Writer 1", "Writer 1 IPI", "Writer 1 PRO", "Writer 1 Split %",
        "Writer 2", "Writer 2 IPI", "Writer 2 PRO", "Writer 2 Split %",
        "Publisher 1", "Publisher 1 IPI",
        "MRI Song ID", "AKA Titles", "Registration Status",
    ])

    for w in works:
        writers_data = []
        publishers_data = []
        for ww in w.work_writers:
            writers_data.append((
                ww.writer.full_name,
                ww.writer.ipi or "",
                ww.writer.pro or "",
                ww.writer_percentage,
            ))
            if ww.publisher:
                publishers_data.append((ww.publisher, ww.publisher_ipi or ""))

        # Pad to at least 2 writer slots
        while len(writers_data) < 2:
            writers_data.append(("", "", "", ""))
        while len(publishers_data) < 1:
            publishers_data.append(("", ""))

        aka_str = "; ".join(a.title for a in w.aka_titles)

        row = [
            w.title,
            w.iswc or "",
            w.contract_date.strftime("%m/%d/%Y") if w.contract_date else "",
            writers_data[0][0], writers_data[0][1], writers_data[0][2], writers_data[0][3],
            writers_data[1][0], writers_data[1][1], writers_data[1][2], writers_data[1][3],
            publishers_data[0][0], publishers_data[0][1],
            w.mri_song_id or "",
            aka_str,
            w.registration_status,
        ]
        writer.writerow(row)

    content = output.getvalue()
    date_str = datetime.date.today().strftime("%Y%m%d")
    filename = f"registration_{status}_{date_str}.csv"

    resp = make_response(content)
    resp.headers["Content-Type"] = "text/csv; charset=utf-8"
    resp.headers["Content-Disposition"] = f"attachment; filename={filename}"
    return resp
