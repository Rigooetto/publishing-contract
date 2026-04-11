import io
import datetime
import json as _json

from flask import Blueprint, request, redirect, url_for, flash, render_template_string, send_file
from flask import current_app
from sqlalchemy import func as _func

from extensions import db
from models import (Work, WorkWriter, ProRegistration, PublisherConfig,
                    Release, Track, TrackWork)
from utils import auth_required, role_required, FULL_ACCESS_ROLES
from ui import REPORTS_INDEX_HTML, PUBLISHER_CONFIG_HTML, PRO_REGISTRATION_HTML

bp = Blueprint("reports", __name__)

AFINARTE_PUBLISHERS = ["Songs of Afinarte", "Melodies of Afinarte", "Music of Afinarte"]


def _attach_track_info(work):
    """Attach first linked track + release data to a Work instance for display."""
    tracks = (Track.query
              .join(TrackWork, TrackWork.track_id == Track.id)
              .filter(TrackWork.work_id == work.id)
              .all())
    work._tracks = tracks
    work._first_track = tracks[0] if tracks else None
    work._first_release = tracks[0].release if tracks and tracks[0].release else None


def _is_controlled(publisher_name):
    if not publisher_name:
        return False
    return any(ap.lower() in publisher_name.lower() for ap in AFINARTE_PUBLISHERS)


# ── Publisher Config ──────────────────────────────────────────────────────────

@bp.route("/publisher-config", methods=["GET", "POST"])
def publisher_config():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(FULL_ACCESS_ROLES):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))
    if request.method == "POST":
        names = request.form.getlist("publisher_name[]")
        pros = request.form.getlist("pro[]")
        ipis = request.form.getlist("publisher_ipi[]")
        mlcs = request.form.getlist("mlc_publisher_number[]")
        addresses = request.form.getlist("address[]")
        cities = request.form.getlist("city[]")
        states = request.form.getlist("state[]")
        zips = request.form.getlist("zip_code[]")
        emails = request.form.getlist("contact_email[]")
        phones = request.form.getlist("contact_phone[]")
        pids = request.form.getlist("pub_id[]")
        try:
            for i, name in enumerate(names):
                name = name.strip()
                if not name:
                    continue
                pid = pids[i] if i < len(pids) else ""
                if pid:
                    pc = PublisherConfig.query.get(int(pid))
                else:
                    pc = PublisherConfig.query.filter_by(publisher_name=name).first()
                    if not pc:
                        pc = PublisherConfig(publisher_name=name)
                        db.session.add(pc)
                pc.publisher_name = name
                pc.pro = pros[i].strip() if i < len(pros) else ""
                pc.publisher_ipi = ipis[i].strip() if i < len(ipis) else ""
                pc.mlc_publisher_number = mlcs[i].strip() if i < len(mlcs) else ""
                pc.address = addresses[i].strip() if i < len(addresses) else ""
                pc.city = cities[i].strip() if i < len(cities) else ""
                pc.state = states[i].strip() if i < len(states) else ""
                pc.zip_code = zips[i].strip() if i < len(zips) else ""
                pc.contact_email = emails[i].strip() if i < len(emails) else ""
                pc.contact_phone = phones[i].strip() if i < len(phones) else ""
            db.session.commit()
            flash("Publisher configuration saved.")
        except Exception as e:
            db.session.rollback()
            flash("Error saving: " + str(e))
        return redirect(url_for("reports.publisher_config"))

    # Pre-populate with Afinarte publishers if not yet configured
    existing_names = {c.publisher_name for c in PublisherConfig.query.all()}
    for ap in AFINARTE_PUBLISHERS:
        if ap not in existing_names:
            db.session.add(PublisherConfig(publisher_name=ap))
    if len(existing_names) < len(AFINARTE_PUBLISHERS):
        db.session.commit()

    configs = PublisherConfig.query.order_by(PublisherConfig.publisher_name).all()
    return render_template_string(PUBLISHER_CONFIG_HTML, configs=configs)


# ── PRO Registration Queue ────────────────────────────────────────────────────

@bp.route("/pro-registration")
def pro_registration():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(FULL_ACCESS_ROLES):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    tab = request.args.get("tab", "unregistered")
    q = (request.args.get("q") or "").strip()
    try:
        page = max(1, int(request.args.get("page") or 1))
    except (ValueError, TypeError):
        page = 1
    per_page = 50

    registered_work_ids = db.session.query(ProRegistration.work_id).distinct()

    unregistered_q = (
        Work.query
        .join(WorkWriter, WorkWriter.work_id == Work.id)
        .filter(WorkWriter.publisher.in_(AFINARTE_PUBLISHERS))
        .filter(Work.id.notin_(registered_work_ids))
        .distinct()
    )
    registered_q = (
        Work.query
        .join(ProRegistration, ProRegistration.work_id == Work.id)
        .distinct()
    )

    if q:
        like_q = f"%{q.lower()}%"
        unregistered_q = unregistered_q.filter(_func.lower(Work.title).like(like_q))
        registered_q = registered_q.filter(_func.lower(Work.title).like(like_q))

    unregistered_q = unregistered_q.order_by(Work.created_at.desc())
    registered_q = registered_q.order_by(Work.title)

    if tab == "registered":
        pagination = registered_q.paginate(page=page, per_page=per_page, error_out=False)
        registered = pagination.items
        for w in registered:
            w.registrations = ProRegistration.query.filter_by(work_id=w.id).order_by(ProRegistration.registered_at.desc()).all()
            _attach_track_info(w)
        unregistered_count = unregistered_q.count()
        unregistered = []
    else:
        pagination = unregistered_q.paginate(page=page, per_page=per_page, error_out=False)
        unregistered = pagination.items
        for w in unregistered:
            _attach_track_info(w)
        registered = []
        unregistered_count = pagination.total

    registered_count = registered_q.count()

    today = datetime.date.today().strftime("%Y-%m-%d")
    return render_template_string(PRO_REGISTRATION_HTML,
        unregistered=unregistered, registered=registered,
        unregistered_count=unregistered_count, registered_count=registered_count,
        pagination=pagination, tab=tab, q=q, today=today)


@bp.route("/pro-registration/mark", methods=["POST"])
def pro_registration_mark():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(FULL_ACCESS_ROLES):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))
    work_ids = request.form.getlist("work_ids[]")
    pro = request.form.get("pro", "").strip()
    pro_work_number = request.form.get("pro_work_number", "").strip()
    mlc_song_code = request.form.get("mlc_song_code", "").strip()
    registered_by = request.form.get("registered_by", "Omar").strip()
    registered_at_str = request.form.get("registered_at", "").strip()
    notes = request.form.get("notes", "").strip()

    try:
        registered_at = datetime.datetime.strptime(registered_at_str, "%Y-%m-%d").date() if registered_at_str else datetime.date.today()
    except ValueError:
        registered_at = datetime.date.today()

    if not pro or not work_ids:
        flash("Please select at least one work and a PRO.")
        return redirect(url_for("reports.pro_registration"))

    try:
        for wid in work_ids:
            reg = ProRegistration(
                work_id=int(wid),
                pro=pro,
                pro_work_number=pro_work_number,
                mlc_song_code=mlc_song_code,
                registered_at=registered_at,
                registered_by=registered_by,
                notes=notes,
            )
            db.session.add(reg)
        db.session.commit()
        flash(f"{len(work_ids)} work(s) marked as registered with {pro}.")
    except Exception as e:
        db.session.rollback()
        flash("Error: " + str(e))
    return redirect(url_for("reports.pro_registration", tab="registered"))


@bp.route("/pro-registration/<int:reg_id>/delete", methods=["POST"])
def pro_registration_delete(reg_id):
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(FULL_ACCESS_ROLES):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))
    reg = ProRegistration.query.get_or_404(reg_id)
    db.session.delete(reg)
    db.session.commit()
    flash("Registration removed.")
    return redirect(url_for("reports.pro_registration", tab="registered"))


# ── Reports Index ─────────────────────────────────────────────────────────────

@bp.route("/reports")
def reports_index():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(FULL_ACCESS_ROLES):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))
    work_count = (Work.query
                  .join(WorkWriter, WorkWriter.work_id == Work.id)
                  .filter(WorkWriter.publisher.in_(AFINARTE_PUBLISHERS))
                  .distinct().count())
    release_count = Release.query.count()
    return render_template_string(REPORTS_INDEX_HTML, work_count=work_count, release_count=release_count)


# ── MLC Export ───────────────────────────────────────────────────────────────

@bp.route("/reports/export/mlc")
def export_mlc():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(FULL_ACCESS_ROLES):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))
    try:
        import openpyxl
        from openpyxl import load_workbook

        wb = load_workbook("template/MLCBulkWork_V1.2-2.xlsx")
        ws = wb["Format"]

        # Clear example rows (keep header row 1)
        for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
            for cell in row:
                cell.value = None

        works = (Work.query
                 .join(WorkWriter, WorkWriter.work_id == Work.id)
                 .filter(WorkWriter.publisher.in_(AFINARTE_PUBLISHERS))
                 .distinct()
                 .order_by(Work.title)
                 .all())

        row_idx = 2
        for work in works:
            writers = WorkWriter.query.filter_by(work_id=work.id).all()
            first_writer = True
            for ww in writers:
                w = ww.writer
                pub_config = PublisherConfig.query.filter(
                    _func.lower(PublisherConfig.publisher_name) == (ww.publisher or "").lower()
                ).first()
                mlc_pub_num = pub_config.mlc_publisher_number if pub_config else ""

                tracks = (Track.query
                          .join(TrackWork, TrackWork.track_id == Track.id)
                          .filter(TrackWork.work_id == work.id)
                          .all())
                rec_title = tracks[0].primary_title if tracks else ""
                rec_artist = ""
                if tracks:
                    try:
                        al = _json.loads(tracks[0].artists or "[]")
                        rec_artist = ", ".join(al)
                    except Exception:
                        pass
                rec_isrc = tracks[0].isrc if tracks else ""
                rec_label = tracks[0].track_label if tracks else ""

                ws.cell(row=row_idx, column=1).value = work.title if first_writer else None
                ws.cell(row=row_idx, column=2).value = None  # MLC Song Code
                ws.cell(row=row_idx, column=3).value = f"LM{work.id:06d}"
                ws.cell(row=row_idx, column=4).value = work.iswc or None
                ws.cell(row=row_idx, column=5).value = work.aka_title or None
                ws.cell(row=row_idx, column=6).value = work.aka_title_type_code or None
                ws.cell(row=row_idx, column=7).value = w.last_names
                ws.cell(row=row_idx, column=8).value = w.first_name
                ws.cell(row=row_idx, column=9).value = w.ipi or None
                ws.cell(row=row_idx, column=10).value = ww.writer_role_code or "CA"
                ws.cell(row=row_idx, column=11).value = mlc_pub_num or None
                ws.cell(row=row_idx, column=12).value = ww.publisher or None
                ws.cell(row=row_idx, column=13).value = ww.publisher_ipi or None
                ws.cell(row=row_idx, column=14).value = None
                ws.cell(row=row_idx, column=15).value = ww.administrator_name or None
                ws.cell(row=row_idx, column=16).value = ww.administrator_ipi or None
                ws.cell(row=row_idx, column=17).value = ww.writer_percentage or None
                ws.cell(row=row_idx, column=18).value = rec_title or None
                ws.cell(row=row_idx, column=19).value = rec_artist or None
                ws.cell(row=row_idx, column=20).value = rec_isrc or None
                ws.cell(row=row_idx, column=21).value = rec_label or None

                row_idx += 1
                first_writer = False

        output = io.BytesIO()
        wb.save(output)
        output.seek(0)
        filename = f"MLC_BulkWork_{datetime.date.today().strftime('%Y%m%d')}.xlsx"
        return send_file(output, download_name=filename,
                         mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                         as_attachment=True)
    except Exception as e:
        current_app.logger.error("MLC export error: %s", e)
        import traceback; current_app.logger.error(traceback.format_exc())
        flash("Error generating MLC export: " + str(e))
        return redirect(url_for("reports.reports_index"))


# ── Music Reports Export ──────────────────────────────────────────────────────

@bp.route("/reports/export/music-reports")
def export_music_reports():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(FULL_ACCESS_ROLES):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))
    try:
        import xlwt

        wb = xlwt.Workbook()
        ws = wb.add_sheet("Catalog Template")

        headers = [
            "SONG TITLE*", "AKA TITLE", "MRI SONG ID", "PUBLISHER'S SONG ID", "ISWC",
            "COMPOSER LAST NAME*", "COMPOSER FIRST NAME*", "COMPOSER MIDDLE NAME",
            "COMPOSER PRO*", "COMPOSER IPI NUMBER", "CONTROLLED COMPOSER (Y/N)*",
            "COMPOSER SHARE %*", "COMPOSER ROLE CODE", "PUBLISHER NAME *",
            "PUBLISHER PRO*", "PUBLISHER IPI NUMBER *", "CONTROLLED PUBLISHER (Y/N)*",
            "ADMINISTRATOR NAME", "SHARE %*", "TERRITORY CONTROLLED*",
            "TERRITORY EXCLUSIONS (OPTIONAL)", "PUBLISHER MAILING ADDRESS*",
            "PUBLISHER CONTACT*", "RECORDING ARTIST NAME", "RECORDING LABEL",
            "RECORDING ISRC", "UPC/EAN"
        ]
        hdr_style = xlwt.easyxf('font: bold true; pattern: pattern solid, fore_colour light_green;')
        for ci, h in enumerate(headers):
            ws.write(0, ci, h, hdr_style)

        works = (Work.query
                 .join(WorkWriter, WorkWriter.work_id == Work.id)
                 .filter(WorkWriter.publisher.in_(AFINARTE_PUBLISHERS))
                 .distinct()
                 .order_by(Work.title)
                 .all())

        row_idx = 1
        for work in works:
            writers = WorkWriter.query.filter_by(work_id=work.id).all()
            tracks = (Track.query
                      .join(TrackWork, TrackWork.track_id == Track.id)
                      .filter(TrackWork.work_id == work.id)
                      .all())
            rec_artist = rec_isrc = rec_label = upc = ""
            if tracks:
                try:
                    al = _json.loads(tracks[0].artists or "[]")
                    rec_artist = ", ".join(al)
                except Exception:
                    pass
                rec_isrc = tracks[0].isrc or ""
                rec_label = tracks[0].track_label or ""
                if tracks[0].release:
                    upc = tracks[0].release.upc or ""

            first_writer = True
            for ww in writers:
                w = ww.writer
                pub_config = PublisherConfig.query.filter(
                    _func.lower(PublisherConfig.publisher_name) == (ww.publisher or "").lower()
                ).first()
                pub_address = pub_contact = pub_pro = ""
                if pub_config:
                    parts = [pub_config.address, pub_config.city]
                    if pub_config.state:
                        parts.append(pub_config.state)
                    if pub_config.zip_code:
                        parts.append(pub_config.zip_code)
                    pub_address = ", ".join(p for p in parts if p)
                    pub_contact = pub_config.contact_email or pub_config.contact_phone or ""
                    pub_pro = pub_config.pro or ""

                controlled = "Y" if _is_controlled(ww.publisher) else "N"

                ws.write(row_idx, 0, work.title if first_writer else "")
                ws.write(row_idx, 1, work.aka_title or "")
                ws.write(row_idx, 2, work.mri_song_id or "")
                ws.write(row_idx, 3, f"LM{work.id:06d}")
                ws.write(row_idx, 4, work.iswc or "")
                ws.write(row_idx, 5, w.last_names or "")
                ws.write(row_idx, 6, w.first_name or "")
                ws.write(row_idx, 7, w.middle_name or "")
                ws.write(row_idx, 8, w.pro or "")
                ws.write(row_idx, 9, w.ipi or "")
                ws.write(row_idx, 10, controlled)
                ws.write(row_idx, 11, ww.writer_percentage or 0)
                ws.write(row_idx, 12, ww.writer_role_code or "CA")
                ws.write(row_idx, 13, ww.publisher or "")
                ws.write(row_idx, 14, pub_pro)
                ws.write(row_idx, 15, ww.publisher_ipi or "")
                ws.write(row_idx, 16, controlled)
                ws.write(row_idx, 17, ww.administrator_name or "")
                ws.write(row_idx, 18, ww.writer_percentage or 0)
                ws.write(row_idx, 19, ww.territory_controlled or "World")
                ws.write(row_idx, 20, "")
                ws.write(row_idx, 21, pub_address)
                ws.write(row_idx, 22, pub_contact)
                ws.write(row_idx, 23, rec_artist if first_writer else "")
                ws.write(row_idx, 24, rec_label if first_writer else "")
                ws.write(row_idx, 25, rec_isrc if first_writer else "")
                ws.write(row_idx, 26, upc if first_writer else "")

                row_idx += 1
                first_writer = False

        output = io.BytesIO()
        wb.save(output)
        output.seek(0)
        filename = f"MusicReports_{datetime.date.today().strftime('%Y%m%d')}.xls"
        return send_file(output, download_name=filename,
                         mimetype="application/vnd.ms-excel",
                         as_attachment=True)
    except Exception as e:
        current_app.logger.error("Music Reports export error: %s", e)
        import traceback; current_app.logger.error(traceback.format_exc())
        flash("Error generating Music Reports export: " + str(e))
        return redirect(url_for("reports.reports_index"))


# ── SoundExchange Export ──────────────────────────────────────────────────────

@bp.route("/reports/export/soundexchange")
def export_soundexchange():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(FULL_ACCESS_ROLES):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))
    try:
        from openpyxl import load_workbook

        wb = load_workbook("template/Sound Exchange ISRC Ingest Form.xlsx")
        ws = wb["Form"]

        # Clear data below header (row 10 is header, data starts row 11)
        data_start = 11
        for row in ws.iter_rows(min_row=data_start, max_row=ws.max_row):
            for cell in row:
                cell.value = None

        tracks = (Track.query
                  .join(Release, Release.id == Track.release_id)
                  .order_by(Release.title, Track.track_number)
                  .all())

        row_idx = data_start
        for t in tracks:
            artist = ""
            try:
                al = _json.loads(t.artists or "[]")
                artist = ", ".join(al)
            except Exception:
                pass
            if not artist and t.release:
                try:
                    ral = _json.loads(t.release.artists or "[]")
                    artist = ", ".join(ral)
                except Exception:
                    pass

            ws.cell(row=row_idx, column=1).value = artist
            ws.cell(row=row_idx, column=2).value = t.primary_title
            ws.cell(row=row_idx, column=3).value = t.isrc or ""
            ws.cell(row=row_idx, column=4).value = "Copyright Owner"
            ws.cell(row=row_idx, column=5).value = 100
            ws.cell(row=row_idx, column=6).value = (t.release.release_date.strftime("%m/%d/%Y")
                                                     if t.release and t.release.release_date else "")
            ws.cell(row=row_idx, column=7).value = ""
            ws.cell(row=row_idx, column=8).value = ""
            ws.cell(row=row_idx, column=9).value = ""
            ws.cell(row=row_idx, column=10).value = t.duration or ""
            ws.cell(row=row_idx, column=11).value = t.genre or ""
            ws.cell(row=row_idx, column=12).value = (t.recording_date.strftime("%m/%d/%Y")
                                                      if t.recording_date else "")
            ws.cell(row=row_idx, column=13).value = t.country_of_recording or "US"
            ws.cell(row=row_idx, column=14).value = ""
            ws.cell(row=row_idx, column=15).value = "US"
            row_idx += 1

        output = io.BytesIO()
        wb.save(output)
        output.seek(0)
        filename = f"SoundExchange_ISRC_{datetime.date.today().strftime('%Y%m%d')}.xlsx"
        return send_file(output, download_name=filename,
                         mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                         as_attachment=True)
    except Exception as e:
        current_app.logger.error("SoundExchange export error: %s", e)
        import traceback; current_app.logger.error(traceback.format_exc())
        flash("Error generating SoundExchange export: " + str(e))
        return redirect(url_for("reports.reports_index"))
