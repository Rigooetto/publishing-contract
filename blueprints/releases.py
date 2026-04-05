import json
import datetime
import traceback

from flask import Blueprint, request, redirect, url_for, flash, render_template_string
from flask import current_app

from extensions import db
from models import Release, Track, TrackWork
from utils import auth_required
from ui import RELEASES_LIST_HTML, RELEASE_FORM_HTML, RELEASE_DETAIL_HTML

bp = Blueprint("releases", __name__)


def _parse_artists(form, prefix, count=8):
    # Try getlist first (dynamic rows all share same name e.g. artist_1)
    vals = form.getlist(f"{prefix}_1")
    if vals:
        return [v.strip() for v in vals if v.strip()]
    # Fallback: numbered fields artist_1..artist_N
    return [form.get(f"{prefix}_{i+1}", "").strip() for i in range(count) if form.get(f"{prefix}_{i+1}", "").strip()]


@bp.route("/releases")
def releases_list():
    if auth_required():
        return redirect(url_for("publishing.login"))
    releases = Release.query.order_by(Release.created_at.desc()).all()
    for r in releases:
        r.artists_list = json.loads(r.artists) if r.artists else []
        r.artist_display = ", ".join(r.artists_list[:3]) + (" +" + str(len(r.artists_list)-3) + " more" if len(r.artists_list) > 3 else "")
        for t in r.tracks:
            t.artists_list = json.loads(t.artists) if t.artists else []
    return render_template_string(RELEASES_LIST_HTML, releases=releases)


@bp.route("/releases/new", methods=["GET", "POST"])
def release_new():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if request.method == "POST":
        return _save_release(None)
    return render_template_string(RELEASE_FORM_HTML, release=None, tracks=[], artists=[])


@bp.route("/releases/<int:release_id>/edit", methods=["GET", "POST"])
def release_edit(release_id):
    if auth_required():
        return redirect(url_for("publishing.login"))
    r = Release.query.get_or_404(release_id)
    if request.method == "POST":
        return _save_release(r)
    artists = json.loads(r.artists) if r.artists else []
    tracks = r.tracks
    for t in tracks:
        t.artists_list = json.loads(t.artists) if t.artists else []
    return render_template_string(RELEASE_FORM_HTML, release=r, tracks=tracks, artists=artists)


@bp.route("/releases/<int:release_id>")
def release_detail(release_id):
    if auth_required():
        return redirect(url_for("publishing.login"))
    r = Release.query.get_or_404(release_id)
    r.artists_list = json.loads(r.artists) if r.artists else []
    r.artist_display = ", ".join(r.artists_list)
    for t in r.tracks:
        t.artists_list = json.loads(t.artists) if t.artists else []
        t.artist_display = ", ".join(t.artists_list[:2])
    return render_template_string(RELEASE_DETAIL_HTML, release=r)


@bp.route("/releases/<int:release_id>/delete", methods=["POST"])
def release_delete(release_id):
    if auth_required():
        return redirect(url_for("publishing.login"))
    r = Release.query.get_or_404(release_id)
    db.session.delete(r)
    db.session.commit()
    flash("Release deleted.")
    return redirect(url_for("releases.releases_list"))


def _save_release(existing):
    form = request.form
    try:
        artists = _parse_artists(form, "artist")
        if not artists:
            flash("At least one album artist is required.")
            return redirect(request.referrer or url_for("releases.releases_list"))

        if existing:
            r = existing
        else:
            r = Release()
            db.session.add(r)

        r.release_type = form.get("release_type", "").strip()
        r.title = form.get("title", "").strip()
        r.upc = form.get("upc", "").strip() or None
        r.release_date = datetime.datetime.strptime(form["release_date"], "%Y-%m-%d").date() if form.get("release_date") else None
        r.distributor = form.get("distributor", "").strip()
        r.status = form.get("status", "draft")
        r.artists = json.dumps(artists)

        # --- Tracks ---
        track_ids = form.getlist("track_id[]")
        primary_titles = form.getlist("primary_title[]")
        track_numbers = form.getlist("track_number[]")
        durations = form.getlist("duration[]")
        isrcs = form.getlist("isrc[]")
        recording_titles = form.getlist("recording_title[]")
        aka_titles = form.getlist("aka_title[]")
        aka_type_codes = form.getlist("aka_type_code[]")
        genres = form.getlist("genre[]")
        track_labels = form.getlist("track_label[]")
        track_p_lines = form.getlist("track_p_line[]")
        recording_dates = form.getlist("recording_date[]")
        producers = form.getlist("producer[]")
        recording_engineers = form.getlist("recording_engineer[]")
        executive_producers = form.getlist("executive_producer[]")

        # flush to get release id for new records
        db.session.flush()

        kept_track_ids = set()
        for i, pt in enumerate(primary_titles):
            if not pt.strip():
                continue
            tid = track_ids[i] if i < len(track_ids) else ""
            if tid:
                t = Track.query.get(int(tid))
                if not t:
                    t = Track(release_id=r.id)
                    db.session.add(t)
            else:
                t = Track(release_id=r.id)
                db.session.add(t)

            t.primary_title = pt.strip()
            t.track_number = int(track_numbers[i]) if i < len(track_numbers) and track_numbers[i].strip() else None
            t.duration = durations[i].strip() if i < len(durations) else ""
            t.isrc = isrcs[i].strip() or None if i < len(isrcs) else None
            t.recording_title = recording_titles[i].strip() if i < len(recording_titles) else ""
            t.aka_title = aka_titles[i].strip() if i < len(aka_titles) else ""
            t.aka_type_code = aka_type_codes[i].strip() if i < len(aka_type_codes) else ""
            t.genre = genres[i].strip() if i < len(genres) else ""
            t.track_label = track_labels[i].strip() if i < len(track_labels) else ""
            t.track_p_line = track_p_lines[i].strip() if i < len(track_p_lines) else ""
            t.producer = producers[i].strip() if i < len(producers) else ""
            t.recording_engineer = recording_engineers[i].strip() if i < len(recording_engineers) else ""
            t.executive_producer = executive_producers[i].strip() if i < len(executive_producers) else ""
            rd = recording_dates[i].strip() if i < len(recording_dates) else ""
            t.recording_date = datetime.datetime.strptime(rd, "%Y-%m-%d").date() if rd else None

            # track artists — collected as track_artist_new_N[] for new, track_artist_X_X[] for existing
            tartist_key = f"track_artist_new_{i+1}[]" if not tid else None
            if tartist_key:
                tartists = [v.strip() for v in form.getlist(tartist_key) if v.strip()]
            else:
                tartists = []
                for j in range(8):
                    v = form.get(f"track_artist_{j}_{j}[]", "")
                    if isinstance(v, list):
                        all_vals = form.getlist(f"track_artist_{j}_{j}[]")
                        if i < len(all_vals) and all_vals[i].strip():
                            tartists.append(all_vals[i].strip())
                    elif v.strip():
                        tartists.append(v.strip())
            t.artists = json.dumps(tartists)

            db.session.flush()

            # linked works — clear and re-link
            TrackWork.query.filter_by(track_id=t.id).delete()
            work_key = f"linked_work_ids_{t.id}[]" if tid else f"linked_work_ids_new_{i+1}[]"
            notes_key = f"linked_work_notes_{t.id}[]" if tid else f"linked_work_notes_new_{i+1}[]"
            wids = form.getlist(work_key)
            wnotes = form.getlist(notes_key)
            for wi, wid in enumerate(wids):
                if wid:
                    tw = TrackWork(track_id=t.id, work_id=int(wid), notes=wnotes[wi] if wi < len(wnotes) else "")
                    db.session.add(tw)

            kept_track_ids.add(t.id)

        # remove tracks no longer in form
        if existing:
            for t in existing.tracks:
                if t.id not in kept_track_ids:
                    db.session.delete(t)

        manual_num = form.get("num_tracks", "").strip()
        r.num_tracks = int(manual_num) if manual_num.isdigit() else len(kept_track_ids)
        db.session.commit()
        flash("Release saved.")
        return redirect(url_for("releases.release_detail", release_id=r.id))

    except Exception as e:
        db.session.rollback()
        current_app.logger.error("RELEASE SAVE ERROR: %s", e)
        current_app.logger.error(traceback.format_exc())
        flash("Error saving release: " + str(e))
        return redirect(request.referrer or url_for("releases.releases_list"))
