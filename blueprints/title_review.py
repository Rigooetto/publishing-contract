from flask import Blueprint, render_template_string, request, redirect, url_for, flash, jsonify

from extensions import db
from models import Work, Release, Track
from utils import auth_required, role_required, FULL_ACCESS_ROLES, normalize_for_match
from ui import TITLE_REVIEW_HTML

bp = Blueprint("title_review", __name__)


@bp.route("/title-review")
def title_review():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(FULL_ACCESS_ROLES):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    works    = Work.query.order_by(Work.title).all()
    releases = Release.query.order_by(Release.title).all()
    tracks   = Track.query.order_by(Track.primary_title).all()

    return render_template_string(
        TITLE_REVIEW_HTML,
        works=works,
        releases=releases,
        tracks=tracks,
    )


@bp.route("/title-review/update-work", methods=["POST"])
def update_work_title():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(FULL_ACCESS_ROLES):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    work_id   = request.form.get("work_id", type=int)
    new_title = (request.form.get("title") or "").strip()
    if not work_id or not new_title:
        flash("Missing data.", "error")
        return redirect(url_for("title_review.title_review"))

    work = Work.query.get_or_404(work_id)
    work.title            = new_title
    work.normalized_title = normalize_for_match(new_title)
    is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"
    try:
        db.session.commit()
        if is_ajax:
            return jsonify({"ok": True, "title": new_title})
        flash(f'Work title updated to "{new_title}".', "success")
    except Exception as e:
        db.session.rollback()
        if is_ajax:
            return jsonify({"error": str(e)}), 500
        flash(f"Error: {e}", "error")
    return redirect(url_for("title_review.title_review") + "#works")


@bp.route("/title-review/update-release", methods=["POST"])
def update_release_title():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(FULL_ACCESS_ROLES):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    release_id = request.form.get("release_id", type=int)
    new_title  = (request.form.get("title") or "").strip()
    if not release_id or not new_title:
        flash("Missing data.", "error")
        return redirect(url_for("title_review.title_review"))

    release = Release.query.get_or_404(release_id)
    release.title = new_title
    is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"
    try:
        db.session.commit()
        if is_ajax:
            return jsonify({"ok": True, "title": new_title})
        flash(f'Release title updated to "{new_title}".', "success")
    except Exception as e:
        db.session.rollback()
        if is_ajax:
            return jsonify({"error": str(e)}), 500
        flash(f"Error: {e}", "error")
    return redirect(url_for("title_review.title_review") + "#releases")


@bp.route("/title-review/update-track", methods=["POST"])
def update_track_title():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(FULL_ACCESS_ROLES):
        flash("Access restricted.", "error")
        return redirect(url_for("publishing.works_list"))

    track_id  = request.form.get("track_id", type=int)
    new_title = (request.form.get("title") or "").strip()
    if not track_id or not new_title:
        flash("Missing data.", "error")
        return redirect(url_for("title_review.title_review"))

    track = Track.query.get_or_404(track_id)
    track.primary_title = new_title
    is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"
    try:
        db.session.commit()
        if is_ajax:
            return jsonify({"ok": True, "title": new_title})
        flash(f'Track title updated to "{new_title}".', "success")
    except Exception as e:
        db.session.rollback()
        if is_ajax:
            return jsonify({"error": str(e)}), 500
        flash(f"Error: {e}", "error")
    return redirect(url_for("title_review.title_review") + "#tracks")
