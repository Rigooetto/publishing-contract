import traceback

from flask import Blueprint, request, redirect, url_for, flash, render_template_string
from flask import current_app
from sqlalchemy import func, or_

from extensions import db
import datetime
import decimal

from models import Artist, Release, ArtistContract
from utils import auth_required, safe_json_loads
from ui import ARTISTS_LIST_HTML, ARTIST_DETAIL_HTML, ARTIST_FORM_HTML

bp = Blueprint("artists", __name__)


@bp.route("/artists")
def artists_list():
    if auth_required():
        return redirect(url_for("publishing.login"))

    q    = (request.args.get("q") or "").strip()
    sort = (request.args.get("sort") or "newest").strip()

    query = Artist.query

    if q:
        like_q = f"%{q.lower()}%"
        query = query.filter(or_(
            func.lower(Artist.name).like(like_q),
            func.lower(Artist.legal_name).like(like_q),
            func.lower(Artist.aka).like(like_q),
            func.lower(Artist.email).like(like_q),
        ))

    if sort == "oldest":
        query = query.order_by(Artist.created_at.asc())
    elif sort == "name_asc":
        query = query.order_by(func.lower(Artist.name).asc())
    elif sort == "name_desc":
        query = query.order_by(func.lower(Artist.name).desc())
    else:
        query = query.order_by(Artist.created_at.desc())

    try:
        page = max(1, int(request.args.get("page") or 1))
    except (ValueError, TypeError):
        page = 1
    per_page = 50
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)

    artists = pagination.items
    for a in artists:
        rels = a.releases.order_by(Release.release_date.desc()).all()
        a.releases_list = rels
        a.release_count = len(rels)
        for r in a.releases_list:
            r.artists_list = safe_json_loads(r.artists)
            r.artist_display = ", ".join(r.artists_list[:2])

    return render_template_string(ARTISTS_LIST_HTML, artists=artists, q=q, sort=sort, pagination=pagination)


@bp.route("/artists/new", methods=["GET", "POST"])
def artist_new():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if request.method == "POST":
        return _save_artist(None)
    return render_template_string(ARTIST_FORM_HTML, artist=None)


@bp.route("/artists/<int:artist_id>")
def artist_detail(artist_id):
    if auth_required():
        return redirect(url_for("publishing.login"))
    from models import ArtistRelease
    artist = Artist.query.get_or_404(artist_id)
    releases = (
        Release.query
        .join(ArtistRelease, ArtistRelease.release_id == Release.id)
        .filter(ArtistRelease.artist_id == artist.id)
        .order_by(Release.release_date.desc())
        .all()
    )
    for r in releases:
        r.artists_list = safe_json_loads(r.artists)
        r.artist_display = ", ".join(r.artists_list)
    contracts = ArtistContract.query.filter_by(artist_id=artist.id).order_by(ArtistContract.start_date.desc()).all()
    return render_template_string(ARTIST_DETAIL_HTML, artist=artist, releases=releases, contracts=contracts)


@bp.route("/artists/<int:artist_id>/contracts/add", methods=["POST"])
def artist_contract_add(artist_id):
    if auth_required():
        return redirect(url_for("publishing.login"))
    artist = Artist.query.get_or_404(artist_id)
    form = request.form
    try:
        start_str = form.get("start_date", "").strip()
        end_str   = form.get("end_date", "").strip()
        pct_str   = form.get("royalty_percentage", "").strip()
        if not start_str or not pct_str:
            flash("Start date and royalty % are required.")
            return redirect(url_for("artists.artist_detail", artist_id=artist_id))
        start_date = datetime.date.fromisoformat(start_str)
        end_date   = datetime.date.fromisoformat(end_str) if end_str else None
        pct        = decimal.Decimal(pct_str)
        if pct < 0 or pct > 100:
            flash("Royalty % must be between 0 and 100.")
            return redirect(url_for("artists.artist_detail", artist_id=artist_id))
        # Overlap check
        existing = ArtistContract.query.filter_by(artist_id=artist.id).all()
        for c in existing:
            c_end = c.end_date or datetime.date(9999, 12, 31)
            new_end = end_date or datetime.date(9999, 12, 31)
            if start_date <= c_end and new_end >= c.start_date:
                flash(f"Date range overlaps with existing contract ({c.start_date} – {c.end_date or 'open'}).")
                return redirect(url_for("artists.artist_detail", artist_id=artist_id))
        db.session.add(ArtistContract(
            artist_id=artist.id,
            start_date=start_date,
            end_date=end_date,
            royalty_percentage=pct,
            notes=form.get("notes", "").strip(),
        ))
        db.session.commit()
        flash("Contract added.")
    except Exception as e:
        db.session.rollback()
        flash(f"Error adding contract: {e}")
    return redirect(url_for("artists.artist_detail", artist_id=artist_id))


@bp.route("/artists/<int:artist_id>/contracts/<int:contract_id>/delete", methods=["POST"])
def artist_contract_delete(artist_id, contract_id):
    if auth_required():
        return redirect(url_for("publishing.login"))
    c = ArtistContract.query.filter_by(id=contract_id, artist_id=artist_id).first_or_404()
    try:
        db.session.delete(c)
        db.session.commit()
        flash("Contract deleted.")
    except Exception as e:
        db.session.rollback()
        flash(f"Error deleting contract: {e}")
    return redirect(url_for("artists.artist_detail", artist_id=artist_id))


@bp.route("/artists/<int:artist_id>/edit", methods=["GET", "POST"])
def artist_edit(artist_id):
    if auth_required():
        return redirect(url_for("publishing.login"))
    artist = Artist.query.get_or_404(artist_id)
    if request.method == "POST":
        return _save_artist(artist)
    return render_template_string(ARTIST_FORM_HTML, artist=artist)


@bp.route("/artists/<int:artist_id>/delete", methods=["POST"])
def artist_delete(artist_id):
    if auth_required():
        return redirect(url_for("publishing.login"))
    artist = Artist.query.get_or_404(artist_id)
    try:
        db.session.delete(artist)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        current_app.logger.error("artist_delete error: %s", e)
        flash("Error deleting artist.")
        return redirect(url_for("artists.artist_detail", artist_id=artist_id))
    flash("Artist deleted.")
    return redirect(url_for("artists.artists_list"))


def _save_artist(existing):
    form = request.form
    try:
        name = form.get("name", "").strip()
        if not name:
            flash("Artist name is required.")
            return redirect(request.referrer or url_for("artists.artists_list"))

        legal_name   = form.get("legal_name", "").strip()
        aka          = form.get("aka", "").strip()
        email        = form.get("email", "").strip()
        phone_number = form.get("phone_number", "").strip()
        address      = form.get("address", "").strip()
        city         = form.get("city", "").strip()
        state        = form.get("state", "").strip()
        zip_code     = form.get("zip_code", "").strip()

        # Duplicate check
        dupe = Artist.query.filter(
            func.lower(Artist.name) == name.lower(),
            Artist.id != (existing.id if existing else -1)
        ).first()
        if dupe:
            flash(f'An artist named "{dupe.name}" already exists.')
            return redirect(request.referrer or url_for("artists.artists_list"))

        if existing:
            a = existing
        else:
            a = Artist()
            db.session.add(a)

        a.name         = name
        a.legal_name   = legal_name
        a.aka          = aka
        a.email        = email or None
        a.phone_number = phone_number
        a.address      = address
        a.city         = city
        a.state        = state
        a.zip_code     = zip_code

        db.session.commit()
        flash("Artist saved.")
        return redirect(url_for("artists.artist_detail", artist_id=a.id))

    except Exception as e:
        db.session.rollback()
        current_app.logger.error("ARTIST SAVE ERROR: %s", e)
        current_app.logger.error(traceback.format_exc())
        flash("Error saving artist: " + str(e))
        return redirect(request.referrer or url_for("artists.artists_list"))
