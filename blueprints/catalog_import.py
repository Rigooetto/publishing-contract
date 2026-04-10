"""
Catalog CSV import blueprint.

Reads the Afinarte catalog CSV (one row per track) and bulk-upserts:
  - Release + Artist + ArtistRelease  (every row)
  - Track                              (every row)
  - Work + Writer + WorkWriter + TrackWork  (Publishing == TRUE rows only)
"""
import csv
import datetime
import io
import json
import traceback

from flask import Blueprint, request, redirect, url_for, flash, render_template_string, current_app

from extensions import db
from models import Release, Track, Artist, ArtistRelease, Work, Writer, WorkWriter, TrackWork
from utils import auth_required
from ui import CATALOG_IMPORT_HTML, CATALOG_IMPORT_RESULT_HTML

bp = Blueprint("catalog_import", __name__)

# ── Column layout ─────────────────────────────────────────────────────────────

RELEASE_ARTIST_COLS = [
    "Release Artist 1", "Release Artist 2", "Release Artist 3", "Release Artist 4",
    "Release Artist 5", "Release Artist 6", "Release Artist 7", "Release Artist 8",
]

TRACK_ARTIST_COLS = [
    "Track Artist Name 1", "Track Artist Name 2", "Track Artist Name 3",
    "Track Artist Name 4", "Track Artist Name 5", "Track Artist Name 6",
]

# Composers 1–6 have split name columns; 7–8 have full name only.
COMPOSER_COLS = [
    # (full_name_col, first_name_col, middle_name_col, last_name_col, ipi_col, split_col, pro_col)
    ("Composer 1", "Composer 1 First Name", "Composer 1 Middle Name", "Composer 1 Last Name", "Composer 1 IPI/CAE#", "Comp 1 Split %", "omp PRO"),
    ("Composer 2", "Composer 2 First Name", "Composer 2 Middle Name", "Composer 2 Last Name", "Composer 2 IPI/CAE#", "Comp 2 Split %", "Comp 2 PRO"),
    ("Composer 3", "Composer 3 First Name", "Composer 3 Middle Name", "Composer 3 Last Name", "Composer 3 IPI/CAE#", "Comp 3 Split %", "Comp 3 PRO"),
    ("Composer 4", "Composer 4 First Name", "Composer 4 Middle Name", "Composer 4 Last Name", "Composer 4 IPI/CAE#", "Comp 4 Split %", "Comp 4 PRO"),
    ("Composer 5", "Composer 5 First Name", "Composer 5 Middle Name", "Composer 5 Last Name", "Composer 5 IPI/CAE#", "Comp 5 Split %", "Comp 5 PRO"),
    ("Composer 6", "Composer 6 First Name", "Composer 6 Middle Name", "Composer 6 Last Name", "Composer 6 IPI/CAE#", "Comp 6 Split %", "Comp 6 PRO"),
    # Composers 7–8: no split name columns (use None placeholders)
    ("Composer 7", None, None, None, "Composer 7 IPI/CAE#", "Comp 7 Split %", "Comp 7 PRO"),
    ("Composer 8", None, None, None, "Composer 8 IPI/CAE#", "Comp 8 Split %", "Comp 8 PRO"),
]

PUBLISHER_COLS = [
    ("Publisher 1", "Publisher 1 IPI", "Publisher 1 PRO"),
    ("Publisher 2", "Publisher 2 IPI", "Publisher 2 PRO"),
    ("Publisher 3", "Publisher 3 IPI", "Publisher 3 PRO"),
    ("Publisher 4", "Publisher 4 IPI", "Publisher 4 PRO"),
    ("Publisher 5", "Publisher 5 IPI", "Publisher 5 PRO"),
    ("Publisher 6", "Publisher 6 IPI", "Publisher 6 PRO"),
    ("Publisher 7", "Publisher 7 IPI", "Publisher 7 PRO"),
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_duration(raw):
    """
    Normalise duration to MM:SS.
    Handles:
      - '02:33:00'          → '02:33'
      - '2020-09-10 03:41:00' (Excel date contamination) → '03:41'
      - '03:41'             → '03:41'
    """
    raw = (raw or "").strip()
    if not raw:
        return ""
    # Excel date+time: '2020-09-10 03:41:00'
    if " " in raw:
        raw = raw.split(" ", 1)[1]  # take time part
    parts = raw.split(":")
    if len(parts) >= 3:
        return f"{parts[0]}:{parts[1]}"
    return raw


def _parse_date(raw):
    if not raw:
        return None
    raw = raw.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.datetime.strptime(raw, fmt).date()
        except ValueError:
            pass
    return None


def _derive_release_type(num_tracks_str):
    try:
        n = int(num_tracks_str or 0)
    except (ValueError, TypeError):
        n = 0
    if n <= 1:
        return "Single"
    if n <= 6:
        return "EP"
    return "Album"


def _get_release_artists(row):
    """Return list of non-empty release artist names from Release Artist 1–8."""
    artists = []
    for col in RELEASE_ARTIST_COLS:
        name = row.get(col, "").strip()
        if name:
            artists.append(name)
    return artists


def _get_track_artists(row):
    """Return list of non-empty track artist names from Track Artist Name 1–6."""
    artists = []
    for col in TRACK_ARTIST_COLS:
        name = row.get(col, "").strip()
        if name:
            artists.append(name)
    return artists


def _release_key(row):
    upc = row["UPC"].strip()
    if upc:
        return ("upc", upc)
    primary_artist = row.get("Release Artist 1", "").strip()
    return ("title", row["Album Title"].strip().lower() + "||" + primary_artist.lower())


# ── Import logic ──────────────────────────────────────────────────────────────

def _run_import(file_bytes):
    stats = {
        "releases_created": 0,
        "releases_updated": 0,
        "tracks_created": 0,
        "tracks_updated": 0,
        "artists_created": 0,
        "works_created": 0,
        "writers_created": 0,
        "rows_skipped": 0,
        "errors": [],
    }

    # Decode — try utf-8-sig, fall back to latin-1
    try:
        text = file_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = file_bytes.decode("latin-1")

    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)

    # ── Group rows by release ──────────────────────────────────────────────
    release_groups = {}
    for row in rows:
        k = _release_key(row)
        release_groups.setdefault(k, []).append(row)

    # ── Pre-load all existing DB records into memory ──────────────────────
    # This avoids thousands of individual SELECT queries during the import,
    # keeping the Postgres connection short-lived and preventing timeouts.
    artist_cache  = {a.name.lower(): a for a in Artist.query.all()}
    writer_cache  = {w.full_name.lower(): w for w in Writer.query.all()}
    writer_ipi_cache = {w.ipi: w for w in Writer.query.all() if w.ipi}
    work_cache    = {w.title.lower(): w for w in Work.query.all()}
    release_by_upc   = {r.upc: r for r in Release.query.all() if r.upc}
    release_by_title = {r.title.lower(): r for r in Release.query.all() if not r.upc}
    track_by_isrc    = {t.isrc: t for t in Track.query.all() if t.isrc}
    # (release_id, lower_title) → Track  for tracks without ISRC
    track_by_rid_title = {(t.release_id, t.primary_title.lower()): t for t in Track.query.all()}
    # Sets of (artist_id, release_id) and (track_id, work_id) and (work_id, writer_id)
    artist_release_set = {(ar.artist_id, ar.release_id) for ar in ArtistRelease.query.all()}
    track_work_set     = {(tw.track_id, tw.work_id) for tw in TrackWork.query.all()}
    work_writer_set    = {(ww.work_id, ww.writer_id) for ww in WorkWriter.query.all()}

    def _get_or_create_artist(name):
        key = name.lower()
        if key not in artist_cache:
            obj = Artist(name=name)
            db.session.add(obj)
            db.session.flush()
            artist_cache[key] = obj
            stats["artists_created"] += 1
        return artist_cache[key]

    def _get_or_create_writer(full_name, ipi, pro, first_name="", middle_name="", last_names=""):
        # IPI lookup takes priority
        if ipi and ipi in writer_ipi_cache:
            obj = writer_ipi_cache[ipi]
            # Update any missing/blank fields from the CSV
            if pro and not obj.pro:
                obj.pro = pro
            if first_name and not obj.first_name:
                obj.first_name = first_name
            if middle_name and not obj.middle_name:
                obj.middle_name = middle_name
            if last_names and not obj.last_names:
                obj.last_names = last_names
            writer_cache[full_name.lower()] = obj
            return obj
        key = full_name.lower()
        if key not in writer_cache:
            obj = Writer(
                full_name=full_name,
                pro=pro or "",
                first_name=first_name or "",
                middle_name=middle_name or "",
                last_names=last_names or "",
            )
            if ipi:
                obj.ipi = ipi
            db.session.add(obj)
            db.session.flush()
            writer_cache[key] = obj
            if ipi:
                writer_ipi_cache[ipi] = obj
            stats["writers_created"] += 1
        return writer_cache[key]

    def _get_or_create_work(title, contract_date):
        key = title.lower()
        if key not in work_cache:
            obj = Work(title=title, normalized_title=key, contract_date=contract_date)
            db.session.add(obj)
            db.session.flush()
            work_cache[key] = obj
            stats["works_created"] += 1
        return work_cache[key]

    # ── Process each release group ─────────────────────────────────────────
    for _, rrows in release_groups.items():
        first = rrows[0]
        try:
            upc = first["UPC"].strip() or None
            album_title = first["Album Title"].strip()
            release_artists = _get_release_artists(first)
            num_tracks_str = first["# Tracks"].strip()
            release_date = _parse_date(first["Digital release date"])
            distributor = first["Track label"].strip()
            release_type = _derive_release_type(num_tracks_str)

            # ── Upsert Release ─────────────────────────────────────────────
            r = None
            if upc:
                r = release_by_upc.get(upc)
            # Only fall back to title matching when no UPC is available.
            if not r and not upc:
                r = release_by_title.get(album_title.lower())

            if r:
                r.title = album_title
                r.release_date = release_date or r.release_date
                r.distributor = distributor or r.distributor
                if upc and not r.upc:
                    r.upc = upc
                    release_by_upc[upc] = r
                stats["releases_updated"] += 1
            else:
                r = Release(
                    release_type=release_type,
                    title=album_title,
                    upc=upc,
                    release_date=release_date,
                    distributor=distributor,
                    status="ready",
                    artists=json.dumps(release_artists),
                    num_tracks=int(num_tracks_str) if num_tracks_str.isdigit() else None,
                )
                db.session.add(r)
                db.session.flush()
                if upc:
                    release_by_upc[upc] = r
                else:
                    release_by_title[album_title.lower()] = r
                stats["releases_created"] += 1

            # ── Album-level artists ────────────────────────────────────────
            for artist_name in release_artists:
                art = _get_or_create_artist(artist_name)
                key = (art.id, r.id)
                if key not in artist_release_set:
                    db.session.add(ArtistRelease(artist_id=art.id, release_id=r.id))
                    artist_release_set.add(key)

            # ── Process each track row ─────────────────────────────────────
            for row in rrows:
                try:
                    track_title = row["Track title"].strip()
                    if not track_title:
                        stats["rows_skipped"] += 1
                        continue

                    isrc         = row["ISRC"].strip() or None
                    track_num_s  = row["Track number"].strip()
                    track_num    = int(track_num_s) if track_num_s.isdigit() else None
                    duration     = _parse_duration(row["Duration"])
                    track_label  = row["Track label"].strip()
                    track_p_line = row["Track P Line"].strip()
                    publishing   = row["Publishing"].strip().upper() == "TRUE"

                    track_artists = _get_track_artists(row)
                    if not track_artists:
                        track_artists = release_artists[:]

                    # Upsert Track
                    t = None
                    if isrc:
                        t = track_by_isrc.get(isrc)
                    if not t:
                        t = track_by_rid_title.get((r.id, track_title.lower()))

                    if t:
                        t.duration     = duration or t.duration
                        t.track_label  = track_label or t.track_label
                        t.track_p_line = track_p_line or t.track_p_line
                        if isrc and not t.isrc:
                            t.isrc = isrc
                            track_by_isrc[isrc] = t
                        if track_num and not t.track_number:
                            t.track_number = track_num
                        stats["tracks_updated"] += 1
                    else:
                        t = Track(
                            release_id=r.id,
                            primary_title=track_title,
                            track_number=track_num,
                            duration=duration,
                            isrc=isrc,
                            track_label=track_label,
                            track_p_line=track_p_line,
                            artists=json.dumps(track_artists),
                        )
                        db.session.add(t)
                        db.session.flush()
                        if isrc:
                            track_by_isrc[isrc] = t
                        track_by_rid_title[(r.id, track_title.lower())] = t
                        stats["tracks_created"] += 1

                    # Track-level artist links
                    release_artist_lower = {a.lower() for a in release_artists}
                    for ta_name in track_artists:
                        if ta_name.lower() not in release_artist_lower:
                            ta = _get_or_create_artist(ta_name)
                            key = (ta.id, r.id)
                            if key not in artist_release_set:
                                db.session.add(ArtistRelease(artist_id=ta.id, release_id=r.id))
                                artist_release_set.add(key)

                    # ── Publishing == TRUE → Work + Writers ────────────────
                    if publishing:
                        work = _get_or_create_work(track_title, release_date)

                        tw_key = (t.id, work.id)
                        if tw_key not in track_work_set:
                            db.session.add(TrackWork(track_id=t.id, work_id=work.id))
                            track_work_set.add(tw_key)

                        # Writers / WorkWriters
                        for i, (nc, fnc, mnc, lnc, ic, sc, pc) in enumerate(COMPOSER_COLS):
                            cname = row.get(nc, "").strip()
                            if not cname or cname.lower() in ("no registrada", ""):
                                continue
                            cipi  = row.get(ic, "").strip() or None
                            cpro  = row.get(pc, "").strip()
                            try:
                                csplit = float(row.get(sc, "0").strip() or 0)
                            except ValueError:
                                csplit = 0.0

                            cfirst  = row.get(fnc, "").strip() if fnc else ""
                            cmiddle = row.get(mnc, "").strip() if mnc else ""
                            clast   = row.get(lnc, "").strip() if lnc else ""

                            pub_name, pub_ipi = "", ""
                            if i < len(PUBLISHER_COLS):
                                pnc, pic, _ = PUBLISHER_COLS[i]
                                pub_name = row.get(pnc, "").strip()
                                pub_ipi  = row.get(pic, "").strip()

                            writer = _get_or_create_writer(
                                cname, cipi, cpro,
                                first_name=cfirst,
                                middle_name=cmiddle,
                                last_names=clast,
                            )

                            ww_key = (work.id, writer.id)
                            if ww_key not in work_writer_set:
                                db.session.add(WorkWriter(
                                    work_id=work.id,
                                    writer_id=writer.id,
                                    writer_percentage=csplit,
                                    publisher=pub_name,
                                    publisher_ipi=pub_ipi,
                                ))
                                work_writer_set.add(ww_key)

                except Exception as row_err:
                    stats["errors"].append(
                        f"Row '{row.get('Track title', '?')}': {row_err}"
                    )
                    current_app.logger.error("CATALOG IMPORT ROW ERROR: %s", row_err)

            db.session.commit()
            db.session.expire_all()  # free session memory between releases

        except Exception as rel_err:
            db.session.rollback()
            stats["errors"].append(
                f"Release '{first.get('Album Title', '?')}': {rel_err}"
            )
            current_app.logger.error("CATALOG IMPORT RELEASE ERROR: %s\n%s", rel_err, traceback.format_exc())

    return stats


# ── Routes ────────────────────────────────────────────────────────────────────

@bp.route("/admin/import-catalog-csv", methods=["GET"])
def catalog_import_form():
    if auth_required():
        return redirect(url_for("publishing.login"))
    return render_template_string(CATALOG_IMPORT_HTML)


@bp.route("/admin/import-catalog-csv", methods=["POST"])
def catalog_import_run():
    if auth_required():
        return redirect(url_for("publishing.login"))

    f = request.files.get("catalog_file")
    if not f or not f.filename:
        flash("Please select a CSV file.")
        return redirect(url_for("catalog_import.catalog_import_form"))

    try:
        file_bytes = f.read()
        stats = _run_import(file_bytes)
    except Exception as e:
        current_app.logger.error("CATALOG IMPORT FATAL: %s\n%s", e, traceback.format_exc())
        flash(f"Import failed: {e}")
        return redirect(url_for("catalog_import.catalog_import_form"))

    return render_template_string(CATALOG_IMPORT_RESULT_HTML, stats=stats)
