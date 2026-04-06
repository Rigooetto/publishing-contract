import datetime
from extensions import db


class Camp(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False, unique=True, index=True)
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)


class GenerationBatch(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    session_name = db.Column(db.String(255))
    contract_date = db.Column(db.Date)
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    created_by = db.Column(db.String(255))
    status = db.Column(db.String(50))


class Writer(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    first_name = db.Column(db.String(100), default="", index=True)
    middle_name = db.Column(db.String(100), default="")
    last_names = db.Column(db.String(150), default="")
    full_name = db.Column(db.String(250), nullable=False, unique=True, index=True)
    writer_aka = db.Column(db.String(250), default="")
    ipi = db.Column(db.String(50), nullable=True, unique=True, index=True)
    pro = db.Column(db.String(20), default="")
    email = db.Column(db.String(255), nullable=True, index=True)
    phone_number = db.Column(db.String(50), default="")
    address = db.Column(db.String(255), default="")
    city = db.Column(db.String(100), default="")
    state = db.Column(db.String(100), default="")
    zip_code = db.Column(db.String(20), default="")
    has_master_contract = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    default_publisher = db.Column(db.String(255), default="")
    default_publisher_ipi = db.Column(db.String(50), default="")


class Work(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(255), nullable=False, index=True)
    normalized_title = db.Column(db.String(255), index=True, default="")
    batch_id = db.Column(db.Integer, db.ForeignKey("generation_batch.id"), nullable=True)
    contract_date = db.Column(db.Date, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    batch = db.relationship("GenerationBatch", foreign_keys=[batch_id], lazy="select")
    work_writers = db.relationship("WorkWriter", backref="work", lazy=True, cascade="all, delete-orphan")
    contract_documents = db.relationship("ContractDocument", backref="work", lazy=True, cascade="all, delete-orphan")


class WorkWriter(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    work_id = db.Column(db.Integer, db.ForeignKey("work.id"), nullable=False)
    writer_id = db.Column(db.Integer, db.ForeignKey("writer.id"), nullable=False)
    writer_percentage = db.Column(db.Float, default=0.0)
    publisher = db.Column(db.String(255), default="")
    publisher_ipi = db.Column(db.String(50), default="")
    publisher_address = db.Column(db.String(255), default="")
    publisher_city = db.Column(db.String(100), default="")
    publisher_state = db.Column(db.String(100), default="")
    publisher_zip_code = db.Column(db.String(20), default="")
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    writer = db.relationship("Writer", backref="work_links")


class ContractDocument(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    batch_id = db.Column(db.Integer, db.ForeignKey("generation_batch.id"), nullable=True)
    work_id = db.Column(db.Integer, db.ForeignKey("work.id"), nullable=True)
    writer_id = db.Column(db.Integer, db.ForeignKey("writer.id"), nullable=False)
    document_type = db.Column(db.String(50), nullable=False)
    file_name = db.Column(db.String(255), nullable=False)
    writer_name_snapshot = db.Column(db.String(250), nullable=False)
    work_title_snapshot = db.Column(db.String(255), nullable=False)
    drive_file_id = db.Column(db.String(255), nullable=True)
    drive_web_view_link = db.Column(db.String(500), nullable=True)
    signed_file_name = db.Column(db.String(255), nullable=True)
    signed_drive_file_id = db.Column(db.String(255), nullable=True)
    signed_web_view_link = db.Column(db.String(500), nullable=True)
    signed_uploaded_at = db.Column(db.DateTime, nullable=True)
    status = db.Column(db.String(50), default="generated")
    generated_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    writer = db.relationship("Writer", backref="contract_documents")
    docusign_envelope_id = db.Column(db.String(100), nullable=True)
    docusign_status = db.Column(db.String(50), nullable=True)
    sent_for_signature_at = db.Column(db.DateTime, nullable=True)
    completed_at = db.Column(db.DateTime, nullable=True)
    signed_pdf_drive_file_id = db.Column(db.String(255), nullable=True)
    signed_pdf_drive_web_view_link = db.Column(db.String(500), nullable=True)
    certificate_drive_file_id = db.Column(db.String(255), nullable=True)
    certificate_drive_web_view_link = db.Column(db.String(500), nullable=True)


# ── Phase 2 — Catalog Models ──────────────────────────────────────────────────

class Release(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    release_type = db.Column(db.String(20), nullable=False)  # Album, EP, Single
    title = db.Column(db.String(255), nullable=False)
    upc = db.Column(db.String(50), nullable=True)
    artists = db.Column(db.Text, default="")          # JSON list
    num_tracks = db.Column(db.Integer, nullable=True)
    release_date = db.Column(db.Date, nullable=True)
    distributor = db.Column(db.String(255), default="")
    status = db.Column(db.String(30), default="draft")  # draft, ready, delivered
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    tracks = db.relationship("Track", backref="release", lazy=True, cascade="all, delete-orphan", order_by="Track.track_number")


class Track(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    release_id = db.Column(db.Integer, db.ForeignKey("release.id"), nullable=False)
    track_number = db.Column(db.Integer, nullable=True)
    primary_title = db.Column(db.String(255), nullable=False)
    recording_title = db.Column(db.String(255), default="")
    aka_title = db.Column(db.String(255), default="")
    aka_type_code = db.Column(db.String(50), default="")
    duration = db.Column(db.String(10), default="")        # mm:ss
    isrc = db.Column(db.String(20), nullable=True)
    track_label = db.Column(db.String(255), default="")
    track_p_line = db.Column(db.String(255), default="")
    artists = db.Column(db.Text, default="")               # JSON list
    genre = db.Column(db.String(100), default="")
    recording_date = db.Column(db.Date, nullable=True)
    recording_engineer = db.Column(db.String(255), default="")
    producer = db.Column(db.String(255), default="")
    executive_producer = db.Column(db.String(255), default="")
    is_cover = db.Column(db.Boolean, default=False, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    track_works = db.relationship("TrackWork", backref="track", lazy=True, cascade="all, delete-orphan")


class TrackWork(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    track_id = db.Column(db.Integer, db.ForeignKey("track.id"), nullable=False)
    work_id = db.Column(db.Integer, db.ForeignKey("work.id"), nullable=False)
    notes = db.Column(db.String(255), default="")
    work = db.relationship("Work", backref="track_works")


class ArtistRelease(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    artist_id = db.Column(db.Integer, db.ForeignKey("artist.id"), nullable=False, index=True)
    release_id = db.Column(db.Integer, db.ForeignKey("release.id"), nullable=False, index=True)
    __table_args__ = (db.UniqueConstraint("artist_id", "release_id"),)


class Artist(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False, unique=True, index=True)
    legal_name = db.Column(db.String(255), default="")
    aka = db.Column(db.String(255), default="")
    email = db.Column(db.String(255), nullable=True, index=True)
    phone_number = db.Column(db.String(50), default="")
    address = db.Column(db.String(255), default="")
    city = db.Column(db.String(100), default="")
    state = db.Column(db.String(100), default="")
    zip_code = db.Column(db.String(20), default="")
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    releases = db.relationship("Release", secondary="artist_release", backref="linked_artists", lazy="dynamic")
