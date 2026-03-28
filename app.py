from flask import Flask, render_template_string, request, send_file, session, redirect, url_for, jsonify, flash
from docx import Document
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, or_
from babel.dates import format_date
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io
import os
import re
import json
import zipfile
import datetime
import base64
import xml.etree.ElementTree as ET
import traceback

from docusign_esign import ApiClient, EnvelopesApi, EnvelopeDefinition, Document as DocusignDocument, Signer, SignHere, Tabs, Recipients

DOCUSIGN_ACCOUNT_ID = os.getenv("DOCUSIGN_ACCOUNT_ID", "")
DOCUSIGN_BASE_PATH = os.getenv("DOCUSIGN_BASE_PATH", "https://demo.docusign.net/restapi")
DOCUSIGN_AUTH_SERVER = os.getenv("DOCUSIGN_AUTH_SERVER", "account-d.docusign.com")
DOCUSIGN_INTEGRATION_KEY = os.getenv("DOCUSIGN_INTEGRATION_KEY", "")
DOCUSIGN_USER_ID = os.getenv("DOCUSIGN_USER_ID", "")
DOCUSIGN_PRIVATE_KEY = os.getenv("DOCUSIGN_PRIVATE_KEY", "")

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "change-this-secret-key")

raw_db_url = os.getenv("DATABASE_URL", "sqlite:///writers.db")
if raw_db_url.startswith("postgres://"):
    raw_db_url = raw_db_url.replace("postgres://", "postgresql://", 1)
if raw_db_url.startswith("postgresql://") and "sslmode=" not in raw_db_url:
    joiner = "&" if "?" in raw_db_url else "?"
    raw_db_url = f"{raw_db_url}{joiner}sslmode=require"

app.config["SQLALCHEMY_DATABASE_URI"] = raw_db_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["MAX_CONTENT_LENGTH"] = 25 * 1024 * 1024  # 25 MB upload limit

db = SQLAlchemy(app)

BASE_TEMPLATE_DIR = os.getenv("TEMPLATE_DIR", "template")
FULL_CONTRACT_TEMPLATE = os.getenv(
    "FULL_CONTRACT_TEMPLATE",
    os.path.join(BASE_TEMPLATE_DIR, "PUBLISHING_AGREEMENT_CONTRACT.docx"),
)
SCHEDULE_1_TEMPLATE = os.getenv(
    "SCHEDULE_1_TEMPLATE",
    os.path.join(BASE_TEMPLATE_DIR, "SCHEDULE_1.docx"),
)
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "generated_contracts")

TEAM_USERNAME = os.getenv("TEAM_USERNAME")
TEAM_PASSWORD = os.getenv("TEAM_PASSWORD")

DEFAULT_PUBLISHER_ADDRESS = "3840 E. Miraloma Ave"
DEFAULT_PUBLISHER_CITY = "Anaheim"
DEFAULT_PUBLISHER_STATE = "CA"
DEFAULT_PUBLISHER_ZIP = "92806"

GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")

app.logger.warning("ENV CHECK: folder=%s json=%s", bool(GOOGLE_DRIVE_FOLDER_ID), bool(GOOGLE_SERVICE_ACCOUNT_JSON))
app.logger.warning("JSON LEN: %s", len(GOOGLE_SERVICE_ACCOUNT_JSON or ""))

os.makedirs(OUTPUT_DIR, exist_ok=True)


def slugify(value: str) -> str:
    value = (value or "").strip()
    value = re.sub(r"[^\w\s-]", "", value, flags=re.UNICODE)
    value = re.sub(r"[-\s]+", "_", value)
    return value or "file"


def parse_float(value: str) -> float:
    try:
        return float((value or "").strip())
    except ValueError:
        return 0.0


def build_full_name(first_name: str, middle_name: str, last_names: str) -> str:
    return " ".join(
        part.strip() for part in [first_name, middle_name, last_names] if part and part.strip()
    ).strip()


def normalize_text(value: str) -> str:
    return " ".join((value or "").lower().strip().split())


def normalize_title(title: str) -> str:
    return normalize_text(title)


def build_writer_identity_from_row(row: dict) -> str:
    ipi = (row.get("ipi") or "").strip()
    if ipi:
        return f"ipi:{ipi.lower()}"
    selected_writer_id = (row.get("selected_writer_id") or "").strip()
    if selected_writer_id:
        return f"id:{selected_writer_id}"
    return f"name:{normalize_text(row.get('full_name', ''))}"


def build_writer_identity_from_workwriter(work_writer) -> str:
    if work_writer.writer and work_writer.writer.ipi:
        return f"ipi:{work_writer.writer.ipi.lower()}"
    if work_writer.writer_id:
        return f"id:{work_writer.writer_id}"
    return f"name:{normalize_text(work_writer.writer.full_name if work_writer.writer else '')}"


def default_publisher_for_pro(pro: str) -> str:
    return {
        "BMI": "Songs of Afinarte",
        "ASCAP": "Melodies of Afinarte",
        "SESAC": "Music of Afinarte",
    }.get((pro or "").strip(), "")


def default_publisher_ipi_for_pro(pro: str) -> str:
    return {
        "BMI": "817874992",
        "ASCAP": "807953316",
        "SESAC": "817094629",
    }.get((pro or "").strip(), "")


def get_drive_service():
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        raise ValueError("Missing GOOGLE_SERVICE_ACCOUNT_JSON")

    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    credentials = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=credentials, cache_discovery=False)

def get_docusign_api_client():
    private_key = (DOCUSIGN_PRIVATE_KEY or "").replace("\\n", "\n")
    private_key_bytes = private_key.encode("utf-8")

    api_client = ApiClient()
    api_client.host = DOCUSIGN_BASE_PATH

    token = api_client.request_jwt_user_token(
        client_id=DOCUSIGN_INTEGRATION_KEY,
        user_id=DOCUSIGN_USER_ID,
        oauth_host_name=DOCUSIGN_AUTH_SERVER,
        private_key_bytes=private_key_bytes,
        expires_in=3600,
        scopes=["signature", "impersonation"],
    )

    access_token = token.access_token
    api_client.set_default_header("Authorization", f"Bearer {access_token}")
    return api_client


def upload_bytes_to_drive(file_name: str, file_bytes: bytes, parent_folder_id: str, mime_type: str):
    service = get_drive_service()

    media = MediaIoBaseUpload(io.BytesIO(file_bytes), mimetype=mime_type, resumable=False)
    metadata = {"name": file_name}
    if parent_folder_id:
        metadata["parents"] = [parent_folder_id]

    created = service.files().create(
        body=metadata,
        media_body=media,
        fields="id, webViewLink",
        supportsAllDrives=True,
    ).execute()

    return {
        "file_id": created.get("id"),
        "web_view_link": created.get("webViewLink"),
    }


class Camp(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False, unique=True, index=True)
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)

    works = db.relationship("Work", backref="camp", lazy=True)
    batches = db.relationship("GenerationBatch", backref="camp", lazy=True)


class GenerationBatch(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    camp_id = db.Column(db.Integer, db.ForeignKey("camp.id"), nullable=True)
    contract_date = db.Column(db.Date, nullable=False)
    created_by = db.Column(db.String(100), default="")
    status = db.Column(db.String(50), default="draft")  # draft / docs_generated / signed_partial / signed_complete
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)

    works = db.relationship("Work", backref="batch", lazy=True)
    documents = db.relationship("ContractDocument", backref="batch", lazy=True)


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
    
    address = db.Column(db.String(255), default="")
    city = db.Column(db.String(100), default="")
    state = db.Column(db.String(100), default="")
    zip_code = db.Column(db.String(20), default="")

    has_master_contract = db.Column(db.Boolean, default=False)

    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    updated_at = db.Column(
        db.DateTime,
        default=datetime.datetime.utcnow,
        onupdate=datetime.datetime.utcnow,
    )


class Work(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(255), nullable=False, index=True)
    normalized_title = db.Column(db.String(255), index=True, default="")
    camp_id = db.Column(db.Integer, db.ForeignKey("camp.id"), nullable=True)
    batch_id = db.Column(db.Integer, db.ForeignKey("generation_batch.id"), nullable=True)
    contract_date = db.Column(db.Date, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)

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


def init_db():
    with app.app_context():
        db.create_all()


@app.context_processor
def inject_globals():
    return {
        "team_auth_enabled": bool(TEAM_USERNAME and TEAM_PASSWORD)
    }


LOGIN_HTML = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Team Login</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-light">
  <div class="container py-5">
    <div class="row justify-content-center">
      <div class="col-md-4">
        <div class="card shadow-sm">
          <div class="card-body p-4">
            <h3 class="mb-3">Team Login</h3>
            {% with messages = get_flashed_messages() %}
              {% if messages %}
                {% for message in messages %}
                  <div class="alert alert-danger">{{ message }}</div>
                {% endfor %}
              {% endif %}
            {% endwith %}
            <form method="post">
              <div class="mb-3">
                <label class="form-label">Username</label>
                <input class="form-control" name="username" required>
              </div>
              <div class="mb-3">
                <label class="form-label">Password</label>
                <input type="password" class="form-control" name="password" required>
              </div>
              <button class="btn btn-primary w-100">Log in</button>
            </form>
          </div>
        </div>
      </div>
    </div>
  </div>
</body>
</html>
"""

FORM_HTML = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Create Work</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    body { background: #f8f9fa; }
    .card { border-radius: 18px; }
    h4 { margin-top: 8px; margin-bottom: 14px; font-weight: 700; }
    .form-label { font-weight: 600; font-size: 0.95rem; margin-bottom: 6px; }
    .card-body { max-width: 1250px; margin: 0 auto; }
    .writer-row {
      border: 1px solid #e9ecef;
      border-radius: 14px;
      padding: 16px;
      margin-bottom: 16px;
      background: #fff;
      position: relative;
    }
    .autocomplete-wrap { position: relative; }
    .autocomplete-box {
      position: absolute;
      top: 100%;
      left: 0;
      right: 0;
      z-index: 1000;
      background: white;
      border: 1px solid #dee2e6;
      border-top: none;
      max-height: 220px;
      overflow-y: auto;
      display: none;
      box-shadow: 0 6px 18px rgba(0,0,0,0.08);
    }
    .autocomplete-item {
      padding: 10px 12px;
      cursor: pointer;
      border-bottom: 1px solid #f1f3f5;
    }
    .autocomplete-item:hover { background: #f1f5ff; }
    .status-pill {
      display: inline-block;
      padding: 4px 10px;
      border-radius: 999px;
      font-size: 0.8rem;
      font-weight: 600;
      margin-right: 8px;
    }
    .status-new { background: #fff3cd; color: #7a5a00; }
    .status-existing { background: #d1e7dd; color: #0f5132; }
    .status-full { background: #cfe2ff; color: #084298; }
    .status-s1 { background: #d1e7dd; color: #0f5132; }
    .writer-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 12px;
    }
    .writer-meta { min-height: 28px; }
    .sticky-summary {
      position: sticky;
      top: 10px;
      z-index: 100;
      background: #fff;
      border: 1px solid #e9ecef;
      border-radius: 12px;
      padding: 10px 14px;
      margin-bottom: 16px;
    }
  </style>
</head>
<body>
<div class="container py-4">
  {% with messages = get_flashed_messages() %}
    {% if messages %}
      {% for message in messages %}
        <div class="alert alert-warning">{{ message }}</div>
      {% endfor %}
    {% endif %}
  {% endwith %}

  <div class="card shadow-sm">
    <div class="card-body p-4">
      <div class="d-flex justify-content-between align-items-center mb-4">
        <div>
          <h2 class="mb-1">Create Work</h2>
          <p class="text-muted mb-0">Create one work, add multiple writers, and save it into a batch for review.</p>
        </div>
        <div class="d-flex gap-2">
          <a href="{{ url_for('works_list') }}" class="btn btn-outline-primary btn-sm">Works</a>
          <a href="{{ url_for('batches_list') }}" class="btn btn-outline-primary btn-sm">Batches</a>
          {% if team_auth_enabled and session.get('logged_in') %}
            <a href="{{ url_for('logout') }}" class="btn btn-outline-secondary btn-sm">Log out</a>
          {% endif %}
        </div>
      </div>

      <div class="sticky-summary">
        <strong>Split Total:</strong> <span id="splitTotal">0.00</span>%
        <span id="splitStatus" class="ms-2 badge text-bg-secondary">Incomplete</span>
      </div>

      <form method="post" id="workForm">
        <input type="hidden" name="force_create" value="{{ force_create or '' }}">

        <h4>Work</h4>

<div class="row mb-4">
  <div class="col-md-4">
    <label class="form-label">Add to Existing Batch</label>
    <select class="form-control" name="existing_batch_id">
      <option value="">Create new batch</option>
      {% for batch in batches %}
        <option value="{{ batch.id }}" {% if selected_batch_id == (batch.id|string) %}selected{% endif %}>
          Batch {{ batch.id }}
          {% if batch.camp %} — {{ batch.camp.name }}{% endif %}
          — {{ batch.contract_date.strftime('%Y-%m-%d') }}
        </option>
      {% endfor %}
    </select>
  </div>

  <div class="col-md-4">
    <label class="form-label">Camp</label>
    <select class="form-control" name="camp_id">
      <option value="">Select existing camp</option>
      {% for camp in camps %}
        <option value="{{ camp.id }}">{{ camp.name }}</option>
      {% endfor %}
    </select>
  </div>

  <div class="col-md-4">
    <label class="form-label">Or Create New Camp</label>
    <input class="form-control" name="new_camp_name" placeholder="New Camp Name">
  </div>
</div>

<div class="row mb-4">
  <div class="col-md-6">
    <label class="form-label">Work Title</label>
    <input class="form-control" name="work_title" required placeholder="Work Title">
  </div>
  <div class="col-md-6">
    <label class="form-label">Contract Date</label>
    <input class="form-control" name="contract_date" type="date" required>
  </div>
</div>

        <h4>Writers</h4>
        <div id="writerRows"></div>

        <div class="d-flex gap-2">
          <button type="button" class="btn btn-outline-primary" onclick="addWriterRow()">Add Writer</button>
          <button type="submit" class="btn btn-success">Save Work to Batch</button>
        </div>
      </form>
    </div>
  </div>
</div>

<script>
const proPublisherMap = {
  BMI: {
    name: 'Songs of Afinarte',
    ipi: '817874992'
  },
  ASCAP: {
    name: 'Melodies of Afinarte',
    ipi: '807953316'
  },
  SESAC: {
    name: 'Music of Afinarte',
    ipi: '817094629'
  }
};

const defaultPublisherAddress = "{{ default_publisher_address }}";
const defaultPublisherCity = "{{ default_publisher_city }}";
const defaultPublisherState = "{{ default_publisher_state }}";
const defaultPublisherZip = "{{ default_publisher_zip }}";

let writerRowIndex = 0;

function statusHtml(writerStatus, contractType) {
  const writerClass = writerStatus === 'Existing Writer' ? 'status-existing' : 'status-new';
  const contractClass = contractType === 'Schedule 1' ? 'status-s1' : 'status-full';
  return `
    <span class="status-pill ${writerClass}">${writerStatus}</span>
    <span class="status-pill ${contractClass}">${contractType}</span>
  `;
}

function writerRowTemplate(index) {
  return `
    <div class="writer-row" data-index="${index}">
      <div class="writer-header">
        <strong>Writer ${index + 1}</strong>
        <button type="button" class="btn btn-sm btn-outline-danger" onclick="removeWriterRow(this)">Remove</button>
      </div>

      <input type="hidden" name="writer_id" class="writer-id-field">
      <div class="writer-meta">${statusHtml('New Writer', 'Full Contract')}</div>

      <div class="row mt-3">
        <div class="col-md-3 autocomplete-wrap">
          <label class="form-label">First Name</label>
          <input class="form-control writer-first-name" name="writer_first_name" placeholder="First Name" autocomplete="off">
          <div class="autocomplete-box writer-suggestions"></div>
        </div>
        <div class="col-md-3">
          <label class="form-label">Middle Name</label>
          <input class="form-control writer-middle-name" name="writer_middle_name" placeholder="Middle Name" autocomplete="off">
        </div>
        <div class="col-md-3">
          <label class="form-label">Last Name(s)</label>
          <input class="form-control writer-last-names" name="writer_last_names" placeholder="Last Name(s)" autocomplete="off">
        </div>
        <div class="col-md-3">
          <label class="form-label">Writer AKA</label>
          <input class="form-control writer-aka" name="writer_aka" placeholder="AKA / Stage Name">
        </div>
      </div>

      
        <div class="row mt-3">
          <div class="col-md-2">
            <label class="form-label">Writer IPI #</label>
            <input class="form-control writer-ipi" name="writer_ipi" placeholder="IPI Number">
        </div>
        <div class="col-md-3">
          <label class="form-label">Writer Email</label>
          <input class="form-control writer-email" name="writer_email" placeholder="writer@email.com">
        </div>
        <div class="col-md-2">
          <label class="form-label">PRO</label>
          <select class="form-control writer-pro" name="writer_pro" onchange="syncPublisherFromPro(this)">
            <option value="">Select PRO</option>
            <option value="BMI">BMI</option>
            <option value="ASCAP">ASCAP</option>
            <option value="SESAC">SESAC</option>
          </select>
        </div>
        <div class="col-md-2">
          <label class="form-label">Writer %</label>
          <input class="form-control writer-split" name="writer_percentage" placeholder="Writer %" type="number" step="0.01" min="0" max="100">
        </div>
        <div class="col-md-3">
          <label class="form-label">Publisher</label>
          <input class="form-control writer-publisher" name="writer_publisher" placeholder="Publisher">
        </div>
      </div>


      <div class="row mt-3">
        <div class="col-md-3">
          <label class="form-label">Publisher IPI</label>
          <input class="form-control writer-publisher-ipi" name="publisher_ipi" placeholder="Publisher IPI">
        </div>
        <div class="col-md-5">
          <label class="form-label">Publisher Address</label>
          <input class="form-control writer-publisher-address" name="publisher_address" value="${defaultPublisherAddress}" placeholder="Publisher Address">
        </div>
        <div class="col-md-2">
          <label class="form-label">Publisher City</label>
          <input class="form-control writer-publisher-city" name="publisher_city" value="${defaultPublisherCity}" placeholder="Publisher City">
        </div>
        <div class="col-md-1">
          <label class="form-label">State</label>
          <input class="form-control writer-publisher-state" name="publisher_state" value="${defaultPublisherState}" placeholder="State">
        </div>
        <div class="col-md-1">
          <label class="form-label">Zip</label>
          <input class="form-control writer-publisher-zip" name="publisher_zip_code" value="${defaultPublisherZip}" placeholder="Zip">
        </div>
      </div>

      <div class="row mt-3">
        <div class="col-md-6">
          <label class="form-label">Writer Address</label>
          <input class="form-control writer-address" name="writer_address" placeholder="Address">
        </div>
        <div class="col-md-2">
          <label class="form-label">City</label>
          <input class="form-control writer-city" name="writer_city" placeholder="City">
        </div>
        <div class="col-md-2">
          <label class="form-label">State</label>
          <input class="form-control writer-state" name="writer_state" placeholder="State">
        </div>
        <div class="col-md-2">
          <label class="form-label">Zip Code</label>
          <input class="form-control writer-zip" name="writer_zip_code" placeholder="Zip Code">
        </div>
      </div>
    </div>
  `;
}

function addWriterRow() {
  const container = document.getElementById('writerRows');
  container.insertAdjacentHTML('beforeend', writerRowTemplate(writerRowIndex));
  setupWriterRow(container.lastElementChild);
  writerRowIndex += 1;
  updateSplitSummary();
}

function removeWriterRow(button) {
  const row = button.closest('.writer-row');
  row.remove();
  updateSplitSummary();
}

function syncPublisherFromPro(selectEl) {
  const row = selectEl.closest('.writer-row');
  const publisherInput = row.querySelector('.writer-publisher');
  const publisherIpiInput = row.querySelector('.writer-publisher-ipi');
  const selected = proPublisherMap[selectEl.value];

  if (!selected) return;

  publisherInput.value = selected.name;
  publisherIpiInput.value = selected.ipi;
}

function getFullNameFromRow(row) {
  const first = row.querySelector('.writer-first-name').value.trim();
  const middle = row.querySelector('.writer-middle-name').value.trim();
  const last = row.querySelector('.writer-last-names').value.trim();
  return [first, middle, last].filter(Boolean).join(' ');
}

function setRowStatus(row, writerStatus, contractType) {
  row.querySelector('.writer-meta').innerHTML = statusHtml(writerStatus, contractType);
}

function fillWriterRow(row, writer) {
  row.querySelector('.writer-id-field').value = writer.id || '';
  row.querySelector('.writer-first-name').value = writer.first_name || '';
  row.querySelector('.writer-middle-name').value = writer.middle_name || '';
  row.querySelector('.writer-last-names').value = writer.last_names || '';
  row.querySelector('.writer-aka').value = writer.writer_aka || '';
  row.querySelector('.writer-ipi').value = writer.ipi || '';
  row.querySelector('.writer-email').value = writer.email || '';
  row.querySelector('.writer-pro').value = writer.pro || '';
  row.querySelector('.writer-address').value = writer.address || '';
  row.querySelector('.writer-city').value = writer.city || '';
  row.querySelector('.writer-state').value = writer.state || '';
  row.querySelector('.writer-zip').value = writer.zip_code || '';

  const publisherData = proPublisherMap[writer.pro] || {};
  row.querySelector('.writer-publisher').value = writer.default_publisher || publisherData.name || '';
  row.querySelector('.writer-publisher-ipi').value = writer.default_publisher_ipi || publisherData.ipi || '';

  setRowStatus(
    row,
    'Existing Writer',
    writer.has_master_contract ? 'Schedule 1' : 'Full Contract'
  );
  hideSuggestions(row);
}

function hideSuggestions(row) {
  const box = row.querySelector('.writer-suggestions');
  box.style.display = 'none';
  box.innerHTML = '';
}

function resetRowToNew(row) {
  row.querySelector('.writer-id-field').value = '';
  setRowStatus(row, 'New Writer', 'Full Contract');
}

function setupWriterRow(row) {
  const firstName = row.querySelector('.writer-first-name');
  const middleName = row.querySelector('.writer-middle-name');
  const lastNames = row.querySelector('.writer-last-names');
  const suggestionsBox = row.querySelector('.writer-suggestions');
  const splitInput = row.querySelector('.writer-split');

  async function searchWriters() {
    const q = getFullNameFromRow(row);
    if (q.length < 2) {
      hideSuggestions(row);
      resetRowToNew(row);
      return;
    }

    const resp = await fetch(`/writers/search?q=${encodeURIComponent(q)}`);
    const writers = await resp.json();

    if (!writers.length) {
      hideSuggestions(row);
      resetRowToNew(row);
      return;
    }

    suggestionsBox.innerHTML = writers.map(writer => `
      <div class="autocomplete-item" data-writer='${JSON.stringify(writer).replaceAll("'", "&#39;")}'>
        <strong>${writer.full_name}</strong><br>
        <small>${writer.city || ''}${writer.city && writer.state ? ', ' : ''}${writer.state || ''}</small>
      </div>
    `).join('');
    suggestionsBox.style.display = 'block';

    suggestionsBox.querySelectorAll('.autocomplete-item').forEach(item => {
      item.addEventListener('click', () => {
        fillWriterRow(row, JSON.parse(item.dataset.writer));
      });
    });
  }

  [firstName, middleName, lastNames].forEach(input => {
    input.addEventListener('input', () => {
      row.querySelector('.writer-id-field').value = '';
      setRowStatus(row, 'New Writer', 'Full Contract');
      searchWriters();
    });
  });

  splitInput.addEventListener('input', updateSplitSummary);

  document.addEventListener('click', function(e) {
    if (!suggestionsBox.contains(e.target) &&
        e.target !== firstName &&
        e.target !== middleName &&
        e.target !== lastNames) {
      hideSuggestions(row);
    }
  });
}

function updateSplitSummary() {
  const splitInputs = document.querySelectorAll('.writer-split');
  let total = 0;
  splitInputs.forEach(input => {
    total += parseFloat(input.value || '0') || 0;
  });
  const rounded = total.toFixed(2);
  document.getElementById('splitTotal').textContent = rounded;

  const status = document.getElementById('splitStatus');
  if (Math.abs(total - 100) < 0.001) {
    status.className = 'ms-2 badge text-bg-success';
    status.textContent = 'Valid';
  } else {
    status.className = 'ms-2 badge text-bg-secondary';
    status.textContent = 'Incomplete';
  }
}

document.getElementById('workForm').addEventListener('submit', function(e) {
  const rows = document.querySelectorAll('.writer-row');
  if (!rows.length) {
    e.preventDefault();
    alert('Add at least one writer.');
    return;
  }

  let hasValidWriter = false;
  for (const row of rows) {
    const fullName = getFullNameFromRow(row);
    const split = parseFloat(row.querySelector('.writer-split').value || '0') || 0;
    if (fullName) {
      hasValidWriter = true;
      if (split <= 0) {
        e.preventDefault();
        alert('Each writer must have a split greater than 0.');
        return;
      }
    }
  }

  if (!hasValidWriter) {
    e.preventDefault();
    alert('Add at least one writer with a name.');
    return;
  }

  const total = parseFloat(document.getElementById('splitTotal').textContent || '0') || 0;
  if (Math.abs(total - 100) >= 0.001) {
    e.preventDefault();
    alert('Total writer split must equal 100%.');
  }
});

addWriterRow();
</script>
</body>
</html>
"""

DUPLICATE_WARNING_HTML = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Possible Duplicate</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-light">
<div class="container py-5">
  <div class="card shadow-sm">
    <div class="card-body p-4">
      <h3 class="mb-3">Possible Duplicate Work Found</h3>
      <p class="text-muted">The system found one or more existing works with the same title and writer set.</p>

      <ul class="mb-4">
        {% for item in duplicates %}
          <li>
            <strong>{{ item.title }}</strong>
            {% if item.camp_name %} — {{ item.camp_name }}{% endif %}
            — Created {{ item.created_at }}
          </li>
        {% endfor %}
      </ul>

      <form method="post">
        {% for key, value in form_data.items() %}
          {% if value is string %}
            <input type="hidden" name="{{ key }}" value="{{ value }}">
          {% else %}
            {% for item in value %}
              <input type="hidden" name="{{ key }}" value="{{ item }}">
            {% endfor %}
          {% endif %}
        {% endfor %}
        <input type="hidden" name="force_create" value="1">

        <div class="d-flex gap-2">
          <button type="submit" class="btn btn-danger">Continue Anyway</button>
          <a href="{{ url_for('formulario') }}" class="btn btn-outline-secondary">Cancel</a>
        </div>
      </form>
    </div>
  </div>
</div>
</body>
</html>
"""

WORKS_LIST_HTML = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Works</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-light">
<div class="container py-4">
  <div class="d-flex justify-content-between align-items-center mb-4">
    <h2 class="mb-0">Works</h2>
    <div class="d-flex gap-2">
      <a href="{{ url_for('formulario') }}" class="btn btn-primary">Create Work</a>
      <a href="{{ url_for('batches_list') }}" class="btn btn-outline-primary">Batches</a>
    </div>
  </div>

  <div class="card shadow-sm">
    <div class="card-body">
      <form method="get" class="mb-3">
        <div class="row">
          <div class="col-md-6">
            <input class="form-control" name="q" value="{{ q }}" placeholder="Search work title">
          </div>
          <div class="col-md-2">
            <button class="btn btn-outline-primary w-100">Search</button>
          </div>
        </div>
      </form>

      <div class="table-responsive">
        <table class="table table-striped">
          <thead>
            <tr>
              <th>Work Title</th>
              <th>Camp</th>
              <th>Batch</th>
              <th>Contract Date</th>
              <th>Writers</th>
              <th>Created</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {% for work in works %}
              <tr>
                <td>{{ work.title }}</td>
                <td>{{ work.camp.name if work.camp else '' }}</td>
                <td>
                  {% if work.batch_id %}
                    <a href="{{ url_for('batch_detail', batch_id=work.batch_id) }}">Batch {{ work.batch_id }}</a>
                  {% endif %}
                </td>
                <td>{{ work.contract_date.strftime('%Y-%m-%d') if work.contract_date else '' }}</td>
                <td>{{ work.work_writers|length }}</td>
                <td>{{ work.created_at.strftime('%Y-%m-%d') }}</td>
                <td><a href="{{ url_for('work_detail', work_id=work.id) }}" class="btn btn-sm btn-outline-secondary">View</a></td>
              </tr>
            {% endfor %}
            {% if not works %}
              <tr><td colspan="7" class="text-center text-muted">No works found.</td></tr>
            {% endif %}
          </tbody>
        </table>
      </div>
    </div>
  </div>
</div>
</body>
</html>
"""

BATCHES_LIST_HTML = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Batches</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-light">
<div class="container py-4">
  <div class="d-flex justify-content-between align-items-center mb-4">
    <h2 class="mb-0">Batches</h2>
    <a href="{{ url_for('formulario') }}" class="btn btn-primary">Create Work</a>
  </div>

  <div class="card shadow-sm">
    <div class="card-body">
      <div class="table-responsive">
        <table class="table table-striped">
          <thead>
            <tr>
              <th>Batch</th>
              <th>Camp</th>
              <th>Contract Date</th>
              <th>Status</th>
              <th>Works</th>
              <th>Created</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {% for batch in batches %}
              <tr>
                <td>Batch {{ batch.id }}</td>
                <td>{{ batch.camp.name if batch.camp else '' }}</td>
                <td>{{ batch.contract_date.strftime('%Y-%m-%d') }}</td>
                <td>{{ batch.status }}</td>
                <td>{{ batch.works|length }}</td>
                <td>{{ batch.created_at.strftime('%Y-%m-%d') }}</td>
                <td><a href="{{ url_for('batch_detail', batch_id=batch.id) }}" class="btn btn-sm btn-outline-secondary">View</a></td>
              </tr>
            {% endfor %}
            {% if not batches %}
              <tr><td colspan="7" class="text-center text-muted">No batches found.</td></tr>
            {% endif %}
          </tbody>
        </table>
      </div>
    </div>
  </div>
</div>
</body>
</html>
"""

BATCH_DETAIL_HTML = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Batch Detail</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-light">
<div class="container py-4">
  {% with messages = get_flashed_messages() %}
    {% if messages %}
      {% for message in messages %}
        <div class="alert alert-warning">{{ message }}</div>
      {% endfor %}
    {% endif %}
  {% endwith %}

  <div class="d-flex justify-content-between align-items-center mb-4">
    <h2 class="mb-0">Batch {{ batch.id }}</h2>
    <div class="d-flex gap-2">
      <a href="{{ url_for('batches_list') }}" class="btn btn-outline-secondary">Back</a>
      <a href="{{ url_for('formulario', batch_id=batch.id) }}" class="btn btn-outline-primary">
       Add Another Work to This Batch
      </a>
      <form method="post" action="{{ url_for('generate_batch_documents', batch_id=batch.id) }}" id="generateBatchForm">
        <button type="submit" class="btn btn-success" id="generateBatchButton">
          <span class="btn-label">Generate Batch Documents</span>
          <span class="spinner-border spinner-border-sm d-none" role="status" aria-hidden="true"></span>
        </button>
      </form>
    </div>
  </div>

  <div class="card shadow-sm mb-4">
    <div class="card-body">
      <h5>Batch Info</h5>
      <p class="mb-1"><strong>Camp:</strong> {{ batch.camp.name if batch.camp else '—' }}</p>
      <p class="mb-1"><strong>Contract Date:</strong> {{ batch.contract_date.strftime('%Y-%m-%d') }}</p>
      <p class="mb-1"><strong>Status:</strong> {{ batch.status }}</p>
      <p class="mb-0"><strong>Created:</strong> {{ batch.created_at.strftime('%Y-%m-%d %H:%M') }}</p>
    </div>
  </div>

  <div class="card shadow-sm mb-4">
    <div class="card-body">
      <h5>Works in Batch</h5>
      <div class="table-responsive">
        <table class="table table-striped">
          <thead>
            <tr>
              <th>Work Title</th>
              <th>Writers</th>
              <th>Created</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {% for work in works %}
              <tr>
                <td>{{ work.title }}</td>
                <td>{{ work.work_writers|length }}</td>
                <td>{{ work.created_at.strftime('%Y-%m-%d') }}</td>
                <td><a href="{{ url_for('work_detail', work_id=work.id) }}" class="btn btn-sm btn-outline-secondary">View Work</a></td>
              </tr>
            {% endfor %}
            {% if not works %}
              <tr><td colspan="4" class="text-center text-muted">No works in this batch.</td></tr>
            {% endif %}
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <div class="card shadow-sm mb-4">
    <div class="card-body">
      <h5>Writer Summary</h5>
      <div class="table-responsive">
        <table class="table table-striped">
          <thead>
            <tr>
              <th>Writer</th>
              <th>AKA</th>
              <th>IPI</th>
              <th>PRO</th>
              <th>Works in Batch</th>
              <th>Master Contract</th>
            </tr>
          </thead>
          <tbody>
            {% for item in writer_summary %}
              <tr>
                <td>{{ item.writer.full_name }}</td>
                <td>{{ item.writer.writer_aka }}</td>
                <td>{{ item.writer.ipi or '' }}</td>
                <td>{{ item.writer.pro }}</td>
                <td>{{ item.work_count }}</td>
                <td>{{ 'Yes' if item.writer.has_master_contract else 'No' }}</td>
              </tr>
            {% endfor %}
            {% if not writer_summary %}
              <tr><td colspan="6" class="text-center text-muted">No writers in this batch.</td></tr>
            {% endif %}
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <div class="card shadow-sm">
  <div class="card-body">
    <h5>Generated Documents</h5>
    <div class="table-responsive">
      <table class="table table-striped align-middle">
        <thead>
          <tr>
            <th>Writer</th>
            <th>Document Type</th>
            <th>File Name</th>
            <th>Generated</th>
            <th>DocuSign</th>
            <th>DocuSign Status</th>
            <th>Certificate</th>
            <th>Upload Signed</th>
            <th>Signed</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody id="generatedDocumentsBody">
          {% for doc in documents %}
            <tr data-doc-id="{{ doc.id }}">
              <td>{{ doc.writer_name_snapshot }}</td>
              <td>{{ doc.document_type }}</td>
              <td>{{ doc.file_name }}</td>

              <td>
                {% if doc.drive_web_view_link %}
                  <a href="{{ doc.drive_web_view_link }}" target="_blank" class="btn btn-sm btn-outline-primary">Open</a>
                {% else %}
                  —
                {% endif %}
              </td>

              <td>
                <form method="post"
                      action="{{ url_for('send_document_docusign', document_id=doc.id) }}"
                      class="docusign-action-form">
                  <button type="submit" class="btn btn-sm btn-outline-dark">
                    <span class="btn-label">
                      {% if doc.docusign_status == 'completed' %}
                        Resend
                      {% elif doc.docusign_status == 'sent' %}
                        Sent
                      {% elif doc.docusign_status == 'delivered' %}
                        Delivered
                      {% else %}
                        Send
                      {% endif %}
                    </span>
                    <span class="spinner-border spinner-border-sm d-none" role="status" aria-hidden="true"></span>
                  </button>
                </form>
              </td>

              <td>{{ doc.docusign_status or '—' }}</td>

              <td>
                {% if doc.certificate_drive_web_view_link %}
                  <a href="{{ doc.certificate_drive_web_view_link }}" target="_blank" class="btn btn-sm btn-outline-secondary">Open Certificate</a>
                {% else %}
                  —
                {% endif %}
              </td>

              <td style="min-width: 220px;">
                <form method="post" action="{{ url_for('upload_signed_document', document_id=doc.id) }}" enctype="multipart/form-data">
                  <input type="file" name="signed_file" class="form-control form-control-sm mb-1" required>
                  <button type="submit" class="btn btn-sm btn-outline-success">Upload</button>
                </form>
              </td>

              <td>
                {% if doc.signed_pdf_drive_web_view_link %}
                  <a href="{{ doc.signed_pdf_drive_web_view_link }}" target="_blank" class="btn btn-sm btn-outline-secondary">Open Signed</a>
                {% elif doc.signed_web_view_link %}
                  <a href="{{ doc.signed_web_view_link }}" target="_blank" class="btn btn-sm btn-outline-secondary">Open Signed</a>
                {% else %}
                  —
                {% endif %}
              </td>

              <td>{{ doc.status or '—' }}</td>
            </tr>
          {% endfor %}

          {% if not documents %}
            <tr>
              <td colspan="10" class="text-center text-muted">No documents generated for this batch yet.</td>
            </tr>
          {% endif %}
        </tbody>
      </table>
    </div>
  </div>
</div>
</div>
<script>
  const SEND_DOCUSIGN_URL_TEMPLATE = "{{ url_for('send_document_docusign', document_id=0) }}";
  const batchId = {{ batch.id }};
  let batchPollingInterval = null;

  function escapeHtml(value) {
    return (value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function renderDocActionCell(doc) {
    const docId = doc.id || doc.document_id;
    const actionUrl = SEND_DOCUSIGN_URL_TEMPLATE.replace("/0/send-docusign", `/${docId}/send-docusign`);

    let label = "Send";
    if (doc.docusign_status === "completed") {
      label = "Resend";
    } else if (doc.docusign_status === "delivered") {
      label = "Delivered";
    } else if (doc.docusign_status === "sent") {
      label = "Sent";
    }

    return `
      <form method="post" action="${actionUrl}" class="docusign-action-form">
        <button type="submit" class="btn btn-sm btn-outline-dark">
          <span class="btn-label">${label}</span>
          <span class="spinner-border spinner-border-sm d-none" role="status" aria-hidden="true"></span>
        </button>
      </form>
    `;
  }

  function renderGeneratedButton(doc) {
    if (doc.drive_web_view_link) {
      return `<a href="${doc.drive_web_view_link}" target="_blank" class="btn btn-sm btn-outline-primary">Open</a>`;
    }
    return "—";
  }

  function renderCertificateButton(doc) {
    if (doc.certificate_drive_web_view_link) {
      return `<a href="${doc.certificate_drive_web_view_link}" target="_blank" class="btn btn-sm btn-outline-secondary">Certificate</a>`;
    }
    return "—";
  }

  function renderSignedButton(doc) {
    if (doc.signed_pdf_drive_web_view_link) {
      return `<a href="${doc.signed_pdf_drive_web_view_link}" target="_blank" class="btn btn-sm btn-outline-success">Signed</a>`;
    }
    return "—";
  }

  function stopGenerateSpinner() {
    const button = document.getElementById("generateBatchButton");
    if (!button) return;

    const spinner = button.querySelector(".spinner-border");
    const label = button.querySelector(".btn-label");

    button.disabled = false;
    if (spinner) spinner.classList.add("d-none");
    if (label) label.textContent = "Generate Batch Documents";
  }

  function updateDocumentsTable(data) {
  const tbody = document.getElementById("generatedDocumentsBody");
  if (!tbody || !data.documents) return;

  tbody.innerHTML = data.documents.map(doc => `
    <tr data-doc-id="${doc.id}">
      <td>${escapeHtml(doc.writer_name_snapshot)}</td>
      <td>${escapeHtml(doc.document_type)}</td>
      <td>${escapeHtml(doc.file_name)}</td>

      <td>${renderGeneratedButton(doc)}</td>

      <td>${renderDocActionCell(doc)}</td>

      <td>${escapeHtml(doc.docusign_status || "—")}</td>

      <td>${renderCertificateButton(doc)}</td>

      <td style="min-width: 220px;">
        <form method="post" action="/documents/${doc.id}/upload-signed" enctype="multipart/form-data">
          <input type="file" name="signed_file" class="form-control form-control-sm mb-1" required>
          <button type="submit" class="btn btn-sm btn-outline-success">Upload</button>
        </form>
      </td>

      <td>${renderSignedButton(doc)}</td>

      <td>${escapeHtml(doc.status || "—")}</td>
    </tr>
  `).join("");

  bindActionSpinners();

  if (data.documents && data.documents.length > 0) {
    stopGenerateSpinner();
  }
}

  async function pollBatchStatus() {
    try {
      const response = await fetch(`/batches/${batchId}/status-json`, { cache: "no-store" });
      if (!response.ok) return;
      const data = await response.json();
      updateDocumentsTable(data);
    } catch (err) {
      console.error("Batch polling failed", err);
    }
  }

  function startBatchPolling() {
    if (batchPollingInterval) return;
    batchPollingInterval = setInterval(pollBatchStatus, 5000);
  }

  function bindActionSpinners() {
    document.querySelectorAll(".docusign-action-form").forEach(form => {
      if (form.dataset.bound === "1") return;
      form.dataset.bound = "1";

      form.addEventListener("submit", function(e) {
        e.preventDefault();

        const button = form.querySelector("button");
        const spinner = form.querySelector(".spinner-border");
        const label = form.querySelector(".btn-label");

        if (button) button.disabled = true;
        if (spinner) spinner.classList.remove("d-none");
        if (label) label.textContent = "Processing...";

        setTimeout(() => {
          form.submit();
        }, 150);
      });
    });
  }

  document.addEventListener("DOMContentLoaded", function() {
    bindActionSpinners();

    const generateForm = document.getElementById("generateBatchForm");
    if (generateForm) {
      generateForm.addEventListener("submit", function(e) {
        e.preventDefault();

        const button = document.getElementById("generateBatchButton");
        if (!button) return;

        const spinner = button.querySelector(".spinner-border");
        const label = button.querySelector(".btn-label");

        button.disabled = true;
        if (spinner) spinner.classList.remove("d-none");
        if (label) label.textContent = "Generating...";

        setTimeout(() => {
          generateForm.submit();
        }, 150);

        setTimeout(() => {
          window.location.reload();
        }, 4000);
      });
    }

    startBatchPolling();
  });
</script>
</body>
</html>
"""

WORK_DETAIL_HTML = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Work Detail</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-light">
<div class="container py-4">
  <div class="d-flex justify-content-between align-items-center mb-4">
    <h2 class="mb-0">{{ work.title }}</h2>
    <div class="d-flex gap-2">
      {% if work.batch_id %}
        <a href="{{ url_for('batch_detail', batch_id=work.batch_id) }}" class="btn btn-outline-primary">View Batch</a>
      {% endif %}
      <a href="{{ url_for('works_list') }}" class="btn btn-outline-secondary">Back</a>
    </div>
  </div>

  <div class="card shadow-sm mb-4">
    <div class="card-body">
      <h5>Work Info</h5>
      <p class="mb-1"><strong>Camp:</strong> {{ work.camp.name if work.camp else '—' }}</p>
      <p class="mb-1"><strong>Batch:</strong>
        {% if work.batch_id %}
          <a href="{{ url_for('batch_detail', batch_id=work.batch_id) }}">Batch {{ work.batch_id }}</a>
        {% else %}
          —
        {% endif %}
      </p>
      <p class="mb-1"><strong>Contract Date:</strong> {{ work.contract_date.strftime('%Y-%m-%d') if work.contract_date else '—' }}</p>
      <p class="mb-0"><strong>Created:</strong> {{ work.created_at.strftime('%Y-%m-%d %H:%M') }}</p>
    </div>
  </div>

  <div class="card shadow-sm mb-4">
    <div class="card-body">
      <h5>Writers & Splits</h5>
      <div class="table-responsive">
        <table class="table table-striped">
          <thead>
            <tr>
              <th>Writer</th>
              <th>AKA</th>
              <th>IPI</th>
              <th>PRO</th>
              <th>Split %</th>
              <th>Publisher</th>
              <th>Publisher IPI</th>
              <th>Master Contract</th>
            </tr>
          </thead>
          <tbody>
            {% for ww in work.work_writers %}
              <tr>
                <td>{{ ww.writer.full_name }}</td>
                <td>{{ ww.writer.writer_aka }}</td>
                <td>{{ ww.writer.ipi or '' }}</td>
                <td>{{ ww.writer.pro }}</td>
                <td>{{ "%.2f"|format(ww.writer_percentage) }}</td>
                <td>{{ ww.publisher }}</td>
                <td>{{ ww.publisher_ipi }}</td>
                <td>{{ 'Yes' if ww.writer.has_master_contract else 'No' }}</td>
              </tr>
            {% endfor %}
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <div class="card shadow-sm">
    <div class="card-body">
      <h5>Generated Documents</h5>

      <div class="table-responsive">
        <table class="table table-striped">
          <thead>
            <tr>
              <th>Writer</th>
              <th>Type</th>
              <th>File</th>
              <th>Generated At</th>
              <th>Generated Doc</th>
              <th>DocuSign</th>
              <th>DocuSign Status</th>
              <th>Certificate</th>
              <th>Signed</th>
              <th>Status</th>
            </tr>
          </thead>

          <tbody id="generatedDocumentsBody">
            {% for doc in documents %}
              <tr data-doc-id="{{ doc.id }}">
                <td>{{ doc.writer_name_snapshot }}</td>
                <td>{{ doc.document_type }}</td>
                <td>{{ doc.file_name }}</td>

                <td>
                  {% if doc.drive_web_view_link %}
                    <a href="{{ doc.drive_web_view_link }}" target="_blank" class="btn btn-sm btn-outline-primary">Open</a>
                  {% else %}
                    —
                  {% endif %}
                </td>

                <td>
                  <form method="post"
                        action="{{ url_for('send_document_docusign', document_id=doc.id) }}"
                        class="docusign-action-form">
                    <button type="submit" class="btn btn-sm btn-outline-dark">
                      <span class="btn-label">
                        {% if doc.docusign_status == 'completed' %}
                          Resend
                        {% elif doc.docusign_status == 'delivered' %}
                          Delivered
                        {% elif doc.docusign_status == 'sent' %}
                          Sent
                        {% else %}
                          Send
                        {% endif %}
                      </span>
                      <span class="spinner-border spinner-border-sm d-none" role="status" aria-hidden="true"></span>
                    </button>
                  </form>
                </td>

                <td>{{ doc.docusign_status or '—' }}</td>

                <td>
                  {% if doc.certificate_drive_web_view_link %}
                    <a href="{{ doc.certificate_drive_web_view_link }}" target="_blank" class="btn btn-sm btn-outline-secondary">
                      Certificate
                    </a>
                  {% else %}
                    —
                  {% endif %}
                </td>

                <td style="min-width: 220px;">
                  <form method="post" action="{{ url_for('upload_signed_document', document_id=doc.id) }}" enctype="multipart/form-data">
                    <input type="file" name="signed_file" class="form-control form-control-sm mb-1" required>
                    <button type="submit" class="btn btn-sm btn-outline-success">Upload</button>
                  </form>
                </td>

                <td>
                  {% if doc.signed_pdf_drive_web_view_link %}
                    <a href="{{ doc.signed_pdf_drive_web_view_link }}" target="_blank" class="btn btn-sm btn-outline-success">
                      Open Signed
                    </a>
                  {% elif doc.signed_web_view_link %}
                    <a href="{{ doc.signed_web_view_link }}" target="_blank" class="btn btn-sm btn-outline-success">
                      Open Signed
                    </a>
                  {% else %}
                    —
                  {% endif %}
                </td>

                <td>{{ doc.status or '—' }}</td>
              </tr>
            {% endfor %}

            {% if not documents %}
              <tr>
                <td colspan="10" class="text-center text-muted">No documents generated for this batch yet.</td>
              </tr>
            {% endif %}
          </tbody>
        </table>
      </div>
      
    </div>
  </div>
</div>
</body>
</html>
"""


def auth_required():
    if not (TEAM_USERNAME and TEAM_PASSWORD):
        return False
    return not session.get("logged_in")


def get_or_create_camp(existing_camp_id: str, new_camp_name: str):
    new_camp_name = (new_camp_name or "").strip()
    if new_camp_name:
        existing = Camp.query.filter(func.lower(Camp.name) == new_camp_name.lower()).first()
        if existing:
            return existing
        camp = Camp(name=new_camp_name)
        db.session.add(camp)
        db.session.flush()
        return camp

    if existing_camp_id:
        return Camp.query.get(int(existing_camp_id))

    return None


def find_existing_writer(selected_writer_id: str):
    if selected_writer_id:
        writer = Writer.query.get(int(selected_writer_id))
        if writer:
            return writer
    return None


def render_docx_template(template_path: str, data: dict, works_for_table=None) -> io.BytesIO:
    if not os.path.exists(template_path):
        raise FileNotFoundError(template_path)

    doc = Document(template_path)

    def replace_all(paragraph):
        text = "".join(run.text for run in paragraph.runs)
        for k, v in data.items():
            text = text.replace(f"[[{k}]]", str(v))
        for run in paragraph.runs:
            run.text = ""
        if paragraph.runs:
            paragraph.runs[0].text = text

    for p in doc.paragraphs:
        replace_all(p)

    for table in doc.tables:
        for row in table.rows:
            for cell in row.cells:
                for p in cell.paragraphs:
                    replace_all(p)

    for p in doc.paragraphs:
        if "[[ContractTable]]" in p.text:
            table = doc.add_table(rows=1, cols=5)
            table.style = "Table Grid"
            table.rows[0].cells[0].text = "Work Title"
            table.rows[0].cells[1].text = "Songwriter Name"
            table.rows[0].cells[2].text = "Songwriter Share"
            table.rows[0].cells[3].text = "Publisher Name"
            table.rows[0].cells[4].text = "Publisher Share"

            for item in works_for_table or []:
                row = table.add_row().cells
                row[0].text = item.get("work_title", "")
                row[1].text = item.get("writer_name", "")
                row[2].text = item.get("writer_percentage", "")
                row[3].text = item.get("publisher", "")
                row[4].text = item.get("writer_percentage", "")
            p.text = ""
            p._element.addnext(table._element)
            break

    buffer = io.BytesIO()
    doc.save(buffer)
    buffer.seek(0)
    return buffer


def collect_form_context():
    selected_batch_id = request.form.get("existing_batch_id") or ""

    return {
        "camps": Camp.query.order_by(Camp.name.asc()).all(),
        "batches": GenerationBatch.query.order_by(GenerationBatch.created_at.desc()).all(),
        "default_publisher_address": DEFAULT_PUBLISHER_ADDRESS,
        "default_publisher_city": DEFAULT_PUBLISHER_CITY,
        "default_publisher_state": DEFAULT_PUBLISHER_STATE,
        "default_publisher_zip": DEFAULT_PUBLISHER_ZIP,
        "force_create": request.form.get("force_create", ""),
        "selected_batch_id": selected_batch_id,
    }
    


def get_batch_writer_summary(batch_id: int):
    work_writers = (
        WorkWriter.query
        .join(Work)
        .filter(Work.batch_id == batch_id)
        .all()
    )

    grouped = {}
    for ww in work_writers:
        if ww.writer_id not in grouped:
            grouped[ww.writer_id] = {
                "writer": ww.writer,
                "work_titles": set(),
            }
        grouped[ww.writer_id]["work_titles"].add(ww.work.title)

    summary = []
    for item in grouped.values():
        summary.append({
            "writer": item["writer"],
            "work_count": len(item["work_titles"]),
        })

    summary.sort(key=lambda x: x["writer"].full_name.lower())
    return summary


@app.route("/login", methods=["GET", "POST"])
def login():
    if not (TEAM_USERNAME and TEAM_PASSWORD):
        return redirect(url_for("formulario"))

    if request.method == "POST":
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        if username == TEAM_USERNAME and password == TEAM_PASSWORD:
            session["logged_in"] = True
            return redirect(url_for("formulario"))
        flash("Incorrect username or password.")

    return render_template_string(LOGIN_HTML)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/", methods=["GET", "POST"])
def formulario():
    if auth_required():
        return redirect(url_for("login"))

    if request.method == "POST":
        work_title = (request.form.get("work_title") or "").strip()
        contract_date_str = (request.form.get("contract_date") or "").strip()

        if not work_title:
            flash("Work title is required.")
            return render_template_string(FORM_HTML, **collect_form_context())

        if not contract_date_str:
            flash("Contract date is required.")
            return render_template_string(FORM_HTML, **collect_form_context())

        try:
            contract_date = datetime.datetime.strptime(contract_date_str, "%Y-%m-%d").date()
        except ValueError:
            flash("Please enter a valid contract date.")
            return render_template_string(FORM_HTML, **collect_form_context())

        writer_ids = request.form.getlist("writer_id")
        first_names = request.form.getlist("writer_first_name")
        middle_names = request.form.getlist("writer_middle_name")
        last_names_list = request.form.getlist("writer_last_names")
        writer_akas = request.form.getlist("writer_aka")
        ipis = request.form.getlist("writer_ipi")
        emails = request.form.getlist("writer_email")
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

        for idx in range(len(first_names)):
            first_name = (first_names[idx] or "").strip()
            middle_name = (middle_names[idx] or "").strip()
            last_names = (last_names_list[idx] or "").strip()
            full_name = build_full_name(first_name, middle_name, last_names)

            if not full_name:
                continue

            split_value = parse_float(percentages[idx] if idx < len(percentages) else "0")
            if split_value <= 0:
                flash(f"Writer '{full_name}' must have a split greater than 0.")
                return render_template_string(FORM_HTML, **collect_form_context())

            total_split += split_value

            writer_rows.append({
                "selected_writer_id": writer_ids[idx] if idx < len(writer_ids) else "",
                "first_name": first_name,
                "middle_name": middle_name,
                "last_names": last_names,
                "full_name": full_name,
                "writer_aka": (writer_akas[idx] or "").strip(),
                "ipi": (ipis[idx] or "").strip(),
                "email": (emails[idx] or "").strip(),
                "pro": (pros[idx] or "").strip(),
                "writer_percentage": split_value,
                "publisher": (publishers[idx] or "").strip(),
                "publisher_ipi": (publisher_ipis[idx] or "").strip(),
                "publisher_address": (publisher_addresses[idx] or DEFAULT_PUBLISHER_ADDRESS).strip(),
                "publisher_city": (publisher_cities[idx] or DEFAULT_PUBLISHER_CITY).strip(),
                "publisher_state": (publisher_states[idx] or DEFAULT_PUBLISHER_STATE).strip(),
                "publisher_zip_code": (publisher_zips[idx] or DEFAULT_PUBLISHER_ZIP).strip(),
                "address": (addresses[idx] or "").strip(),
                "city": (cities[idx] or "").strip(),
                "state": (states[idx] or "").strip(),
                "zip_code": (zip_codes[idx] or "").strip(),
            })

        if not writer_rows:
            flash("Add at least one writer.")
            return render_template_string(FORM_HTML, **collect_form_context())

        if abs(total_split - 100.0) >= 0.001:
            flash(f"Total writer split must equal 100%. Current total: {total_split:.2f}%")
            return render_template_string(FORM_HTML, **collect_form_context())

        seen_writer_ids = set()
        seen_ipis = set()
        seen_names = set()

        for row in writer_rows:
            selected_writer_id = (row["selected_writer_id"] or "").strip()
            ipi = (row["ipi"] or "").strip()
            normalized_name = normalize_text(row["full_name"])

            if selected_writer_id:
                if selected_writer_id in seen_writer_ids:
                    flash(f"Duplicate writer selected in this work: {row['full_name']}")
                    return render_template_string(FORM_HTML, **collect_form_context())
                seen_writer_ids.add(selected_writer_id)

            if ipi:
                ipi_key = ipi.lower()
                if ipi_key in seen_ipis:
                    flash(f"Duplicate IPI in this work: {ipi}")
                    return render_template_string(FORM_HTML, **collect_form_context())
                seen_ipis.add(ipi_key)
            else:
                if normalized_name in seen_names:
                    flash(f"Duplicate writer name in this work: {row['full_name']}")
                    return render_template_string(FORM_HTML, **collect_form_context())
                seen_names.add(normalized_name)

        warnings = []

        for row in writer_rows:
            if row["ipi"]:
                existing_ipi_writer = Writer.query.filter(func.lower(Writer.ipi) == row["ipi"].lower()).first()
                if existing_ipi_writer:
                    selected_id = (row["selected_writer_id"] or "").strip()
                    if not selected_id or str(existing_ipi_writer.id) != selected_id:
                        flash(f"IPI {row['ipi']} already belongs to {existing_ipi_writer.full_name}. Please select the existing writer.")
                        return render_template_string(FORM_HTML, **collect_form_context())

        for row in writer_rows:
            if not row["ipi"]:
                existing_name_writer = Writer.query.filter(
                    func.lower(Writer.full_name) == normalize_text(row["full_name"])
                ).first()
                if existing_name_writer:
                    warnings.append(f"Writer '{row['full_name']}' already exists in the system without using an IPI match.")

        normalized_title = normalize_title(work_title)
        writer_identity_set = sorted([build_writer_identity_from_row(row) for row in writer_rows])

        possible_duplicates = []
        existing_works = Work.query.filter_by(normalized_title=normalized_title).all()
        for existing_work in existing_works:
            existing_identities = sorted([
                build_writer_identity_from_workwriter(ww) for ww in existing_work.work_writers
            ])
            if existing_identities == writer_identity_set:
                possible_duplicates.append({
                    "title": existing_work.title,
                    "camp_name": existing_work.camp.name if existing_work.camp else "",
                    "created_at": existing_work.created_at.strftime("%Y-%m-%d"),
                })

        if possible_duplicates and not request.form.get("force_create"):
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
        
        if existing_batch_id:
            batch = GenerationBatch.query.get(int(existing_batch_id))
            if not batch:
                flash("Selected batch was not found.")
                return render_template_string(FORM_HTML, **collect_form_context())

            camp = batch.camp
            contract_date = batch.contract_date
        else:
            camp = get_or_create_camp(request.form.get("camp_id"), request.form.get("new_camp_name"))

            batch = GenerationBatch(
                camp_id=camp.id if camp else None,
                contract_date=contract_date,
                created_by="",
                status="draft",
            )
            db.session.add(batch)
            db.session.flush()

        work = Work(
            title=work_title,
            normalized_title=normalized_title,
            camp_id=camp.id if camp else None,
            batch_id=batch.id,
            contract_date=contract_date,
        )
        db.session.add(work)
        db.session.flush()

        for row in writer_rows:
            writer = find_existing_writer(row["selected_writer_id"])

            if writer:
                if not writer.ipi and row["ipi"]:
                    writer.ipi = row["ipi"] or None
                if not writer.email and row["email"]:
                    writer.email = row["email"]
                if not writer.pro and row["pro"]:
                    writer.pro = row["pro"]
                if not writer.address and row["address"]:
                    writer.address = row["address"]
                if not writer.city and row["city"]:
                    writer.city = row["city"]
                if not writer.state and row["state"]:
                    writer.state = row["state"]
                if not writer.zip_code and row["zip_code"]:
                    writer.zip_code = row["zip_code"]
                if not writer.writer_aka and row["writer_aka"]:
                    writer.writer_aka = row["writer_aka"]
            else:
                writer = Writer(
                    first_name=row["first_name"],
                    middle_name=row["middle_name"],
                    last_names=row["last_names"],
                    full_name=row["full_name"],
                    writer_aka=row["writer_aka"],
                    ipi=row["ipi"] or None,
                    email=row["email"],
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
        return redirect(url_for("batch_detail", batch_id=batch.id))

    return render_template_string(FORM_HTML, **collect_form_context())


@app.route("/writers/search")
def search_writers():
    if auth_required():
        return jsonify([])

    q = (request.args.get("q") or "").strip()
    if len(q) < 2:
        return jsonify([])

    like_q = f"%{q.lower()}%"

    writers = (
        Writer.query
        .filter(
            or_(
                func.lower(Writer.full_name).like(like_q),
                func.lower(Writer.first_name).like(like_q),
                func.lower(Writer.middle_name).like(like_q),
                func.lower(Writer.last_names).like(like_q),
                func.lower(Writer.writer_aka).like(like_q),
                func.lower(Writer.ipi).like(like_q),
            )
        )
        .order_by(Writer.full_name.asc())
        .limit(8)
        .all()
    )

    return jsonify([
        {
            "id": writer.id,
            "first_name": writer.first_name,
            "middle_name": writer.middle_name,
            "last_names": writer.last_names,
            "full_name": writer.full_name,
            "writer_aka": writer.writer_aka,
            "ipi": writer.ipi or "",
            "email": writer.email or "",
            "pro": writer.pro,
            "address": writer.address,
            "city": writer.city,
            "state": writer.state,
            "zip_code": writer.zip_code,
            "has_master_contract": writer.has_master_contract,
            "default_publisher": default_publisher_for_pro(writer.pro),
            "default_publisher_ipi": default_publisher_ipi_for_pro(writer.pro),
        }
        for writer in writers
    ])


@app.route("/works")
def works_list():
    if auth_required():
        return redirect(url_for("login"))

    q = (request.args.get("q") or "").strip()
    query = Work.query
    if q:
        query = query.filter(func.lower(Work.title).like(f"%{q.lower()}%"))
    works = query.order_by(Work.created_at.desc()).all()
    return render_template_string(WORKS_LIST_HTML, works=works, q=q)


@app.route("/batches")
def batches_list():
    if auth_required():
        return redirect(url_for("login"))

    batches = GenerationBatch.query.order_by(GenerationBatch.created_at.desc()).all()
    return render_template_string(BATCHES_LIST_HTML, batches=batches)


@app.route("/batches/<int:batch_id>")
def batch_detail(batch_id):
    if auth_required():
        return redirect(url_for("login"))

    batch = GenerationBatch.query.get_or_404(batch_id)

    works = (
        Work.query
        .filter_by(batch_id=batch.id)
        .order_by(Work.created_at.asc())
        .all()
    )

    documents = (
        ContractDocument.query
        .filter_by(batch_id=batch.id)
        .order_by(ContractDocument.generated_at.desc())
        .all()
    )

    writer_summary = get_batch_writer_summary(batch.id)

    return render_template_string(
        BATCH_DETAIL_HTML,
        batch=batch,
        works=works,
        documents=documents,
        writer_summary=writer_summary,
    )

@app.route("/batches/<int:batch_id>/status-json")
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
                "generated_at": doc.generated_at.strftime('%Y-%m-%d %H:%M') if doc.generated_at else "",
                "drive_web_view_link": doc.drive_web_view_link,
                "docusign_status": doc.docusign_status,
                "status": doc.status,
                "signed_pdf_drive_web_view_link": getattr(doc, "signed_pdf_drive_web_view_link", None),
                "certificate_drive_web_view_link": getattr(doc, "certificate_drive_web_view_link", None),
            }
            for doc in documents
        ]
    })


@app.route("/batches/<int:batch_id>/generate", methods=["POST"])
def generate_batch_documents(batch_id):
    if auth_required():
        return redirect(url_for("login"))

    batch = GenerationBatch.query.get_or_404(batch_id)

    work_writers = (
        WorkWriter.query
        .join(Work)
        .filter(Work.batch_id == batch.id)
        .order_by(Work.id.asc(), WorkWriter.id.asc())
        .all()
    )

    if not work_writers:
        flash("No works found in this batch.")
        return redirect(url_for("batch_detail", batch_id=batch.id))

    grouped = {}
    for ww in work_writers:
        if ww.writer_id not in grouped:
            grouped[ww.writer_id] = {
                "writer": ww.writer,
                "rows": []
            }
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
                    "writer_percentage": f"{ww.writer_percentage:.2f}%",
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
                "Date": f"{contract_date.strftime('%B')} {day}{suffix}, {contract_date.year}",
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

            batch_label = batch.camp.name if batch.camp else f"batch_{batch.id}"
            file_name = f"{prefix}_{slugify(writer.full_name)}_{slugify(batch_label)}_{batch.contract_date.isoformat()}.docx"

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
                   app.logger.warning("DRIVE SUCCESS: %s", drive_info)
                except Exception as e:
                    import traceback
                    app.logger.error("DRIVE FAILURE: %s", e)
                    traceback.print_exc()
                    flash(f"Drive upload failed for {file_name}: {e}")
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
    zip_name = f"batch_{batch.id}_documents.zip"
    return send_file(
        zip_buffer,
        as_attachment=True,
        download_name=zip_name,
        mimetype="application/zip",
    )


@app.route("/docusign/webhook", methods=["POST"])
def docusign_webhook():
    raw_data = request.data
    app.logger.warning(f"RAW BODY: {raw_data}")

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

        app.logger.warning(f"WEBHOOK ENVELOPE ID: {envelope_id}")
        app.logger.warning(f"WEBHOOK STATUS: {raw_status}")

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
        app.logger.error("DocuSign webhook failed: %s", e)

    return "ok", 200
    

@app.route("/documents/<int:document_id>/send-docusign", methods=["POST"])
def send_document_docusign(document_id):
    document = None

    try:
        if auth_required():
            return redirect(url_for("login"))

        document = ContractDocument.query.get(document_id)
        if not document:
            flash("This document no longer exists. Please refresh the batch page.")
            return redirect(request.referrer or url_for("batches_list"))

        writer = Writer.query.get(document.writer_id)

        app.logger.warning(f"DOCUSIGN SEND START document_id={document_id}")
        app.logger.warning(f"DOCUSIGN SEND writer_id={document.writer_id if document else None}")
        app.logger.warning(f"DOCUSIGN SEND writer_email={getattr(writer, 'email', None)}")
        app.logger.warning(f"DOCUSIGN SEND drive_file_id={getattr(document, 'drive_file_id', None)}")

        if not writer:
            flash("Writer not found.")
            return redirect(url_for("batch_detail", batch_id=document.batch_id))

        if not getattr(writer, "email", None):
            flash("Writer email is required before sending to DocuSign.")
            return redirect(url_for("batch_detail", batch_id=document.batch_id))

        if not document.drive_file_id:
            flash("Generated document file is missing.")
            return redirect(url_for("batch_detail", batch_id=document.batch_id))

        if not DOCUSIGN_ACCOUNT_ID or not DOCUSIGN_INTEGRATION_KEY or not DOCUSIGN_USER_ID or not DOCUSIGN_PRIVATE_KEY:
            flash("DocuSign environment variables are not fully configured.")
            return redirect(url_for("batch_detail", batch_id=document.batch_id))

        service = get_drive_service()
        file_bytes = service.files().get_media(
            fileId=document.drive_file_id,
            supportsAllDrives=True
        ).execute()

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

        webhook_url = request.url_root.rstrip("/") + url_for("docusign_webhook")

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
            email_subject=f"Please sign: {document.file_name}",
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
        return redirect(url_for("batch_detail", batch_id=document.batch_id))

    except Exception as e:
        db.session.rollback()
        app.logger.error(f"DOCUSIGN SEND ERROR: {e}")
        app.logger.error(traceback.format_exc())
        flash(f"DocuSign send failed: {e}")

        if document:
            return redirect(url_for("batch_detail", batch_id=document.batch_id))
        return redirect(request.referrer or url_for("batches_list"))
        
@app.route("/documents/<int:document_id>/upload-signed", methods=["POST"])
def upload_signed_document(document_id):
    if auth_required():
        return redirect(url_for("login"))

    document = ContractDocument.query.get_or_404(document_id)
    uploaded_file = request.files.get("signed_file")

    if not uploaded_file or not uploaded_file.filename:
        flash("Please choose a signed file to upload.")
        return redirect(url_for("batch_detail", batch_id=document.batch_id))

    if not GOOGLE_DRIVE_FOLDER_ID or not GOOGLE_SERVICE_ACCOUNT_JSON:
        flash("Google Drive is not configured yet.")
        return redirect(url_for("batch_detail", batch_id=document.batch_id))

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
        flash(f"Signed upload failed: {e}")
        return redirect(url_for("batch_detail", batch_id=document.batch_id))

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
    return redirect(url_for("batch_detail", batch_id=document.batch_id))


@app.route("/works/<int:work_id>")
def work_detail(work_id: int):
    if auth_required():
        return redirect(url_for("login"))

    work = Work.query.get_or_404(work_id)
    documents = (
        ContractDocument.query
        .filter_by(work_id=work.id)
        .order_by(ContractDocument.generated_at.desc())
        .all()
    )
    return render_template_string(WORK_DETAIL_HTML, work=work, documents=documents)


try:
    init_db()
except Exception as e:
    print("DB INIT ERROR:", e)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.getenv("PORT", "5052")))
