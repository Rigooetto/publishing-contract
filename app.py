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
    status = db.Column(db.String(50), default="draft")
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


# ══════════════════════════════════════════════════════
#  REDESIGNED HTML TEMPLATES — LabelMind Dark UI
# ══════════════════════════════════════════════════════

_STYLE = """
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;0,9..40,700;1,9..40,400&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg0:#070b12;--bg1:#0c1120;--bg2:#101724;--bg3:#0a0e19;--bg4:#161f32;--bg5:#1c2740;
  --b0:rgba(255,255,255,.07);--b1:rgba(255,255,255,.04);--bf:rgba(99,133,255,.5);
  --a:#6385ff;--ae:#a55bff;--ag:#34d399;--ar:#ff4f6a;--am:#f59e0b;--ac:#22d3ee;
  --t1:#edf0f8;--t2:#8a96b0;--t3:#4a5470;
  --rs:7px;--rm:11px;--rl:15px;
  --sb:220px;--tb:54px;
  --sh:0 4px 28px rgba(0,0,0,.45);
  --f:'DM Sans',system-ui,sans-serif;
  --fm:'DM Mono','Fira Mono',monospace;
}
html,body{height:100%;background:var(--bg0);color:var(--t1);font-family:var(--f);font-size:14px;line-height:1.55;-webkit-font-smoothing:antialiased}
.app{display:flex;min-height:100vh}
.main{margin-left:var(--sb);flex:1;min-height:100vh}
.page{max-width:1140px;margin:0 auto;padding:28px 30px 100px}
.sb{width:var(--sb);min-height:100vh;background:var(--bg1);border-right:1px solid var(--b0);display:flex;flex-direction:column;position:fixed;left:0;top:0;z-index:50}
.sb-logo{display:flex;align-items:center;gap:10px;padding:19px 17px 15px;border-bottom:1px solid var(--b0);margin-bottom:5px;text-decoration:none}
.sb-ico{width:28px;height:28px;background:linear-gradient(135deg,var(--a),var(--ae));border-radius:7px;display:flex;align-items:center;justify-content:center;font-size:13px;flex-shrink:0}
.sb-name{font-size:14px;font-weight:700;color:var(--t1);letter-spacing:-.02em}
.sb-sec{font-size:9.5px;font-weight:700;letter-spacing:.11em;text-transform:uppercase;color:var(--t3);padding:13px 17px 4px}
.sb-nav a{display:flex;align-items:center;gap:9px;padding:8px 17px;color:var(--t2);text-decoration:none;font-size:13px;font-weight:500;transition:color .14s,background .14s;position:relative}
.sb-nav a:hover{color:var(--t1);background:rgba(255,255,255,.03)}
.sb-nav a.on{color:var(--a);background:rgba(99,133,255,.08)}
.sb-nav a.on::before{content:'';position:absolute;left:0;top:6px;bottom:6px;width:2px;background:var(--a);border-radius:0 2px 2px 0}
.sb-nav .ni{font-size:13px;flex-shrink:0;opacity:.85}
.sb-foot{margin-top:auto;padding:13px 17px;border-top:1px solid var(--b0);font-size:11px;color:var(--t3)}
.sb-foot b{color:var(--t2);font-size:11.5px;display:block;margin-bottom:2px}
.topbar{position:sticky;top:0;z-index:40;background:rgba(7,11,18,.9);backdrop-filter:blur(16px);-webkit-backdrop-filter:blur(16px);border-bottom:1px solid var(--b0);height:var(--tb);display:flex;align-items:center;padding:0 26px;gap:12px}
.tb-search{display:flex;align-items:center;gap:8px;background:var(--bg3);border:1px solid var(--b0);border-radius:var(--rs);padding:6px 11px;width:220px;color:var(--t3);font-size:12px}
.tb-kbd{margin-left:auto;font-size:10px;font-family:var(--fm);opacity:.45}
.tb-right{display:flex;align-items:center;gap:7px;margin-left:auto}
.pill-group{display:flex;gap:3px}
.pill{padding:5px 12px;border-radius:var(--rs);font-size:12.5px;font-weight:500;text-decoration:none;color:var(--t2);border:1px solid transparent;transition:all .14s}
.pill:hover{color:var(--t1);background:var(--bg4)}
.pill.on{color:var(--t1);background:var(--bg4);border-color:var(--b0)}
.tb-ibtn{width:31px;height:31px;display:flex;align-items:center;justify-content:center;background:var(--bg4);border:1px solid var(--b0);border-radius:var(--rs);color:var(--t2);cursor:pointer;text-decoration:none;transition:all .14s;font-size:13px}
.tb-ibtn:hover{border-color:var(--bf);color:var(--t1)}
.avatar{width:29px;height:29px;border-radius:50%;background:linear-gradient(135deg,var(--a),var(--ae));display:flex;align-items:center;justify-content:center;font-size:10.5px;font-weight:700;color:#fff;flex-shrink:0}
.ph{display:flex;align-items:flex-start;justify-content:space-between;gap:14px;margin-bottom:22px}
.ph-left{display:flex;align-items:center;gap:12px}
.ph-icon{width:36px;height:36px;background:linear-gradient(135deg,rgba(99,133,255,.16),rgba(165,91,255,.16));border:1px solid rgba(99,133,255,.2);border-radius:var(--rm);display:flex;align-items:center;justify-content:center;font-size:16px;flex-shrink:0}
.ph-title{font-size:19px;font-weight:700;letter-spacing:-.03em;line-height:1.2}
.ph-sub{font-size:12px;color:var(--t2);margin-top:2px}
.ph-actions{display:flex;gap:7px;align-items:center;flex-shrink:0}
.flash-list{margin-bottom:14px}
.flash-item{padding:10px 14px;border-radius:var(--rs);font-size:12.5px;margin-bottom:6px;background:rgba(245,158,11,.1);border:1px solid rgba(245,158,11,.22);color:var(--am)}
.card{background:var(--bg2);border:1px solid var(--b0);border-radius:var(--rl);margin-bottom:12px;box-shadow:var(--sh);overflow:hidden}
.card-hd{display:flex;align-items:center;gap:9px;padding:13px 17px;border-bottom:1px solid var(--b0)}
.card-ico{width:25px;height:25px;background:rgba(99,133,255,.1);border:1px solid rgba(99,133,255,.14);border-radius:6px;display:flex;align-items:center;justify-content:center;font-size:11px;flex-shrink:0}
.card-title{font-size:13px;font-weight:600}
.card-actions{margin-left:auto;display:flex;gap:6px}
.card-body{padding:17px}
.g{display:grid;gap:12px}
.g2{grid-template-columns:1fr 1fr}
.g3{grid-template-columns:1fr 1fr 1fr}
.g4{grid-template-columns:1fr 1fr 1fr 1fr}
.g5{grid-template-columns:1fr 1.5fr .75fr .75fr 1.5fr}
.g52{grid-template-columns:1fr 2fr 1fr .55fr .55fr}
.g4a{grid-template-columns:2fr 1fr .55fr .55fr}
.field{display:flex;flex-direction:column;gap:5px}
.label{font-size:10px;font-weight:700;letter-spacing:.07em;text-transform:uppercase;color:var(--t2)}
.inp{background:var(--bg3);border:1px solid var(--b0);border-radius:var(--rs);color:var(--t1);font-family:var(--f);font-size:13px;padding:8px 11px;width:100%;outline:none;transition:border-color .14s,box-shadow .14s;-webkit-appearance:none;appearance:none}
.inp::placeholder{color:var(--t3)}
.inp:focus{border-color:var(--bf);box-shadow:0 0 0 3px rgba(99,133,255,.1)}
select.inp{background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='11' height='11' viewBox='0 0 24 24' fill='none' stroke='%234a5470' stroke-width='2.5'%3E%3Cpolyline points='6 9 12 15 18 9'/%3E%3C/svg%3E");background-repeat:no-repeat;background-position:right 9px center;padding-right:28px;cursor:pointer}
select.inp option{background:var(--bg2);color:var(--t1)}
.inp-wrap{position:relative}
.inp-ico{position:absolute;left:9px;top:50%;transform:translateY(-50%);font-size:12px;color:var(--t3);pointer-events:none}
.inp-wrap .inp{padding-left:28px}
.btn{display:inline-flex;align-items:center;gap:6px;padding:8px 15px;border-radius:var(--rs);font-family:var(--f);font-size:13px;font-weight:600;cursor:pointer;border:1px solid transparent;text-decoration:none;transition:all .15s;white-space:nowrap;line-height:1}
.btn-primary{background:linear-gradient(135deg,var(--a),var(--ae));color:#fff;border:none;box-shadow:0 2px 14px rgba(99,133,255,.28)}
.btn-primary:hover{transform:translateY(-1px);box-shadow:0 5px 22px rgba(99,133,255,.42)}
.btn-primary:active{transform:translateY(0)}
.btn-sec{background:transparent;color:var(--t2);border-color:var(--b0)}
.btn-sec:hover{color:var(--t1);border-color:rgba(255,255,255,.14);background:var(--bg4)}
.btn-danger{background:rgba(255,79,106,.1);color:var(--ar);border-color:rgba(255,79,106,.2)}
.btn-danger:hover{background:rgba(255,79,106,.18);border-color:rgba(255,79,106,.38)}
.btn-success{background:rgba(52,211,153,.1);color:var(--ag);border-color:rgba(52,211,153,.22)}
.btn-success:hover{background:rgba(52,211,153,.18);border-color:rgba(52,211,153,.4)}
.btn-cyan{background:rgba(34,211,238,.1);color:var(--ac);border-color:rgba(34,211,238,.22)}
.btn-cyan:hover{background:rgba(34,211,238,.18);border-color:rgba(34,211,238,.4)}
.btn-sm{padding:5px 10px;font-size:11.5px}
.btn-xs{padding:3px 7px;font-size:10.5px}
.tag{display:inline-flex;align-items:center;padding:2px 7px;border-radius:99px;font-size:10px;font-weight:700;white-space:nowrap}
.tag-new{background:rgba(245,158,11,.12);color:var(--am);border:1px solid rgba(245,158,11,.2)}
.tag-exist{background:rgba(52,211,153,.12);color:var(--ag);border:1px solid rgba(52,211,153,.2)}
.tag-full{background:rgba(99,133,255,.12);color:var(--a);border:1px solid rgba(99,133,255,.2)}
.tag-s1{background:rgba(52,211,153,.12);color:var(--ag);border:1px solid rgba(52,211,153,.2)}
.status{display:inline-flex;align-items:center;gap:4px;padding:3px 8px;border-radius:99px;font-size:10.5px;font-weight:700}
.status-dot{width:5px;height:5px;border-radius:50%;background:currentColor;flex-shrink:0}
.s-draft{background:rgba(255,255,255,.06);color:var(--t2);border:1px solid var(--b0)}
.s-generated{background:rgba(99,133,255,.12);color:var(--a);border:1px solid rgba(99,133,255,.2)}
.s-sent{background:rgba(34,211,238,.12);color:var(--ac);border:1px solid rgba(34,211,238,.2)}
.s-delivered{background:rgba(245,158,11,.12);color:var(--am);border:1px solid rgba(245,158,11,.2)}
.s-completed,.s-signed,.s-signed_uploaded,.s-signed_complete{background:rgba(52,211,153,.12);color:var(--ag);border:1px solid rgba(52,211,153,.2)}
.s-signed_partial{background:rgba(245,158,11,.12);color:var(--am);border:1px solid rgba(245,158,11,.2)}
.tbl-wrap{overflow-x:auto}
.tbl{width:100%;border-collapse:collapse}
.tbl th{font-size:10px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--t3);padding:9px 13px;text-align:left;border-bottom:1px solid var(--b0);white-space:nowrap}
.tbl td{padding:11px 13px;font-size:12.5px;color:var(--t1);border-bottom:1px solid var(--b1);vertical-align:middle}
.tbl tr:last-child td{border-bottom:none}
.tbl tbody tr:hover td{background:rgba(255,255,255,.02)}
.tbl .empty td{color:var(--t3);text-align:center;padding:26px;font-size:13px}
.tbl a{color:var(--a);text-decoration:none}
.tbl a:hover{text-decoration:underline}
.split-banner{background:var(--bg2);border:1px solid var(--b0);border-radius:var(--rl);padding:13px 17px;margin-bottom:14px;display:flex;align-items:center;gap:16px;position:sticky;top:var(--tb);z-index:30;backdrop-filter:blur(12px)}
.split-info{flex:1;min-width:0}
.split-lr{display:flex;justify-content:space-between;align-items:center;margin-bottom:6px}
.split-lbl{font-size:10px;font-weight:700;letter-spacing:.07em;text-transform:uppercase;color:var(--t2)}
.split-pct{font-family:var(--fm);font-size:12px;font-weight:600;color:var(--t1)}
.split-track{height:5px;background:rgba(255,255,255,.05);border-radius:99px;overflow:hidden}
.split-fill{height:100%;border-radius:99px;transition:width .35s ease,background .35s ease;width:0;background:linear-gradient(90deg,var(--a),var(--ae))}
.split-badge{display:inline-flex;align-items:center;gap:5px;padding:5px 11px;border-radius:99px;font-size:11px;font-weight:700;white-space:nowrap;transition:all .3s}
.sb-inc{background:rgba(245,158,11,.1);color:var(--am);border:1px solid rgba(245,158,11,.2)}
.sb-ok{background:rgba(52,211,153,.1);color:var(--ag);border:1px solid rgba(52,211,153,.2)}
.sb-over{background:rgba(255,79,106,.1);color:var(--ar);border:1px solid rgba(255,79,106,.2)}
.sb-dot{width:5px;height:5px;border-radius:50%;background:currentColor}
.wc{background:var(--bg4);border:1px solid var(--b0);border-radius:var(--rm);margin-bottom:9px;overflow:hidden;transition:border-color .2s}
.wc:hover{border-color:rgba(99,133,255,.2)}
.wc-hd{display:flex;align-items:center;gap:10px;padding:11px 14px;cursor:pointer;border-bottom:1px solid transparent;transition:background .14s,border-color .2s;user-select:none}
.wc-hd:hover{background:rgba(255,255,255,.02)}
.wc.open .wc-hd{border-bottom-color:var(--b0)}
.wc-av{width:29px;height:29px;border-radius:50%;background:linear-gradient(135deg,rgba(99,133,255,.22),rgba(165,91,255,.22));border:1px solid rgba(99,133,255,.18);display:flex;align-items:center;justify-content:center;font-size:12px;color:var(--a);flex-shrink:0}
.wc-nw{flex:1;min-width:0}
.wc-name{font-size:13px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.wc-sub{font-size:11px;color:var(--t3);margin-top:1px}
.wc-tags{display:flex;align-items:center;gap:5px}
.wc-chev{font-size:9px;color:var(--t3);transition:transform .2s}
.wc.open .wc-chev{transform:rotate(180deg)}
.wc-body{display:none;padding:14px}
.wc.open .wc-body{display:block}
.wc-sec{font-size:9.5px;font-weight:700;letter-spacing:.09em;text-transform:uppercase;color:var(--t3);padding-bottom:7px;margin-bottom:10px;border-bottom:1px solid var(--b0);margin-top:13px}
.wc-sec:first-child{margin-top:0}
.ac-wrap{position:relative}
.ac-box{position:absolute;top:calc(100% + 3px);left:0;right:0;z-index:200;background:var(--bg2);border:1px solid var(--b0);border-radius:var(--rs);max-height:190px;overflow-y:auto;display:none;box-shadow:0 8px 36px rgba(0,0,0,.55)}
.ac-item{padding:8px 12px;cursor:pointer;border-bottom:1px solid var(--b1);transition:background .1s}
.ac-item:last-child{border-bottom:none}
.ac-item:hover{background:var(--bg4)}
.ac-item strong{color:var(--t1);font-size:12.5px}
.ac-item small{color:var(--t3);font-size:11px}
.action-bar{position:fixed;bottom:0;left:var(--sb);right:0;background:rgba(7,11,18,.94);backdrop-filter:blur(18px);-webkit-backdrop-filter:blur(18px);border-top:1px solid var(--b0);padding:12px 30px;display:flex;align-items:center;gap:9px;z-index:45}
.ab-space{flex:1}
.upl-form{display:flex;gap:6px;align-items:center}
.upl-inp{background:var(--bg3);border:1px solid var(--b0);border-radius:var(--rs);color:var(--t2);font-size:11px;font-family:var(--f);padding:4px 7px;cursor:pointer;flex:1;min-width:0;max-width:160px}
.upl-inp::-webkit-file-upload-button{background:var(--bg4);border:1px solid var(--b0);border-radius:5px;color:var(--t2);font-family:var(--f);font-size:10.5px;padding:3px 7px;cursor:pointer;margin-right:6px}
.spin{display:inline-block;width:11px;height:11px;border:2px solid rgba(255,255,255,.25);border-top-color:#fff;border-radius:50%;animation:spin .6s linear infinite;display:none}
.spin.on{display:inline-block}
@keyframes spin{to{transform:rotate(360deg)}}
.info-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px 22px}
.info-item label{font-size:10px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--t3);display:block;margin-bottom:1px}
.info-item span,.info-item a{font-size:13px;color:var(--t1)}
.info-item a{color:var(--a);text-decoration:none}
.info-item a:hover{text-decoration:underline}
::-webkit-scrollbar{width:5px;height:5px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:rgba(255,255,255,.07);border-radius:99px}
.login-wrap{min-height:100vh;display:flex;align-items:center;justify-content:center;background:var(--bg0);padding:20px}
.login-card{width:100%;max-width:360px;background:var(--bg2);border:1px solid var(--b0);border-radius:var(--rl);padding:36px 32px;box-shadow:var(--sh)}
.login-logo{display:flex;align-items:center;gap:10px;margin-bottom:28px}
.login-logo-ico{width:34px;height:34px;background:linear-gradient(135deg,var(--a),var(--ae));border-radius:9px;display:flex;align-items:center;justify-content:center;font-size:16px}
.login-logo-name{font-size:17px;font-weight:700;letter-spacing:-.02em}
.login-h{font-size:17px;font-weight:700;margin-bottom:4px}
.login-sub{font-size:12.5px;color:var(--t2);margin-bottom:22px}
.login-field{margin-bottom:14px}
@media(max-width:860px){.g3{grid-template-columns:1fr 1fr}.g5,.g52{grid-template-columns:1fr 1fr}.g4,.g4a{grid-template-columns:1fr 1fr}}
@media(max-width:640px){.sb{display:none}.main{margin-left:0}.page{padding:16px 13px 90px}.g3,.g2,.g4,.g4a,.g5,.g52{grid-template-columns:1fr}.topbar{padding:0 13px}.tb-search{display:none}.action-bar{left:0;padding:11px 14px}}
</style>"""

LOGIN_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Login — LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="login-wrap">
  <div class="login-card">
    <div class="login-logo">
      <div class="login-logo-ico">🎵</div>
      <span class="login-logo-name">LabelMind</span>
    </div>
    <div class="login-h">Welcome back</div>
    <div class="login-sub">Music Publishing Contracts</div>
    {% with messages = get_flashed_messages() %}
      {% if messages %}
        <div class="flash-list">
          {% for m in messages %}<div class="flash-item">⚠️ {{ m }}</div>{% endfor %}
        </div>
      {% endif %}
    {% endwith %}
    <form method="post">
      <div class="login-field">
        <label class="label">Username</label>
        <input class="inp" name="username" required autocomplete="username" placeholder="your username">
      </div>
      <div class="login-field">
        <label class="label">Password</label>
        <input class="inp" type="password" name="password" required autocomplete="current-password" placeholder="••••••••">
      </div>
      <button class="btn btn-primary" style="width:100%;justify-content:center;margin-top:4px;">Log in →</button>
    </form>
  </div>
</div>
</body>
</html>"""

FORM_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Create Work — LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="app">
  <aside class="sb">
    <a class="sb-logo" href="{{ url_for('formulario') }}">
      <div class="sb-ico">🎵</div><span class="sb-name">LabelMind</span>
    </a>
    <div class="sb-sec">Contracts</div>
    <nav class="sb-nav">
      <a href="{{ url_for('formulario') }}" class="on"><span class="ni">🎵</span>Works</a>
      <a href="{{ url_for('batches_list') }}"><span class="ni">📦</span>Batches</a>
      <a href="#"><span class="ni">📄</span>Templates</a>
    </nav>
    <div class="sb-sec">Resources</div>
    <nav class="sb-nav">
      <a href="#"><span class="ni">👥</span>Writer Directory</a>
      <a href="#"><span class="ni">⚙️</span>Settings</a>
    </nav>
    <div class="sb-foot"><b>LabelMind</b>Music Publishing Contracts<br>©️ 2026 LabelMind.ai</div>
  </aside>
  <main class="main">
    <header class="topbar">
      <div class="tb-search">🔍 Search works, batches, writers…<span class="tb-kbd">⌘K</span></div>
      <div class="tb-right">
        <div class="pill-group">
          <a href="{{ url_for('works_list') }}" class="pill on">Works</a>
          <a href="{{ url_for('batches_list') }}" class="pill">Batches</a>
        </div>
        {% if team_auth_enabled and session.get('logged_in') %}
          <a href="{{ url_for('logout') }}" class="tb-ibtn" title="Log out">🚪</a>
        {% endif %}
        <div class="avatar">IS</div>
      </div>
    </header>
    <div class="page">
      {% with messages = get_flashed_messages() %}
        {% if messages %}
          <div class="flash-list">{% for m in messages %}<div class="flash-item">⚠️ {{ m }}</div>{% endfor %}</div>
        {% endif %}
      {% endwith %}
      <div class="ph">
        <div class="ph-left">
          <div class="ph-icon">🎵</div>
          <div>
            <div class="ph-title">Create Work</div>
            <div class="ph-sub">Create one work, add multiple writers, and save into a batch for review.</div>
          </div>
        </div>
      </div>
      <div class="split-banner">
        <div class="split-info">
          <div class="split-lr">
            <span class="split-lbl">Split Total</span>
            <span class="split-pct"><span id="splitTotal">0.00</span>% Complete</span>
          </div>
          <div class="split-track"><div class="split-fill" id="splitFill"></div></div>
        </div>
        <div class="split-badge sb-inc" id="splitBadge"><span class="sb-dot"></span>Incomplete</div>
      </div>
      <form method="post" id="workForm">
        <input type="hidden" name="force_create" value="{{ force_create or '' }}">
        <div class="card">
          <div class="card-hd">
            <div class="card-ico">📋</div><span class="card-title">Work Information</span>
          </div>
          <div class="card-body">
            <div class="g g3" style="margin-bottom:12px">
              <div class="field">
                <label class="label">Add to Existing Batch</label>
                <div class="inp-wrap">
                  <span class="inp-ico">📦</span>
                  <select class="inp" name="existing_batch_id">
                    <option value="">— Create new batch</option>
                    {% for batch in batches %}
                      <option value="{{ batch.id }}" {% if selected_batch_id == (batch.id|string) %}selected{% endif %}>
                        Batch #{{ batch.id }}{% if batch.camp %} — {{ batch.camp.name }}{% endif %} — {{ batch.contract_date.strftime('%Y-%m-%d') }}
                      </option>
                    {% endfor %}
                  </select>
                </div>
              </div>
              <div class="field">
                <label class="label">Camp</label>
                <div class="inp-wrap">
                  <span class="inp-ico">🎪</span>
                  <select class="inp" name="camp_id">
                    <option value="">— Select existing camp</option>
                    {% for camp in camps %}
                      <option value="{{ camp.id }}">{{ camp.name }}</option>
                    {% endfor %}
                  </select>
                </div>
              </div>
              <div class="field">
                <label class="label">Or Create New Camp</label>
                <input class="inp" name="new_camp_name" placeholder="New Camp Name">
              </div>
            </div>
            <div class="g g2">
              <div class="field">
                <label class="label">Work Title</label>
                <div class="inp-wrap">
                  <span class="inp-ico">🎵</span>
                  <input class="inp" name="work_title" required placeholder="e.g. La Serenata">
                </div>
              </div>
              <div class="field">
                <label class="label">Contract Date</label>
                <div class="inp-wrap">
                  <span class="inp-ico">📅</span>
                  <input class="inp" name="contract_date" type="date" required>
                </div>
              </div>
            </div>
          </div>
        </div>
        <div class="card">
          <div class="card-hd">
            <div class="card-ico">👥</div>
            <span class="card-title">Writers</span>
            <div class="card-actions">
              <button type="button" class="btn btn-sec btn-sm" onclick="addWriter()">+ Add Writer</button>
            </div>
          </div>
          <div class="card-body" id="writerRows" style="padding-bottom:6px"></div>
        </div>
        <div class="action-bar">
          <button type="button" class="btn btn-sec" onclick="addWriter()">+ Add Writer</button>
          <div class="ab-space"></div>
          <button type="submit" class="btn btn-primary">✓ Save Work to Batch</button>
        </div>
      </form>
    </div>
  </main>
</div>
<script>
const proMap={BMI:{name:'Songs of Afinarte',ipi:'817874992'},ASCAP:{name:'Melodies of Afinarte',ipi:'807953316'},SESAC:{name:'Music of Afinarte',ipi:'817094629'}};
const defAddr="{{ default_publisher_address }}",defCity="{{ default_publisher_city }}",defState="{{ default_publisher_state }}",defZip="{{ default_publisher_zip }}";
let idx=0;
function statusHtml(ws,ct){const wc=ws==='Existing Writer'?'tag-exist':'tag-new';const cc=ct==='Schedule 1'?'tag-s1':'tag-full';return `<span class="tag ${wc}">${ws}</span><span class="tag ${cc}">${ct}</span>`;}
function writerTpl(i){return `
  <div class="wc open" data-i="${i}">
    <div class="wc-hd" onclick="toggleWC(this)">
      <div class="wc-av">👤</div>
      <div class="wc-nw"><div class="wc-name wc-dn">Writer ${i+1}</div><div class="wc-sub wc-ds">New · — · —%</div></div>
      <div class="wc-tags wc-meta">${statusHtml('New Writer','Full Contract')}</div>
      <span class="wc-chev">▼</span>
      <button type="button" class="btn btn-danger btn-sm" onclick="rmWriter(event,this)" style="margin-left:8px">Remove</button>
    </div>
    <div class="wc-body">
      <input type="hidden" name="writer_id" class="wid">
      <div class="wc-sec">Identity</div>
      <div class="g g4" style="gap:10px">
        <div class="field ac-wrap">
          <label class="label">First Name</label>
          <input class="inp wfn" name="writer_first_name" placeholder="First" autocomplete="off">
          <div class="ac-box wsug"></div>
        </div>
        <div class="field"><label class="label">Middle Name</label><input class="inp wmn" name="writer_middle_name" placeholder="— —" autocomplete="off"></div>
        <div class="field"><label class="label">Last Name(s)</label><input class="inp wln" name="writer_last_names" placeholder="Last Name" autocomplete="off"></div>
        <div class="field"><label class="label">AKA / Stage</label><input class="inp waka" name="writer_aka" placeholder="Stage Name"></div>
      </div>
      <div class="wc-sec">Publishing</div>
      <div class="g g5" style="gap:10px">
        <div class="field"><label class="label">IPI #</label><input class="inp wipi" name="writer_ipi" placeholder="IPI Number"></div>
        <div class="field"><label class="label">Email</label><input class="inp wem" name="writer_email" placeholder="writer@email.com" type="email"></div>
        <div class="field"><label class="label">PRO</label><select class="inp wpro" name="writer_pro" onchange="syncPro(this)"><option value="">PRO</option><option value="BMI">BMI</option><option value="ASCAP">ASCAP</option><option value="SESAC">SESAC</option></select></div>
        <div class="field"><label class="label">Writer %</label><input class="inp wspl" name="writer_percentage" placeholder="0" type="number" step="0.01" min="0" max="100"></div>
        <div class="field"><label class="label">Publisher</label><input class="inp wpub" name="writer_publisher" placeholder="Publisher Name"></div>
      </div>
      <div class="wc-sec">Publisher Details</div>
      <div class="g g52" style="gap:10px">
        <div class="field"><label class="label">Publisher IPI</label><input class="inp wpipi" name="publisher_ipi" placeholder="Publisher IPI"></div>
        <div class="field"><label class="label">Address</label><input class="inp wpaddr" name="publisher_address" value="${defAddr}" placeholder="Address"></div>
        <div class="field"><label class="label">City</label><input class="inp wpcity" name="publisher_city" value="${defCity}" placeholder="City"></div>
        <div class="field"><label class="label">State</label><input class="inp wpst" name="publisher_state" value="${defState}" placeholder="ST"></div>
        <div class="field"><label class="label">Zip</label><input class="inp wpzip" name="publisher_zip_code" value="${defZip}" placeholder="Zip"></div>
      </div>
      <div class="wc-sec">Writer Address</div>
      <div class="g g4a" style="gap:10px">
        <div class="field"><label class="label">Street</label><input class="inp waddr" name="writer_address" placeholder="Street Address"></div>
        <div class="field"><label class="label">City</label><input class="inp wcity" name="writer_city" placeholder="City"></div>
        <div class="field"><label class="label">State</label><input class="inp wst" name="writer_state" placeholder="ST"></div>
        <div class="field"><label class="label">Zip</label><input class="inp wzip" name="writer_zip_code" placeholder="Zip"></div>
      </div>
    </div>
  </div>`;}
function toggleWC(hd){hd.closest('.wc').classList.toggle('open');}
function addWriter(){const c=document.getElementById('writerRows');c.insertAdjacentHTML('beforeend',writerTpl(idx));setupWriter(c.lastElementChild);idx++;recalc();}
function rmWriter(e,btn){e.stopPropagation();btn.closest('.wc').remove();recalc();}
function syncPro(sel){const r=sel.closest('.wc');const p=proMap[sel.value];if(!p)return;r.querySelector('.wpub').value=p.name;r.querySelector('.wpipi').value=p.ipi;updateHdr(r);}
function fullName(r){return[r.querySelector('.wfn').value,r.querySelector('.wmn').value,r.querySelector('.wln').value].map(s=>s.trim()).filter(Boolean).join(' ');}
function updateHdr(r){const n=fullName(r)||`Writer ${parseInt(r.dataset.i)+1}`;const pro=r.querySelector('.wpro').value||'—';const pct=r.querySelector('.wspl').value||'—';r.querySelector('.wc-dn').textContent=n;r.querySelector('.wc-ds').textContent=`${pro} · ${pct}%`;}
function setStatus(r,ws,ct){r.querySelector('.wc-meta').innerHTML=statusHtml(ws,ct);}
function hideSug(r){const b=r.querySelector('.wsug');b.style.display='none';b.innerHTML='';}
function resetNew(r){r.querySelector('.wid').value='';setStatus(r,'New Writer','Full Contract');}
function fillWriter(r,w){r.querySelector('.wid').value=w.id||'';r.querySelector('.wfn').value=w.first_name||'';r.querySelector('.wmn').value=w.middle_name||'';r.querySelector('.wln').value=w.last_names||'';r.querySelector('.waka').value=w.writer_aka||'';r.querySelector('.wipi').value=w.ipi||'';r.querySelector('.wem').value=w.email||'';r.querySelector('.wpro').value=w.pro||'';r.querySelector('.waddr').value=w.address||'';r.querySelector('.wcity').value=w.city||'';r.querySelector('.wst').value=w.state||'';r.querySelector('.wzip').value=w.zip_code||'';const pd=proMap[w.pro]||{};r.querySelector('.wpub').value=w.default_publisher||pd.name||'';r.querySelector('.wpipi').value=w.default_publisher_ipi||pd.ipi||'';updateHdr(r);setStatus(r,'Existing Writer',w.has_master_contract?'Schedule 1':'Full Contract');hideSug(r);}
function setupWriter(r){const fn=r.querySelector('.wfn'),mn=r.querySelector('.wmn'),ln=r.querySelector('.wln'),sug=r.querySelector('.wsug'),spl=r.querySelector('.wspl'),pro=r.querySelector('.wpro');
async function search(){const q=fullName(r);if(q.length<2){hideSug(r);resetNew(r);return;}const res=await fetch('/writers/search?q='+encodeURIComponent(q));const ws=await res.json();if(!ws.length){hideSug(r);resetNew(r);return;}sug.innerHTML=ws.map(w=>`<div class="ac-item" data-w='${JSON.stringify(w).replaceAll("'","&#39;")}'><strong>${w.full_name}</strong><br><small>${w.city||''}${w.city&&w.state?', ':''}${w.state||''}</small></div>`).join('');sug.style.display='block';sug.querySelectorAll('.ac-item').forEach(item=>{item.addEventListener('click',()=>fillWriter(r,JSON.parse(item.dataset.w)));});}
[fn,mn,ln].forEach(inp=>inp.addEventListener('input',()=>{resetNew(r);updateHdr(r);search();}));
spl.addEventListener('input',()=>{updateHdr(r);recalc();});
pro.addEventListener('change',()=>updateHdr(r));
document.addEventListener('click',e=>{if(![fn,mn,ln,sug].some(el=>el.contains(e.target)))hideSug(r);});}
function recalc(){let total=0;document.querySelectorAll('.wspl').forEach(i=>{total+=parseFloat(i.value||0)||0;});const rounded=total.toFixed(2);document.getElementById('splitTotal').textContent=rounded;const fill=document.getElementById('splitFill');const badge=document.getElementById('splitBadge');fill.style.width=Math.min(total,100)+'%';if(Math.abs(total-100)<0.001){fill.style.background='linear-gradient(90deg,#34d399,#059669)';badge.className='split-badge sb-ok';badge.innerHTML='<span class="sb-dot"></span>Complete ✓';}else if(total>100){fill.style.background='linear-gradient(90deg,#ff4f6a,#c0152d)';badge.className='split-badge sb-over';badge.innerHTML='<span class="sb-dot"></span>Over 100%';}else{fill.style.background='linear-gradient(90deg,var(--a),var(--ae))';badge.className='split-badge sb-inc';badge.innerHTML='<span class="sb-dot"></span>Incomplete';}}
document.getElementById('workForm').addEventListener('submit',function(e){const rows=document.querySelectorAll('.wc');if(!rows.length){e.preventDefault();alert('Add at least one writer.');return;}let ok=false;for(const r of rows){const n=fullName(r);const s=parseFloat(r.querySelector('.wspl').value||0)||0;if(n){ok=true;if(s<=0){e.preventDefault();alert('Each writer must have a split > 0.');return;}}}if(!ok){e.preventDefault();alert('Add at least one writer with a name.');return;}const t=parseFloat(document.getElementById('splitTotal').textContent||0)||0;if(Math.abs(t-100)>=0.001){e.preventDefault();alert('Total split must equal 100%.');}});
addWriter();
</script>
</body>
</html>"""

DUPLICATE_WARNING_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Possible Duplicate — LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="app">
  <aside class="sb">
    <a class="sb-logo" href="{{ url_for('formulario') }}"><div class="sb-ico">🎵</div><span class="sb-name">LabelMind</span></a>
    <div class="sb-sec">Contracts</div>
    <nav class="sb-nav">
      <a href="{{ url_for('formulario') }}" class="on"><span class="ni">🎵</span>Works</a>
      <a href="{{ url_for('batches_list') }}"><span class="ni">📦</span>Batches</a>
    </nav>
    <div class="sb-foot"><b>LabelMind</b>©️ 2026 LabelMind.ai</div>
  </aside>
  <main class="main">
    <header class="topbar">
      <div class="tb-search">🔍 Search…<span class="tb-kbd">⌘K</span></div>
      <div class="tb-right"><div class="pill-group"><a href="{{ url_for('works_list') }}" class="pill on">Works</a><a href="{{ url_for('batches_list') }}" class="pill">Batches</a></div><div class="avatar">IS</div></div>
    </header>
    <div class="page">
      <div class="ph"><div class="ph-left"><div class="ph-icon">⚠️</div><div><div class="ph-title">Possible Duplicate Found</div><div class="ph-sub">Existing works match this title and writer set.</div></div></div></div>
      <div class="card">
        <div class="card-hd"><div class="card-ico">🔍</div><span class="card-title">Matching Works</span></div>
        <div class="card-body">
          <table class="tbl" style="margin-bottom:18px">
            <thead><tr><th>Title</th><th>Camp</th><th>Created</th></tr></thead>
            <tbody>
              {% for item in duplicates %}
              <tr><td style="font-weight:600">{{ item.title }}</td><td>{{ item.camp_name or '—' }}</td><td style="color:var(--t2)">{{ item.created_at }}</td></tr>
              {% endfor %}
            </tbody>
          </table>
          <form method="post">
            {% for key, value in form_data.items() %}
              {% if value is string %}<input type="hidden" name="{{ key }}" value="{{ value }}">
              {% else %}{% for item in value %}<input type="hidden" name="{{ key }}" value="{{ item }}">{% endfor %}{% endif %}
            {% endfor %}
            <input type="hidden" name="force_create" value="1">
            <div style="display:flex;gap:10px">
              <button type="submit" class="btn btn-danger">Continue Anyway</button>
              <a href="{{ url_for('formulario') }}" class="btn btn-sec">Cancel</a>
            </div>
          </form>
        </div>
      </div>
    </div>
  </main>
</div>
</body>
</html>"""

WORKS_LIST_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Works — LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="app">
  <aside class="sb">
    <a class="sb-logo" href="{{ url_for('formulario') }}"><div class="sb-ico">🎵</div><span class="sb-name">LabelMind</span></a>
    <div class="sb-sec">Contracts</div>
    <nav class="sb-nav">
      <a href="{{ url_for('formulario') }}" class="on"><span class="ni">🎵</span>Works</a>
      <a href="{{ url_for('batches_list') }}"><span class="ni">📦</span>Batches</a>
      <a href="#"><span class="ni">📄</span>Templates</a>
    </nav>
    <div class="sb-sec">Resources</div>
    <nav class="sb-nav"><a href="#"><span class="ni">👥</span>Writer Directory</a><a href="#"><span class="ni">⚙️</span>Settings</a></nav>
    <div class="sb-foot"><b>LabelMind</b>Music Publishing Contracts<br>©️ 2026 LabelMind.ai</div>
  </aside>
  <main class="main">
    <header class="topbar">
      <div class="tb-search">🔍 Search works, batches, writers…<span class="tb-kbd">⌘K</span></div>
      <div class="tb-right">
        <div class="pill-group"><a href="{{ url_for('works_list') }}" class="pill on">Works</a><a href="{{ url_for('batches_list') }}" class="pill">Batches</a></div>
        {% if team_auth_enabled and session.get('logged_in') %}<a href="{{ url_for('logout') }}" class="tb-ibtn">🚪</a>{% endif %}
        <div class="avatar">IS</div>
      </div>
    </header>
    <div class="page">
      <div class="ph">
        <div class="ph-left"><div class="ph-icon">🎵</div><div><div class="ph-title">Works</div><div class="ph-sub">All registered musical works</div></div></div>
        <div class="ph-actions"><a href="{{ url_for('formulario') }}" class="btn btn-primary">+ Create Work</a></div>
      </div>
      <div class="card">
        <div class="card-hd"><div class="card-ico">🔍</div><span class="card-title">Search</span></div>
        <div class="card-body">
          <form method="get" style="display:flex;gap:8px">
            <input class="inp" name="q" value="{{ q }}" placeholder="Search work title…" style="max-width:340px">
            <button class="btn btn-sec" type="submit">Search</button>
            {% if q %}<a href="{{ url_for('works_list') }}" class="btn btn-sec">Clear</a>{% endif %}
          </form>
        </div>
      </div>
      <div class="card">
        <div class="card-hd"><div class="card-ico">📋</div><span class="card-title">All Works</span></div>
        <div class="tbl-wrap">
          <table class="tbl">
            <thead><tr><th>Work Title</th><th>Camp</th><th>Batch</th><th>Contract Date</th><th>Writers</th><th>Created</th><th></th></tr></thead>
            <tbody>
              {% for work in works %}
              <tr>
                <td style="font-weight:600">{{ work.title }}</td>
                <td style="color:var(--t2)">{{ work.camp.name if work.camp else '—' }}</td>
                <td>{% if work.batch_id %}<a href="{{ url_for('batch_detail', batch_id=work.batch_id) }}"><span class="status s-draft">Batch #{{ work.batch_id }}</span></a>{% else %}—{% endif %}</td>
                <td style="color:var(--t2);font-size:12px">{{ work.contract_date.strftime('%b %d, %Y') if work.contract_date else '—' }}</td>
                <td><span style="background:rgba(99,133,255,.1);color:var(--a);border:1px solid rgba(99,133,255,.2);border-radius:99px;padding:2px 8px;font-size:11px;font-weight:700">{{ work.work_writers|length }}</span></td>
                <td style="color:var(--t3);font-size:12px">{{ work.created_at.strftime('%b %d, %Y') }}</td>
                <td><a href="{{ url_for('work_detail', work_id=work.id) }}" class="btn btn-sec btn-sm">View →</a></td>
              </tr>
              {% endfor %}
              {% if not works %}<tr class="empty"><td colspan="7">No works found{% if q %} for "{{ q }}"{% endif %}.</td></tr>{% endif %}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </main>
</div>
</body>
</html>"""

BATCHES_LIST_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Batches — LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="app">
  <aside class="sb">
    <a class="sb-logo" href="{{ url_for('formulario') }}"><div class="sb-ico">🎵</div><span class="sb-name">LabelMind</span></a>
    <div class="sb-sec">Contracts</div>
    <nav class="sb-nav">
      <a href="{{ url_for('formulario') }}"><span class="ni">🎵</span>Works</a>
      <a href="{{ url_for('batches_list') }}" class="on"><span class="ni">📦</span>Batches</a>
      <a href="#"><span class="ni">📄</span>Templates</a>
    </nav>
    <div class="sb-sec">Resources</div>
    <nav class="sb-nav"><a href="#"><span class="ni">👥</span>Writer Directory</a><a href="#"><span class="ni">⚙️</span>Settings</a></nav>
    <div class="sb-foot"><b>LabelMind</b>Music Publishing Contracts<br>©️ 2026 LabelMind.ai</div>
  </aside>
  <main class="main">
    <header class="topbar">
      <div class="tb-search">🔍 Search works, batches, writers…<span class="tb-kbd">⌘K</span></div>
      <div class="tb-right">
        <div class="pill-group"><a href="{{ url_for('works_list') }}" class="pill">Works</a><a href="{{ url_for('batches_list') }}" class="pill on">Batches</a></div>
        {% if team_auth_enabled and session.get('logged_in') %}<a href="{{ url_for('logout') }}" class="tb-ibtn">🚪</a>{% endif %}
        <div class="avatar">IS</div>
      </div>
    </header>
    <div class="page">
      <div class="ph">
        <div class="ph-left"><div class="ph-icon">📦</div><div><div class="ph-title">Batches</div><div class="ph-sub">Groups of works ready for contract generation</div></div></div>
        <div class="ph-actions"><a href="{{ url_for('formulario') }}" class="btn btn-primary">+ Create Work</a></div>
      </div>
      <div class="card">
        <div class="card-hd"><div class="card-ico">📦</div><span class="card-title">All Batches</span></div>
        <div class="tbl-wrap">
          <table class="tbl">
            <thead><tr><th>Batch</th><th>Camp</th><th>Contract Date</th><th>Status</th><th>Works</th><th>Created</th><th></th></tr></thead>
            <tbody>
              {% for batch in batches %}
              <tr>
                <td style="font-weight:600">Batch #{{ batch.id }}</td>
                <td style="color:var(--t2)">{{ batch.camp.name if batch.camp else '—' }}</td>
                <td style="color:var(--t2);font-size:12px">{{ batch.contract_date.strftime('%b %d, %Y') }}</td>
                <td><span class="status s-{{ batch.status }}"><span class="status-dot"></span>{{ batch.status | replace('_',' ') | title }}</span></td>
                <td><span style="background:rgba(99,133,255,.1);color:var(--a);border:1px solid rgba(99,133,255,.2);border-radius:99px;padding:2px 8px;font-size:11px;font-weight:700">{{ batch.works|length }}</span></td>
                <td style="color:var(--t3);font-size:12px">{{ batch.created_at.strftime('%b %d, %Y') }}</td>
                <td><a href="{{ url_for('batch_detail', batch_id=batch.id) }}" class="btn btn-sec btn-sm">View →</a></td>
              </tr>
              {% endfor %}
              {% if not batches %}<tr class="empty"><td colspan="7">No batches yet. Create a work to get started.</td></tr>{% endif %}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </main>
</div>
</body>
</html>"""

BATCH_DETAIL_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Batch {{ batch.id }} — LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="app">
  <aside class="sb">
    <a class="sb-logo" href="{{ url_for('formulario') }}"><div class="sb-ico">🎵</div><span class="sb-name">LabelMind</span></a>
    <div class="sb-sec">Contracts</div>
    <nav class="sb-nav">
      <a href="{{ url_for('formulario') }}"><span class="ni">🎵</span>Works</a>
      <a href="{{ url_for('batches_list') }}" class="on"><span class="ni">📦</span>Batches</a>
    </nav>
    <div class="sb-foot"><b>LabelMind</b>©️ 2026 LabelMind.ai</div>
  </aside>
  <main class="main">
    <header class="topbar">
      <div class="tb-search">🔍 Search…<span class="tb-kbd">⌘K</span></div>
      <div class="tb-right">
        <div class="pill-group"><a href="{{ url_for('works_list') }}" class="pill">Works</a><a href="{{ url_for('batches_list') }}" class="pill on">Batches</a></div>
        {% if team_auth_enabled and session.get('logged_in') %}<a href="{{ url_for('logout') }}" class="tb-ibtn">🚪</a>{% endif %}
        <div class="avatar">IS</div>
      </div>
    </header>
    <div class="page">
      {% with messages = get_flashed_messages() %}{% if messages %}<div class="flash-list">{% for m in messages %}<div class="flash-item">⚠️ {{ m }}</div>{% endfor %}</div>{% endif %}{% endwith %}
      <div class="ph">
        <div class="ph-left"><div class="ph-icon">📦</div><div><div class="ph-title">Batch #{{ batch.id }}</div><div class="ph-sub">{{ batch.camp.name if batch.camp else 'No camp' }} · {{ batch.contract_date.strftime('%b %d, %Y') }}</div></div></div>
        <div class="ph-actions">
          <a href="{{ url_for('batches_list') }}" class="btn btn-sec btn-sm">← Back</a>
          <a href="{{ url_for('formulario', batch_id=batch.id) }}" class="btn btn-sec btn-sm">+ Add Work</a>
          <form method="post" action="{{ url_for('generate_batch_documents', batch_id=batch.id) }}" id="genForm" style="display:inline">
            <button type="submit" class="btn btn-primary btn-sm" id="genBtn">
              <span id="genLabel">⚡ Generate Docs</span><span class="spin" id="genSpin"></span>
            </button>
          </form>
        </div>
      </div>
      <div class="card">
        <div class="card-hd"><div class="card-ico">ℹ️</div><span class="card-title">Batch Info</span></div>
        <div class="card-body">
          <div class="info-grid">
            <div class="info-item"><label>Camp</label><span>{{ batch.camp.name if batch.camp else '—' }}</span></div>
            <div class="info-item"><label>Contract Date</label><span>{{ batch.contract_date.strftime('%B %d, %Y') }}</span></div>
            <div class="info-item"><label>Status</label><span class="status s-{{ batch.status }}"><span class="status-dot"></span>{{ batch.status | replace('_',' ') | title }}</span></div>
            <div class="info-item"><label>Created</label><span>{{ batch.created_at.strftime('%b %d, %Y %H:%M') }}</span></div>
          </div>
        </div>
      </div>
      <div class="card">
        <div class="card-hd"><div class="card-ico">🎵</div><span class="card-title">Works in Batch</span></div>
        <div class="tbl-wrap">
          <table class="tbl">
            <thead><tr><th>Work Title</th><th>Writers</th><th>Created</th><th></th></tr></thead>
            <tbody>
              {% for work in works %}
              <tr>
                <td style="font-weight:600">{{ work.title }}</td>
                <td><span style="background:rgba(99,133,255,.1);color:var(--a);border:1px solid rgba(99,133,255,.2);border-radius:99px;padding:2px 8px;font-size:11px;font-weight:700">{{ work.work_writers|length }}</span></td>
                <td style="color:var(--t3);font-size:12px">{{ work.created_at.strftime('%b %d, %Y') }}</td>
                <td><a href="{{ url_for('work_detail', work_id=work.id) }}" class="btn btn-sec btn-sm">View →</a></td>
              </tr>
              {% endfor %}
              {% if not works %}<tr class="empty"><td colspan="4">No works in this batch.</td></tr>{% endif %}
            </tbody>
          </table>
        </div>
      </div>
      <div class="card">
        <div class="card-hd"><div class="card-ico">👥</div><span class="card-title">Writer Summary</span></div>
        <div class="tbl-wrap">
          <table class="tbl">
            <thead><tr><th>Writer</th><th>AKA</th><th>IPI</th><th>PRO</th><th>Works</th><th>Master Contract</th></tr></thead>
            <tbody>
              {% for item in writer_summary %}
              <tr>
                <td style="font-weight:600">{{ item.writer.full_name }}</td>
                <td style="color:var(--t2)">{{ item.writer.writer_aka or '—' }}</td>
                <td style="font-family:var(--fm);font-size:12px;color:var(--t2)">{{ item.writer.ipi or '—' }}</td>
                <td><span class="tag tag-full">{{ item.writer.pro or '—' }}</span></td>
                <td>{{ item.work_count }}</td>
                <td>{% if item.writer.has_master_contract %}<span class="tag tag-s1">Yes</span>{% else %}<span style="color:var(--t3)">No</span>{% endif %}</td>
              </tr>
              {% endfor %}
              {% if not writer_summary %}<tr class="empty"><td colspan="6">No writers in this batch.</td></tr>{% endif %}
            </tbody>
          </table>
        </div>
      </div>
      <div class="card">
        <div class="card-hd"><div class="card-ico">📄</div><span class="card-title">Generated Documents</span></div>
        <div class="tbl-wrap">
          <table class="tbl" style="min-width:900px">
            <thead><tr><th>Writer</th><th>Type</th><th>File</th><th>Generated</th><th>DocuSign</th><th>DS Status</th><th>Certificate</th><th>Upload Signed</th><th>Signed</th><th>Status</th></tr></thead>
            <tbody id="generatedDocumentsBody">
              {% for doc in documents %}
              <tr data-doc-id="{{ doc.id }}">
                <td style="font-weight:600;white-space:nowrap">{{ doc.writer_name_snapshot }}</td>
                <td><span class="tag tag-full">{{ doc.document_type }}</span></td>
                <td style="color:var(--t2);font-size:11.5px;max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{{ doc.file_name }}</td>
                <td>{% if doc.drive_web_view_link %}<a href="{{ doc.drive_web_view_link }}" target="_blank" class="btn btn-cyan btn-xs">Open ↗️</a>{% else %}—{% endif %}</td>
                <td>
                  <form method="post" action="{{ url_for('send_document_docusign', document_id=doc.id) }}" class="ds-form">
                    <button type="submit" class="btn btn-sec btn-xs ds-btn">
                      <span class="ds-lbl">{% if doc.docusign_status == 'completed' %}Resend{% elif doc.docusign_status == 'sent' %}Sent{% elif doc.docusign_status == 'delivered' %}Delivered{% else %}Send{% endif %}</span>
                      <span class="spin ds-spin"></span>
                    </button>
                  </form>
                </td>
                <td>{% if doc.docusign_status %}<span class="status s-{{ doc.docusign_status }}"><span class="status-dot"></span>{{ doc.docusign_status | title }}</span>{% else %}—{% endif %}</td>
                <td>{% if doc.certificate_drive_web_view_link %}<a href="{{ doc.certificate_drive_web_view_link }}" target="_blank" class="btn btn-sec btn-xs">Cert ↗️</a>{% else %}—{% endif %}</td>
                <td>
                  <form method="post" action="{{ url_for('upload_signed_document', document_id=doc.id) }}" enctype="multipart/form-data" class="upl-form">
                    <input type="file" name="signed_file" class="upl-inp" required>
                    <button type="submit" class="btn btn-success btn-xs">Upload</button>
                  </form>
                </td>
                <td>{% if doc.signed_pdf_drive_web_view_link %}<a href="{{ doc.signed_pdf_drive_web_view_link }}" target="_blank" class="btn btn-success btn-xs">Signed ↗️</a>{% elif doc.signed_web_view_link %}<a href="{{ doc.signed_web_view_link }}" target="_blank" class="btn btn-success btn-xs">Signed ↗️</a>{% else %}—{% endif %}</td>
                <td>{% if doc.status %}<span class="status s-{{ doc.status }}"><span class="status-dot"></span>{{ doc.status | replace('_',' ') | title }}</span>{% else %}—{% endif %}</td>
              </tr>
              {% endfor %}
              {% if not documents %}<tr class="empty"><td colspan="10">No documents generated yet. Click "Generate Docs" above.</td></tr>{% endif %}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </main>
</div>
<script>
const SEND_URL_TPL="{{ url_for('send_document_docusign', document_id=0) }}";
const batchId={{ batch.id }};
function esc(v){return(v||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}
function renderDsBtn(doc){const id=doc.id||doc.document_id;const url=SEND_URL_TPL.replace('/0/send-docusign',`/${id}/send-docusign`);let lbl='Send';if(doc.docusign_status==='completed')lbl='Resend';else if(doc.docusign_status==='delivered')lbl='Delivered';else if(doc.docusign_status==='sent')lbl='Sent';return `<form method="post" action="${url}" class="ds-form"><button type="submit" class="btn btn-sec btn-xs ds-btn"><span class="ds-lbl">${lbl}</span><span class="spin ds-spin"></span></button></form>`;}
function renderStatus(val,cls){if(!val)return '—';const c=cls+val.replace(/[ _]/g,'_');const l=val.replace(/_/g,' ').replace(/\b\w/g,x=>x.toUpperCase());return `<span class="status ${c}"><span class="status-dot"></span>${l}</span>`;}
function updateDocs(data){const tb=document.getElementById('generatedDocumentsBody');if(!tb||!data.documents)return;tb.innerHTML=data.documents.map(doc=>`<tr data-doc-id="${doc.id}"><td style="font-weight:600;white-space:nowrap">${esc(doc.writer_name_snapshot)}</td><td><span class="tag tag-full">${esc(doc.document_type)}</span></td><td style="color:var(--t2);font-size:11.5px;max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(doc.file_name)}</td><td>${doc.drive_web_view_link?`<a href="${doc.drive_web_view_link}" target="_blank" class="btn btn-cyan btn-xs">Open ↗️</a>`:'—'}</td><td>${renderDsBtn(doc)}</td><td>${renderStatus(doc.docusign_status,'s-')}</td><td>${doc.certificate_drive_web_view_link?`<a href="${doc.certificate_drive_web_view_link}" target="_blank" class="btn btn-sec btn-xs">Cert ↗️</a>`:'—'}</td><td><form method="post" action="/documents/${doc.id}/upload-signed" enctype="multipart/form-data" class="upl-form"><input type="file" name="signed_file" class="upl-inp" required><button type="submit" class="btn btn-success btn-xs">Upload</button></form></td><td>${doc.signed_pdf_drive_web_view_link?`<a href="${doc.signed_pdf_drive_web_view_link}" target="_blank" class="btn btn-success btn-xs">Signed ↗️</a>`:doc.signed_web_view_link
