from flask import Flask, render_template_string, request, send_file, session, redirect, url_for, jsonify, flash
from docx import Document
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, or_
from babel.dates import format_date
import io
import os
import re
import zipfile
import datetime

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


class Camp(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False, unique=True, index=True)
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)

    works = db.relationship("Work", backref="camp", lazy=True)


class Writer(db.Model):
    id = db.Column(db.Integer, primary_key=True)

    first_name = db.Column(db.String(100), default="", index=True)
    middle_name = db.Column(db.String(100), default="")
    last_names = db.Column(db.String(150), default="")
    full_name = db.Column(db.String(250), nullable=False, unique=True, index=True)

    ipi = db.Column(db.String(50), default="", index=True)
    pro = db.Column(db.String(20), default="")

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
    camp_id = db.Column(db.Integer, db.ForeignKey("camp.id"), nullable=True)
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

    work_id = db.Column(db.Integer, db.ForeignKey("work.id"), nullable=False)
    writer_id = db.Column(db.Integer, db.ForeignKey("writer.id"), nullable=False)

    document_type = db.Column(db.String(50), nullable=False)
    file_name = db.Column(db.String(255), nullable=False)

    writer_name_snapshot = db.Column(db.String(250), nullable=False)
    work_title_snapshot = db.Column(db.String(255), nullable=False)

    generated_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)

    writer = db.relationship("Writer", backref="contract_documents")


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
          <p class="text-muted mb-0">Create one work, add multiple writers, and generate the correct document per writer.</p>
        </div>
        <div class="d-flex gap-2">
          <a href="{{ url_for('works_list') }}" class="btn btn-outline-primary btn-sm">Works</a>
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
        <h4>Work</h4>
        <div class="row mb-4">
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
          <div class="col-md-4">
            <label class="form-label">Work Title</label>
            <input class="form-control" name="work_title" required placeholder="Work Title">
          </div>
        </div>

        <h4>Writers</h4>
        <div id="writerRows"></div>

        <div class="d-flex gap-2">
          <button type="button" class="btn btn-outline-primary" onclick="addWriterRow()">Add Writer</button>
          <button type="submit" class="btn btn-success">Create Work & Generate Documents</button>
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
        <div class="col-md-4 autocomplete-wrap">
          <label class="form-label">First Name</label>
          <input class="form-control writer-first-name" name="writer_first_name" placeholder="First Name" autocomplete="off">
          <div class="autocomplete-box writer-suggestions"></div>
        </div>
        <div class="col-md-4">
          <label class="form-label">Middle Name</label>
          <input class="form-control writer-middle-name" name="writer_middle_name" placeholder="Middle Name" autocomplete="off">
        </div>
        <div class="col-md-4">
          <label class="form-label">Last Name(s)</label>
          <input class="form-control writer-last-names" name="writer_last_names" placeholder="Last Name(s)" autocomplete="off">
        </div>
      </div>

      <div class="row mt-3">
        <div class="col-md-3">
          <label class="form-label">Writer IPI #</label>
          <input class="form-control writer-ipi" name="writer_ipi" placeholder="IPI Number">
        </div>
        <div class="col-md-3">
          <label class="form-label">PRO</label>
          <select class="form-control writer-pro" name="writer_pro" onchange="syncPublisherFromPro(this)">
            <option value="">Select PRO</option>
            <option value="BMI">BMI</option>
            <option value="ASCAP">ASCAP</option>
            <option value="SESAC">SESAC</option>
          </select>
        </div>
        <div class="col-md-3">
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

  if (!publisherInput.value.trim()) {
    publisherInput.value = selected.name;
  }

  if (!publisherIpiInput.value.trim()) {
    publisherIpiInput.value = selected.ipi;
  }
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
  row.querySelector('.writer-ipi').value = writer.ipi || '';
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
    <a href="{{ url_for('formulario') }}" class="btn btn-primary">Create Work</a>
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
                <td>{{ work.work_writers|length }}</td>
                <td>{{ work.created_at.strftime('%Y-%m-%d') }}</td>
                <td><a href="{{ url_for('work_detail', work_id=work.id) }}" class="btn btn-sm btn-outline-secondary">View</a></td>
              </tr>
            {% endfor %}
            {% if not works %}
              <tr><td colspan="5" class="text-center text-muted">No works found.</td></tr>
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
    <a href="{{ url_for('works_list') }}" class="btn btn-outline-secondary">Back</a>
  </div>

  <div class="card shadow-sm mb-4">
    <div class="card-body">
      <h5>Work Info</h5>
      <p class="mb-1"><strong>Camp:</strong> {{ work.camp.name if work.camp else '—' }}</p>
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
                <td>{{ ww.writer.ipi }}</td>
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
              <th>Document Type</th>
              <th>File Name</th>
              <th>Generated At</th>
            </tr>
          </thead>
          <tbody>
            {% for doc in documents %}
              <tr>
                <td>{{ doc.writer_name_snapshot }}</td>
                <td>{{ doc.document_type }}</td>
                <td>{{ doc.file_name }}</td>
                <td>{{ doc.generated_at.strftime('%Y-%m-%d %H:%M') }}</td>
              </tr>
            {% endfor %}
            {% if not documents %}
              <tr><td colspan="4" class="text-center text-muted">No documents generated yet.</td></tr>
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


def build_document_data(writer: Writer, work: Work, work_writer: WorkWriter):
    today = datetime.datetime.utcnow().date()
    day = today.day
    suffix = "th" if 11 <= day <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")

    return {
        "Date": f"{today.strftime('%B')} {day}{suffix}, {today.year}",
        "Fecha": format_date(today, format="d 'de' MMMM 'del' y", locale="es"),
        "WriterName": writer.full_name,
        "WriterFirstName": writer.first_name,
        "WriterMiddleName": writer.middle_name,
        "WriterLastNames": writer.last_names,
        "WriterIPI": writer.ipi,
        "WriterAddress": writer.address,
        "WriterCity": writer.city,
        "WriterState": writer.state,
        "WriterZipCode": writer.zip_code,
        "PRO": writer.pro,
        "PublisherName": work_writer.publisher or "",
        "PublisherIPI": work_writer.publisher_ipi or "",
        "PublisherAddress": work_writer.publisher_address or "",
        "PublisherCity": work_writer.publisher_city or "",
        "PublisherState": work_writer.publisher_state or "",
        "PublisherZipCode": work_writer.publisher_zip_code or "",
        "WorkTitle": work.title,
    }


def generate_writer_document(writer: Writer, work: Work, work_writer: WorkWriter):
    if writer.has_master_contract:
        document_type = "schedule_1"
        template_path = SCHEDULE_1_TEMPLATE
    else:
        document_type = "full_contract"
        template_path = FULL_CONTRACT_TEMPLATE

    data = build_document_data(writer, work, work_writer)
    works_for_table = [{
        "work_title": work.title,
        "writer_name": writer.full_name,
        "writer_percentage": f"{work_writer.writer_percentage:.2f}%",
        "publisher": work_writer.publisher or "",
    }]

    file_buffer = render_docx_template(template_path, data, works_for_table=works_for_table)

    safe_writer = slugify(writer.full_name)
    safe_work = slugify(work.title)
    prefix = "S1" if document_type == "schedule_1" else "FULL"
    file_name = f"{prefix}_{safe_writer}_{safe_work}.docx"

    if document_type == "full_contract":
        writer.has_master_contract = True

    return document_type, file_name, file_buffer


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
        if not work_title:
            flash("Work title is required.")
            return render_template_string(
                FORM_HTML,
                camps=Camp.query.order_by(Camp.name.asc()).all(),
                default_publisher_address=DEFAULT_PUBLISHER_ADDRESS,
                default_publisher_city=DEFAULT_PUBLISHER_CITY,
                default_publisher_state=DEFAULT_PUBLISHER_STATE,
                default_publisher_zip=DEFAULT_PUBLISHER_ZIP,
            )

        camp = get_or_create_camp(request.form.get("camp_id"), request.form.get("new_camp_name"))
        work = Work(title=work_title, camp_id=camp.id if camp else None)
        db.session.add(work)
        db.session.flush()

        writer_ids = request.form.getlist("writer_id")
        first_names = request.form.getlist("writer_first_name")
        middle_names = request.form.getlist("writer_middle_name")
        last_names_list = request.form.getlist("writer_last_names")
        ipis = request.form.getlist("writer_ipi")
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
                return render_template_string(
                    FORM_HTML,
                    camps=Camp.query.order_by(Camp.name.asc()).all(),
                    default_publisher_address=DEFAULT_PUBLISHER_ADDRESS,
                    default_publisher_city=DEFAULT_PUBLISHER_CITY,
                    default_publisher_state=DEFAULT_PUBLISHER_STATE,
                    default_publisher_zip=DEFAULT_PUBLISHER_ZIP,
                )

            total_split += split_value

            writer_rows.append({
                "selected_writer_id": writer_ids[idx] if idx < len(writer_ids) else "",
                "first_name": first_name,
                "middle_name": middle_name,
                "last_names": last_names,
                "full_name": full_name,
                "ipi": (ipis[idx] or "").strip(),
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
            return render_template_string(
                FORM_HTML,
                camps=Camp.query.order_by(Camp.name.asc()).all(),
                default_publisher_address=DEFAULT_PUBLISHER_ADDRESS,
                default_publisher_city=DEFAULT_PUBLISHER_CITY,
                default_publisher_state=DEFAULT_PUBLISHER_STATE,
                default_publisher_zip=DEFAULT_PUBLISHER_ZIP,
            )

        if abs(total_split - 100.0) >= 0.001:
            flash(f"Total writer split must equal 100%. Current total: {total_split:.2f}%")
            return render_template_string(
                FORM_HTML,
                camps=Camp.query.order_by(Camp.name.asc()).all(),
                default_publisher_address=DEFAULT_PUBLISHER_ADDRESS,
                default_publisher_city=DEFAULT_PUBLISHER_CITY,
                default_publisher_state=DEFAULT_PUBLISHER_STATE,
                default_publisher_zip=DEFAULT_PUBLISHER_ZIP,
            )

        zip_buffer = io.BytesIO()

        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            for row in writer_rows:
                writer = find_existing_writer(row["selected_writer_id"])

                if writer:
                    if not writer.ipi and row["ipi"]:
                        writer.ipi = row["ipi"]
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
                else:
                    writer = Writer(
                        first_name=row["first_name"],
                        middle_name=row["middle_name"],
                        last_names=row["last_names"],
                        full_name=row["full_name"],
                        ipi=row["ipi"],
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
                db.session.flush()

                document_type, file_name, file_buffer = generate_writer_document(writer, work, work_writer)
                zip_file.writestr(file_name, file_buffer.getvalue())

                doc_record = ContractDocument(
                    work_id=work.id,
                    writer_id=writer.id,
                    document_type=document_type,
                    file_name=file_name,
                    writer_name_snapshot=writer.full_name,
                    work_title_snapshot=work.title,
                )
                db.session.add(doc_record)

        db.session.commit()

        zip_buffer.seek(0)
        zip_name = f"{slugify(work.title)}_documents.zip"
        return send_file(
            zip_buffer,
            as_attachment=True,
            download_name=zip_name,
            mimetype="application/zip",
        )

    camps = Camp.query.order_by(Camp.name.asc()).all()
    return render_template_string(
        FORM_HTML,
        camps=camps,
        default_publisher_address=DEFAULT_PUBLISHER_ADDRESS,
        default_publisher_city=DEFAULT_PUBLISHER_CITY,
        default_publisher_state=DEFAULT_PUBLISHER_STATE,
        default_publisher_zip=DEFAULT_PUBLISHER_ZIP,
    )


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
            "ipi": writer.ipi,
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
