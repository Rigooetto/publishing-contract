from flask import Flask, render_template_string, request, send_file, session, redirect, url_for, jsonify, flash
from docx import Document
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, or_
from babel.dates import format_date
import io
import datetime
import os

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "change-this-secret-key")

raw_db_url = os.getenv("DATABASE_URL", "sqlite:///writers.db")
if raw_db_url.startswith("postgres://"):
    raw_db_url = raw_db_url.replace("postgres://", "postgresql://", 1)

app.config["SQLALCHEMY_DATABASE_URI"] = raw_db_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)

TEMPLATE_PATH = os.getenv("TEMPLATE_PATH", "template/PUBLISHING_AGREEMENT_CONTRACT.docx")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "contratos_generados")
TEAM_USERNAME = os.getenv("TEAM_USERNAME")
TEAM_PASSWORD = os.getenv("TEAM_PASSWORD")

os.makedirs(OUTPUT_DIR, exist_ok=True)


class Writer(db.Model):
    id = db.Column(db.Integer, primary_key=True)

    writer_first_name = db.Column(db.String(100), default="", index=True)
    writer_middle_name = db.Column(db.String(100), default="")
    writer_last_names = db.Column(db.String(150), default="")
    writer_full_name = db.Column(db.String(250), nullable=False, unique=True, index=True)

    writer_address = db.Column(db.String(255), default="")
    writer_city = db.Column(db.String(100), default="")
    writer_state = db.Column(db.String(100), default="")
    writer_zip_code = db.Column(db.String(20), default="")

    writer_ipi = db.Column(db.String(50), default="")
    pro = db.Column(db.String(20), default="")

    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)


def init_db():
    with app.app_context():
        db.create_all()


@app.context_processor
def inject_globals():
    return {
        "team_auth_enabled": bool(TEAM_USERNAME and TEAM_PASSWORD)
    }


FORM_HTML = """<!DOCTYPE html>
<html>
<head>
  <meta charset='UTF-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1'>
  <title>Publishing Agreement</title>
  <link href='https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css' rel='stylesheet'>
  <style>
    body { background: #f8f9fa; }
    .card { border-radius: 18px; }

    h4 {
      margin-top: 8px;
      margin-bottom: 14px;
      font-weight: 700;
    }

    .form-label {
      font-weight: 600;
      font-size: 0.95rem;
      margin-bottom: 6px;
    }

    .card-body {
      max-width: 1100px;
      margin: 0 auto;
    }

    .row.mb-4 {
      padding-bottom: 8px;
      border-bottom: 1px solid #f1f3f5;
    }

    .autocomplete-wrap {
      position: relative;
    }

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
      box-shadow: 0 6px 18px rgba(0, 0, 0, 0.08);
    }

    .autocomplete-item {
      padding: 10px 12px;
      cursor: pointer;
      border-bottom: 1px solid #f1f3f5;
    }

    .autocomplete-item:hover {
      background: #f1f5ff;
    }
  </style>
</head>
<body>
<div class='container py-4'>
  {% with messages = get_flashed_messages() %}
    {% if messages %}
      {% for message in messages %}
        <div class='alert alert-warning'>{{ message }}</div>
      {% endfor %}
    {% endif %}
  {% endwith %}

  <div class='card shadow-sm'>
    <div class='card-body p-4'>
      <div class='d-flex justify-content-between align-items-center mb-4'>
        <div>
          <h2 class='mb-1'>Publishing Agreement</h2>
          <p class='text-muted mb-0'>Generate contracts and save writer info for future auto-fill.</p>
        </div>
        {% if team_auth_enabled and session.get('logged_in') %}
          <a href='{{ url_for("logout") }}' class='btn btn-outline-secondary btn-sm'>Log out</a>
        {% endif %}
      </div>

      <form method='post' autocomplete='off'>
        <h4 class='mb-3'>Writer</h4>

        <div class='row mb-3'>
          <div class='col-md-4 autocomplete-wrap'>
            <label class='form-label'>First Name</label>
            <input class='form-control' name='WriterFirstName' id='WriterFirstName' placeholder='First Name'
              autocomplete='off' autocorrect='off' autocapitalize='off' spellcheck='false'>
            <div id='writerSuggestions' class='autocomplete-box'></div>
          </div>

          <div class='col-md-4'>
            <label class='form-label'>Middle Name</label>
            <input class='form-control' name='WriterMiddleName' id='WriterMiddleName' placeholder='Middle Name'
              autocomplete='off' autocorrect='off' autocapitalize='off' spellcheck='false'>
          </div>

          <div class='col-md-4'>
            <label class='form-label'>Last Name(s)</label>
            <input class='form-control' name='WriterLastNames' id='WriterLastNames' placeholder='Last Name(s)'
              autocomplete='off' autocorrect='off' autocapitalize='off' spellcheck='false'>
          </div>
        </div>

        <div class='row mb-3'>
          <div class='col-md-4'>
            <label class='form-label'>Writer IPI #</label>
            <input class='form-control' name='WriterIPI' id='WriterIPI' placeholder='IPI Number' autocomplete='off'>
          </div>
        </div>

        <div class='row mb-4'>
          <div class='col-md-6'>
            <label class='form-label'>Address</label>
            <input class='form-control' name='WriterAddress' id='WriterAddress' placeholder='Address' autocomplete='new-password'>
          </div>
          <div class='col-md-2'>
            <label class='form-label'>City</label>
            <input class='form-control' name='WriterCity' id='WriterCity' placeholder='City' autocomplete='new-password'>
          </div>
          <div class='col-md-2'>
            <label class='form-label'>State</label>
            <input class='form-control' name='WriterState' id='WriterState' placeholder='State' autocomplete='new-password'>
          </div>
          <div class='col-md-2'>
            <label class='form-label'>Zip Code</label>
            <input class='form-control' name='WriterZipCode' id='WriterZipCode' placeholder='Zip Code' autocomplete='new-password'>
          </div>
        </div>

        <h4 class='mb-3'>Publisher</h4>

        <div class='row mb-3'>
          <div class='col-md-4'>
            <label class='form-label'>PRO</label>
            <select class='form-control' name='PRO' id='PRO' onchange='updatePublisher()'>
              <option value=''>Select PRO</option>
              <option value='BMI'>BMI</option>
              <option value='ASCAP'>ASCAP</option>
              <option value='SESAC'>SESAC</option>
            </select>
          </div>

          <div class='col-md-8'>
            <label class='form-label'>Publisher Name</label>
            <input class='form-control' name='PublisherName' id='PublisherName' placeholder='Publisher Name' autocomplete='new-password'>
          </div>
        </div>

        <div class='row mb-4'>
          <div class='col-md-6'>
            <label class='form-label'>Publisher Address Line 1</label>
            <input class='form-control' name='PublisherAddressLine1' id='PublisherAddressLine1' placeholder='Publisher Address Line 1' value='3840 E. Miraloma Ave'>
          </div>
          <div class='col-md-6'>
            <label class='form-label'>Publisher Address Line 2</label>
            <input class='form-control' name='PublisherAddressLine2' id='PublisherAddressLine2' placeholder='Publisher Address Line 2' value='Anaheim CA 92806'>
          </div>
        </div>

        <h4 class='mb-3'>Contract</h4>
        <div class='row mb-3'>
          <div class='col-md-4'>
            <label class='form-label'>Date</label>
            <input class='form-control' name='Date' type='date' required>
          </div>
        </div>

        <div id='workRows'>
          <div class='row mb-2 work-row'>
            <div class='col-md-8'>
              <input class='form-control' name='WorkTitle' placeholder='Work Title' autocomplete='new-password'>
            </div>
            <div class='col-md-4 input-group'>
              <input class='form-control' name='WriterSplit' placeholder='Writer Split' autocomplete='new-password'>
              <span class='input-group-text'>%</span>
            </div>
          </div>
        </div>

        <div class='d-flex gap-2 mt-3'>
          <button type='button' class='btn btn-outline-primary' onclick='addWorkRow()'>Add Another Work</button>
          <button type='submit' class='btn btn-success'>Generate Contract</button>
        </div>
      </form>
    </div>
  </div>
</div>

<script>
function updatePublisher() {
  const pro = document.getElementById('PRO').value;
  const map = {
    BMI: 'Songs of Afinarte',
    ASCAP: 'Melodies of Afinarte',
    SESAC: 'Music of Afinarte'
  };
  document.getElementById('PublisherName').value = map[pro] || '';
}

function addWorkRow() {
  const container = document.getElementById('workRows');
  const div = document.createElement('div');
  div.className = 'row mb-2 work-row';
  div.innerHTML = `
    <div class='col-md-8'><input class='form-control' name='WorkTitle' placeholder='Work Title' autocomplete='new-password'></div>
    <div class='col-md-4 input-group'>
      <input class='form-control' name='WriterSplit' placeholder='Writer Split' autocomplete='new-password'>
      <span class='input-group-text'>%</span>
    </div>`;
  container.appendChild(div);
}

const writerFirstName = document.getElementById('WriterFirstName');
const writerMiddleName = document.getElementById('WriterMiddleName');
const writerLastNames = document.getElementById('WriterLastNames');
const writerSuggestions = document.getElementById('writerSuggestions');

function getWriterSearchText() {
  return [
    writerFirstName.value.trim(),
    writerMiddleName.value.trim(),
    writerLastNames.value.trim()
  ].filter(Boolean).join(' ');
}

function hideSuggestions() {
  writerSuggestions.style.display = 'none';
  writerSuggestions.innerHTML = '';
}

function fillWriter(writer) {
  document.getElementById('WriterFirstName').value = writer.writer_first_name || '';
  document.getElementById('WriterMiddleName').value = writer.writer_middle_name || '';
  document.getElementById('WriterLastNames').value = writer.writer_last_names || '';
  document.getElementById('WriterIPI').value = writer.writer_ipi || '';
  document.getElementById('WriterAddress').value = writer.writer_address || '';
  document.getElementById('WriterCity').value = writer.writer_city || '';
  document.getElementById('WriterState').value = writer.writer_state || '';
  document.getElementById('WriterZipCode').value = writer.writer_zip_code || '';

  if (writer.pro) {
    document.getElementById('PRO').value = writer.pro;
    updatePublisher();
  }

  hideSuggestions();
}

let writerSearchTimeout;

async function runWriterSearch() {
  clearTimeout(writerSearchTimeout);

  const q = getWriterSearchText();
  if (q.length < 2) {
    hideSuggestions();
    return;
  }

  writerSearchTimeout = setTimeout(async () => {
    const resp = await fetch(`/writers/search?q=${encodeURIComponent(q)}`);
    const writers = await resp.json();

    if (!writers.length) {
      hideSuggestions();
      return;
    }

    writerSuggestions.innerHTML = writers.map(writer => `
      <div class="autocomplete-item" data-writer='${JSON.stringify(writer).replaceAll("'", "&#39;")}'>
        <strong>${writer.writer_full_name}</strong><br>
        <small>${writer.writer_city || ''}${writer.writer_city && writer.writer_state ? ', ' : ''}${writer.writer_state || ''}</small>
      </div>
    `).join('');

    writerSuggestions.style.display = 'block';

    writerSuggestions.querySelectorAll('.autocomplete-item').forEach(item => {
      item.addEventListener('click', () => fillWriter(JSON.parse(item.dataset.writer)));
    });
  }, 200);
}

writerFirstName.addEventListener('input', runWriterSearch);
writerMiddleName.addEventListener('input', runWriterSearch);
writerLastNames.addEventListener('input', runWriterSearch);

document.addEventListener('click', function (e) {
  if (
    !writerSuggestions.contains(e.target) &&
    e.target !== writerFirstName &&
    e.target !== writerMiddleName &&
    e.target !== writerLastNames
  ) {
    hideSuggestions();
  }
});
</script>
</body>
</html>
"""


LOGIN_HTML = """<!DOCTYPE html>
<html>
<head>
  <meta charset='UTF-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1'>
  <title>Team Login</title>
  <link href='https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css' rel='stylesheet'>
</head>
<body class='bg-light'>
  <div class='container py-5'>
    <div class='row justify-content-center'>
      <div class='col-md-4'>
        <div class='card shadow-sm'>
          <div class='card-body p-4'>
            <h3 class='mb-3'>Team Login</h3>
            {% with messages = get_flashed_messages() %}
              {% if messages %}
                {% for message in messages %}
                  <div class='alert alert-danger'>{{ message }}</div>
                {% endfor %}
              {% endif %}
            {% endwith %}
            <form method='post'>
              <div class='mb-3'>
                <label class='form-label'>Username</label>
                <input class='form-control' name='username' required>
              </div>
              <div class='mb-3'>
                <label class='form-label'>Password</label>
                <input type='password' class='form-control' name='password' required>
              </div>
              <button class='btn btn-primary w-100'>Log in</button>
            </form>
          </div>
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
        data = {k: request.form[k] for k in request.form if k not in ["WorkTitle", "WriterSplit"]}

        works = [
            (title.strip(), split.strip())
            for title, split in zip(request.form.getlist("WorkTitle"), request.form.getlist("WriterSplit"))
            if title.strip()
        ]

        try:
            date_obj = datetime.datetime.strptime(data["Date"], "%Y-%m-%d")
        except ValueError:
            flash("Please enter a valid contract date.")
            return render_template_string(FORM_HTML)

        day = date_obj.day
        suffix = "th" if 11 <= day <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
        data["Date"] = f"{date_obj.strftime('%B')} {day}{suffix}, {date_obj.year}"
        data["Fecha"] = format_date(date_obj, format="d 'de' MMMM 'del' y", locale="es")

        first_name = data.get("WriterFirstName", "").strip()
        middle_name = data.get("WriterMiddleName", "").strip()
        last_names = data.get("WriterLastNames", "").strip()
        full_name = " ".join(part for part in [first_name, middle_name, last_names] if part)

        data["WriterName"] = full_name
        data["WriterFirstName"] = first_name
        data["WriterMiddleName"] = middle_name
        data["WriterLastNames"] = last_names
        data["WriterIPI"] = data.get("WriterIPI", "").strip()
        data["WriterAddress"] = data.get("WriterAddress", "").strip()
        data["WriterCity"] = data.get("WriterCity", "").strip()
        data["WriterState"] = data.get("WriterState", "").strip()
        data["WriterZipCode"] = data.get("WriterZipCode", "").strip()

        save_writer(
            writer_first_name=first_name,
            writer_middle_name=middle_name,
            writer_last_names=last_names,
            writer_full_name=full_name,
            writer_address=data.get("WriterAddress", "").strip(),
            writer_city=data.get("WriterCity", "").strip(),
            writer_state=data.get("WriterState", "").strip(),
            writer_zip_code=data.get("WriterZipCode", "").strip(),
            writer_ipi=data.get("WriterIPI", "").strip(),
            pro=data.get("PRO", "").strip(),
        )

        try:
            filled = fill_contract(data, works)
        except FileNotFoundError:
            flash(f"Template not found: {TEMPLATE_PATH}")
            return render_template_string(FORM_HTML)

        filename_name = full_name or "Writer"
        filename = f"PA {filename_name}.docx"
        return send_file(filled, as_attachment=True, download_name=filename)

    return render_template_string(FORM_HTML)


@app.route("/writers/search")
def search_writers():
    if auth_required():
        return jsonify([])

    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])

    like_q = f"%{q.lower()}%"

    writers = (
        Writer.query
        .filter(
            or_(
                func.lower(Writer.writer_full_name).like(like_q),
                func.lower(Writer.writer_first_name).like(like_q),
                func.lower(Writer.writer_middle_name).like(like_q),
                func.lower(Writer.writer_last_names).like(like_q),
            )
        )
        .order_by(Writer.writer_full_name.asc())
        .limit(8)
        .all()
    )

    return jsonify([
        {
            "id": writer.id,
            "writer_first_name": writer.writer_first_name,
            "writer_middle_name": writer.writer_middle_name,
            "writer_last_names": writer.writer_last_names,
            "writer_full_name": writer.writer_full_name,
            "writer_address": writer.writer_address,
            "writer_city": writer.writer_city,
            "writer_state": writer.writer_state,
            "writer_zip_code": writer.writer_zip_code,
            "writer_ipi": writer.writer_ipi,
            "pro": writer.pro,
        }
        for writer in writers
    ])


def save_writer(
    writer_first_name="",
    writer_middle_name="",
    writer_last_names="",
    writer_full_name="",
    writer_address="",
    writer_city="",
    writer_state="",
    writer_zip_code="",
    writer_ipi="",
    pro=""
):
    if not writer_full_name:
        return

    existing = Writer.query.filter(func.lower(Writer.writer_full_name) == writer_full_name.lower()).first()

    if existing:
        existing.writer_first_name = writer_first_name
        existing.writer_middle_name = writer_middle_name
        existing.writer_last_names = writer_last_names
        existing.writer_full_name = writer_full_name
        existing.writer_address = writer_address
        existing.writer_city = writer_city
        existing.writer_state = writer_state
        existing.writer_zip_code = writer_zip_code
        existing.writer_ipi = writer_ipi
        existing.pro = pro
    else:
        db.session.add(Writer(
            writer_first_name=writer_first_name,
            writer_middle_name=writer_middle_name,
            writer_last_names=writer_last_names,
            writer_full_name=writer_full_name,
            writer_address=writer_address,
            writer_city=writer_city,
            writer_state=writer_state,
            writer_zip_code=writer_zip_code,
            writer_ipi=writer_ipi,
            pro=pro,
        ))

    db.session.commit()


def fill_contract(data, works):
    if not os.path.exists(TEMPLATE_PATH):
        raise FileNotFoundError(TEMPLATE_PATH)

    doc = Document(TEMPLATE_PATH)

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

            for title, split in works:
                row = table.add_row().cells
                row[0].text = title
                row[1].text = data.get("WriterName", "")
                row[2].text = f"{split}%"
                row[3].text = data.get("PublisherName", "")
                row[4].text = f"{split}%"

            p.text = ""
            p._element.addnext(table._element)
            break

    buffer = io.BytesIO()
    doc.save(buffer)
    buffer.seek(0)
    return buffer


init_db()

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.getenv("PORT", "5052")))
