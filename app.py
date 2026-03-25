from flask import Flask, render_template_string, , send_file, session, redirect, url_for, jsonify, flash
from docx import Document
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func
from sqlalchemy.engine import make_url
from babel.dates import format_date
import io
import datetime
import os

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'change-this-secret-key')

# Support Render/Railway style Postgres URLs while keeping SQLite for local development
raw_db_url = os.getenv('DATABASE_URL', 'sqlite:///writers.db')
if raw_db_url.startswith('postgres://'):
    raw_db_url = raw_db_url.replace('postgres://', 'postgresql://', 1)
app.config['SQLALCHEMY_DATABASE_URI'] = raw_db_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

TEMPLATE_PATH = os.getenv('TEMPLATE_PATH', 'template/PUBLISHING_AGREEMENT_CONTRACT.docx')
OUTPUT_DIR = os.getenv('OUTPUT_DIR', 'contratos_generados')
TEAM_USERNAME = os.getenv('TEAM_USERNAME')
TEAM_PASSWORD = os.getenv('TEAM_PASSWORD')

os.makedirs(OUTPUT_DIR, exist_ok=True)


class Writer(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    writer_name = db.Column(db.String(200), nullable=False, unique=True, index=True)
    writer_address_line1 = db.Column(db.String(255), default='')
    writer_address_line2 = db.Column(db.String(255), default='')
    pro = db.Column(db.String(20), default='')
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)


def init_db():
    with app.app_context():
        db.create_all()


@app.context_processor
def inject_globals():
    return {
        'team_auth_enabled': bool(TEAM_USERNAME and TEAM_PASSWORD)
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
    .autocomplete-box {
      position: absolute;
      z-index: 1000;
      width: 100%;
      background: white;
      border: 1px solid #dee2e6;
      border-top: none;
      max-height: 220px;
      overflow-y: auto;
      display: none;
    }
    .autocomplete-item {
      padding: 10px 12px;
      cursor: pointer;
      border-bottom: 1px solid #f1f3f5;
    }
    .autocomplete-item:hover { background: #f1f5ff; }
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
        <div class='row mb-3 position-relative'>
          <div class='col'>
          <input type="text" name="fakeusernameremembered" style="display:none">
            <label class='form-label'>Writer Name</label>
            <input id='writerInput' class='form-control' name='writer_name_custom_123' placeholder='Writer Name' autocomplete='off' autocorrect='off' autocapitalize='off' spellcheck='false'>
            <div id='writerSuggestions' class='autocomplete-box'></div>
          </div>
        </div>
        <div class='row mb-3'>
          <div class='col'>
            <label class='form-label'>Writer Address Line 1</label>
            <input class='form-control' name='WriterAddressLine1' id='WriterAddressLine1' placeholder='Writer Address Line 1' autocomplete='new-password'>
          </div>
          <div class='col'>
            <label class='form-label'>Writer Address Line 2</label>
            <input class='form-control' name='WriterAddressLine2' id='WriterAddressLine2' placeholder='Writer Address Line 2' autocomplete='new-password'>
          </div>
        </div>

        <h4 class='mb-3'>Publisher</h4>
        <div class='row mb-3'>
          <div class='col'>
            <label class='form-label'>PRO</label>
            <select class='form-control' name='PRO' id='PRO' onchange='updatePublisher()'>
              <option value=''>Select PRO</option>
              <option value='BMI'>BMI</option>
              <option value='ASCAP'>ASCAP</option>
              <option value='SESAC'>SESAC</option>
            </select>
          </div>
          <div class='col'>
            <label class='form-label'>Publisher Name</label>
            <input class='form-control' name='PublisherName' id='PublisherName' placeholder='Publisher Name' autocomplete='new-password'>
          </div>
        </div>
        <div class='row mb-3'>
          <div class='col'>
            <label class='form-label'>Publisher Address Line 1</label>
            <input class='form-control' name='PublisherAddressLine1' placeholder='Publisher Address Line 1' value='3840 E. Miraloma Ave'>
          </div>
          <div class='col'>
            <label class='form-label'>Publisher Address Line 2</label>
            <input class='form-control' name='PublisherAddressLine2' placeholder='Publisher Address Line 2' value='Anaheim CA 92806'>
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
  const map = { BMI: 'Songs of Afinarte', ASCAP: 'Melodies of Afinarte', SESAC: 'Music of Afinarte' };
  document.getElementById('PublisherName').value = map[pro] || '';
}

function addWorkRow() {
  const container = document.getElementById('workRows');
  const div = document.createElement('div');
  div.className = 'row mb-2 work-row';
  div.innerHTML = `
    <div class='col-md-8'><input class='form-control' name='WorkTitle' placeholder='Work Title'></div>
    <div class='col-md-4 input-group'>
      <input class='form-control' name='WriterSplit' placeholder='Writer Split'>
      <span class='input-group-text'>%</span>
    </div>`;
  container.appendChild(div);
}

const writerInput = document.getElementById('writerInput');
const suggestionsBox = document.getElementById('writerSuggestions');

function hideSuggestions() {
  suggestionsBox.style.display = 'none';
  suggestionsBox.innerHTML = '';
}

function fillWriter(writer) {
  writerInput.value = writer.writer_name || '';
  document.getElementById('WriterAddressLine1').value = writer.writer_address_line1 || '';
  document.getElementById('WriterAddressLine2').value = writer.writer_address_line2 || '';
  if (writer.pro) {
    document.getElementById('PRO').value = writer.pro;
    updatePublisher();
  }
  hideSuggestions();
}

let writerSearchTimeout;
writerInput.addEventListener('input', function () {
  clearTimeout(writerSearchTimeout);
  const q = this.value.trim();
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

    suggestionsBox.innerHTML = writers.map(writer => `
      <div class="autocomplete-item" data-writer='${JSON.stringify(writer).replaceAll("'", "&#39;")}'>
        <strong>${writer.writer_name}</strong><br>
        <small>${writer.writer_address_line1 || ''} ${writer.writer_address_line2 || ''}</small>
      </div>
    `).join('');
    suggestionsBox.style.display = 'block';

    suggestionsBox.querySelectorAll('.autocomplete-item').forEach(item => {
      item.addEventListener('click', () => fillWriter(JSON.parse(item.dataset.writer)));
    });
  }, 200);
});

document.addEventListener('click', function (e) {
  if (!suggestionsBox.contains(e.target) && e.target !== writerInput) {
    hideSuggestions();
  }
});
</script>
</body>
</html>"""


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
</html>"""


def auth_required():
    if not (TEAM_USERNAME and TEAM_PASSWORD):
        return False
    return not session.get('logged_in')


@app.route('/login', methods=['GET', 'POST'])
def login():
    if not (TEAM_USERNAME and TEAM_PASSWORD):
        return redirect(url_for('formulario'))

    if request.method == 'POST':
        username = request.form.get('username', '')
        password = request.form.get('password', '')
        if username == TEAM_USERNAME and password == TEAM_PASSWORD:
            session['logged_in'] = True
            return redirect(url_for('formulario'))
        flash('Incorrect username or password.')
    return render_template_string(LOGIN_HTML)


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


@app.route('/', methods=['GET', 'POST'])
def formulario():
    if auth_required():
        return redirect(url_for('login'))

    if request.method == 'POST':
        data = {k: request.form[k] for k in request.form if k not in ['WorkTitle', 'WriterSplit']}
        works = [
            (title.strip(), split.strip())
            for title, split in zip(request.form.getlist('WorkTitle'), request.form.getlist('WriterSplit'))
            if title.strip()
        ]

        try:
            date_obj = datetime.datetime.strptime(data['Date'], '%Y-%m-%d')
        except ValueError:
            flash('Please enter a valid contract date.')
            return render_template_string(FORM_HTML)

        day = date_obj.day
        suffix = 'th' if 11 <= day <= 13 else {1: 'st', 2: 'nd', 3: 'rd'}.get(day % 10, 'th')
        data['Date'] = f"{date_obj.strftime('%B')} {day}{suffix}, {date_obj.year}"
        data['Fecha'] = format_date(date_obj, format="d 'de' MMMM 'del' y", locale='es')

        save_writer(
            writer_name=data.get('writer_name_custom_123', '').strip(),
            writer_address_line1=data.get('WriterAddressLine1', '').strip(),
            writer_address_line2=data.get('WriterAddressLine2', '').strip(),
            pro=data.get('PRO', '').strip(),
        )

        try:
            filled = fill_contract(data, works)
        except FileNotFoundError:
            flash(f'Template not found: {TEMPLATE_PATH}')
            return render_template_string(FORM_HTML)

        writer_name = data.get('writer_name_custom_123', 'Writer').strip() or 'Writer'
        filename = f"PA {writer_name}.docx"
        return send_file(filled, as_attachment=True, download_name=filename)

    return render_template_string(FORM_HTML)


@app.route('/writers/search')
def search_writers():
    if auth_required():
        return jsonify([])

    q = .args.get('q', '').strip()
    if len(q) < 2:
        return jsonify([])

    writers = (
        Writer.query
        .filter(func.lower(Writer.writer_name).like(f"%{q.lower()}%"))
        .order_by(Writer.writer_name.asc())
        .limit(8)
        .all()
    )

    return jsonify([
        {
            'id': writer.id,
            'writer_name': writer.writer_name,
            'writer_address_line1': writer.writer_address_line1,
            'writer_address_line2': writer.writer_address_line2,
            'pro': writer.pro,
        }
        for writer in writers
    ])


def save_writer(writer_name, writer_address_line1='', writer_address_line2='', pro=''):
    if not writer_name:
        return

    existing = Writer.query.filter(func.lower(Writer.writer_name) == writer_name.lower()).first()
    if existing:
        existing.writer_name = writer_name
        existing.writer_address_line1 = writer_address_line1
        existing.writer_address_line2 = writer_address_line2
        existing.pro = pro
    else:
        db.session.add(Writer(
            writer_name=writer_name,
            writer_address_line1=writer_address_line1,
            writer_address_line2=writer_address_line2,
            pro=pro,
        ))
    db.session.commit()


def fill_contract(data, works):
    if not os.path.exists(TEMPLATE_PATH):
        raise FileNotFoundError(TEMPLATE_PATH)

    doc = Document(TEMPLATE_PATH)

    def replace_all(paragraph):
        text = ''.join(run.text for run in paragraph.runs)
        for k, v in data.items():
            text = text.replace(f'[[{k}]]', str(v))
        for run in paragraph.runs:
            run.text = ''
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
        if '[[ContractTable]]' in p.text:
            table = doc.add_table(rows=1, cols=5)
            table.style = 'Table Grid'
            table.rows[0].cells[0].text = 'Work Title'
            table.rows[0].cells[1].text = 'Songwriter Name'
            table.rows[0].cells[2].text = 'Songwriter Share'
            table.rows[0].cells[3].text = 'Publisher Name'
            table.rows[0].cells[4].text = 'Publisher Share'

            for title, split in works:
                row = table.add_row().cells
                row[0].text = title
                row[1].text = data.get('writer_name_custom_123', '')
                row[2].text = f'{split}%'
                row[3].text = data.get('PublisherName', '')
                row[4].text = f'{split}%'

            p.text = ''
            p._element.addnext(table._element)
            break

    buffer = io.BytesIO()
    doc.save(buffer)
    buffer.seek(0)
    return buffer


init_db()

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.getenv('PORT', '5052')))
