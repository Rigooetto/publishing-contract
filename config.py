import os

# ── DocuSign ──────────────────────────────────────────────────────────────────
DOCUSIGN_ACCOUNT_ID       = os.getenv("DOCUSIGN_ACCOUNT_ID", "")
DOCUSIGN_BASE_PATH        = os.getenv("DOCUSIGN_BASE_PATH", "https://demo.docusign.net/restapi")
DOCUSIGN_AUTH_SERVER      = os.getenv("DOCUSIGN_AUTH_SERVER", "account-d.docusign.com")
DOCUSIGN_INTEGRATION_KEY  = os.getenv("DOCUSIGN_INTEGRATION_KEY", "")
DOCUSIGN_USER_ID          = os.getenv("DOCUSIGN_USER_ID", "")
DOCUSIGN_PRIVATE_KEY      = os.getenv("DOCUSIGN_PRIVATE_KEY", "")

# ── Auth ──────────────────────────────────────────────────────────────────────
TEAM_USERNAME = os.getenv("TEAM_USERNAME")
TEAM_PASSWORD = os.getenv("TEAM_PASSWORD")

# ── Publisher defaults ────────────────────────────────────────────────────────
DEFAULT_PUBLISHER_ADDRESS = "3840 E. Miraloma Ave"
DEFAULT_PUBLISHER_CITY    = "Anaheim"
DEFAULT_PUBLISHER_STATE   = "CA"
DEFAULT_PUBLISHER_ZIP     = "92806"

# ── Google Drive ──────────────────────────────────────────────────────────────
GOOGLE_DRIVE_FOLDER_ID      = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")

# ── Contract templates ────────────────────────────────────────────────────────
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
