"""Public compliance metadata and participant-disclosure release gates."""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlencode


APP_NAME = "Wearable Research Manager"
OPERATOR = "Stress & Psychopathology Lab, School of Psychological Sciences, University of Haifa"
PI_NAME = "Prof. Roee Admon"
PI_EMAIL = "radmon@psy.haifa.ac.il"
PI_PHONE = "+972-4-824-0964"
STUDY_NUMBER = "385/23"
DEFAULT_BASE_URL = "https://app.admontracker.online"
DRAFT_DISCLOSURE_VERSION = "DRAFT-NOT-ETHICS-APPROVED"


@dataclass(frozen=True)
class ResearchDocument:
    key: str
    path: Path
    download_name: str
    mime_type: str
    title_en: str
    title_he: str
    status_en: str
    status_he: str

    @property
    def exists(self) -> bool:
        return self.path.is_file()

    def sha256(self) -> str:
        digest = hashlib.sha256()
        with self.path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()


PROJECT_ROOT = Path(__file__).resolve().parent.parent
COMPLIANCE_ASSETS = PROJECT_ROOT / "assets" / "compliance"
DEFAULT_APPROVED_ADDENDUM_PATH = PROJECT_ROOT / "docs" / "compliance" / "google_health_addendum_approved.pdf"
RESEARCH_DOCUMENTS = (
    ResearchDocument(
        key="study_approval",
        path=COMPLIANCE_ASSETS / "ethics-approval-385-23-en.pdf",
        download_name="ethics-approval-385-23-en.pdf",
        mime_type="application/pdf",
        title_en="Study ethics approval 385/23 (English)",
        title_he="אישור ועדת האתיקה למחקר 385/23 (אנגלית)",
        status_en="Project-specific ethics approval dated 17 February 2026.",
        status_he="אישור אתיקה ייעודי למחקר מיום 17 בפברואר 2026.",
    ),
    ResearchDocument(
        key="institutional_accreditation",
        path=COMPLIANCE_ASSETS / "institutional-ethics-governance-he.pdf",
        download_name="institutional-ethics-governance-he.pdf",
        mime_type="application/pdf",
        title_en="University ethics committee framework (Hebrew)",
        title_he="מסגרת ועדות האתיקה של האוניברסיטה (עברית)",
        status_en="University of Haifa ethics committee framework.",
        status_he="מסגרת ועדות האתיקה של אוניברסיטת חיפה.",
    ),
    ResearchDocument(
        key="pilot_consent",
        path=COMPLIANCE_ASSETS / "pilot-consent-baseline-he.pdf",
        download_name="participant-informed-consent-385-23-he.pdf",
        mime_type="application/pdf",
        title_en="Participant informed-consent form (Hebrew)",
        title_he="טופס הסכמה מדעת למשתתף (עברית)",
        status_en="Participant information and informed-consent form for the study.",
        status_he="דף מידע וטופס הסכמה מדעת למשתתפי המחקר.",
    ),
    ResearchDocument(
        key="google_health_addendum",
        path=DEFAULT_APPROVED_ADDENDUM_PATH,
        download_name="google-health-addendum-385-23-approved.pdf",
        mime_type="application/pdf",
        title_en="Google Health participant addendum",
        title_he="נספח משתתף עבור Google Health",
        status_en="Participant information for Google Health authorization.",
        status_he="מידע למשתתף לצורך הרשאת Google Health.",
    ),
)


def research_documents() -> tuple[ResearchDocument, ...]:
    """Return deployment-aware documents without mutating the stable metadata."""
    approved_path = approved_addendum_path()
    return tuple(
        ResearchDocument(**{**document.__dict__, "path": approved_path})
        if document.key == "google_health_addendum"
        else document
        for document in RESEARCH_DOCUMENTS
    )


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().casefold() in {"1", "true", "yes", "on"}


def participant_disclosure_enforced() -> bool:
    return env_flag("PARTICIPANT_DISCLOSURE_ENFORCED", False)


def disclosure_version() -> str:
    return os.getenv("PARTICIPANT_DISCLOSURE_VERSION", DRAFT_DISCLOSURE_VERSION).strip()


def retention_text(language: str) -> str:
    suffix = "HE" if language == "he" else "EN"
    return os.getenv(f"RESEARCH_RETENTION_TEXT_{suffix}", "").strip()


def deletion_text(language: str) -> str:
    suffix = "HE" if language == "he" else "EN"
    return os.getenv(f"RESEARCH_DELETION_TEXT_{suffix}", "").strip()


def approved_disclosure_ready() -> tuple[bool, list[str]]:
    missing: list[str] = []
    version = disclosure_version()
    if not version or version.upper().startswith("DRAFT"):
        missing.append("an ethics-approved disclosure version")
    if not approved_addendum_path().is_file():
        missing.append("the deployed ethics-approved addendum PDF")
    for language in ("en", "he"):
        if not retention_text(language):
            missing.append(f"approved retention wording ({language})")
        if not deletion_text(language):
            missing.append(f"approved deletion wording ({language})")
    return not missing, missing


def approved_addendum_path() -> Path:
    configured = os.getenv("APPROVED_GOOGLE_HEALTH_ADDENDUM_PATH", "").strip()
    return Path(configured) if configured else DEFAULT_APPROVED_ADDENDUM_PATH


def assert_disclosure_can_be_enforced() -> None:
    ready, missing = approved_disclosure_ready()
    if participant_disclosure_enforced() and not ready:
        raise RuntimeError(
            "Participant disclosure enforcement is enabled without " + ", ".join(missing)
        )


def app_base_url() -> str:
    return os.getenv("APP_BASE_URL", DEFAULT_BASE_URL).strip().rstrip("/")


def participant_authorization_url(state: str) -> str:
    return f"{app_base_url()}/Participant_Authorization?{urlencode({'state': state})}"


def disclosure_document_hash() -> str:
    material = "\n".join(
        [
            disclosure_version(),
            retention_text("en"),
            retention_text("he"),
            deletion_text("en"),
            deletion_text("he"),
        ]
    )
    digest = hashlib.sha256(material.encode("utf-8"))
    path = approved_addendum_path()
    if path.is_file():
        digest.update(path.read_bytes())
    return digest.hexdigest()


def participant_disclosure_text(language: str, provider: str) -> str:
    provider_label = "Google Health" if provider == "google_health" else "Fitbit"
    if language == "he":
        return f"""הודעה למשתתף — {provider_label} — מחקר {STUDY_NUMBER}
גרסה: {disclosure_version()}

מטרת הגישה: איסוף מדדים לבישים שאושרו לצורך בחינת הקשר בין חוויות ילדות,
דפוסי שינה ותגובות רגשיות, קוגניטיביות והתנהגותיות בבגרות.

המידע: צעדים ופעילות, קלוריות, דופק, שונות קצב לב, טמפרטורת עור, קצב נשימה,
שינה וחותמות זמן, לפי זמינות המכשיר והפרוטוקול המאושר. הגישה היא לקריאה בלבד.

אחסון ואבטחה: מזהה מחקר בדוי; מטא-נתונים ב-Google Sheets; ארכיון גולמי
ב-Shared Drive אוניברסיטאי מוגבל; סודות ואסימוני OAuth ב-Google Secret Manager;
אירוח ב-Heroku. הגישה מוגבלת לצוות המחקר ולספקים מאושרים. המידע אינו נמכר,
אינו משמש לפרסום ואינו משמש לאימון מודלי AI/ML כלליים.

סיכונים ותועלת: קיים סיכון קטן לפגיעה בפרטיות, לשגיאת חיבור או לאי-נוחות.
לא מובטחת תועלת רפואית. השירות אינו מכשיר רפואי או מערכת חירום.

משך ושמירה: {retention_text('he')}

פרישה ומחיקה: {deletion_text('he')}

ההשתתפות וההרשאה הן מרצון. ניתן לעצור איסוף עתידי ללא קנס. ליצירת קשר:
{PI_NAME}, {PI_EMAIL}, {PI_PHONE}.
"""
    return f"""Participant disclosure — {provider_label} — study {STUDY_NUMBER}
Version: {disclosure_version()}

Purpose: collect ethics-approved wearable measurements to examine relationships
between childhood experiences, sleep patterns, and emotional, cognitive, and
behavioral responses in adulthood.

Data: steps/activity, calories, heart rate, heart-rate variability, skin
temperature, respiratory rate, sleep, and timestamps, subject to device
availability and the approved protocol. Access is read-only.

Storage and security: a pseudonymous study ID is used; metadata is stored in
Google Sheets; raw archives in a restricted University Shared Drive; OAuth
secrets and tokens in Google Secret Manager; and the app is hosted on Heroku.
Access is limited to the study team and approved processors. Data is not sold,
used for advertising, or used to train general-purpose AI/ML models.

Risks and benefits: there is a small risk of privacy loss, connection errors, or
discomfort. No direct medical benefit is promised. This is not a medical device
or emergency-monitoring system.

Duration and retention: {retention_text('en')}

Withdrawal and deletion: {deletion_text('en')}

Participation and authorization are voluntary. Future collection can be stopped
without penalty. Contact: {PI_NAME}, {PI_EMAIL}, {PI_PHONE}.
"""
