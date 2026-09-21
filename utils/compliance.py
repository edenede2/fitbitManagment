"""Public compliance metadata and participant-disclosure release gates."""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlencode


APP_NAME = "AdmonTracker"
OPERATOR = "Stress & Psychopathology Lab, School of Psychological Sciences, University of Haifa"
PI_NAME = "Prof. Roee Admon"
PI_EMAIL = "radmon@psy.haifa.ac.il"
PI_PHONE = "+972-4-824-0964"
STUDY_NUMBER = "385/23"
DEFAULT_BASE_URL = "https://app.admontracker.online"
PUBLIC_SITE_BASE_URL = "https://admontracker.online"
PUBLIC_HOME_URL = f"{PUBLIC_SITE_BASE_URL}/"
PUBLIC_PRIVACY_URL = f"{PUBLIC_SITE_BASE_URL}/privacy.html"
PUBLIC_TERMS_URL = f"{PUBLIC_SITE_BASE_URL}/terms.html"
PUBLIC_ETHICS_URL = f"{PUBLIC_SITE_BASE_URL}/research-ethics.html"
APPROVED_DISCLOSURE_VERSION = "385-23-GOOGLE-HEALTH-v1.0"
DEFAULT_RETENTION_TEXT = {
    "en": (
        "Measurement data will be retained during the one month collection period and for "
        "one additional month for processing and quality checks. No later than one month "
        "after measurement ends, Google Health data and the link to the participant's "
        "identity will be permanently deleted. The signed consent form and administrative "
        "records that do not contain the measurements will be retained according to "
        "University and research requirements."
    ),
    "he": (
        "נתוני המדידה יישמרו במהלך חודש האיסוף ולמשך חודש נוסף לצורך עיבוד ובדיקת "
        "איכות. לא יאוחר מחודש לאחר סיום המדידה, נתוני Google Health והקישור לזהות "
        "המשתתף/ת יימחקו לצמיתות. טופס ההסכמה ורישומים מנהליים שאינם כוללים את "
        "נתוני המדידה יישמרו בהתאם לכללי האוניברסיטה והמחקר."
    ),
}
DEFAULT_DELETION_TEXT = {
    "en": (
        "If a participant withdraws, collection will stop immediately and the application's "
        "authorization in the demo account assigned to them will be canceled. All collected data "
        "will be permanently deleted unless the participant gives separate and explicit "
        "consent at that time to keep them. Summary results already published cannot be "
        "removed from an existing publication."
    ),
    "he": (
        "במקרה של פרישה, האיסוף ייפסק מיד והרשאת האפליקציה בחשבון הדמו שהוקצה לך "
        "תבוטל. כל הנתונים שנאספו יימחקו לצמיתות, אלא אם תיתן/י באותו מועד הסכמה "
        "נפרדת ומפורשת לשמור אותם. לא ניתן להסיר תוצאות מסכמות שכבר פורסמו."
    ),
}


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
DEFAULT_APPROVED_ADDENDUM_PATHS = {
    "en": COMPLIANCE_ASSETS / "google-health-addendum-385-23-en-approved-v1.0.pdf",
    "he": COMPLIANCE_ASSETS / "google-health-addendum-385-23-he-approved-v1.0.pdf",
}
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
        key="google_health_addendum_en",
        path=DEFAULT_APPROVED_ADDENDUM_PATHS["en"],
        download_name="google-health-addendum-385-23-en-approved-v1.0.pdf",
        mime_type="application/pdf",
        title_en="Approved Google Health participant addendum (English)",
        title_he="נספח Google Health המאושר למשתתף/ת (אנגלית)",
        status_en="Ethics-approved Google Health addendum, final version 1.0.",
        status_he="נספח Google Health שאושר על-ידי ועדת האתיקה, גרסה סופית 1.0.",
    ),
    ResearchDocument(
        key="google_health_addendum_he",
        path=DEFAULT_APPROVED_ADDENDUM_PATHS["he"],
        download_name="google-health-addendum-385-23-he-approved-v1.0.pdf",
        mime_type="application/pdf",
        title_en="Approved Google Health participant addendum (Hebrew)",
        title_he="נספח Google Health המאושר למשתתף/ת (עברית)",
        status_en="Ethics-approved Google Health addendum, final version 1.0.",
        status_he="נספח Google Health שאושר על-ידי ועדת האתיקה, גרסה סופית 1.0.",
    ),
)


def research_documents() -> tuple[ResearchDocument, ...]:
    """Return deployment-aware documents without mutating the stable metadata."""
    approved_paths = approved_addendum_paths()
    return tuple(
        ResearchDocument(
            **{
                **document.__dict__,
                "path": approved_paths["he" if document.key.endswith("_he") else "en"],
            }
        )
        if document.key.startswith("google_health_addendum_")
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
    return os.getenv("PARTICIPANT_DISCLOSURE_VERSION", APPROVED_DISCLOSURE_VERSION).strip()


def retention_text(language: str) -> str:
    suffix = "HE" if language == "he" else "EN"
    return os.getenv(
        f"RESEARCH_RETENTION_TEXT_{suffix}", DEFAULT_RETENTION_TEXT[language]
    ).strip()


def deletion_text(language: str) -> str:
    suffix = "HE" if language == "he" else "EN"
    return os.getenv(
        f"RESEARCH_DELETION_TEXT_{suffix}", DEFAULT_DELETION_TEXT[language]
    ).strip()


def approved_disclosure_ready() -> tuple[bool, list[str]]:
    missing: list[str] = []
    version = disclosure_version()
    if not version or version.upper().startswith("DRAFT"):
        missing.append("an ethics-approved disclosure version")
    for language, path in approved_addendum_paths().items():
        if not path.is_file():
            missing.append(f"the deployed ethics-approved addendum PDF ({language})")
    for language in ("en", "he"):
        if not retention_text(language):
            missing.append(f"approved retention wording ({language})")
        if not deletion_text(language):
            missing.append(f"approved deletion wording ({language})")
    return not missing, missing


def approved_addendum_paths() -> dict[str, Path]:
    """Return the approved bilingual addenda, retaining the legacy override."""
    legacy = os.getenv("APPROVED_GOOGLE_HEALTH_ADDENDUM_PATH", "").strip()
    return {
        language: Path(
            os.getenv(f"APPROVED_GOOGLE_HEALTH_ADDENDUM_PATH_{language.upper()}", "").strip()
            or legacy
            or default_path
        )
        for language, default_path in DEFAULT_APPROVED_ADDENDUM_PATHS.items()
    }


def approved_addendum_path(language: str = "en") -> Path:
    """Backward-compatible accessor for a single approved addendum."""
    return approved_addendum_paths()["he" if language == "he" else "en"]


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
    for language, path in sorted(approved_addendum_paths().items()):
        digest.update(language.encode("ascii"))
        if path.is_file():
            digest.update(path.read_bytes())
    return digest.hexdigest()


def participant_disclosure_text(language: str, provider: str) -> str:
    provider_label = "Google Health" if provider == "google_health" else "Fitbit"
    if provider != "google_health":
        if language == "he":
            return f"""הודעה למשתתף — {provider_label} — מחקר {STUDY_NUMBER}
גרסה: {disclosure_version()}

החיבור מאפשר לצוות המחקר גישת קריאה בלבד למדדים הלבישים שאושרו במסמכי המחקר.
ההשתתפות וההרשאה הן מרצון. ניתן לנתק את החיבור או לפרוש ללא קנס בהתאם לטופס
ההסכמה המאושר. השירות אינו מערכת רפואית או מערכת חירום.

לשאלות או לפרישה: {PI_NAME}, {PI_EMAIL}, {PI_PHONE}.
"""
        return f"""Participant disclosure — {provider_label} — study {STUDY_NUMBER}
Version: {disclosure_version()}

This connection gives the study team read-only access to the wearable measurements
approved in the study documents. Participation and authorization are voluntary.
You may disconnect or withdraw without penalty as described in the approved consent.
This is not a medical or emergency system.

Questions or withdrawal: {PI_NAME}, {PI_EMAIL}, {PI_PHONE}.
"""

    if language == "he":
        return f"""הודעה למשתתף — Google Health — מחקר {STUDY_NUMBER}
גרסה: {disclosure_version()}

המחקר בוחן את השפעת דפוסי השינה על הקשר בין חוויות ילדות לבין תגובות רגשיות,
התנהגותיות וקוגניטיביות בבגרות. החיבור מתבצע באמצעות חשבון Google ייעודי למחקר
(חשבון דמו), ולא באמצעות חשבון Google האישי שלך.

המידע שייאסף: דופק, צעדים, נתוני שינה, פעילות גופנית וקצב נשימה, בהתאם לזמינות
במכשיר ולהרשאות שתאשר/י. הגישה היא לקריאה בלבד; AdmonTracker אינה יכולה לשנות
או למחוק מידע בחשבון הדמו.

לצורכי תפעול ובקרת איכות ייאסף גם מידע טכני על השעון שהוקצה למחקר: דגם השעון,
מצב ורמת הסוללה ומועד הסנכרון האחרון. מידע זה ישמש לאיתור בעיות טעינה או סנכרון
ולבדיקת שלמות איסוף הנתונים.

הנתונים ייאספו במשך חודש אחד ממועד הפעלת החיבור. האיסוף ייפסק בתום החודש,
בעת ניתוק החיבור או בעת פרישה, לפי המועד המוקדם. המידע ישמש רק למחקר זה ולבדיקות
האיכות הנחוצות לביצועו.

הנתונים יישמרו תחת קוד מחקר. הקישור לזהותך יישמר בנפרד במערכת מוגנת של
אוניברסיטת חיפה. הגישה לנתוני המחקר מוגבלת לצוות מורשה; הגישה לקישור לזהות
מוגבלת לחוקר הראשי, לראש המעבדה ולמתכנת המורשה. נעשה שימוש בשירותים מאובטחים
של האוניברסיטה, Google ו-Heroku, בחיבור מוצפן ובהרשאות גישה.

המידע לא יימכר ולא ישמש לפרסום, לשיווק, להחלטות אשראי, תעסוקה או ביטוח,
לאימון מודלים כלליים של בינה מלאכותית או למטרה שאינה קשורה למחקר זה.

הסיכונים האפשריים כוללים פגיעה בפרטיות במקרה של גישה לא מורשית, תקלות בחיבור
או נתונים חלקיים. לא מובטחת תועלת רפואית ישירה. AdmonTracker אינה מערכת רפואית
או מערכת חירום.

משך ושמירה: {retention_text('he')}

פרישה ומחיקה: {deletion_text('he')}

ההשתתפות וההרשאה הן מרצון. ניתן לסרב, לנתק או לפרוש בכל עת ללא קנס. ליצירת קשר:
{PI_NAME}, {PI_EMAIL}, {PI_PHONE}.
"""
    return f"""Participant disclosure — Google Health — study {STUDY_NUMBER}
Version: {disclosure_version()}

This study examines the effect of sleep patterns on the association between childhood
experiences and emotional, behavioral, and cognitive responses in adulthood. The
connection uses a Google account designated for the study (a demo account), not your
personal Google account.

Data collected: heart rate, steps, sleep data, physical activity, and respiratory
rate, depending on device availability and the permissions you approve. Access is
read-only; AdmonTracker cannot change or delete information in the demo account.

The study will also collect the assigned watch's model, battery level and status,
and last synchronization time to identify charging or synchronization problems and
check data completeness.

Data will be collected for one month after activation and will stop at the end of
that month, on disconnection, or on withdrawal, whichever occurs first. It will be
used only for this study and the quality checks necessary to conduct it.

Data is stored under a study code. The identity link is kept separately in a
protected University of Haifa system. Research-data access is limited to authorized
study-team members; access to the identity link is limited to the Principal
Investigator, laboratory head, and authorized programmer. Secure University, Google,
and Heroku services, encrypted transfer, and access controls are used.

The information will not be sold or used for advertising, marketing, credit,
employment, insurance, training general artificial-intelligence models, or a purpose
unrelated to this study.

Possible risks include loss of privacy following unauthorized access, connection
problems, or incomplete data. No direct medical benefit is promised. AdmonTracker
is not a medical or emergency system.

Duration and retention: {retention_text('en')}

Withdrawal and deletion: {deletion_text('en')}

Participation and authorization are voluntary. You may refuse, disconnect, or
withdraw at any time without penalty. Contact: {PI_NAME}, {PI_EMAIL}, {PI_PHONE}.
"""
