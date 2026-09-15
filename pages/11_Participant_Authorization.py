"""Public, consent-aware bridge between a study invitation and provider OAuth."""

import streamlit as st

from entity.Sheet import Spreadsheet
from utils.compliance import (
    PI_EMAIL,
    research_documents,
    assert_disclosure_can_be_enforced,
    disclosure_version,
    participant_disclosure_enforced,
    participant_disclosure_text,
)
from utils.health_connect_links import build_provider_authorize_url
from utils.health_token_store import (
    acknowledge_oauth_state,
    assert_state_authorized_for_callback,
    resolve_oauth_state_any_provider,
)


st.set_page_config(
    page_title="Participant Authorization - Wearable Research Manager",
    page_icon="🔗",
    layout="centered",
)

language_label = st.radio("Language / שפה", ["English", "עברית"], horizontal=True)
language = "he" if language_label == "עברית" else "en"
is_hebrew = language == "he"

st.title("הרשאת משתתף" if is_hebrew else "Participant Authorization")
st.caption("University of Haifa • Study 385/23")

if not participant_disclosure_enforced():
    st.warning(
        "זרימת ההסכמה של Google Health טרם הופעלה. יש לפנות לצוות המחקר לקבלת קישור עדכני."
        if is_hebrew
        else (
            "The ethics-approved Google Health disclosure flow is not active yet. "
            "Contact the study team for a current authorization link."
        )
    )
    st.page_link("pages/10_Research_Ethics.py", label="Ethics documents / מסמכי אתיקה")
    st.stop()

try:
    assert_disclosure_can_be_enforced()
except RuntimeError:
    st.error(
        "ההרשאה אינה זמינה משום שמסמכי האתיקה טרם הוגדרו במלואם."
        if is_hebrew
        else "Authorization is unavailable because the approved ethics wording is incomplete."
    )
    st.stop()

state = str(st.query_params.get("state") or "").strip()
if not state:
    st.info(
        "נדרש קישור אישי ותקף מצוות המחקר."
        if is_hebrew
        else "A valid personal link from the study team is required."
    )
    st.stop()

spreadsheet_key = st.secrets.get("spreadsheet_key", "")
if not spreadsheet_key:
    st.error("שירות ההרשאה אינו זמין." if is_hebrew else "The authorization service is unavailable.")
    st.stop()
spreadsheet = Spreadsheet(name="Fitbit Database", api_key=spreadsheet_key)

try:
    state_row = resolve_oauth_state_any_provider(spreadsheet, state=state)
except Exception:
    st.error(
        "הקישור אינו מוכר, פג תוקף או כבר נוצל. יש לבקש קישור חדש."
        if is_hebrew
        else "This link is unknown, expired, or already used. Please request a new link."
    )
    st.stop()

provider = str(state_row.get("provider") or "")
provider_label = "Google Health" if provider == "google_health" else "Fitbit"
disclosure = participant_disclosure_text(language, provider)

st.subheader(("חיבור אל " if is_hebrew else "Connect to ") + provider_label)
st.info(
    "ניתן לקרוא ולשמור את ההודעה לפני קבלת החלטה."
    if is_hebrew
    else "You can review and save this disclosure before deciding."
)
st.text(disclosure)
st.download_button(
    "שמירת ההודעה" if is_hebrew else "Save this disclosure",
    data=disclosure.encode("utf-8"),
    file_name=f"participant-disclosure-{disclosure_version()}-{language}.txt",
    mime="text/plain; charset=utf-8",
)

with st.expander("מסמכי מחקר" if is_hebrew else "Study documents"):
    for document in research_documents():
        if not document.exists:
            continue
        st.download_button(
            document.title_he if is_hebrew else document.title_en,
            data=document.path.read_bytes(),
            file_name=document.download_name,
            mime=document.mime_type,
            key=f"participant_{document.key}_{language}",
        )

already_acknowledged = bool(str(state_row.get("participant_acknowledged_at") or "").strip())
if not already_acknowledged:
    with st.form("participant_disclosure_confirmation"):
        adult = st.checkbox(
            "מלאו לי 18 שנים." if is_hebrew else "I am at least 18 years old."
        )
        consent = st.checkbox(
            "קיבלתי והשלמתי את ההסכמה למחקר שאושרה אתית."
            if is_hebrew
            else "I received and completed the ethics-approved study consent."
        )
        reviewed = st.checkbox(
            "קראתי את ההודעה, יכולתי לשמור עותק, ואני מאשר/ת מרצון את הגישה המפורטת."
            if is_hebrew
            else (
                "I reviewed the disclosure, could save a copy, and voluntarily authorize "
                "the listed read-only access."
            )
        )
        understands = st.checkbox(
            "ברור לי כיצד לפרוש, לנתק את החיבור ולבקש מחיקה."
            if is_hebrew
            else "I understand how to withdraw, disconnect, and request deletion."
        )
        submitted = st.form_submit_button(
            "אישור והמשך" if is_hebrew else "Acknowledge and continue",
            type="primary",
        )

    if submitted:
        if not all((adult, consent, reviewed, understands)):
            st.error(
                "יש לאשר את כל הסעיפים כדי להמשיך."
                if is_hebrew
                else "All confirmations are required before continuing."
            )
            st.stop()
        try:
            state_row = acknowledge_oauth_state(
                spreadsheet,
                state_row=state_row,
                language=language,
            )
            already_acknowledged = True
        except Exception:
            st.error(
                "לא ניתן לשמור את האישור באופן מאובטח. יש לפנות לצוות המחקר."
                if is_hebrew
                else "The acknowledgement could not be stored securely. Contact the study team."
            )
            st.stop()

if already_acknowledged:
    try:
        assert_state_authorized_for_callback(state_row)
        provider_url = build_provider_authorize_url(spreadsheet, state_row=state_row)
    except Exception:
        st.error(
            "לא ניתן להכין את החיבור. יש לבקש קישור חדש."
            if is_hebrew
            else "The provider connection could not be prepared. Please request a new link."
        )
        st.stop()
    st.success("האישור נשמר." if is_hebrew else "Your acknowledgement was recorded.")
    st.link_button(
        ("המשך אל " if is_hebrew else "Continue to ") + provider_label,
        provider_url,
        type="primary",
    )

st.divider()
st.write(("לשאלות או לפרישה: " if is_hebrew else "Questions or withdrawal: ") + PI_EMAIL)
cols = st.columns(3)
with cols[0]:
    st.page_link("pages/08_Privacy_Policy.py", label="Privacy / פרטיות")
with cols[1]:
    st.page_link("pages/09_Terms_of_Service.py", label="Terms / תנאים")
with cols[2]:
    st.page_link("pages/10_Research_Ethics.py", label="Ethics / אתיקה")
