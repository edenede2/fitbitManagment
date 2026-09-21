"""Public, consent-aware bridge between a study invitation and provider OAuth."""

import streamlit as st

from entity.Sheet import Spreadsheet
from utils.branding import render_app_logo
from utils.compliance import (
    PI_EMAIL,
    PUBLIC_ETHICS_URL,
    PUBLIC_PRIVACY_URL,
    PUBLIC_TERMS_URL,
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
    page_title="Participant Authorization - AdmonTracker",
    page_icon="🔗",
    layout="centered",
)
render_app_logo()

language_label = st.radio("Language / שפה", ["English", "עברית"], horizontal=True)
language = "he" if language_label == "עברית" else "en"
is_hebrew = language == "he"

st.title("הרשאת משתתף" if is_hebrew else "Participant Authorization")
st.caption("University of Haifa • Study 385/23")

if not participant_disclosure_enforced():
    st.info(
        "הרשאת משתתפים זמינה באמצעות קישור אישי מצוות המחקר."
        if is_hebrew
        else "Participant authorization is available through a personal link from the study team."
    )
    st.page_link(PUBLIC_ETHICS_URL, label="Ethics documents / מסמכי אתיקה")
    st.stop()

try:
    assert_disclosure_can_be_enforced()
except RuntimeError:
    st.error(
        "שירות ההרשאה אינו זמין כעת. יש לפנות לצוות המחקר."
        if is_hebrew
        else "The authorization service is currently unavailable. Contact the study team."
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
if provider == "google_health":
    st.warning(
        "יש להתחבר רק באמצעות חשבון Google הייעודי למחקר (חשבון הדמו) שהוקצה לך, ולא באמצעות החשבון האישי שלך. אין למסור לצוות המחקר את פרטי הכניסה לחשבון האישי."
        if is_hebrew
        else "Use only the Google account designated for the study (the demo account) assigned to you, not your personal Google account. Never give the research team your personal account sign-in details."
    )
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
        if provider == "google_health" and document.key.startswith("google_health_addendum_"):
            selected_key = "google_health_addendum_he" if is_hebrew else "google_health_addendum_en"
            if document.key != selected_key:
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
        if provider == "google_health":
            confirmations = [
                st.checkbox(
                    "מלאו לי 18 שנים והשלמתי את טופס ההסכמה למחקר."
                    if is_hebrew
                    else "I am at least 18 years old and completed the study consent form."
                ),
                st.checkbox(
                    "קראתי את הנספח וקיבלתי אפשרות לשאול שאלות ולקבל עותק."
                    if is_hebrew
                    else "I read this addendum and had an opportunity to ask questions and receive a copy."
                ),
                st.checkbox(
                    "ברור לי שהחיבור מתבצע באמצעות חשבון Google ייעודי למחקר ולא באמצעות החשבון האישי שלי."
                    if is_hebrew
                    else "I understand that the connection uses a Google account designated for the study and not my personal account."
                ),
                st.checkbox(
                    "אני מבין/ה אילו נתונים ייאספו, למה הם ישמשו, כיצד יישמרו ומי יוכל לגשת אליהם."
                    if is_hebrew
                    else "I understand what data will be collected, how they will be used and stored, and who may access them."
                ),
                st.checkbox(
                    "אני מסכים/ה מרצון לגישת קריאה בלבד לנתונים המפורטים בנספח."
                    if is_hebrew
                    else "I voluntarily agree to read-only access to the data described in this addendum."
                ),
                st.checkbox(
                    "אני מבין/ה שאפשר לנתק את החיבור או לפרוש, וכי הנתונים יימחקו אלא אם אסכים במפורש לשמור אותם."
                    if is_hebrew
                    else "I understand that I may disconnect or withdraw, and that my data will be deleted unless I explicitly agree to keep them."
                ),
            ]
        else:
            confirmations = [
                st.checkbox(
                    "מלאו לי 18 שנים." if is_hebrew else "I am at least 18 years old."
                ),
                st.checkbox(
                    "קיבלתי והשלמתי את ההסכמה למחקר שאושרה אתית."
                    if is_hebrew
                    else "I received and completed the ethics-approved study consent."
                ),
                st.checkbox(
                    "קראתי את ההודעה, יכולתי לשמור עותק, ואני מאשר/ת מרצון את הגישה המפורטת."
                    if is_hebrew
                    else "I reviewed the disclosure, could save a copy, and voluntarily authorize the listed read-only access."
                ),
                st.checkbox(
                    "ברור לי כיצד לפרוש, לנתק את החיבור ולבקש מחיקה."
                    if is_hebrew
                    else "I understand how to withdraw, disconnect, and request deletion."
                ),
            ]
        submitted = st.form_submit_button(
            "אישור והמשך" if is_hebrew else "Acknowledge and continue",
            type="primary",
        )

    if submitted:
        if not all(confirmations):
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
    st.page_link(PUBLIC_PRIVACY_URL, label="Privacy / פרטיות")
with cols[1]:
    st.page_link(PUBLIC_TERMS_URL, label="Terms / תנאים")
with cols[2]:
    st.page_link(PUBLIC_ETHICS_URL, label="Ethics / אתיקה")
