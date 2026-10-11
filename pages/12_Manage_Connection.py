"""Public pseudonymous connection-management page."""

import streamlit as st
import secrets

from entity.Sheet import Spreadsheet
from utils.branding import render_app_logo
from utils.compliance import PI_EMAIL, PI_PHONE, PUBLIC_PRIVACY_URL, deletion_text
from utils.connection_management import (
    disconnect_connection,
    record_deletion_request,
)
from utils.participant_sessions import exchange_management_code, resolve_management_session
from utils.collection_control import collection_key
from utils.security_store import get_security_store, security_key


st.set_page_config(
    page_title="Manage Connection - AdmonTracker",
    page_icon="🛡️",
    layout="centered",
)
render_app_logo(show_navigation=False)

language_label = st.radio("Language / שפה", ["English", "עברית"], horizontal=True)
language = "he" if language_label == "עברית" else "en"
is_hebrew = language == "he"
st.title("ניהול חיבור" if is_hebrew else "Manage Your Connection")

if st.query_params.get("token"):
    st.query_params.clear()
    st.info("Contact the study team for a replacement access code. / פנו לצוות לקבלת קוד חדש.")
if "management_browser_nonce" not in st.session_state:
    st.session_state.management_browser_nonce = secrets.token_urlsafe(32)
if not st.session_state.get("management_session_id"):
    with st.form("management_code_exchange"):
        code = st.text_input("Access code / קוד גישה", type="password", max_chars=128)
        submitted = st.form_submit_button("Continue / המשך")
    if submitted:
        try:
            st.session_state.management_session_id = exchange_management_code(
                code.strip(), browser_nonce=st.session_state.management_browser_nonce)
            st.rerun()
        except Exception:
            st.error("Invalid, expired or rate-limited code. Contact the study team. / פנו לצוות לקבלת קוד חדש.")
    st.stop()

spreadsheet_key = st.secrets.get("spreadsheet_key", "")
if not spreadsheet_key:
    st.error("השירות אינו זמין." if is_hebrew else "The service is unavailable.")
    st.stop()
spreadsheet = Spreadsheet(name="Fitbit Database", api_key=spreadsheet_key)

try:
    management = resolve_management_session(st.session_state.management_session_id,
        browser_nonce=st.session_state.management_browser_nonce)
    management["management_token_hash"] = security_key("management-enrollment", management["enrollment_id"])
    control = get_security_store().read(collection_key(management["watchName"], management["provider"]))
    management["revoked_at"] = control.get("collection_stopped_at", "")
    if control.get("data_deletion_status") == "requested":
        management["deletion_requested_at"] = "recorded"
except Exception:
    st.session_state.pop("management_session_id", None)
    st.error(
        "הקישור אינו תקף או פג תוקף. יש לפנות לצוות המחקר."
        if is_hebrew
        else "This session is invalid or expired. Reload to enter a replacement code from the study team."
    )
    st.stop()

provider = "Google Health" if management.get("provider") == "google_health" else "Fitbit"
st.write(("ספק: " if is_hebrew else "Provider: ") + provider)
st.write(
    "ניתוק מפסיק איסוף עתידי. ביטול הרשאת הספק וניקוי האסימונים מטופלים ומדווחים בנפרד."
    if is_hebrew
    else "Disconnecting stops future collection. Provider revocation and secret cleanup are tracked separately."
)

if management.get("revoked_at"):
    st.success("האיסוף הופסק." if is_hebrew else "Collection is stopped.")
    st.write("Provider revocation: " + str(control.get("provider_revocation_status", "pending")))
    st.write("Secret cleanup: " + str(control.get("secret_cleanup_status", "pending")))
    st.write("Data deletion: " + str(control.get("data_deletion_status", "not_requested")))
else:
    confirm_disconnect = st.checkbox(
        "אני מבקש/ת לנתק ולהפסיק איסוף עתידי."
        if is_hebrew
        else "I want to disconnect and stop future collection."
    )
    if st.button("ניתוק החיבור" if is_hebrew else "Disconnect connection", type="primary"):
        if not confirm_disconnect:
            st.error("נדרש אישור." if is_hebrew else "Confirmation is required.")
        else:
            try:
                warnings = disconnect_connection(spreadsheet, management)
                st.success(
                    "האיסוף העתידי הופסק."
                    if is_hebrew
                    else "Future collection stopped. Revocation and cleanup may still require follow-up."
                )
                if warnings:
                    st.warning(
                        "הספק לא אישר את כל פעולות הביטול מרחוק. פנו לצוות המחקר."
                        if is_hebrew
                        else "The provider did not confirm every remote revocation step. Contact the study team."
                    )
                management = {**management, "revoked_at": "now"}
            except Exception:
                st.error(
                    "לא ניתן להשלים את הניתוק. יש לפנות לצוות המחקר."
                    if is_hebrew
                    else "The disconnection could not be completed. Contact the study team."
                )

st.subheader("בקשת מחיקה" if is_hebrew else "Deletion request")
policy = deletion_text(language)
if policy:
    st.write(policy)
else:
    st.info(
        f"לבקשת מחיקה יש לפנות לחוקר הראשי: {PI_EMAIL}"
        if is_hebrew
        else f"To request deletion, contact the principal investigator: {PI_EMAIL}"
    )
if management.get("deletion_requested_at"):
    st.success("בקשת מחיקה כבר נרשמה." if is_hebrew else "A deletion request is already recorded.")
elif policy and st.button("רישום בקשת מחיקה" if is_hebrew else "Record deletion request"):
    try:
        record_deletion_request(spreadsheet, management, language)
        st.success(
            "הבקשה נרשמה לטיפול צוות המחקר בהתאם להסכמה המאושרת."
            if is_hebrew
            else "The request was recorded for study-team handling under the approved consent."
        )
    except Exception:
        st.error("לא ניתן לרשום את הבקשה." if is_hebrew else "The request could not be recorded.")

st.write(("יצירת קשר: " if is_hebrew else "Contact: ") + f"{PI_EMAIL} • {PI_PHONE}")
st.page_link(PUBLIC_PRIVACY_URL, label="Privacy / פרטיות")
