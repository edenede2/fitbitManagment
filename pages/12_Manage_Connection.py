"""Public pseudonymous connection-management page."""

import streamlit as st

from entity.Sheet import Spreadsheet
from utils.branding import render_app_logo
from utils.compliance import PI_EMAIL, PI_PHONE, PUBLIC_PRIVACY_URL, deletion_text
from utils.connection_management import (
    disconnect_connection,
    record_deletion_request,
    resolve_management_token,
)


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

token = str(st.query_params.get("token") or "").strip()
if not token:
    st.info(
        "יש לפתוח את הקישור הפרטי שנמסר לאחר החיבור."
        if is_hebrew
        else "Open the private management link supplied after authorization."
    )
    st.stop()

spreadsheet_key = st.secrets.get("spreadsheet_key", "")
if not spreadsheet_key:
    st.error("השירות אינו זמין." if is_hebrew else "The service is unavailable.")
    st.stop()
spreadsheet = Spreadsheet(name="Fitbit Database", api_key=spreadsheet_key)

try:
    management = resolve_management_token(spreadsheet, token)
except Exception:
    st.error(
        "הקישור אינו תקף או פג תוקף. יש לפנות לצוות המחקר."
        if is_hebrew
        else "This management link is invalid or expired. Contact the study team."
    )
    st.stop()

provider = "Google Health" if management.get("provider") == "google_health" else "Fitbit"
st.write(("ספק: " if is_hebrew else "Provider: ") + provider)
st.write(
    "ניתוק מפסיק איסוף עתידי ומבטל את אסימוני הגישה."
    if is_hebrew
    else "Disconnecting stops future collection and revokes active access tokens."
)

if management.get("revoked_at"):
    st.success("החיבור נותק." if is_hebrew else "This connection is disconnected.")
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
                    "האיסוף העתידי הופסק והאסימונים המקומיים הוסרו."
                    if is_hebrew
                    else "Future collection stopped and locally stored tokens were removed."
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
