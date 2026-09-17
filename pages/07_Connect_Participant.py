import streamlit as st
from utils.branding import render_app_logo

from controllers.auth_controller import AuthenticationController
from utils.access_control import require_device_management, require_write_access
from utils.demo_ui import render_demo_page


st.set_page_config(page_title="Participant Connect - AdmonTracker", page_icon="🔗", layout="wide")
render_app_logo()

auth_controller = AuthenticationController()
auth_controller.render_auth_ui()
context = auth_controller.get_access_context()

if context.is_anonymous:
    st.warning("Please log in or open the guest demo from the main page.")
    st.stop()

if context.is_guest:
    render_demo_page("participant_connect")
    st.stop()

if not context.can_manage_devices:
    st.warning("Participant connection requires an Admin or Manager account.")
    st.stop()

require_device_management(context)
spreadsheet = auth_controller.get_spreadsheet()
from utils.health_connect_links import create_health_connect_link

st.title("Connect Participant")
participant = st.text_input("Participant pseudonymous ID")
staff_consent_verified = st.checkbox(
    "I verified the participant completed the current ethics-approved study consent"
)
adult_verified = st.checkbox("I verified the participant is at least 18 years old")

if st.button("Generate connect link") and participant:
    require_device_management(context)
    require_write_access(context)
    auth_url = create_health_connect_link(
        spreadsheet,
        watchName=participant.strip(),
        project=context.project,
        provider="fitbit",
        purpose="connect",
        created_by=context.email,
        staff_consent_verified=staff_consent_verified,
        adult_verified=adult_verified,
    )
    st.success("Link generated")
    st.write("Open this link in an incognito window while logged into the participant account:")
    st.code(auth_url)
    st.link_button("Open authorization", auth_url)
