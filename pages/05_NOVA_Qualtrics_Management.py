import streamlit as st
from utils.branding import render_app_logo

from controllers.auth_controller import AuthenticationController
from utils.demo_ui import render_demo_page


st.set_page_config(page_title="NOVA Qualtrics - AdmonTracker", page_icon="📋", layout="wide")
render_app_logo()

auth_controller = AuthenticationController()
auth_controller.render_auth_ui()
context = auth_controller.get_access_context()

if context.is_anonymous:
    st.warning("Please log in or open the guest demo from the main page.")
    st.stop()

if context.is_guest:
    render_demo_page("nova")
    st.stop()

if context.role not in {"Admin", "Manager"} or (context.role != "Admin" and context.project.lower() != "nova"):
    st.warning("You don't have permission to access this page.")
    st.stop()

if "spreadsheet" not in st.session_state:
    st.session_state.spreadsheet = auth_controller.get_spreadsheet()

from view.nova_qualtrics_management import nova_qualtrics_management

nova_qualtrics_management(context.email, context.role, context.project, st.session_state.spreadsheet)
