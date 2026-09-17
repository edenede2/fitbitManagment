import streamlit as st
from utils.branding import render_app_logo

from controllers.auth_controller import AuthenticationController
from utils.demo_ui import render_demo_page


st.set_page_config(page_title="Dashboard - Wearable Research Manager", page_icon="📊", layout="wide")
render_app_logo()

auth_controller = AuthenticationController()
auth_controller.render_auth_ui()
context = auth_controller.get_access_context()

if context.is_anonymous:
    st.warning("Please log in or open the guest demo from the main page.")
    st.stop()

if context.is_guest:
    render_demo_page("dashboard")
    st.stop()

if "spreadsheet" not in st.session_state:
    st.session_state.spreadsheet = auth_controller.get_spreadsheet()

from view.dashboard import display_dashboard

display_dashboard(context.email, context.role, context.project, st.session_state.spreadsheet)
