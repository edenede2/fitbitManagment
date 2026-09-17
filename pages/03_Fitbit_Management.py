import streamlit as st
from utils.branding import render_app_logo

from controllers.auth_controller import AuthenticationController
from utils.demo_ui import render_demo_page


st.set_page_config(page_title="Device Management - Wearable Research Manager", page_icon="⌚", layout="wide")
render_app_logo()

auth_controller = AuthenticationController()
auth_controller.render_auth_ui()
context = auth_controller.get_access_context()

if context.is_anonymous:
    st.warning("Please log in or open the guest demo from the main page.")
    st.stop()

if context.is_guest:
    render_demo_page("devices")
    st.stop()

if not context.can_manage_devices:
    st.warning("Device management requires an Admin or Manager account.")
    st.stop()

if "spreadsheet" not in st.session_state:
    st.session_state.spreadsheet = auth_controller.get_spreadsheet()

from view.fitbit_management import load_fitbit_datatable

load_fitbit_datatable(context.email, context.role, context.project, st.session_state.spreadsheet)
