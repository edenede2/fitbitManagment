import streamlit as st

from controllers.auth_controller import AuthenticationController
from utils.demo_ui import render_demo_page


st.set_page_config(page_title="FIBRO AppSheet - Wearable Research Manager", page_icon="📋", layout="wide")

auth_controller = AuthenticationController()
auth_controller.render_auth_ui()
context = auth_controller.get_access_context()

if context.is_anonymous:
    st.warning("Please log in or open the guest demo from the main page.")
    st.stop()

if context.is_guest:
    render_demo_page("appsheet")
    st.stop()

if context.role != "Admin" and context.project.lower() != "fibro":
    st.warning("You don't have permission to access this page.")
    st.stop()

if "fib_spreadsheet" not in st.session_state:
    st.session_state.fib_spreadsheet = auth_controller.get_fibro_spreasheet()

from view.fibro_appsheet_managment import fibro_appsheet_management

fibro_appsheet_management(context.email, context.role, context.project, st.session_state.fib_spreadsheet)
