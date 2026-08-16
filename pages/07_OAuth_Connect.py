# pages/🔑 OAuth Connect.py
import streamlit as st
from controllers.auth_controller import AuthenticationController
from utils.fitbit_oauth import new_state, build_authorize_url
from utils.fitbit_token_store import save_state
from collections import OrderedDict
from entity.Sheet import GoogleSheetsAdapter

st.set_page_config(page_title="OAuth Connect", page_icon="🔑", layout="wide")

auth_controller = AuthenticationController()
auth_controller.render_auth_ui()

# Require login
try:
    is_streamlit_logged_in = st.user is not None and hasattr(st.user, 'is_logged_in') and st.user.is_logged_in
except Exception:
    is_streamlit_logged_in = False

is_logged_in = is_streamlit_logged_in or st.session_state.get('user_role') is not None
if not is_logged_in:
    st.warning("Please log in to access this page.")
    st.stop()

# ── Spreadsheet (cached in session state, like other pages) ──
if 'spreadsheet' not in st.session_state:
    st.session_state.spreadsheet = auth_controller.get_spreadsheet()
sp = st.session_state.get('spreadsheet', None)
if sp is None:
    st.error("Could not connect to spreadsheet")
    st.stop()

st.title("🔑 Connect Demo Account to a Watch (Fitbit OAuth)")

# ── Single form: add a new watch and generate an OAuth link ──
st.subheader("Add a new watch & generate authorization link")

new_watch = st.text_input("Watch name (unique)", placeholder="e.g., NOVA_013")
project = st.text_input("Project", value=str(st.session_state.get("user_project", "")))
is_active = st.checkbox("Active", value=True)

if st.button("Add watch & generate link"):
    if not new_watch or not new_watch.strip():
        st.error("Watch name is required")
        st.stop()

    watch_name = new_watch.strip()

    # Validate the watch doesn't already exist
    existing = GoogleSheetsAdapter.get_rows(sp, "fitbit", "name", name=watch_name)
    if existing:
        st.warning(f"Watch **{watch_name}** already exists in the fitbit sheet.")
        st.stop()

    # 1) Register the watch in the fitbit sheet
    row = OrderedDict([
        ("project", project.strip()),
        ("name", watch_name),
        ("token", ""),
        ("isActive", "TRUE" if is_active else "FALSE"),
    ])
    GoogleSheetsAdapter.append_rows(sp, "fitbit", [row])

    # 2) Generate OAuth state & authorization URL
    state = new_state()
    save_state(sp, state=state, watch_name=watch_name, project=project.strip())
    url = build_authorize_url(state)

    st.success(f"Watch **{watch_name}** registered! Open the link below in an **incognito** window while logged into the participant demo account.")
    st.code(url)
    st.link_button("Open authorization", url)