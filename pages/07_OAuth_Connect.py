# pages/🔑 OAuth Connect.py
import streamlit as st
from controllers.auth_controller import AuthenticationController
from utils.fitbit_oauth import new_state, build_authorize_url
from utils.fitbit_token_store import save_state

st.set_page_config(page_title="OAuth Connect", page_icon="🔑", layout="wide")

auth_controller = AuthenticationController()
auth_controller.render_auth_ui()

# Require login (זה כלי לצוות המעבדה)
try:
    is_streamlit_logged_in = st.user is not None and hasattr(st.user, 'is_logged_in') and st.user.is_logged_in
except Exception:
    is_streamlit_logged_in = False

is_logged_in = is_streamlit_logged_in or st.session_state.get('user_role') is not None
if not is_logged_in:
    st.warning("Please log in to access this page.")
    st.stop()

# Spreadsheet
sp = auth_controller.get_spreadsheet()
if sp is None:
    st.error("Could not connect to spreadsheet")
    st.stop()

st.title("🔑 Connect Demo Account to a Watch (Fitbit OAuth)")

# pull watches from fitbit sheet (כמו בדאשבורד שלכם)
fitbit_df = sp.get_sheet("fitbit", sheet_type="fitbit").to_dataframe("pandas")
watch_names = sorted([w for w in fitbit_df.get("name", []) if isinstance(w, str) and w.strip()])

watch = st.selectbox("Choose watchName", watch_names)
project = st.text_input("Project (for logging)", value=str(st.session_state.get("user_project", "")))

if st.button("Generate authorization link"):
    if not watch:
        st.error("Missing watchName")
        st.stop()

    state = new_state()
    save_state(sp, state=state, watch_name=watch, project=project)

    url = build_authorize_url(state)

    st.success("Generated! Open this link in an INCOGNITO window while logged into the participant demo account.")
    st.code(url)
    st.link_button("Open authorization", url)