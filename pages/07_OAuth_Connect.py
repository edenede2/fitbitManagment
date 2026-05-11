# pages/🔑 OAuth Connect.py
import streamlit as st
from controllers.auth_controller import AuthenticationController
from collections import OrderedDict
from entity.Sheet import GoogleSheetsAdapter
from utils.google_health_callback import handle_google_health_callback
from utils.health_connect_links import create_health_connect_link
from utils.health_oauth_clients import (
    DEFAULT_GOOGLE_HEALTH_SCOPES,
    load_active_oauth_clients,
    make_default_client_key,
    parse_google_oauth_client_json,
    scopes_to_string,
    upsert_oauth_client_config,
)

st.set_page_config(page_title="OAuth Connect", page_icon="🔑", layout="wide")

auth_controller = AuthenticationController()

if handle_google_health_callback(auth_controller):
    st.stop()

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

st.title("🔑 Connect Account to a Watch")


def _active_google_client_rows():
    try:
        return load_active_oauth_clients(sp, provider="google_health")
    except Exception as e:
        st.warning(f"Could not load Google Health OAuth clients: {e}")
        return []


def _client_label(row: dict) -> str:
    env = row.get("enviroment") or row.get("environment") or "staging"
    notes = row.get("notes") or row.get("client_id") or ""
    suffix = f" - {notes}" if notes else ""
    return f"{row.get('client_key', '')} ({env}){suffix}"


def _select_google_client_key(label: str, *, key: str) -> str:
    client_rows = _active_google_client_rows()
    if not client_rows:
        st.warning("No active Google Health OAuth clients found in health_oauth_clients.")
        return st.text_input(label, value="google_health_staging", key=key)

    selected = st.selectbox(
        label,
        options=client_rows,
        format_func=_client_label,
        key=key,
    )
    return str(selected.get("client_key") or "")


is_admin = str(st.session_state.get("user_role", "")).strip() == "Admin"

if is_admin:
    with st.expander("Admin: upload Google OAuth client JSON"):
        uploaded_client = st.file_uploader(
            "Google OAuth client JSON",
            type=["json"],
            key="google_oauth_client_json",
        )

        if uploaded_client is not None:
            try:
                raw_json = uploaded_client.getvalue().decode("utf-8")
                parsed_client = parse_google_oauth_client_json(raw_json)
                redirect_uris = parsed_client.get("redirect_uris") or []
                default_env = "staging"
                default_client_key = make_default_client_key(parsed_client, default_env)

                st.write(f"Client ID: `{parsed_client['client_id']}`")
                if redirect_uris:
                    redirect_uri = st.selectbox(
                        "Redirect URI",
                        options=redirect_uris,
                        key="uploaded_redirect_uri_select",
                    )
                else:
                    redirect_uri = ""

                with st.form("save_google_oauth_client_form"):
                    client_key = st.text_input("Client key", value=default_client_key)
                    enviroment = st.selectbox("Environment", options=["dev", "staging", "production"], index=1)
                    redirect_uri = st.text_input("Redirect URI to save", value=redirect_uri)
                    scopes = st.text_area(
                        "Scopes",
                        value=scopes_to_string(DEFAULT_GOOGLE_HEALTH_SCOPES),
                        height=90,
                    )
                    status = st.selectbox("Status", options=["active", "disabled", "rotated"])
                    notes = st.text_input("Notes", value=parsed_client.get("project_id", ""))
                    save_client = st.form_submit_button("Save OAuth client")

                if save_client:
                    if not client_key.strip():
                        st.error("Client key is required")
                        st.stop()
                    if not redirect_uri.strip():
                        st.error("Redirect URI is required")
                        st.stop()

                    upsert_oauth_client_config(
                        sp,
                        client_key=client_key.strip(),
                        provider="google_health",
                        enviroment=enviroment,
                        client_id=parsed_client["client_id"],
                        client_secret=parsed_client["client_secret"],
                        credentials_json_raw=raw_json,
                        redirect_uri=redirect_uri.strip(),
                        auth_uri=parsed_client["auth_uri"],
                        token_uri=parsed_client["token_uri"],
                        scopes=scopes,
                        status=status,
                        notes=notes,
                    )
                    st.success(f"Saved OAuth client `{client_key.strip()}`.")
            except Exception as e:
                st.error(f"Could not parse/save OAuth client JSON: {e}")
else:
    st.caption("Only admin users can upload Google OAuth client JSON.")

# ── Single form: add a new watch and generate an OAuth link ──
st.subheader("Add a new watch & generate authorization link")

new_watch = st.text_input("Watch name (unique)", placeholder="e.g., NOVA_013")
project = st.text_input("Project", value=str(st.session_state.get("user_project", "")))
provider = st.selectbox(
    "Provider",
    options=["fitbit", "google_health"],
    format_func=lambda value: "Fitbit legacy" if value == "fitbit" else "Google Health",
)
oauth_client_key = ""
purpose = "connect"
if provider == "google_health":
    oauth_client_key = _select_google_client_key(
        "Google Cloud project / OAuth client",
        key="new_google_oauth_client_key",
    )
    purpose = st.selectbox("Purpose", options=["connect", "reauth", "test"])
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
        ("oauth_type", provider),
        ("provider", provider),
        ("oauth_client_key", oauth_client_key),
        ("auth_status", "not_connected"),
        ("health_user_id", ""),
        ("legacy_fitbit_user_id", ""),
        ("last_successful_fetch_at", ""),
        ("last_data_timestamp", ""),
        ("last_auth_error", ""),
        ("reauth_link", ""),
        ("reauth_link_created_at", ""),
        ("isActive", "TRUE" if is_active else "FALSE"),
    ])
    GoogleSheetsAdapter.append_rows(sp, "fitbit", [row])

    # 2) Generate OAuth state & authorization URL
    try:
        url = create_health_connect_link(
            sp,
            watchName=watch_name,
            project=project.strip(),
            provider=provider,
            oauth_client_key=oauth_client_key,
            purpose=purpose,
            created_by=st.session_state.get("user_email"),
        )
    except Exception as e:
        st.error(f"Failed to generate authorization link: {e}")
        st.stop()

    st.success(f"Watch **{watch_name}** registered! Open the link below in an **incognito** window while logged into the participant account.")
    st.code(url)
    st.link_button("Open authorization", url)

st.subheader("Generate a new link for an existing watch")
existing_watch = st.text_input("Existing watch name", key="existing_watch_name")
existing_project = st.text_input(
    "Existing watch project",
    value=str(st.session_state.get("user_project", "")),
    key="existing_watch_project",
)
existing_provider = st.selectbox(
    "Existing watch provider",
    options=["fitbit", "google_health"],
    format_func=lambda value: "Fitbit legacy" if value == "fitbit" else "Google Health",
    key="existing_provider",
)
existing_client_key = ""
existing_purpose = "reauth"
if existing_provider == "google_health":
    existing_client_key = _select_google_client_key(
        "Existing watch Google Cloud project / OAuth client",
        key="existing_client_key",
    )
    existing_purpose = st.selectbox(
        "Existing watch link purpose",
        options=["reauth", "connect", "test"],
        key="existing_purpose",
    )

if st.button("Generate link for existing watch"):
    if not existing_watch or not existing_watch.strip():
        st.error("Existing watch name is required")
        st.stop()

    try:
        url = create_health_connect_link(
            sp,
            watchName=existing_watch.strip(),
            project=existing_project.strip(),
            provider=existing_provider,
            oauth_client_key=existing_client_key,
            purpose=existing_purpose,
            created_by=st.session_state.get("user_email"),
        )
    except Exception as e:
        st.error(f"Failed to generate authorization link: {e}")
        st.stop()

    st.success(f"Authorization link generated for **{existing_watch.strip()}**.")
    st.code(url)
    st.link_button("Open authorization", url)
