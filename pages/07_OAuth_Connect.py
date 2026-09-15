# pages/🔑 OAuth Connect.py
import streamlit as st
from controllers.auth_controller import AuthenticationController
from collections import OrderedDict
from entity.Sheet import GoogleSheetsAdapter
from utils.fitbit_callback import handle_fitbit_callback
from utils.google_health_callback import handle_google_health_callback
from utils.rate_limit_ui import show_rate_limit_notice
from utils.access_control import require_device_management, require_write_access
from utils.demo_ui import render_demo_page
from utils.compliance import approved_disclosure_ready, participant_disclosure_enforced

st.set_page_config(page_title="OAuth Connect - Wearable Research Manager", page_icon="🔑", layout="wide")

auth_controller = AuthenticationController()

if handle_google_health_callback(auth_controller):
    st.stop()
if handle_fitbit_callback(auth_controller):
    st.stop()

auth_controller.render_auth_ui()
context = auth_controller.get_access_context()

if context.is_anonymous:
    st.warning("Please log in or open the guest demo from the main page.")
    st.stop()

if context.is_guest:
    render_demo_page("oauth")
    st.stop()

if not context.can_manage_devices:
    st.warning("OAuth device management requires an Admin or Manager account.")
    st.stop()

require_device_management(context)

from utils.health_connect_links import create_health_connect_link
from utils.health_oauth_clients import (
    DEFAULT_GOOGLE_HEALTH_SCOPES,
    load_active_oauth_clients,
    make_default_client_key,
    parse_google_oauth_client_json,
    scopes_to_string,
    suggest_google_health_redirect_uri,
    upsert_oauth_client_config,
)

# ── Spreadsheet (cached in session state, like other pages) ──
if 'spreadsheet' not in st.session_state:
    st.session_state.spreadsheet = auth_controller.get_spreadsheet()
sp = st.session_state.get('spreadsheet', None)
if sp is None:
    st.error("Could not connect to spreadsheet")
    st.stop()

st.title("🔑 Connect Account to a Watch")

if participant_disclosure_enforced():
    disclosure_ready, disclosure_missing = approved_disclosure_ready()
    if disclosure_ready:
        st.success("The ethics-approved participant disclosure gate is enforced.")
    else:
        st.error("Disclosure enforcement is misconfigured: " + ", ".join(disclosure_missing))
        st.stop()
else:
    st.warning(
        "Participant disclosure enforcement is OFF. Links currently go directly to the provider. "
        "Do not submit the OAuth app for Google verification until the approved addendum is "
        "configured and PARTICIPANT_DISCLOSURE_ENFORCED=true."
    )


def _active_google_client_rows():
    try:
        return load_active_oauth_clients(sp, provider="google_health")
    except Exception as e:
        if not show_rate_limit_notice(
            e,
            provider="google_sheets",
            key="oauth_connect_google_clients",
            context="loading Google Health OAuth clients",
        ):
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


def _upsert_fitbit_watch_row(
    spreadsheet,
    row: OrderedDict,
    *,
    overwrite: bool = False,
    access_context=None,
) -> str:
    """
    Add or intentionally overwrite a watch row in the fitbit sheet.

    Existing token values are not preserved when overwrite=True; the connect
    flow is expected to replace them in the OAuth callback.
    """
    require_device_management()
    watch_name = str(row.get("name") or "").strip()
    existing = GoogleSheetsAdapter.get_rows(spreadsheet, "fitbit", "name", name=watch_name)
    if access_context is not None and access_context.role != "Admin":
        row["project"] = access_context.project
        if any(
            str(item.get("project") or "").strip().lower()
            != access_context.project.strip().lower()
            for item in existing
        ):
            raise PermissionError("Managers may manage watches only in their assigned project.")
    if existing and not overwrite:
        raise ValueError(f"Watch '{watch_name}' already exists")

    if not existing:
        GoogleSheetsAdapter.append_rows(spreadsheet, "fitbit", [row])
        return "added"

    workbook = spreadsheet.get_gspread_connection()
    ws = workbook.worksheet("fitbit")
    headers = [str(header or "").strip() for header in ws.row_values(1)]
    missing_headers = [key for key in row.keys() if key not in headers]
    if missing_headers:
        headers = headers + missing_headers
        ws.resize(cols=len(headers))
        ws.update("1:1", [headers])

    name_col = headers.index("name") + 1
    row_number = None
    for idx, value in enumerate(ws.col_values(name_col), start=1):
        if idx == 1:
            continue
        if str(value).strip() == watch_name:
            row_number = idx
            break

    if row_number is None:
        GoogleSheetsAdapter.append_rows(spreadsheet, "fitbit", [row])
        return "added"

    updates = []
    for col_idx, header in enumerate(headers, start=1):
        if header in row:
            updates.append({
                "range": f"fitbit!{GoogleSheetsAdapter._col_num_to_letter(col_idx)}{row_number}",
                "values": [[row.get(header, "")]],
            })
    if updates:
        workbook.values_batch_update({"data": updates, "valueInputOption": "RAW"})
    return "overwritten"


is_admin = context.role == "Admin"

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
                        "Redirect URI from JSON",
                        options=redirect_uris,
                        key="uploaded_redirect_uri_select",
                    )
                else:
                    redirect_uri = ""
                suggested_redirect_uri = suggest_google_health_redirect_uri(redirect_uri)
                if redirect_uri != suggested_redirect_uri:
                    st.warning(
                        "The JSON redirect URI is Streamlit's internal login callback. "
                        "Google Health OAuth must use the app callback URI below, and that exact URI must be added in Google Cloud."
                    )

                with st.form("save_google_oauth_client_form"):
                    client_key = st.text_input("Client key", value=default_client_key)
                    enviroment = st.selectbox("Environment", options=["dev", "staging", "production"], index=1)
                    redirect_uri = st.text_input("Redirect URI to save", value=suggested_redirect_uri)
                    scopes = st.text_area(
                        "Scopes",
                        value=scopes_to_string(DEFAULT_GOOGLE_HEALTH_SCOPES),
                        height=90,
                    )
                    status = st.selectbox("Status", options=["active", "disabled", "rotated"])
                    notes = st.text_input("Notes", value=parsed_client.get("project_id", ""))
                    save_client = st.form_submit_button("Save OAuth client")

                if save_client:
                    require_write_access(context)
                    if not client_key.strip():
                        st.error("Client key is required")
                        st.stop()
                    if not redirect_uri.strip():
                        st.error("Redirect URI is required")
                        st.stop()

                    try:
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
                    except Exception as e:
                        if not show_rate_limit_notice(
                            e,
                            provider="google_sheets",
                            key="save_google_oauth_client",
                            context="saving the OAuth client",
                        ):
                            raise
                        st.stop()
                    st.success(f"Saved OAuth client `{client_key.strip()}`.")
            except Exception as e:
                st.error(f"Could not parse/save OAuth client JSON: {e}")
else:
    st.caption("Only admin users can upload Google OAuth client JSON.")

# ── Single form: add a new watch and generate an OAuth link ──
st.subheader("Add a new watch & generate authorization link")

new_watch = st.text_input("Watch name (unique)", placeholder="e.g., NOVA_013")
project = st.text_input(
    "Project",
    value=str(st.session_state.get("user_project", "")),
    disabled=not is_admin,
)
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
overwrite_existing = st.checkbox(
    "Overwrite existing watch row if this watch name already exists",
    value=False,
    help=(
        "Use this for reauthorization/replacement. The existing fitbit row with this "
        "watch name will be updated before generating the OAuth link."
    ),
)
staff_consent_verified = st.checkbox(
    "I verified that the participant completed the current ethics-approved study consent",
    key="new_staff_consent_verified",
)
adult_verified = st.checkbox(
    "I verified that the participant is at least 18 years old",
    key="new_adult_verified",
)

if st.button("Add watch & generate link"):
    require_device_management(context)
    require_write_access(context)
    if not new_watch or not new_watch.strip():
        st.error("Watch name is required")
        st.stop()

    watch_name = new_watch.strip()

    # 1) Register or intentionally overwrite the watch in the fitbit sheet.
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
    try:
        watch_row_action = _upsert_fitbit_watch_row(
            sp,
            row,
            overwrite=overwrite_existing,
            access_context=context,
        )
    except ValueError:
        st.warning(
            f"Watch **{watch_name}** already exists in the fitbit sheet. "
            "Enable overwrite if you want to replace that row before generating the link."
        )
        st.stop()
    except Exception as e:
        if not show_rate_limit_notice(
            e,
            provider="google_sheets",
            key="register_watch",
            context="registering the watch",
        ):
            st.error(f"Failed to register watch: {e}")
        st.stop()

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
            staff_consent_verified=staff_consent_verified,
            adult_verified=adult_verified,
        )
    except Exception as e:
        if not show_rate_limit_notice(
            e,
            key="new_watch_authorization_link",
            context="generating the authorization link",
        ):
            st.error(f"Failed to generate authorization link: {e}")
        st.stop()

    if watch_row_action == "overwritten":
        st.success(
            f"Watch **{watch_name}** overwritten. Open the link below in an "
            "**incognito** window while logged into the participant account."
        )
    else:
        st.success(
            f"Watch **{watch_name}** registered! Open the link below in an "
            "**incognito** window while logged into the participant account."
        )
    st.code(url)
    st.link_button("Open authorization", url)

st.subheader("Generate a new link for an existing watch")
existing_watch = st.text_input("Existing watch name", key="existing_watch_name")
existing_project = st.text_input(
    "Existing watch project",
    value=str(st.session_state.get("user_project", "")),
    key="existing_watch_project",
    disabled=not is_admin,
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
existing_staff_consent_verified = st.checkbox(
    "I verified the existing participant's current ethics-approved consent",
    key="existing_staff_consent_verified",
)
existing_adult_verified = st.checkbox(
    "I verified that the existing participant is at least 18 years old",
    key="existing_adult_verified",
)

if st.button("Generate link for existing watch"):
    require_device_management(context)
    require_write_access(context)
    if not existing_watch or not existing_watch.strip():
        st.error("Existing watch name is required")
        st.stop()

    existing_rows = GoogleSheetsAdapter.get_rows(
        sp,
        "fitbit",
        "name",
        name=existing_watch.strip(),
    )
    if not existing_rows:
        st.error("The existing watch was not found.")
        st.stop()
    existing_row = existing_rows[-1]
    row_project = str(existing_row.get("project") or "").strip()
    if not is_admin and row_project.lower() != context.project.strip().lower():
        st.error("Managers may generate links only for watches in their assigned project.")
        st.stop()

    try:
        url = create_health_connect_link(
            sp,
            watchName=existing_watch.strip(),
            project=row_project or existing_project.strip(),
            provider=existing_provider,
            oauth_client_key=existing_client_key,
            purpose=existing_purpose,
            created_by=st.session_state.get("user_email"),
            staff_consent_verified=existing_staff_consent_verified,
            adult_verified=existing_adult_verified,
        )
    except Exception as e:
        if not show_rate_limit_notice(
            e,
            key="existing_watch_authorization_link",
            context="generating the authorization link",
        ):
            st.error(f"Failed to generate authorization link: {e}")
        st.stop()

    st.success(f"Authorization link generated for **{existing_watch.strip()}**.")
    st.code(url)
    st.link_button("Open authorization", url)
