"""Scoped, explicit enrollment setup and participant replacement access."""
from datetime import datetime, time
from zoneinfo import ZoneInfo
import streamlit as st

from controllers.auth_controller import AuthenticationController
from entity.Sheet import GoogleSheetsAdapter
from utils.access_control import require_device_management, require_project_access
from utils.branding import render_app_logo
from utils.collection_control import collection_key
from utils.connection_management import create_management_code, disconnect_connection
from utils.enrollment_security import create_enrollment, deadline_reminders
from utils.security_store import get_security_store, security_key
from utils.staff_sessions import issue_mfa_enrollment

st.set_page_config(page_title="Research Operations - AdmonTracker", layout="centered")
render_app_logo()
auth = AuthenticationController()
auth.render_auth_ui()
context = auth.get_access_context()
if not context.can_manage_devices:
    st.info("Verified Admin or Manager access is required.")
    st.stop()
require_device_management(context)
sp = auth.get_spreadsheet()
st.title("Research operations")
st.caption("Managers own collection/deletion deadlines; Admins verify completion and handle escalations. No automatic data deletion.")
if context.role == "Admin" and context.project == "*":
    devices = sp.get_sheet("fitbit").data
else:
    devices = []
    for project in context.project.split(";"):
        devices.extend(GoogleSheetsAdapter.get_rows(sp, "fitbit", "project", project=project.strip()))
choices = {(str(r.get("project") or ""), str(r.get("name") or "")): r for r in devices}
if not choices:
    st.info("No devices assigned to this account.")
    st.stop()
selected = st.selectbox("Assigned project / watch", list(choices), format_func=lambda v: " / ".join(v))
project, watch = selected
require_project_access(project, context)
device = choices[selected]
provider = "google_health" if str(device.get("provider") or device.get("oauth_type") or "").lower() in {"google", "google_health", "health"} else "fitbit"
control = get_security_store().read(collection_key(watch, provider))

if control:
    st.write({k: control.get(k) for k in ("enrollment_id", "activated_at", "collection_end", "measurement_end", "deletion_due",
        "collection_stopped", "provider_revocation_status", "secret_cleanup_status", "data_deletion_status")})
    for notice in deadline_reminders(control, now=datetime.now(ZoneInfo("Asia/Jerusalem"))):
        st.warning(f"{notice['task']} — {notice['stage']} — owner: {notice['owner']}")
    if st.button("Issue replacement participant code"):
        require_project_access(project)
        code = create_management_code(sp, watch_name=watch, project=project, provider=provider)
        st.code(code, language=None)
        st.caption("Provide privately after identifying the subject. Expires in 24 hours; prior codes and sessions are revoked.")
    if st.button("Stop collection / retry revocation and cleanup"):
        require_project_access(project)
        warnings = disconnect_connection(sp, {**control,
            "management_token_hash": security_key("management-enrollment", control["enrollment_id"])})
        st.success("Collection is stopped. Research-data deletion is a separate manual task.")
        for warning in warnings:
            st.warning(warning)

with st.expander("Initialize a reviewed enrollment or a new subject"):
    st.write("Use the verified original activation date for migration. Reconnection must not extend deadlines. Stop the previous enrollment before reassignment.")
    activation_date = st.date_input("Verified activation date")
    activation_time = st.time_input("Verified activation time (Asia/Jerusalem)", value=time(0))
    policy = st.text_input("PI-approved consent / retention policy reference")
    attested = st.checkbox("I verified the subject's approved consent, policy coverage and original activation date; this does not extend existing consent.")
    if st.button("Create reviewed enrollment"):
        require_project_access(project)
        if not attested or not policy.strip():
            st.error("Review and approval are required.")
        else:
            try:
                create_enrollment(watch_name=watch, project=project, provider=provider,
                    activated_at=datetime.combine(activation_date, activation_time, ZoneInfo("Asia/Jerusalem")),
                    approved_policy_ref=policy.strip(), context=context)
                st.success("Enrollment initialized. Generate a consent-aware provider link from Connect wearable.")
            except Exception:
                st.error("Enrollment creation refused. Review the previous assignment and contact an Admin.")

if context.role == "Admin":
    with st.expander("Approve staff authenticator enrollment or recovery"):
        bindings = [b for b in st.secrets.get("staff_identities", []) if b.get("enabled") is True]
        if bindings:
            target = st.selectbox("Approved staff binding", bindings, format_func=lambda b: str(b.get("email", "")))
            confirmed = st.checkbox("I independently verified this approved account's identity and recovery request.")
            if st.button("Issue one-use MFA setup approval") and confirmed:
                require_device_management()
                principal = security_key("staff", target["issuer"], target["subject"])
                st.code(issue_mfa_enrollment(principal, context), language=None)
                st.caption("Provide privately. Expires in 15 minutes. Re-enrollment revokes old sessions and recovery codes.")
