"""Safe, read-only renderers for every guest-visible product page."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from utils.access_control import render_demo_banner
from utils.demo_data import DEMO_HEALTH_SERIES, DEMO_PROJECT, demo_dataframe


def _disabled_action(label: str, *, key: str) -> None:
    st.button(label, key=key, disabled=True, help="Unavailable in the read-only guest demo.")


def _home() -> None:
    st.title("AdmonTracker")
    st.caption("Synthetic operations overview")
    devices = demo_dataframe("fitbit")
    logs = demo_dataframe("FitbitLog")
    c1, c2, c3 = st.columns(3)
    c1.metric("Example watches", len(devices))
    c2.metric("Example projects", devices["project"].nunique())
    c3.metric("Example low-battery alerts", int((pd.to_numeric(logs["lastBattaryVal"]) < 20).sum()))
    st.subheader("Synthetic watch status")
    st.dataframe(
        logs[["watchName", "project", "lastSynced", "lastBattaryVal", "lastHRVal", "lastStepsVal"]],
        width="stretch",
        hide_index=True,
    )


def _dashboard() -> None:
    st.title("Wearable Data Dashboard")
    watches = sorted(DEMO_HEALTH_SERIES["watch"].unique())
    selected_watch = st.selectbox("Choose an example watch", watches, key="demo_dashboard_watch")
    metric = st.radio(
        "Example signal",
        options=["heart_rate", "steps"],
        format_func=lambda value: "Heart rate" if value == "heart_rate" else "Steps",
        horizontal=True,
        key="demo_dashboard_metric",
    )
    frame = DEMO_HEALTH_SERIES[DEMO_HEALTH_SERIES["watch"] == selected_watch].copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"])
    st.subheader(f"Synthetic {metric.replace('_', ' ')} example")
    st.line_chart(frame.set_index("timestamp")[[metric]])
    st.caption("The chart is generated from bundled sample values; no wearable API is contacted.")
    _disabled_action("Refresh from device", key="demo_refresh_device")
    _disabled_action("Send message", key="demo_send_message")


def _devices() -> None:
    st.title("Wearable Device Management")
    devices = demo_dataframe("fitbit")
    display_columns = ["project", "name", "provider", "auth_status", "user", "isActive", "currentStudent"]
    st.data_editor(
        devices[display_columns],
        disabled=True,
        hide_index=True,
        width="stretch",
        key="demo_device_editor",
    )
    col1, col2 = st.columns(2)
    with col1:
        _disabled_action("Add new device", key="demo_add_device")
    with col2:
        _disabled_action("Save changes", key="demo_save_devices")


def _alerts() -> None:
    st.title("Alerts Configuration")
    config = demo_dataframe("fitbit_alerts_config").iloc[0]
    with st.form("demo_alert_configuration"):
        st.text_input("Project", value=DEMO_PROJECT, disabled=True)
        st.number_input("Battery threshold (%)", value=int(config["batteryThr"]), disabled=True)
        st.number_input("Current failed sync threshold", value=int(config["currentSyncThr"]), disabled=True)
        st.text_input("Recipient email", value=str(config["email"]), disabled=True)
        st.form_submit_button("Save configuration", disabled=True)
    st.subheader("Synthetic alert examples")
    st.dataframe(demo_dataframe("FitbitLog"), width="stretch", hide_index=True)


def _participant_connect() -> None:
    st.title("Connect Participant")
    st.info("This page is an illustrative preview. Guest sessions cannot create state or authorization links.")
    st.text_input("Participant pseudonymous ID", value="DEMO-PARTICIPANT-001", disabled=True)
    _disabled_action("Generate connect link", key="demo_participant_link")
    st.code("https://example.invalid/authorization-disabled-in-demo")


def _oauth() -> None:
    st.title("Connect Account to a Watch")
    st.info("OAuth clients, credentials, real watch names, and authorization links are never loaded in guest mode.")
    st.text_input("Watch name", value="DEMO-WATCH-003", disabled=True)
    st.text_input("Project", value=DEMO_PROJECT, disabled=True)
    st.selectbox("Provider", ["Synthetic provider"], disabled=True)
    st.checkbox("Active", value=True, disabled=True)
    _disabled_action("Add watch & generate link", key="demo_oauth_add")
    _disabled_action("Generate link for existing watch", key="demo_oauth_existing")


_RENDERERS = {
    "home": _home,
    "dashboard": _dashboard,
    "devices": _devices,
    "alerts": _alerts,
    "participant_connect": _participant_connect,
    "oauth": _oauth,
}


def render_demo_page(page: str) -> None:
    render_demo_banner()
    renderer = _RENDERERS.get(page)
    if renderer is None:
        st.error("This demo page is not available.")
        return
    renderer()
    st.caption("Synthetic guest demonstration • Read-only • No production connection")
