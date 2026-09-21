"""Wearable-device alert configuration UI."""

from datetime import date, timedelta

import polars as pl
import streamlit as st

from entity.Sheet import GoogleSheetsAdapter, Spreadsheet
from utils.access_control import require_write_access


NUMERIC_COLUMNS = (
    "currentSyncThr",
    "totalSyncThr",
    "currentHrThr",
    "totalHrThr",
    "currentSleepThr",
    "totalSleepThr",
    "currentStepsThr",
    "totalStepsThr",
    "batteryThr",
)


def _default_config(user_email: str) -> dict:
    return {
        "project": "",
        "currentSyncThr": 3,
        "totalSyncThr": 10,
        "currentHrThr": 3,
        "totalHrThr": 10,
        "currentSleepThr": 3,
        "totalSleepThr": 10,
        "currentStepsThr": 3,
        "totalStepsThr": 10,
        "batteryThr": 20,
        "manager": user_email,
        "email": user_email,
        "watch": "",
        "endDate": date.today() + timedelta(days=30),
    }


def _watch_names(spreadsheet: Spreadsheet, user_project: str) -> list[str]:
    watches = spreadsheet.get_sheet("fitbit", "fitbit").to_dataframe(engine="polars")
    if watches.is_empty() or "name" not in watches.columns:
        return []
    if user_project != "Admin" and "project" in watches.columns:
        watches = watches.filter(pl.col("project") == user_project)
    return sorted(watches.select("name").drop_nulls().unique().to_series().to_list())


def get_user_fitbit_config(
    spreadsheet: Spreadsheet,
    user_email: str,
    user_project: str,
) -> tuple[dict, list[str]]:
    """Return the current user's wearable alert configuration and watch list."""
    config = spreadsheet.get_sheet(
        "fitbit_alerts_config", "fitbit_alerts_config"
    ).to_dataframe(engine="polars")
    watches = _watch_names(spreadsheet, user_project)
    if config.is_empty() or "manager" not in config.columns:
        return _default_config(user_email), watches
    matching = config.filter(pl.col("manager") == user_email)
    if matching.is_empty():
        return _default_config(user_email), watches
    return matching.to_dicts()[0], watches


def save_fitbit_config(spreadsheet: Spreadsheet, config_data: dict) -> bool:
    """Insert or replace one wearable alert configuration."""
    require_write_access()
    current = spreadsheet.get_sheet(
        "fitbit_alerts_config", "fitbit_alerts_config"
    ).to_dataframe(engine="polars")
    new = pl.DataFrame([config_data])

    if "battaryThr" in current.columns and "batteryThr" in new.columns:
        current = current.rename({"battaryThr": "batteryThr"})
    if "battaryThr" in new.columns and "batteryThr" in current.columns:
        new = new.rename({"battaryThr": "batteryThr"})

    project = str(config_data.get("project") or "")
    email = str(config_data.get("email") or "")
    watch = str(config_data.get("watch") or "")
    if not current.is_empty() and project and "project" in current.columns:
        same = pl.col("project") == project
        if email and "email" in current.columns:
            same &= pl.col("email") == email
        if watch and "watch" in current.columns:
            same &= pl.col("watch") == watch
        current = current.filter(~same)

    for column in NUMERIC_COLUMNS:
        if column in current.columns:
            current = current.with_columns(pl.col(column).cast(pl.Utf8))
        if column in new.columns:
            new = new.with_columns(pl.col(column).cast(pl.Utf8))

    updated = new if current.is_empty() else pl.concat([current, new])
    spreadsheet.update_sheet("fitbit_alerts_config", updated, strategy="replace")
    GoogleSheetsAdapter.save(spreadsheet, "fitbit_alerts_config")
    return True


def get_project_fitbit_configs(
    spreadsheet: Spreadsheet,
    user_project: str,
) -> pl.DataFrame:
    """Return configurations visible to a project or to an administrator."""
    config = spreadsheet.get_sheet(
        "fitbit_alerts_config", "fitbit_alerts_config"
    ).to_dataframe(engine="polars")
    if config.is_empty() or user_project == "Admin":
        return config
    if "project" not in config.columns:
        return pl.DataFrame()
    return config.filter(pl.col("project") == user_project)


def display_fitbit_configs(configs: pl.DataFrame) -> None:
    if configs.is_empty():
        st.info("No existing wearable alert configurations for this project.")
        return

    st.subheader("Current wearable alert configurations")
    with st.expander("View all configurations", expanded=True):
        summary, details = st.tabs(["Summary", "Detailed view"])
        with summary:
            columns = ["project", "email", "watch", "batteryThr", "endDate"]
            if all(column in configs.columns for column in columns):
                st.dataframe(configs.select(columns), width="stretch")
            else:
                st.warning("Configuration data is missing expected columns.")
        with details:
            st.dataframe(configs, width="stretch")


def alerts_config_page(
    user_email: str,
    spreadsheet: Spreadsheet,
    user_role: str,
    user_project: str,
) -> None:
    """Render the wearable alert configuration page."""
    del user_role
    st.title("Wearable Alerts Configuration")
    display_fitbit_configs(get_project_fitbit_configs(spreadsheet, user_project))

    st.divider()
    st.subheader("Create or edit a configuration")
    config, watch_names = get_user_fitbit_config(spreadsheet, user_email, user_project)

    with st.form("fitbit_config_form"):
        project = st.text_input("Project", value=str(config.get("project", "")))

        st.subheader("Sync thresholds")
        current_sync = st.number_input(
            "Current failed sync threshold",
            min_value=1,
            max_value=100,
            value=int(config.get("currentSyncThr", 3)),
        )
        total_sync = st.number_input(
            "Total failed sync threshold",
            min_value=1,
            max_value=1000,
            value=int(config.get("totalSyncThr", 10)),
        )

        st.subheader("Heart-rate thresholds")
        current_hr = st.number_input(
            "Current failed heart-rate threshold",
            min_value=1,
            max_value=100,
            value=int(config.get("currentHrThr", 3)),
        )
        total_hr = st.number_input(
            "Total failed heart-rate threshold",
            min_value=1,
            max_value=1000,
            value=int(config.get("totalHrThr", 10)),
        )

        st.subheader("Sleep thresholds")
        current_sleep = st.number_input(
            "Current failed sleep threshold",
            min_value=1,
            max_value=100,
            value=int(config.get("currentSleepThr", 3)),
        )
        total_sleep = st.number_input(
            "Total failed sleep threshold",
            min_value=1,
            max_value=1000,
            value=int(config.get("totalSleepThr", 10)),
        )

        st.subheader("Steps thresholds")
        current_steps = st.number_input(
            "Current failed steps threshold",
            min_value=1,
            max_value=100,
            value=int(config.get("currentStepsThr", 3)),
        )
        total_steps = st.number_input(
            "Total failed steps threshold",
            min_value=1,
            max_value=1000,
            value=int(config.get("totalStepsThr", 10)),
        )

        st.subheader("Battery threshold")
        battery = st.number_input(
            "Battery level threshold (%)",
            min_value=5,
            max_value=50,
            value=int(config.get("batteryThr", 20)),
        )

        st.subheader("Recipient")
        recipient = st.text_input("Alert email", value=user_email)
        selected_watch = st.selectbox(
            "Watch (optional)",
            options=["All watches in the project"] + watch_names,
        )
        end_date = st.date_input(
            "End date",
            value=date.today() + timedelta(days=30),
        )

        if st.form_submit_button("Save configuration"):
            saved = save_fitbit_config(
                spreadsheet,
                {
                    "project": project,
                    "currentSyncThr": current_sync,
                    "totalSyncThr": total_sync,
                    "currentHrThr": current_hr,
                    "totalHrThr": total_hr,
                    "currentSleepThr": current_sleep,
                    "totalSleepThr": total_sleep,
                    "currentStepsThr": current_steps,
                    "totalStepsThr": total_steps,
                    "batteryThr": battery,
                    "manager": user_email,
                    "email": recipient,
                    "watch": "" if selected_watch == "All watches in the project" else selected_watch,
                    "endDate": end_date.strftime("%Y-%m-%d"),
                },
            )
            if saved:
                st.success("Wearable alert configuration saved.")
