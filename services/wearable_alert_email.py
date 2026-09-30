"""Clear, minimal email content for wearable-device operational alerts."""

from __future__ import annotations

from datetime import datetime
from html import escape
from typing import Any
from zoneinfo import ZoneInfo


DASHBOARD_URL = "https://app.admontracker.online/Dashboard"
JERUSALEM = ZoneInfo("Asia/Jerusalem")


_REASON_FIELDS = {
    "Current Sync": ("Current sync failures", "CurrentFailedSync", "currentSyncThr"),
    "Total Sync": ("Total sync failures", "TotalFailedSync", "totalSyncThr"),
    "Current HR": ("Current heart-rate data failures", "CurrentFailedHR", "currentHrThr"),
    "Total HR": ("Total heart-rate data failures", "TotalFailedHR", "totalHrThr"),
    "Current Sleep": ("Current sleep-data failures", "CurrentFailedSleep", "currentSleepThr"),
    "Total Sleep": ("Total sleep-data failures", "TotalFailedSleep", "totalSleepThr"),
    "Current Steps": ("Current step-data failures", "CurrentFailedSteps", "currentStepsThr"),
    "Total Steps": ("Total step-data failures", "TotalFailedSteps", "totalStepsThr"),
}


def _display(value: Any, default: str = "Unknown") -> str:
    if value is None or str(value).strip() == "":
        return default
    return str(value).strip()


def _reason_detail(reason: str, log_row: dict, config: dict) -> str:
    if reason.startswith("Battery"):
        level = _display(log_row.get("lastBattaryVal"))
        threshold = _display(config.get("batteryThr"))
        return f"Battery level is {level}% (alert threshold: {threshold}% or lower)."

    label, value_field, threshold_field = _REASON_FIELDS.get(
        reason,
        (reason, "", ""),
    )
    if not value_field:
        return label
    value = _display(log_row.get(value_field), "0")
    threshold = _display(config.get(threshold_field), "0")
    return f"{label}: {value} (alert threshold: {threshold} or more)."


def build_wearable_alert_message(
    *,
    project: str,
    watches: list[dict],
    evaluated_at: datetime,
) -> tuple[str, str]:
    """Return plain-text and HTML versions containing only actionable details."""
    local_time = evaluated_at.astimezone(JERUSALEM).strftime("%Y-%m-%d %H:%M %Z")
    device_word = "device" if len(watches) == 1 else "devices"

    plain_lines = [
        "AdmonTracker wearable-device alert",
        "",
        f"Project: {project}",
        f"Checked: {local_time}",
        f"{len(watches)} {device_word} currently require attention.",
    ]
    html_sections = []

    for watch in watches:
        watch_name = _display(watch.get("watch_name"))
        log_row = watch.get("log_row") or {}
        config = watch.get("config") or {}
        reasons = list(watch.get("alert_reasons") or [])
        details = [_reason_detail(reason, log_row, config) for reason in reasons]
        last_check = _display(log_row.get("lastCheck"))
        last_sync = _display(log_row.get("lastSynced"))

        plain_lines.extend(
            [
                "",
                f"Watch: {watch_name}",
                *(f"- {detail}" for detail in details),
                f"Last check: {last_check}",
                f"Last successful sync: {last_sync}",
            ]
        )

        html_details = "".join(f"<li>{escape(detail)}</li>" for detail in details)
        html_sections.append(
            """
            <section style="margin:16px 0;padding:14px;border:1px solid #d8dee8;border-radius:8px;">
              <h2 style="font-size:17px;margin:0 0 8px;">Watch: {watch}</h2>
              <ul style="margin:0 0 10px;padding-left:22px;">{details}</ul>
              <p style="margin:3px 0;"><strong>Last check:</strong> {last_check}</p>
              <p style="margin:3px 0;"><strong>Last successful sync:</strong> {last_sync}</p>
            </section>
            """.format(
                watch=escape(watch_name),
                details=html_details,
                last_check=escape(last_check),
                last_sync=escape(last_sync),
            )
        )

    plain_lines.extend(
        [
            "",
            f"Open the dashboard: {DASHBOARD_URL}",
            "Alerts continue on the configured schedule until the condition resolves or the watch is muted.",
            "This is an automated operational alert from AdmonTracker.",
        ]
    )

    html = """<!doctype html>
<html lang="en">
<body style="font-family:Arial,sans-serif;line-height:1.5;color:#172033;max-width:680px;margin:auto;">
  <h1 style="font-size:22px;margin-bottom:8px;">Wearable-device alert</h1>
  <p style="margin:3px 0;"><strong>Project:</strong> {project}</p>
  <p style="margin:3px 0;"><strong>Checked:</strong> {checked}</p>
  <p><strong>{count} {device_word}</strong> currently require attention.</p>
  {sections}
  <p style="margin-top:20px;">
    <a href="{dashboard}" style="background:#1769aa;color:#fff;padding:10px 16px;text-decoration:none;border-radius:5px;">
      Open AdmonTracker dashboard
    </a>
  </p>
  <p style="font-size:13px;color:#566173;margin-top:22px;">
    Alerts continue on the configured schedule until the condition resolves or the watch is muted.
    This is an automated operational alert from AdmonTracker.
  </p>
</body>
</html>""".format(
        project=escape(_display(project)),
        checked=escape(local_time),
        count=len(watches),
        device_word=device_word,
        sections="".join(html_sections),
        dashboard=DASHBOARD_URL,
    )
    return "\n".join(plain_lines), html
