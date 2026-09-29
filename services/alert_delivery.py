"""Pure scheduling helpers for persistent wearable alert delivery."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


ALERT_INTERVALS_HOURS = (1, 2, 4, 8, 16, 24)


def as_bool(value: Any, *, default: bool = False) -> bool:
    """Normalize the mixed boolean values used by Sheets and Firestore."""
    if value is None or str(value).strip() == "":
        return default
    return str(value).strip().casefold() in {
        "1",
        "true",
        "yes",
        "on",
        "active",
        "connected",
    }


def _as_int(value: Any, default: int) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def parse_timestamp(value: Any) -> datetime | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def next_interval_hours(send_count: int) -> int:
    """Return 1, 2, 4, 8, 16, then 24 hours, capped at 24."""
    index = min(max(int(send_count) - 1, 0), len(ALERT_INTERVALS_HOURS) - 1)
    return ALERT_INTERVALS_HOURS[index]


def alert_is_due(state: dict[str, Any] | None, now: datetime) -> bool:
    """Return whether an active problem should generate an email now."""
    if not state or not as_bool(state.get("active")):
        return True
    last_sent = parse_timestamp(state.get("last_sent_at"))
    if last_sent is None:
        return True
    interval = max(_as_int(state.get("next_interval_hours"), 1), 0)
    return now.astimezone(timezone.utc) >= last_sent + timedelta(hours=interval)


def detected_state(
    previous: dict[str, Any] | None,
    *,
    project: str,
    watch_name: str,
    reasons: list[str],
    now: datetime,
) -> dict[str, Any]:
    """Create/update state for a detected problem without counting an email."""
    timestamp = now.astimezone(timezone.utc).isoformat()
    was_active = bool(previous and as_bool(previous.get("active")))
    return {
        "project": project,
        "watchName": watch_name,
        "active": "TRUE",
        "muted": "FALSE",
        "first_detected_at": (
            str(previous.get("first_detected_at") or timestamp)
            if was_active and previous
            else timestamp
        ),
        "last_sent_at": str(previous.get("last_sent_at") or "") if was_active and previous else "",
        "next_interval_hours": (
            _as_int(previous.get("next_interval_hours"), 1)
            if was_active and previous
            else 1
        ),
        "send_count": _as_int(previous.get("send_count"), 0) if was_active and previous else 0,
        "alert_reasons": ", ".join(reasons),
        "resolved_at": "",
        "updated_at": timestamp,
    }


def sent_state(
    current: dict[str, Any],
    *,
    reasons: list[str],
    now: datetime,
) -> dict[str, Any]:
    """Advance the delivery interval after a successful email."""
    send_count = _as_int(current.get("send_count"), 0) + 1
    timestamp = now.astimezone(timezone.utc).isoformat()
    return {
        **current,
        "active": "TRUE",
        "muted": "FALSE",
        "last_sent_at": timestamp,
        "next_interval_hours": next_interval_hours(send_count),
        "send_count": send_count,
        "alert_reasons": ", ".join(reasons),
        "resolved_at": "",
        "updated_at": timestamp,
    }


def resolved_state(
    current: dict[str, Any],
    *,
    now: datetime,
    muted: bool = False,
) -> dict[str, Any]:
    """Close and reset a problem so a future recurrence emails immediately."""
    timestamp = now.astimezone(timezone.utc).isoformat()
    return {
        **current,
        "active": "FALSE",
        "muted": "TRUE" if muted else "FALSE",
        "first_detected_at": "",
        "last_sent_at": "",
        "next_interval_hours": 1,
        "send_count": 0,
        "alert_reasons": "",
        "resolved_at": timestamp,
        "updated_at": timestamp,
    }
