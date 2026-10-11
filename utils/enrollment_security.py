"""Explicit enrollment setup and manual deadline planning; never automatic erasure."""
from __future__ import annotations

import calendar
from datetime import datetime, timedelta
from uuid import uuid4
from zoneinfo import ZoneInfo

from utils.access_control import require_project_access
from utils.collection_control import CollectionStopped, collection_key
from utils.security_store import get_security_store, security_key


def calendar_month_after(value: datetime) -> datetime:
    year, month = value.year + (value.month == 12), value.month % 12 + 1
    return value.replace(year=year, month=month, day=min(value.day, calendar.monthrange(year, month)[1]))


def create_enrollment(*, watch_name: str, project: str, provider: str,
                      activated_at: datetime, approved_policy_ref: str, context=None) -> dict:
    """Explicit staff action after approved consent; cannot silently reconnect.

    Existing activations need a reviewed migration using their actual dates.
    This must not be called by token refresh or provider callbacks.
    """
    actor = require_project_access(project, context)
    if provider not in {"fitbit", "google_health"} or not approved_policy_ref or activated_at.tzinfo is None:
        raise ValueError("An approved policy and timezone-aware activation are required")
    enrollment_id = str(uuid4())
    local = activated_at.astimezone(ZoneInfo("Asia/Jerusalem"))
    collection_end = calendar_month_after(local)

    def create(before):
        if before and not actor.allows_project(str(before.get("project") or "")):
            raise CollectionStopped("The previous assignment is outside this account's projects")
        if before and not before.get("collection_stopped"):
            raise CollectionStopped("Stop and review the previous enrollment before reassignment")
        return {
            "record_type": "enrollment", "watchName": watch_name, "project": project, "provider": provider,
            "enrollment_id": enrollment_id, "generation": int(before.get("generation", 0)) + 1,
            "collection_stopped": False, "activated_at": activated_at.isoformat(),
            "collection_end": collection_end.isoformat(), "measurement_end": "",
            "deletion_due": calendar_month_after(collection_end).isoformat(),
            "approved_policy_ref": approved_policy_ref, "task_owner": "Project Manager",
            "task_owner_principal": actor.principal_id,
            "escalation_owner": "Admin", "provider_revocation_status": "not_requested",
            "secret_cleanup_status": "not_requested", "data_deletion_status": "not_requested",
        }

    store = get_security_store()
    result = store.mutate(collection_key(watch_name, provider), create)
    # Immutable enrollment identity/date evidence; no measurements or secrets.
    store.mutate(security_key("enrollment-history", enrollment_id),
                 lambda before: before or result)
    return result


def deadline_reminders(enrollment: dict, *, now: datetime) -> list[dict]:
    """Manual task reminders only. No collection stop or delete side effects."""
    notices = []
    for field, task in (("collection_end", "Stop collection manually"), ("deletion_due", "Complete deletion checklist")):
        if not enrollment.get(field):
            continue
        if field == "collection_end" and enrollment.get("collection_stopped"):
            continue
        if field == "deletion_due" and enrollment.get("data_deletion_status") == "verified_complete":
            continue
        due = datetime.fromisoformat(enrollment[field])
        remaining = due - now
        stage = "overdue" if remaining <= timedelta(0) else (
            "due_today" if remaining <= timedelta(hours=1) else (
                "24h" if remaining <= timedelta(hours=24) else (
                    "72h" if remaining <= timedelta(hours=72) else "")))
        if stage:
            notices.append({"enrollment_id": enrollment["enrollment_id"], "task": task,
                            "deadline": due.isoformat(), "stage": stage,
                            "owner": "Admin" if stage == "overdue" else "Project Manager"})
    return notices
