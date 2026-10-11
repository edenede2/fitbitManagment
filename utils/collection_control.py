"""Durable stop fences, separate from revocation and manual research deletion."""
from __future__ import annotations

from datetime import datetime, timezone
import os

from utils.security_store import collection_controls_enabled, get_security_store, security_key


class CollectionStopped(PermissionError):
    pass


def collection_key(watch_name: str, provider: str) -> str:
    # Watch/provider lookup is deliberately global: a stale project must not
    # bypass a stop fence after the physical watch is reassigned.
    return security_key("collection", watch_name, provider)


def assert_collection_allowed(
    watch_name: str, provider: str, *, project: str = "",
    enrollment_id: str = "", generation: int | None = None,
) -> dict:
    if not collection_controls_enabled():
        return {}
    row = get_security_store().read(collection_key(watch_name, provider))
    if os.getenv("ENROLLMENT_BINDING_REQUIRED", "false").casefold() == "true" and not row.get("enrollment_id"):
        raise CollectionStopped("A reviewed enrollment is required before collection")
    if row.get("collection_stopped"):
        raise CollectionStopped("Collection is stopped for this enrollment")
    if row and (
        (project and project != row.get("project"))
        or (enrollment_id and enrollment_id != row.get("enrollment_id"))
        or (generation is not None and generation != row.get("generation"))
    ):
        raise CollectionStopped("The enrollment is no longer current")
    return row


def stop_collection(watch_name: str, provider: str, *, project: str, enrollment_id: str = "") -> dict:
    if not collection_controls_enabled():
        # A legacy-only deployment cannot claim an atomic, shared stop. It must
        # receive the reviewed Firestore gate backport before CASA cutover.
        raise RuntimeError("Authoritative collection controls are not configured")
    now = datetime.now(timezone.utc).isoformat()
    from utils.enrollment_security import calendar_month_after
    from zoneinfo import ZoneInfo
    processing_due = calendar_month_after(datetime.fromisoformat(now).astimezone(ZoneInfo("Asia/Jerusalem"))).isoformat()

    def stop(before):
        if before and (
            project != before.get("project")
            or (before.get("enrollment_id") and enrollment_id != before.get("enrollment_id"))
        ):
            raise CollectionStopped("The enrollment is no longer current")
        return {
            **before, "project": project, "provider": provider,
            "enrollment_id": enrollment_id or before.get("enrollment_id", ""),
            "collection_stopped": True,
            "collection_stopped_at": before.get("collection_stopped_at") or now,
            "measurement_end": before.get("measurement_end") or now,
            "deletion_due": before.get("deletion_due") if before.get("measurement_end") else processing_due,
            "generation": int(before.get("generation", 0)) + (not before.get("collection_stopped", False)),
            "provider_revocation_status": "confirmed" if before.get("provider_revocation_status") == "confirmed" else "pending",
            "secret_cleanup_status": "confirmed" if before.get("secret_cleanup_status") == "confirmed" else "pending",
            "data_deletion_status": before.get("data_deletion_status", "not_requested"),
        }

    return get_security_store().mutate(collection_key(watch_name, provider), stop)


def update_cleanup_status(watch_name: str, provider: str, *, generation: int, **statuses) -> dict:
    allowed = {"provider_revocation_status", "secret_cleanup_status", "data_deletion_status"}
    if not set(statuses) <= allowed:
        raise ValueError("Unsupported collection status field")

    def update(before):
        if before.get("generation") != generation or not before.get("collection_stopped"):
            raise CollectionStopped("The enrollment changed during cleanup")
        return {**before, **statuses}

    return get_security_store().mutate(collection_key(watch_name, provider), update)


def publish_token_row(spreadsheet, tab: str, row: dict, control: dict) -> bool:
    """Atomic metadata publication; credential calls happen before this function.

    A failed fence may leave a just-written secret version, so callers must
    queue/retry its cleanup rather than publishing stale credentials.
    """
    if not collection_controls_enabled():
        return False
    import time
    from utils.firestore_schema import sanitize_record, spec_for_source_sheet, stable_document_id
    from entity.Sheet import GoogleSheetsAdapter
    spec = spec_for_source_sheet(tab)
    if not spec:
        raise ValueError("Unknown token metadata table")
    clean, _ = sanitize_record(spec, dict(row))
    clean["_migration"] = {"source_sheet": tab, "source_order": time.time_ns(), "runtime_write": True}
    provider = "google_health" if tab == "health_oauth_tokens" else "fitbit"
    get_security_store().fenced_write(
        collection_key(str(row["watchName"]), provider), int(control.get("generation", 0)),
        spec.collection, stable_document_id(spec, clean, 0), clean,
    )
    # The legacy mirror is not an authority and cannot bypass the stop fence.
    GoogleSheetsAdapter._sheets_shadow(spreadsheet, "append", tab, [row])
    return True


def record_token_publication_failure(watch: str, provider: str, control: dict, secret_ref: str) -> None:
    """Queue review, not broad destruction of a possibly committed credential.

    An SDK transport failure can occur after commit. A pinned version must be
    reconciled against authoritative metadata before any irreversible cleanup.
    """
    if not collection_controls_enabled() or not secret_ref:
        return
    task = {"record_type": "credential_publication_review", "watchName": watch,
            "provider": provider, "enrollment_id": control.get("enrollment_id", ""),
            "generation": control.get("generation", 0), "secret_ref": secret_ref,
            "status": "pending_review", "owner": "Admin", "created_at": datetime.now(timezone.utc).isoformat()}
    get_security_store().mutate(security_key("credential-publication-review", secret_ref), lambda before: before or task)
