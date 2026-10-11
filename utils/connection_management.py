"""Pseudonymous self-service connection revocation and deletion requests."""

from __future__ import annotations

from typing import Any

from entity.Sheet import GoogleSheetsAdapter, Spreadsheet
from utils.compliance import deletion_text
from utils.health_token_store import (
    _append,
    _update_row_by_keys,
    new_uuid,
    utc_now_iso,
)
from utils.secret_store import destroy_secret_versions, load_json_secret
from utils.collection_control import stop_collection, update_cleanup_status


MANAGEMENT_TAB = "health_connection_management"
DELETION_REQUESTS_TAB = "health_deletion_requests"
MANAGEMENT_COLUMNS = [
    "management_token_hash", "watchName", "project", "provider", "created_at",
    "expires_at", "revoked_at", "deletion_requested_at",
]
DELETION_COLUMNS = [
    "request_id", "management_token_hash", "watchName", "project", "provider",
    "requested_at", "status", "approved_policy_text",
]


def create_management_link(
    spreadsheet: Spreadsheet,
    *,
    watch_name: str,
    project: str,
    provider: str,
    validity_days: int = 365,
) -> str:
    raise RuntimeError("Bearer management links have been retired; issue an enrollment-bound code")


def create_management_code(spreadsheet: Spreadsheet, *, watch_name: str, project: str, provider: str) -> str:
    from utils.collection_control import collection_key
    from utils.security_store import get_security_store
    from utils.participant_sessions import issue_management_code
    current = get_security_store().read(collection_key(watch_name, provider))
    if current.get("project") != project or not current.get("enrollment_id"):
        raise ValueError("A reviewed enrollment is required")
    return issue_management_code(current)


def resolve_management_token(spreadsheet: Spreadsheet, token: str) -> dict[str, Any]:
    # Legacy bearer links are deliberately not grandfathered across cutover.
    raise ValueError("Legacy management links are retired; contact the study team for a code")


def disconnect_connection(spreadsheet: Spreadsheet, management_row: dict[str, Any]) -> list[str]:
    """Block first; provider revocation and secret cleanup remain separate tasks.

    Repeating this operation retries incomplete cleanup. Neither provider nor
    Secret Manager availability is a prerequisite for the durable stop fence.
    """
    warnings: list[str] = []
    provider = str(management_row.get("provider") or "")
    watch_name = str(management_row.get("watchName") or "")
    project = str(management_row.get("project") or "")
    if provider not in {"google_health", "fitbit"} or not watch_name or not project:
        raise ValueError("Unsupported provider")
    stopped = stop_collection(
        watch_name, provider, project=project,
        enrollment_id=str(management_row.get("enrollment_id") or ""),
    )
    generation = stopped["generation"]
    tab = "health_oauth_tokens" if provider == "google_health" else "fitbit_oauth_tokens"
    # Metadata only. Do not hydrate credentials before the local block.
    try:
        rows = GoogleSheetsAdapter.get_rows(spreadsheet, tab, "watchName", watchName=watch_name)
        rows = [r for r in rows if (not r.get("provider") or r["provider"] == provider)
                and (not r.get("project") or r["project"] == project)
                and (not management_row.get("enrollment_id")
                     or r.get("enrollment_id") == management_row["enrollment_id"])]
        token_row = dict(rows[-1]) if rows else {}
        if provider == "fitbit" and not token_row:
            legacy = GoogleSheetsAdapter.get_rows(spreadsheet, "fitbit", "name", "project",
                                                  name=watch_name, project=project)
            legacy = [r for r in legacy if not management_row.get("enrollment_id")
                      or r.get("enrollment_id") == management_row["enrollment_id"]]
            token_row = dict(legacy[-1]) if legacy else {}
    except Exception:
        warnings.append("Collection stopped; token metadata lookup needs retry")
        return warnings

    try:
        updates = {"access_token": "", "refresh_token": "", "revoked_at": utc_now_iso()}
        updates.update({"is_active": "FALSE", "auth_status": "disconnected"}
                       if provider == "google_health" else {"status": "disconnected"})
        keys = {"watchName": watch_name}
        if management_row.get("enrollment_id"):
            keys["enrollment_id"] = management_row["enrollment_id"]
        if provider == "google_health":
            keys["provider"] = provider
        GoogleSheetsAdapter.update_matching_rows(spreadsheet, tab, keys=keys, updates=updates)
        GoogleSheetsAdapter.update_matching_rows(
            spreadsheet, "fitbit", keys={"name": watch_name, "project": project},
            updates={"isActive": "FALSE", "auth_status": "disconnected", "token": ""},
        )
    except Exception:
        warnings.append("Collection stopped; operational metadata synchronization needs retry")

    revoked = stopped.get("provider_revocation_status") == "confirmed"
    ref = str(token_row.get("token_secret_ref") or "")
    if not revoked:
        try:
            payload = load_json_secret(ref) if ref else {}
            token = str(payload.get("refresh_token") or payload.get("access_token") or "")
            if not token:
                raise RuntimeError("No protected credential available for revocation")
            if provider == "google_health":
                from utils.google_health_oauth import revoke_google_health_token
                revoke_google_health_token(token)
            else:
                from utils.fitbit_oauth import revoke_token
                revoke_token(token)
            revoked = True
        except Exception:
            warnings.append("Collection stopped; provider revocation is unconfirmed and requires follow-up")

    cleaned = stopped.get("secret_cleanup_status") == "confirmed"
    try:
        if ref:
            destroy_secret_versions(ref)
            cleaned = True
        elif not cleaned:
            warnings.append("Collection stopped; secret ownership must be reviewed before cleanup")
    except Exception:
        cleaned = False
        warnings.append("Collection stopped; secret destruction remains pending")
    update_cleanup_status(
        watch_name, provider, generation=generation,
        provider_revocation_status="confirmed" if revoked else "pending_manual",
        secret_cleanup_status="confirmed" if cleaned else "pending",
    )
    # Keep the management grant available to request deletion and retry cleanup.
    # revoked_at historically meant 'disconnected', not verified provider revoke.
    try:
        _update_row_by_keys(spreadsheet, MANAGEMENT_TAB,
                           keys={"management_token_hash": management_row["management_token_hash"]},
                           updates={"revoked_at": stopped["collection_stopped_at"]})
    except Exception:
        warnings.append("Collection stopped; management status synchronization needs retry")
    return warnings


def record_deletion_request(spreadsheet: Spreadsheet, management_row: dict[str, Any], language: str) -> None:
    disconnect_connection(spreadsheet, management_row)
    if str(management_row.get("deletion_requested_at") or ""):
        return
    requested = utc_now_iso()
    from utils.collection_control import collection_key, update_cleanup_status
    from utils.security_store import get_security_store, security_key
    enrollment_id = str(management_row.get("enrollment_id") or "")
    if enrollment_id:
        request = {"request_id": security_key("deletion-request", enrollment_id),
                   "enrollment_id": enrollment_id, "requested_at": requested, "status": "open",
                   "owner": "Project Manager", "escalation_owner": "Admin"}
        request = get_security_store().mutate(request["request_id"], lambda before: before or request)
        requested = request["requested_at"]
        stopped = get_security_store().read(collection_key(management_row["watchName"], management_row["provider"]))
        update_cleanup_status(management_row["watchName"], management_row["provider"],
                              generation=stopped["generation"], data_deletion_status="requested")
        def withdrawal_deadline(current):
            if current.get("enrollment_id") != enrollment_id:
                raise PermissionError("Enrollment reassigned during withdrawal")
            return {**current, "deletion_due": requested, "deletion_requested_at": requested}
        get_security_store().mutate(collection_key(management_row["watchName"], management_row["provider"]), withdrawal_deadline)
    _append(
        spreadsheet,
        DELETION_REQUESTS_TAB,
        DELETION_COLUMNS,
        {
            "request_id": security_key("deletion-request", enrollment_id) if enrollment_id else new_uuid(),
            "management_token_hash": management_row["management_token_hash"],
            "watchName": management_row.get("watchName", ""),
            "project": management_row.get("project", ""),
            "provider": management_row.get("provider", ""),
            "requested_at": requested,
            "status": "open",
            "approved_policy_text": deletion_text(language),
        },
    )
    _update_row_by_keys(
        spreadsheet,
        MANAGEMENT_TAB,
        keys={"management_token_hash": management_row["management_token_hash"]},
        updates={"deletion_requested_at": requested},
    )
