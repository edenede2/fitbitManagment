"""Canonical Firestore layout for operational data migrated from Sheets."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterable


MIGRATION_VERSION = "sheets-to-firestore-v1"

# These values belong in Secret Manager, never Firestore.  Hashes and Secret
# Manager resource references are intentionally not included in this list.
GLOBAL_SECRET_FIELDS = frozenset(
    {
        "token",
        "access_token",
        "refresh_token",
        "client_secret",
        "credentials_json_raw",
        "cookie_secret",
    }
)


def _active(value: Any) -> bool:
    return str(value if value is not None else "").strip().casefold() in {
        "1",
        "true",
        "yes",
        "active",
        "connected",
    }


def _all(_: dict[str, Any]) -> bool:
    return True


def _active_client(row: dict[str, Any]) -> bool:
    return str(row.get("status") or "active").strip().casefold() == "active"


def _active_health_token(row: dict[str, Any]) -> bool:
    return _active(row.get("is_active", "TRUE"))


def _active_fitbit_token(row: dict[str, Any]) -> bool:
    return str(row.get("status") or "connected").strip().casefold() == "connected"


@dataclass(frozen=True)
class SheetMigrationSpec:
    source_sheet: str
    collection: str
    identity_fields: tuple[str, ...]
    include: Callable[[dict[str, Any]], bool] = field(default=_all, compare=False, repr=False)
    sensitive_fields: frozenset[str] = GLOBAL_SECRET_FIELDS
    allowed_fields: frozenset[str] | None = None
    profile: str = "core"


MIGRATION_SPECS: tuple[SheetMigrationSpec, ...] = (
    # Staff role/project authorization is authoritative in st.secrets.  The
    # user tab is retained only for participant/device assignment and alerts.
    SheetMigrationSpec(
        "user",
        "users",
        ("email",),
        allowed_fields=frozenset({"name", "email", "project"}),
    ),
    # Device.project is authoritative for grouping devices.  It intentionally
    # does not depend on a separate project-tab record.
    SheetMigrationSpec(
        "fitbit",
        "devices",
        ("project", "name"),
        allowed_fields=frozenset(
            {
                "project", "name", "token_secret_ref", "oauth_type", "provider",
                "oauth_client_key", "auth_status", "health_user_id",
                "legacy_fitbit_user_id", "last_successful_fetch_at",
                "last_data_timestamp", "last_auth_error", "reauth_link",
                "reauth_link_created_at", "user", "isActive", "currentStudent",
            }
        ),
    ),
    SheetMigrationSpec("fitbit_alerts_config", "alert_configurations", ("project", "manager", "watch")),
    SheetMigrationSpec(
        "health_oauth_clients",
        "oauth_clients",
        ("client_key",),
        include=_active_client,
    ),
    # One current record per watch/provider is sufficient for runtime.  The
    # append-only history remains in the read-only Sheet during the rollout.
    SheetMigrationSpec(
        "health_oauth_tokens",
        "health_oauth_tokens",
        ("watchName", "provider"),
        include=_active_health_token,
    ),
    SheetMigrationSpec(
        "fitbit_oauth_tokens",
        "fitbit_oauth_tokens",
        ("watchName",),
        include=_active_fitbit_token,
    ),
    SheetMigrationSpec("health_oauth_states", "oauth_states", ("state", "provider")),
    SheetMigrationSpec("health_oauth_state_used", "oauth_state_usage", ("state", "provider", "used_at")),
    SheetMigrationSpec("oauth_states", "fitbit_oauth_states", ("state",)),
    SheetMigrationSpec("oauth_state_used", "fitbit_oauth_state_usage", ("state", "used_at")),
    SheetMigrationSpec("health_oauth_consents", "oauth_consents", ("consent_id",)),
    SheetMigrationSpec(
        "health_reauth_queue",
        "reauth_queue",
        ("watchName", "provider"),
    ),
    SheetMigrationSpec(
        "health_connection_management",
        "connection_management",
        ("management_token_hash",),
    ),
    SheetMigrationSpec("health_deletion_requests", "deletion_requests", ("request_id",)),
    SheetMigrationSpec("archive_manifest", "archive_manifest", ("logical_key",)),
    SheetMigrationSpec("job_runs", "job_runs", ("run_id",)),
    SheetMigrationSpec("clock_status", "clock_status", ("process",)),
    SheetMigrationSpec("watch_status_history", "watch_status", ("watch_id", "name")),
    # Compact the large status tables to their latest row per watch.  Historical
    # rows remain in the frozen Sheet and raw research archives remain in Drive.
    SheetMigrationSpec("FitbitLog", "device_status", ("project", "watchName")),
    SheetMigrationSpec("log", "collection_status", ("project", "watchName")),
    SheetMigrationSpec(
        "health_api_logs",
        "health_api_logs",
        ("log_id",),
        profile="history",
    ),
    SheetMigrationSpec(
        "health_webhook_events",
        "health_webhook_events",
        ("event_id", "timestamp"),
        profile="history",
    ),
)


@dataclass(frozen=True)
class MigrationPlan:
    spec: SheetMigrationSpec
    documents: dict[str, dict[str, Any]]
    input_rows: int
    excluded_rows: int
    compacted_rows: int
    redacted_nonempty_fields: int


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _identity(spec: SheetMigrationSpec, row: dict[str, Any], source_row: int) -> dict[str, str]:
    values = {
        field: _text(row.get(field))
        for field in spec.identity_fields
        if _text(row.get(field))
    }
    if not values:
        values = {"source_row": str(source_row)}
    return values


def stable_document_id(
    spec: SheetMigrationSpec,
    row: dict[str, Any],
    source_row: int,
) -> str:
    """Create a stable, non-identifying document ID.

    Email addresses, watch labels, OAuth states, and project names never appear
    directly in a Firestore document path.
    """
    identity = _identity(spec, row, source_row)
    payload = json.dumps(
        {"sheet": spec.source_sheet, "identity": identity},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def sanitize_record(
    spec: SheetMigrationSpec,
    row: dict[str, Any],
) -> tuple[dict[str, Any], int]:
    sanitized: dict[str, Any] = {}
    redacted = 0
    sensitive = {field.casefold() for field in spec.sensitive_fields}
    allowed = (
        {field.casefold() for field in spec.allowed_fields}
        if spec.allowed_fields is not None
        else None
    )
    for raw_key, value in row.items():
        key = str(raw_key or "").strip()
        if not key:
            continue
        if key.casefold() in sensitive:
            if _text(value):
                redacted += 1
            continue
        if allowed is not None and key.casefold() not in allowed:
            continue
        sanitized[key] = value
    return sanitized, redacted


def build_migration_plan(
    spec: SheetMigrationSpec,
    rows: Iterable[dict[str, Any]],
    *,
    migrated_at: str | None = None,
) -> MigrationPlan:
    timestamp = migrated_at or datetime.now(timezone.utc).isoformat()
    documents: dict[str, dict[str, Any]] = {}
    input_rows = 0
    excluded_rows = 0
    redacted = 0

    for source_row, raw in enumerate(rows, start=2):
        input_rows += 1
        row = dict(raw)
        if not spec.include(row):
            excluded_rows += 1
            continue
        clean, removed = sanitize_record(spec, row)
        redacted += removed
        document_id = stable_document_id(spec, clean, source_row)
        clean["_migration"] = {
            "version": MIGRATION_VERSION,
            "source_sheet": spec.source_sheet,
            "source_row": source_row,
            "migrated_at": timestamp,
        }
        # Repeated operational records intentionally compact to the latest row
        # sharing the configured identity.
        documents[document_id] = clean

    selected = input_rows - excluded_rows
    return MigrationPlan(
        spec=spec,
        documents=documents,
        input_rows=input_rows,
        excluded_rows=excluded_rows,
        compacted_rows=max(selected - len(documents), 0),
        redacted_nonempty_fields=redacted,
    )


def without_migration_metadata(document: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in document.items() if key != "_migration"}


def canonical_documents_hash(documents: dict[str, dict[str, Any]]) -> str:
    normalized = [
        [document_id, without_migration_metadata(document)]
        for document_id, document in sorted(documents.items())
    ]
    payload = json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def specs_for_profile(profile: str, only: set[str] | None = None) -> list[SheetMigrationSpec]:
    if profile not in {"core", "all"}:
        raise ValueError("profile must be 'core' or 'all'")
    specs = [
        spec
        for spec in MIGRATION_SPECS
        if profile == "all" or spec.profile == "core"
    ]
    if only:
        known = {spec.source_sheet for spec in MIGRATION_SPECS}
        unknown = sorted(only - known)
        if unknown:
            raise ValueError("Unknown source sheets: " + ", ".join(unknown))
        specs = [spec for spec in specs if spec.source_sheet in only]
    return specs


def spec_for_source_sheet(source_sheet: str) -> SheetMigrationSpec | None:
    return next(
        (spec for spec in MIGRATION_SPECS if spec.source_sheet == source_sheet),
        None,
    )
