"""Small Google Secret Manager adapter for OAuth clients and participant tokens."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from utils.google_credentials import (
    build_google_credentials,
    google_project_id,
    runtime_setting,
)


SECRET_MANAGER_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
_SAFE_ID = re.compile(r"[^a-zA-Z0-9_-]+")


def _runtime_flag(name: str, default: bool = False) -> bool:
    value = runtime_setting(name)
    if not value:
        return default
    return value.casefold() in {"1", "true", "yes", "on"}


def secret_manager_enabled() -> bool:
    return _runtime_flag("SECRET_MANAGER_ENABLED", False)


def plaintext_secret_fallback_allowed() -> bool:
    return _runtime_flag("ALLOW_PLAINTEXT_SECRET_FALLBACK", True)


def safe_secret_id(kind: str, *identifiers: str) -> str:
    readable = "-".join(str(item or "").strip() for item in (kind, *identifiers))
    normalized = _SAFE_ID.sub("-", readable).strip("-").lower()
    digest = hashlib.sha256(readable.encode("utf-8")).hexdigest()[:12]
    prefix = normalized[:220].rstrip("-") or "admontracker-secret"
    return f"{prefix}-{digest}"


def _secret_name(project_id: str, secret_id: str) -> str:
    return f"projects/{project_id}/secrets/{secret_id}"


@dataclass
class GoogleSecretStore:
    project_id: str
    client: Any

    @classmethod
    def from_environment(cls) -> "GoogleSecretStore":
        try:
            from google.cloud import secretmanager
        except ImportError as exc:
            raise RuntimeError(
                "google-cloud-secret-manager is required when SECRET_MANAGER_ENABLED=true"
            ) from exc
        project_id = google_project_id()
        if not project_id:
            raise RuntimeError("GOOGLE_CLOUD_PROJECT is required for Secret Manager")
        credentials = build_google_credentials([SECRET_MANAGER_SCOPE])
        return cls(
            project_id=project_id,
            client=secretmanager.SecretManagerServiceClient(credentials=credentials),
        )

    def put_json(
        self,
        *,
        secret_id: str,
        payload: dict[str, Any],
        existing_ref: str = "",
    ) -> str:
        parent = f"projects/{self.project_id}"
        name = existing_ref.strip().split("/versions/", 1)[0] or _secret_name(
            self.project_id, secret_id
        )
        if not existing_ref:
            try:
                self.client.create_secret(
                    request={
                        "parent": parent,
                        "secret_id": secret_id,
                        "secret": {"replication": {"automatic": {}}},
                    }
                )
            except Exception as exc:
                # Keep provider exception imports out of callers and tests.
                already_exists = "already exists" in str(exc).casefold()
                already_exists = already_exists or "alreadyexists" in type(exc).__name__.casefold()
                if not already_exists:
                    raise
        encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        self.client.add_secret_version(
            request={"parent": name, "payload": {"data": encoded}}
        )
        return name

    def get_json(self, secret_ref: str) -> dict[str, Any]:
        name = secret_ref.strip()
        if "/versions/" not in name:
            name = f"{name}/versions/latest"
        response = self.client.access_secret_version(request={"name": name})
        parsed = json.loads(response.payload.data.decode("utf-8"))
        if not isinstance(parsed, dict):
            raise ValueError("Secret payload must be a JSON object")
        return parsed

    def destroy_versions(self, secret_ref: str) -> None:
        """Destroy enabled versions while retaining the auditable secret shell."""
        parent = secret_ref.split("/versions/", 1)[0].rstrip("/")
        for version in self.client.list_secret_versions(request={"parent": parent}):
            state = str(getattr(version, "state", "")).casefold()
            if "enabled" in state:
                self.client.destroy_secret_version(request={"name": version.name})


@lru_cache(maxsize=1)
def get_secret_store() -> GoogleSecretStore | None:
    if not secret_manager_enabled():
        return None
    return GoogleSecretStore.from_environment()


def store_json_secret(
    kind: str,
    identifiers: list[str],
    payload: dict[str, Any],
    *,
    existing_ref: str = "",
) -> str:
    store = get_secret_store()
    if store is None:
        return ""
    secret_ref = store.put_json(
        secret_id=safe_secret_id(kind, *identifiers),
        payload=payload,
        existing_ref=existing_ref,
    )
    _load_json_secret_cached.cache_clear()
    return secret_ref


@lru_cache(maxsize=512)
def _load_json_secret_cached(secret_ref: str) -> dict[str, Any]:
    store = get_secret_store()
    if store is None:
        raise RuntimeError("Secret Manager is disabled")
    return store.get_json(secret_ref)


def load_json_secret(secret_ref: str) -> dict[str, Any]:
    # Return a copy so callers cannot mutate the cached secret payload.
    return dict(_load_json_secret_cached(secret_ref))


def destroy_secret_versions(secret_ref: str) -> None:
    if not secret_ref:
        return
    store = get_secret_store()
    if store is None:
        raise RuntimeError("Secret Manager is disabled")
    store.destroy_versions(secret_ref)
    _load_json_secret_cached.cache_clear()


def clear_secret_store_cache() -> None:
    """Clear cached clients and payloads after a deployment-time rotation."""
    _load_json_secret_cached.cache_clear()
    get_secret_store.cache_clear()
