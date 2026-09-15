"""Small Google Secret Manager adapter for OAuth clients and participant tokens."""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from typing import Any

from utils.google_credentials import build_google_credentials, google_project_id


SECRET_MANAGER_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
_SAFE_ID = re.compile(r"[^a-zA-Z0-9_-]+")


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().casefold() in {"1", "true", "yes", "on"}


def secret_manager_enabled() -> bool:
    return _env_flag("SECRET_MANAGER_ENABLED", False)


def plaintext_secret_fallback_allowed() -> bool:
    return _env_flag("ALLOW_PLAINTEXT_SECRET_FALLBACK", True)


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
        name = existing_ref.strip() or _secret_name(self.project_id, secret_id)
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
                # AlreadyExists is intentionally tolerated without importing a
                # provider-specific exception hierarchy into callers/tests.
                if "already exists" not in str(exc).casefold() and "alreadyexists" not in type(exc).__name__.casefold():
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
        payload = response.payload.data.decode("utf-8")
        parsed = json.loads(payload)
        if not isinstance(parsed, dict):
            raise ValueError("Secret payload must be a JSON object")
        return parsed

    def destroy_versions(self, secret_ref: str) -> None:
        """Destroy all enabled versions while retaining the auditable secret shell."""
        parent = secret_ref.split("/versions/", 1)[0].rstrip("/")
        for version in self.client.list_secret_versions(request={"parent": parent}):
            state = str(getattr(version, "state", "")).casefold()
            if "enabled" in state:
                self.client.destroy_secret_version(request={"name": version.name})


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
    return store.put_json(
        secret_id=safe_secret_id(kind, *identifiers),
        payload=payload,
        existing_ref=existing_ref,
    )


def load_json_secret(secret_ref: str) -> dict[str, Any]:
    store = get_secret_store()
    if store is None:
        raise RuntimeError("Secret Manager is disabled")
    return store.get_json(secret_ref)


def destroy_secret_versions(secret_ref: str) -> None:
    if not secret_ref:
        return
    store = get_secret_store()
    if store is None:
        raise RuntimeError("Secret Manager is disabled")
    store.destroy_versions(secret_ref)
