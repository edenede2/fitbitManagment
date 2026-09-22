"""Configuration for the staged Sheets-to-Firestore migration.

Sheets remains the default until an operator explicitly completes the import,
verification, shadow-write, and smoke-test gates.  Merely deploying Firestore
support must never change the production data source.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().casefold() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class DataBackendConfig:
    backend: str
    firestore_project_id: str
    firestore_database_id: str
    firestore_shadow_write: bool
    sheets_read_fallback: bool
    sheets_shadow_write: bool

    @classmethod
    def from_environment(cls) -> "DataBackendConfig":
        backend = os.getenv("DATA_BACKEND", "sheets").strip().casefold() or "sheets"
        if backend not in {"sheets", "firestore"}:
            raise ValueError("DATA_BACKEND must be either 'sheets' or 'firestore'")

        project_id = (
            os.getenv("FIRESTORE_PROJECT_ID", "").strip()
            or os.getenv("GOOGLE_CLOUD_PROJECT", "").strip()
        )
        database_id = os.getenv("FIRESTORE_DATABASE_ID", "(default)").strip() or "(default)"
        config = cls(
            backend=backend,
            firestore_project_id=project_id,
            firestore_database_id=database_id,
            firestore_shadow_write=_flag("FIRESTORE_SHADOW_WRITE"),
            sheets_read_fallback=_flag("SHEETS_READ_FALLBACK", default=True),
            sheets_shadow_write=_flag("SHEETS_SHADOW_WRITE"),
        )
        if backend == "firestore" and not project_id:
            raise ValueError(
                "FIRESTORE_PROJECT_ID or GOOGLE_CLOUD_PROJECT is required when "
                "DATA_BACKEND=firestore"
            )
        return config


def firestore_migration_enabled() -> bool:
    config = DataBackendConfig.from_environment()
    return config.backend == "firestore" or config.firestore_shadow_write
