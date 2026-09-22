"""Server-side Firestore access for AdmonTracker operational records."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Iterable

from utils.google_credentials import build_google_credentials, google_project_id


FIRESTORE_SCOPE = "https://www.googleapis.com/auth/cloud-platform"


def _chunks(items: list[Any], size: int) -> Iterable[list[Any]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


@dataclass
class GoogleFirestoreStore:
    client: Any
    project_id: str
    database_id: str = "(default)"

    @classmethod
    def from_environment(cls) -> "GoogleFirestoreStore":
        try:
            from google.cloud import firestore
        except ImportError as exc:
            raise RuntimeError(
                "google-cloud-firestore is required for Firestore migration/runtime access"
            ) from exc

        project_id = (
            os.getenv("FIRESTORE_PROJECT_ID", "").strip()
            or google_project_id()
        )
        if not project_id:
            raise RuntimeError("FIRESTORE_PROJECT_ID or GOOGLE_CLOUD_PROJECT is required")
        database_id = os.getenv("FIRESTORE_DATABASE_ID", "(default)").strip() or "(default)"
        credentials = build_google_credentials([FIRESTORE_SCOPE])
        client = firestore.Client(
            project=project_id,
            credentials=credentials,
            database=database_id,
        )
        return cls(client=client, project_id=project_id, database_id=database_id)

    def write_documents(
        self,
        collection: str,
        documents: dict[str, dict[str, Any]],
        *,
        batch_size: int = 400,
    ) -> int:
        """Idempotently replace documents with deterministic IDs."""
        items = list(documents.items())
        written = 0
        for chunk in _chunks(items, min(max(batch_size, 1), 400)):
            batch = self.client.batch()
            for document_id, data in chunk:
                batch.set(self.client.collection(collection).document(document_id), data)
            batch.commit()
            written += len(chunk)
        return written

    def read_documents(
        self,
        collection: str,
        *,
        source_sheet: str | None = None,
    ) -> dict[str, dict[str, Any]]:
        documents: dict[str, dict[str, Any]] = {}
        for snapshot in self.client.collection(collection).stream():
            data = snapshot.to_dict() or {}
            if source_sheet:
                metadata = data.get("_migration") or {}
                if metadata.get("source_sheet") != source_sheet:
                    continue
            documents[snapshot.id] = data
        return documents

    def write_migration_run(self, run_id: str, values: dict[str, Any]) -> None:
        self.client.collection("_migration_runs").document(run_id).set(values)
