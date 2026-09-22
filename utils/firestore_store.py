"""Server-side Firestore access for AdmonTracker operational records."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Iterable

from utils.firestore_schema import (
    MIGRATION_VERSION,
    build_migration_plan,
    sanitize_record,
    spec_for_source_sheet,
    stable_document_id,
    without_migration_metadata,
)
from utils.google_credentials import build_google_credentials, google_project_id


FIRESTORE_SCOPE = "https://www.googleapis.com/auth/cloud-platform"


def _chunks(items: list[Any], size: int) -> Iterable[list[Any]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


@lru_cache(maxsize=4)
def _firestore_client(project_id: str, database_id: str):
    try:
        from google.cloud import firestore
    except ImportError as exc:
        raise RuntimeError(
            "google-cloud-firestore is required for Firestore migration/runtime access"
        ) from exc
    credentials = build_google_credentials([FIRESTORE_SCOPE])
    return firestore.Client(
        project=project_id,
        credentials=credentials,
        database=database_id,
    )


@dataclass
class GoogleFirestoreStore:
    client: Any
    project_id: str
    database_id: str = "(default)"

    @classmethod
    def from_environment(cls) -> "GoogleFirestoreStore":
        project_id = (
            os.getenv("FIRESTORE_PROJECT_ID", "").strip()
            or google_project_id()
        )
        if not project_id:
            raise RuntimeError("FIRESTORE_PROJECT_ID or GOOGLE_CLOUD_PROJECT is required")
        database_id = os.getenv("FIRESTORE_DATABASE_ID", "(default)").strip() or "(default)"
        client = _firestore_client(project_id, database_id)
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

    def read_sheet_rows(self, source_sheet: str) -> list[dict[str, Any]]:
        spec = spec_for_source_sheet(source_sheet)
        if spec is None:
            raise KeyError(f"No Firestore mapping exists for sheet {source_sheet}")
        documents = self.read_documents(spec.collection, source_sheet=source_sheet)
        ordered = sorted(
            documents.items(),
            key=lambda item: (
                int((item[1].get("_migration") or {}).get("source_order") or 0),
                int((item[1].get("_migration") or {}).get("source_row") or 0),
                item[0],
            ),
        )
        return [without_migration_metadata(document) for _, document in ordered]

    def upsert_sheet_rows(self, source_sheet: str, rows: Iterable[dict[str, Any]]) -> int:
        spec = spec_for_source_sheet(source_sheet)
        if spec is None:
            return 0
        documents: dict[str, dict[str, Any]] = {}
        base_order = time.time_ns()
        for offset, raw in enumerate(rows):
            clean, _ = sanitize_record(spec, dict(raw))
            source_order = base_order + offset
            document_id = stable_document_id(spec, clean, source_order)
            clean["_migration"] = {
                "version": MIGRATION_VERSION,
                "source_sheet": source_sheet,
                "source_row": 0,
                "source_order": source_order,
                "runtime_write": True,
            }
            documents[document_id] = clean
        return self.write_documents(spec.collection, documents)

    def replace_sheet_rows(self, source_sheet: str, rows: Iterable[dict[str, Any]]) -> int:
        """Replace only documents managed for this source sheet."""
        spec = spec_for_source_sheet(source_sheet)
        if spec is None:
            return 0
        plan = build_migration_plan(spec, rows)
        existing = self.read_documents(spec.collection, source_sheet=source_sheet)
        stale_ids = sorted(set(existing) - set(plan.documents))
        self.delete_documents(spec.collection, stale_ids)
        return self.write_documents(spec.collection, plan.documents)

    def update_sheet_rows(
        self,
        source_sheet: str,
        *,
        keys: dict[str, Any],
        updates: dict[str, Any],
        latest_only: bool = True,
    ) -> int:
        spec = spec_for_source_sheet(source_sheet)
        if spec is None:
            return 0
        documents = self.read_documents(spec.collection, source_sheet=source_sheet)

        def equal(left: Any, right: Any) -> bool:
            if isinstance(left, bool):
                left = "TRUE" if left else "FALSE"
            if isinstance(right, bool):
                right = "TRUE" if right else "FALSE"
            return str(left or "").strip() == str(right or "").strip()

        matches = [
            (document_id, document)
            for document_id, document in documents.items()
            if all(equal(document.get(key), value) for key, value in keys.items())
        ]
        matches.sort(
            key=lambda item: (
                int((item[1].get("_migration") or {}).get("source_order") or 0),
                int((item[1].get("_migration") or {}).get("source_row") or 0),
                item[0],
            )
        )
        if latest_only and matches:
            matches = matches[-1:]

        clean_updates, _ = sanitize_record(spec, updates)
        pending: dict[str, dict[str, Any]] = {}
        for document_id, document in matches:
            pending[document_id] = {**document, **clean_updates}
        if pending:
            self.write_documents(spec.collection, pending)
        return len(pending)

    def delete_sheet_rows(
        self,
        source_sheet: str,
        *,
        keys: dict[str, Any],
        latest_only: bool = True,
    ) -> int:
        spec = spec_for_source_sheet(source_sheet)
        if spec is None:
            return 0
        documents = self.read_documents(spec.collection, source_sheet=source_sheet)
        matches = [
            (document_id, document)
            for document_id, document in documents.items()
            if all(str(document.get(key) or "").strip() == str(value or "").strip() for key, value in keys.items())
        ]
        matches.sort(
            key=lambda item: (
                int((item[1].get("_migration") or {}).get("source_order") or 0),
                int((item[1].get("_migration") or {}).get("source_row") or 0),
                item[0],
            )
        )
        if latest_only and matches:
            matches = matches[-1:]
        document_ids = [document_id for document_id, _ in matches]
        self.delete_documents(spec.collection, document_ids)
        return len(matches)

    def delete_documents(self, collection: str, document_ids: Iterable[str]) -> int:
        ids = list(document_ids)
        deleted = 0
        for chunk in _chunks(ids, 400):
            batch = self.client.batch()
            for document_id in chunk:
                batch.delete(self.client.collection(collection).document(document_id))
            batch.commit()
            deleted += len(chunk)
        return deleted
