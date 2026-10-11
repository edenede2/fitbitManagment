"""Authoritative security records. Never falls back to Sheets or process memory.

Transaction callbacks must be pure: no OAuth, Secret Manager, email or Drive calls.
An injected store is used by synthetic tests; production always uses Firestore.
"""
from __future__ import annotations

import hashlib
import json
import os
from copy import deepcopy
from typing import Callable


def security_key(kind: str, *parts: str) -> str:
    value = json.dumps([kind, *parts], separators=(",", ":"))
    return hashlib.sha256(value.encode()).hexdigest()


class FirestoreSecurityStore:
    def __init__(self, client):
        self.client = client

    def read(self, key: str) -> dict:
        snapshot = self.client.collection("_casa_security").document(key).get()
        return snapshot.to_dict() or {} if snapshot.exists else {}

    def mutate(self, key: str, change: Callable[[dict], dict]) -> dict:
        return self.mutate_document("_casa_security", key, change)

    def mutate_document(self, collection: str, key: str, change: Callable[[dict], dict], *,
                        dependency_keys=(), validate_dependencies=None) -> dict:
        from google.cloud import firestore

        reference = self.client.collection(collection).document(key)

        @firestore.transactional
        def update(transaction):
            snapshot = reference.get(transaction=transaction)
            before = snapshot.to_dict() or {} if snapshot.exists else {}
            dependencies = []
            for dependency in dependency_keys:
                dep = self.client.collection("_casa_security").document(dependency).get(transaction=transaction)
                dependencies.append(dep.to_dict() or {} if dep.exists else {})
            if validate_dependencies:
                validate_dependencies(before, dependencies)
            after = change(deepcopy(before))
            transaction.set(reference, after)
            return after

        return update(self.client.transaction())

    def fenced_write(self, gate_key: str, generation: int, collection: str,
                     document_id: str, data: dict) -> None:
        """Publish metadata only if disconnect/reassignment has not won the race."""
        from google.cloud import firestore
        from utils.collection_control import CollectionStopped

        gate = self.client.collection("_casa_security").document(gate_key)
        target = self.client.collection(collection).document(document_id)

        @firestore.transactional
        def publish(transaction):
            snapshot = gate.get(transaction=transaction)
            current = snapshot.to_dict() or {} if snapshot.exists else {}
            if current.get("collection_stopped") or int(current.get("generation", 0)) != generation:
                raise CollectionStopped("The enrollment changed before token publication")
            transaction.set(target, data)

        publish(self.client.transaction())


def get_security_store() -> FirestoreSecurityStore:
    from utils.firestore_store import GoogleFirestoreStore, _firestore_client

    # Allows a reviewed legacy backport to consult the same authoritative gate
    # without migrating its measurement/operational tables away from Sheets.
    project = os.getenv("COLLECTION_CONTROL_PROJECT", "").strip()
    if project:
        database = os.getenv("FIRESTORE_DATABASE_ID", "(default)")
        return FirestoreSecurityStore(_firestore_client(project, database))
    return FirestoreSecurityStore(GoogleFirestoreStore.from_environment().client)


def collection_controls_enabled() -> bool:
    return bool(
        os.getenv("DATA_BACKEND", "").casefold() == "firestore"
        or os.getenv("COLLECTION_CONTROL_PROJECT", "").strip()
    )
