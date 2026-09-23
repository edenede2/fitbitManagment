"""Idempotent Google Shared Drive archive storage."""

from __future__ import annotations

import hashlib
import io
import os
import re
from dataclasses import dataclass
from typing import Any

from utils.google_credentials import build_google_credentials


DRIVE_SCOPE = "https://www.googleapis.com/auth/drive"
FOLDER_MIME = "application/vnd.google-apps.folder"
ZIP_MIME = "application/zip"
JSON_MIME = "application/json"
_UNSAFE_SEGMENT = re.compile(r"[\\/\x00-\x1f\x7f]+")
ARCHIVE_CATEGORIES = ("Physical Activity", "Sleep", "Stress")


def safe_drive_segment(value: str) -> str:
    value = _UNSAFE_SEGMENT.sub("-", str(value or "").strip())
    value = " ".join(value.split()).strip(" .")
    if not value or value in {".", ".."} or not any(character.isalnum() for character in value):
        raise ValueError("Drive archive path segment is empty or unsafe")
    return value[:180]


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class DriveUpload:
    file_id: str
    web_view_link: str
    checksum: str
    size: int
    action: str


class SharedDriveArchive:
    def __init__(self, *, root_folder_id: str, service: Any | None = None):
        self.root_folder_id = str(root_folder_id or "").strip()
        if not self.root_folder_id:
            raise ValueError("GOOGLE_DRIVE_ARCHIVE_ROOT_ID is required")
        if service is None:
            from googleapiclient.discovery import build

            service = build(
                "drive",
                "v3",
                credentials=build_google_credentials([DRIVE_SCOPE]),
                cache_discovery=False,
            )
        self.service = service
        self._folder_cache: dict[tuple[str, str], str] = {}

    @classmethod
    def from_environment(cls) -> "SharedDriveArchive":
        return cls(root_folder_id=os.getenv("GOOGLE_DRIVE_ARCHIVE_ROOT_ID", ""))

    @staticmethod
    def _escape_query(value: str) -> str:
        return value.replace("\\", "\\\\").replace("'", "\\'")

    def _find_child(self, parent_id: str, name: str, mime_type: str) -> dict[str, Any] | None:
        escaped = self._escape_query(name)
        response = self.service.files().list(
            q=(
                f"'{parent_id}' in parents and name = '{escaped}' and "
                f"mimeType = '{mime_type}' and trashed = false"
            ),
            spaces="drive",
            fields="files(id,name,mimeType,webViewLink,modifiedTime,size)",
            pageSize=10,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        files = response.get("files", [])
        if len(files) > 1:
            raise RuntimeError(f"Duplicate Drive archive entries named {name!r} under {parent_id}")
        return files[0] if files else None

    def ensure_folder(self, parent_id: str, name: str) -> str:
        name = safe_drive_segment(name)
        cache_key = (parent_id, name)
        if cache_key in self._folder_cache:
            return self._folder_cache[cache_key]
        existing = self._find_child(parent_id, name, FOLDER_MIME)
        if existing:
            folder_id = existing["id"]
        else:
            created = self.service.files().create(
                body={"name": name, "mimeType": FOLDER_MIME, "parents": [parent_id]},
                fields="id",
                supportsAllDrives=True,
            ).execute()
            folder_id = created["id"]
        self._folder_cache[cache_key] = folder_id
        return folder_id

    def ensure_archive_path(self, project: str, watch_name: str, data_type: str) -> str:
        """Return the legacy ZIP path.

        Kept so existing operator tools and historical ZIPs remain readable. New
        collection writes use :meth:`ensure_provider_layout` instead.
        """
        archive_root_name = os.getenv(
            "GOOGLE_DRIVE_ARCHIVE_SUBFOLDER", "AdmonTracker Raw Archive"
        )
        archive_root = self.ensure_folder(self.root_folder_id, archive_root_name)
        project_id = self.ensure_folder(archive_root, project)
        watch_id = self.ensure_folder(project_id, watch_name)
        return self.ensure_folder(watch_id, data_type)

    def ensure_provider_layout(
        self,
        *,
        project: str,
        watch_name: str,
        provider: str,
    ) -> dict[str, str]:
        """Create the reviewer-approved wearable archive folder hierarchy."""
        archive_root_name = os.getenv(
            "GOOGLE_DRIVE_ARCHIVE_SUBFOLDER", "AdmonTracker Raw Archive"
        )
        archive_root = self.ensure_folder(self.root_folder_id, archive_root_name)
        project_id = self.ensure_folder(archive_root, project)
        watch_id = self.ensure_folder(project_id, watch_name)
        provider_id = self.ensure_folder(watch_id, provider)
        return {
            category: self.ensure_folder(provider_id, category)
            for category in ARCHIVE_CATEGORIES
        }

    def upsert_file(
        self,
        *,
        project: str,
        watch_name: str,
        provider: str,
        category: str,
        filename: str,
        content: bytes,
        mime_type: str = JSON_MIME,
    ) -> DriveUpload:
        """Create or replace one deterministic raw archive file."""
        from googleapiclient.http import MediaIoBaseUpload

        if category not in ARCHIVE_CATEGORIES:
            raise ValueError(f"Unsupported archive category: {category}")
        folders = self.ensure_provider_layout(
            project=project,
            watch_name=watch_name,
            provider=provider,
        )
        parent_id = folders[category]
        filename = safe_drive_segment(filename)
        existing = self._find_child(parent_id, filename, mime_type)
        media = MediaIoBaseUpload(
            io.BytesIO(content),
            mimetype=mime_type,
            resumable=True,
        )
        if existing:
            response = self.service.files().update(
                fileId=existing["id"],
                media_body=media,
                fields="id,webViewLink,size",
                supportsAllDrives=True,
            ).execute()
            action = "updated"
        else:
            response = self.service.files().create(
                body={
                    "name": filename,
                    "mimeType": mime_type,
                    "parents": [parent_id],
                },
                media_body=media,
                fields="id,webViewLink,size",
                supportsAllDrives=True,
            ).execute()
            action = "created"
        return DriveUpload(
            file_id=str(response.get("id") or ""),
            web_view_link=str(response.get("webViewLink") or ""),
            checksum=sha256_bytes(content),
            size=len(content),
            action=action,
        )

    def upsert_zip(
        self,
        *,
        project: str,
        watch_name: str,
        data_type: str,
        filename: str,
        content: bytes,
    ) -> DriveUpload:
        from googleapiclient.http import MediaIoBaseUpload

        parent_id = self.ensure_archive_path(project, watch_name, data_type)
        filename = safe_drive_segment(filename)
        existing = self._find_child(parent_id, filename, ZIP_MIME)
        media = MediaIoBaseUpload(io.BytesIO(content), mimetype=ZIP_MIME, resumable=True)
        if existing:
            response = self.service.files().update(
                fileId=existing["id"],
                media_body=media,
                fields="id,webViewLink,size",
                supportsAllDrives=True,
            ).execute()
            action = "updated"
        else:
            response = self.service.files().create(
                body={"name": filename, "mimeType": ZIP_MIME, "parents": [parent_id]},
                media_body=media,
                fields="id,webViewLink,size",
                supportsAllDrives=True,
            ).execute()
            action = "created"
        return DriveUpload(
            file_id=str(response.get("id") or ""),
            web_view_link=str(response.get("webViewLink") or ""),
            checksum=sha256_bytes(content),
            size=len(content),
            action=action,
        )

    def audit_root_permissions(self) -> dict[str, Any]:
        response = self.service.permissions().list(
            fileId=self.root_folder_id,
            fields="permissions(id,type,emailAddress,domain,role,displayName,deleted)",
            supportsAllDrives=True,
        ).execute()
        permissions = response.get("permissions", [])
        public = [item for item in permissions if item.get("type") == "anyone"]
        return {
            "permission_count": len(permissions),
            "public_permission_count": len(public),
            "is_public": bool(public),
            "permissions": permissions,
        }
