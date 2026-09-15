#!/usr/bin/env python3
"""Audit the configured Shared Drive folder and optionally create the archive child."""

from __future__ import annotations

import argparse
import base64
import io
import json
import os
import zipfile
from pathlib import Path

from services.drive_archive import SharedDriveArchive


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--create-subfolder",
        action="store_true",
        help="Create/reuse GOOGLE_DRIVE_ARCHIVE_SUBFOLDER below the configured root",
    )
    parser.add_argument(
        "--synthetic-upsert-test",
        action="store_true",
        help="Create/update one non-research ZIP in a dedicated integration-test path",
    )
    parser.add_argument(
        "--service-account",
        type=Path,
        help="Ignored local service-account JSON for an operator-run audit",
    )
    args = parser.parse_args()
    if args.service_account:
        raw_credentials = args.service_account.read_bytes()
        parsed_credentials = json.loads(raw_credentials)
        if parsed_credentials.get("type") != "service_account":
            raise SystemExit("--service-account must contain service-account JSON")
        os.environ["GOOGLE_SERVICE_ACCOUNT_JSON_B64"] = base64.b64encode(
            raw_credentials
        ).decode("ascii")
    archive = SharedDriveArchive.from_environment()
    audit = archive.audit_root_permissions()
    summary = {
        "permission_count": audit["permission_count"],
        "public_permission_count": audit["public_permission_count"],
        "is_public": audit["is_public"],
        "permission_types": sorted({str(item.get("type") or "") for item in audit["permissions"]}),
        "permission_domains": sorted({str(item.get("domain") or "") for item in audit["permissions"] if item.get("domain")}),
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    if audit["is_public"]:
        raise SystemExit("Archive root has a public/anyone permission; do not store research data here")
    if args.create_subfolder:
        child = archive.ensure_folder(
            archive.root_folder_id,
            os.getenv("GOOGLE_DRIVE_ARCHIVE_SUBFOLDER", "AdmonTracker Raw Archive"),
        )
        print(f"Archive subfolder is ready: {child}")
    if args.synthetic_upsert_test:
        os.environ["GOOGLE_DRIVE_ARCHIVE_SUBFOLDER"] = "AdmonTracker Synthetic Integration Tests"
        stream = io.BytesIO()
        with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
            info = zipfile.ZipInfo("synthetic.txt", date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            bundle.writestr(
                info,
                b"AdmonTracker synthetic Drive integration test; contains no research data.\n",
            )
        content = stream.getvalue()
        values = {
            "project": "synthetic",
            "watch_name": "integration-test",
            "data_type": "steps",
            "filename": "steps_1970-01.zip",
            "content": content,
        }
        first = archive.upsert_zip(**values)
        second = archive.upsert_zip(**values)
        if first.file_id != second.file_id or first.checksum != second.checksum:
            raise SystemExit("Shared Drive upsert was not idempotent")
        print(
            "Synthetic Shared Drive upsert passed: "
            f"file_id={second.file_id}, first={first.action}, second={second.action}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
