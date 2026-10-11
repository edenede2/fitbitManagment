#!/usr/bin/env python3
"""Operator-approved first-Admin MFA bootstrap. Dry run unless --apply.

Production execution needs explicit approval of the project, binding and change
reference. Output is a private ignored file, never a setup secret in stdout.
"""
import argparse
import json
import os
from pathlib import Path
import secrets
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from utils.security_store import get_security_store, security_key


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--approved-binding", required=True, help="Private JSON for ONE operator-verified Admin binding")
    parser.add_argument("--confirm-project", required=True)
    parser.add_argument("--change-reference", required=True)
    parser.add_argument("--output", required=True, help="New private file under .heroku")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    try:
        binding = json.loads(Path(args.approved_binding).read_text())
        if (binding.get("role") != "Admin" or binding.get("enabled") is not True
                or binding.get("issuer") != "https://accounts.google.com"
                or not binding.get("subject") or "@" not in str(binding.get("email", ""))):
            raise ValueError("An explicitly approved Admin binding is required")
        target = Path(args.output).resolve()
        if not target.is_relative_to(ROOT / ".heroku") or target.exists():
            raise ValueError("Use a NEW ignored output file")
        actual_project = os.getenv("COLLECTION_CONTROL_PROJECT") or os.getenv("FIRESTORE_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")
        if args.apply and actual_project != args.confirm_project:
            raise ValueError("The configured target project does not match the reviewed change")
        if args.apply:
            store = get_security_store()
            principal = security_key("staff", binding["issuer"], binding["subject"])
            authority = store.read(security_key("staff-authority", principal))
            if authority.get("mfa_secret_ref"):
                raise ValueError("Existing MFA requires the authenticated recovery workflow, not bootstrap")
            code = secrets.token_urlsafe(32)
            now = time.time()
            key = security_key("mfa-enrollment", code)
            store.mutate(key, lambda _: {"principal_id": principal, "expires_at": now + 15 * 60,
                "approved_by": "operator_bootstrap", "change_reference": args.change_reference, "used": False})
            target.parent.mkdir(parents=True, exist_ok=True)
            descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w") as handle:
                handle.write(code + "\n")
        print(json.dumps({"mode": "applied_private_output" if args.apply else "dry_run", "cloud_writes": 1 if args.apply else 0,
                          "expiry_minutes": 15, "production_approval_required": True}))
        return 0
    except Exception:
        print("MFA bootstrap refused. Check the approved binding, project, output and change reference.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
