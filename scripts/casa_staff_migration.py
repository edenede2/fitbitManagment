#!/usr/bin/env python3
"""Prepare an ignored, reviewed runtime bundle; default is a redacted dry run.

No Google account is inferred from an old username. No cloud write, key rotation,
upload or role enablement occurs here. An operator verifies and approves bindings.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import toml
from utils.access_control import staff_identity_assignment


def validate_bindings(bindings, parsed):
    if not isinstance(bindings, list) or not bindings:
        raise ValueError("Provide a nonempty list of explicitly approved bindings")
    seen = set()
    counts = {"Admin": 0, "Manager": 0}
    expected_audience = parsed.get("auth", {}).get("google", {}).get("client_id")
    for binding in bindings:
        if not isinstance(binding, dict) or not binding.get("approval_reference"):
            raise ValueError("Each binding needs a recorded operator/Admin approval reference")
        identity = (binding.get("issuer"), binding.get("subject"))
        if identity in seen:
            raise ValueError("Duplicate staff identity")
        seen.add(identity)
        claims = {"is_logged_in": True, "iss": binding.get("issuer"), "sub": binding.get("subject"),
                  "email": binding.get("email"), "email_verified": True, "exp": 4102444800, "aud": expected_audience}
        context = staff_identity_assignment({**parsed, "staff_identities": [binding]}, claims)
        if not context.can_read_real_data:
            raise ValueError("A binding is invalid, disabled, unsupported or insufficiently scoped")
        counts[context.role] += 1
    if not counts["Admin"]:
        raise ValueError("At least one explicitly approved Admin is required")
    return counts


def legacy_assignment_counts(parsed):
    """Count old scalar entries without exposing names/projects or granting access."""
    counts = {"Admin": 0, "Manager": 0, "Student": 0}
    def visit(table):
        if not isinstance(table, dict):
            return
        for value in table.values():
            if isinstance(value, dict):
                visit(value)
            elif isinstance(value, str) and "," in value:
                role = value.split(",", 1)[0].strip().title()
                if role in counts:
                    counts[role] += 1
    visit(parsed)
    return counts


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--payload", required=True)
    parser.add_argument("--approved-bindings")
    parser.add_argument("--inventory-only", action="store_true", help="Count old local scalar assignments; no identities inferred")
    parser.add_argument("--output", help="Write a NEW ignored reviewed payload; otherwise dry run")
    parser.add_argument("--request-google-mfa-claims", action="store_true")
    args = parser.parse_args(argv)
    try:
        payload = json.loads(Path(args.payload).read_text())
        parsed = toml.loads(base64.b64decode(payload["toml_b64"], validate=True).decode())
        legacy = legacy_assignment_counts(parsed)
        if args.inventory_only:
            if args.output or args.approved_bindings or args.request_google_mfa_claims:
                raise ValueError("Inventory mode cannot prepare or change anything")
            print(json.dumps({"mode": "local_inventory_only", "legacy_scalar_role_counts": legacy,
                              "identity_bindings_inferred": 0, "cloud_writes": 0,
                              "live_production_assignments_verified": False}))
            return 0
        if not args.approved_bindings:
            raise ValueError("Approved bindings are required for migration")
        bindings = json.loads(Path(args.approved_bindings).read_text())
        counts = validate_bindings(bindings, parsed)
        parsed["staff_identities"] = bindings
        if args.request_google_mfa_claims:
            google = parsed["auth"]["google"]
            kwargs = google.setdefault("client_kwargs", {})
            kwargs["claims"] = json.dumps({"id_token": {"amr": {"essential": True}, "auth_time": {"essential": True}}})
        if args.output:
            target = Path(args.output).resolve()
            if not target.is_relative_to(ROOT / ".heroku"):
                raise ValueError("Reviewed credentials must remain inside the ignored .heroku directory")
            target.parent.mkdir(parents=True, exist_ok=True)
            reviewed = {**payload, "toml_b64": base64.b64encode(toml.dumps(parsed).encode()).decode()}
            descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w") as handle:
                json.dump(reviewed, handle)
        print(json.dumps({"mode": "prepared_private_file" if args.output else "dry_run", "role_counts": counts,
                          "legacy_scalar_role_counts": legacy,
                          "prefix_entries_grant_access": False, "cloud_writes": 0,
                          "student_access": "disabled", "publication_or_key_changes": False}))
        return 0
    except Exception:
        # Parsing errors can contain TOML/source fragments with secret values.
        print("Staff migration refused. Validate the private input schema and approval records.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
