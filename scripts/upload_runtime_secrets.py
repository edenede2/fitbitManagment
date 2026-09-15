#!/usr/bin/env python3
"""Upload the ignored Streamlit runtime bundle to Google Secret Manager."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Support ``python3 scripts/<name>.py`` from the repository root and Heroku.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.secret_store import GoogleSecretStore


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--payload", type=Path, default=Path(".heroku/runtime-secrets-payload.json"))
    parser.add_argument("--secret-id", default="admontracker-streamlit-production")
    args = parser.parse_args()
    payload = json.loads(args.payload.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not payload.get("toml_b64"):
        raise SystemExit("Runtime payload must contain toml_b64")
    store = GoogleSecretStore.from_environment()
    ref = store.put_json(secret_id=args.secret_id, payload=payload)
    verified = store.get_json(ref)
    if verified.get("toml_b64") != payload["toml_b64"]:
        raise SystemExit("Secret Manager readback verification failed")
    print(f"Uploaded and verified {ref}. Set STREAMLIT_SECRETS_SECRET_REF to this value.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
