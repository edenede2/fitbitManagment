#!/usr/bin/env python3
"""Hash nonignored Python source/dependency declarations; never emits contents."""
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def inventory():
    listing = subprocess.run(["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=ROOT, capture_output=True, check=True).stdout.decode().split("\0")
    files = []
    for relative in sorted(set(listing)):
        path = ROOT / relative
        if path.is_file() and (path.suffix == ".py" or relative == "requirements.txt"):
            files.append({"path": relative, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    digest = hashlib.sha256(json.dumps(files, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return {"scope": "nonignored Python files and requirements.txt", "sha256": digest, "files": files}


if __name__ == "__main__":
    print(json.dumps(inventory(), indent=2))
