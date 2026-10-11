#!/usr/bin/env python3
"""Read-only, redacted credential-pattern scan. Never prints matched values.

This is a narrow local screen, not a replacement for an assessor-approved secret
scanner. Findings need private review; nothing is rotated, deleted or rewritten.
"""
import argparse
import json
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]
PATTERN = r"(-----BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY-----|GOCSPX-[A-Za-z0-9_-]{20,}|AIza[A-Za-z0-9_-]{35}|eyJ[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,})"
COMPILED = re.compile(PATTERN)


def git(*arguments):
    result = subprocess.run(["git", *arguments], cwd=ROOT, capture_output=True, check=False)
    if result.returncode not in (0, 1):
        raise RuntimeError("Read-only Git scan failed")
    return result.stdout.decode("utf-8", errors="replace")


def scan(history=False):
    paths = git("ls-files", "--cached", "--others", "--exclude-standard", "-z").split("\0")
    worktree = []
    for relative in paths:
        if not relative:
            continue
        path = ROOT / relative
        if path.is_file() and path.stat().st_size <= 20_000_000:
            try:
                value = path.read_bytes().decode("utf-8")
            except UnicodeError:
                continue
            if COMPILED.search(value):
                worktree.append(relative)
    occurrences = []
    commits = git("rev-list", "--all").splitlines() if history else []
    for commit in commits:
        # -l returns file paths only, never the source line or matched secret.
        for match in git("grep", "-I", "-l", "-E", PATTERN, commit).splitlines():
            occurrences.append(match)
    return {"scanner": "local-narrow-credential-patterns-v1", "matched_values_in_output": False,
            "scope": "nonignored text worktree and all local Git refs" if history else "nonignored text worktree",
            "commits_examined": len(commits), "worktree_files_flagged": sorted(set(worktree)),
            "historical_files_flagged": sorted({item.split(":", 1)[1] for item in occurrences}),
            "historical_occurrence_count": len(occurrences),
            "limitations": ["Patterns do not detect every OAuth token or password", "Synthetic examples can be false positives",
                            "Ignored private files and cloud stores are not scanned", "Git refs unavailable locally are not examined"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history", action="store_true")
    args = parser.parse_args()
    print(json.dumps(scan(args.history), indent=2))
