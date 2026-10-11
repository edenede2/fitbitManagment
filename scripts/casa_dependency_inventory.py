#!/usr/bin/env python3
"""Safe deployed-artifact inventory: no env, credentials, URLs, or package source."""
import importlib.metadata
import json
import platform
from packaging.requirements import Requirement


def inventory():
    packages = []
    for distribution in importlib.metadata.distributions():
        dependencies = []
        for raw in distribution.requires or []:
            requirement = Requirement(raw)
            # Never output direct URLs: private indexes can embed credentials.
            dependencies.append({"name": requirement.name, "specifier": str(requirement.specifier),
                                 "marker": str(requirement.marker or "")})
        packages.append({"name": distribution.metadata["Name"], "version": distribution.version,
                         "dependencies": dependencies})
    return {"python": platform.python_version(), "packages": sorted(packages, key=lambda p: p["name"].casefold())}


if __name__ == "__main__":
    print(json.dumps(inventory(), indent=2))
