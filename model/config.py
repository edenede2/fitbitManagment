import os
import json
from pathlib import Path


def _merge_dicts(base, override):
    """Recursively merge secret dictionaries, with later files overriding earlier ones."""
    merged = dict(base or {})
    for key, value in dict(override or {}).items():
        if isinstance(merged.get(key), dict) and isinstance(value, dict):
            merged[key] = _merge_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged


def _load_toml_file(path):
    try:
        import toml
        return toml.load(path)
    except Exception as e:
        raise RuntimeError(f"Failed to load local secrets from {path}: {e}")


def get_secrets():
    """Get secrets from the local server file, Streamlit, or a local JSON file."""
    project_root = Path(__file__).resolve().parent.parent
    local_secret_paths = [
        project_root / ".streamlit" / "secrets.toml",
        project_root / "stsecrets.txt",
        project_root / "stsecters.txt",
    ]
    loaded_secrets = {}
    for secret_path in local_secret_paths:
        if secret_path.exists():
            loaded_secrets = _merge_dicts(loaded_secrets, _load_toml_file(secret_path))
    if loaded_secrets:
        return loaded_secrets

    try:
        # Try to import Streamlit
        import streamlit as st
        return st.secrets
    except (ImportError, AttributeError):
        # If Streamlit is not available or secrets not found, use local file
        config_path = Path(__file__).parent / "secrets.json"
        if config_path.exists():
            with open(config_path, "r") as f:
                return json.load(f)
        else:
            raise FileNotFoundError(f"Secrets file not found at {config_path}. Create this file for cron jobs.")
