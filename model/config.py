import os
import json
from pathlib import Path

def get_secrets():
    """Get secrets from the local server file, Streamlit, or a local JSON file."""
    streamlit_secrets_path = Path(__file__).resolve().parent.parent / ".streamlit" / "secrets.toml"
    if streamlit_secrets_path.exists():
        try:
            import toml
            return toml.load(streamlit_secrets_path)
        except Exception as e:
            raise RuntimeError(f"Failed to load local Streamlit secrets from {streamlit_secrets_path}: {e}")

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
