"""Shared visual branding for all Streamlit entry points."""

from pathlib import Path

import streamlit as st

from utils.compliance import PUBLIC_HOME_URL


LOGO_PATH = Path(__file__).resolve().parent.parent / "logo_10.jpg"


def render_app_logo(*, show_in_page: bool = False) -> None:
    """Render the lab logo consistently without exposing asset diagnostics."""
    if not LOGO_PATH.is_file():
        return
    st.logo(str(LOGO_PATH), size="large", link=PUBLIC_HOME_URL)
    if show_in_page:
        st.image(str(LOGO_PATH), width=520)
