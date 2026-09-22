"""Shared visual branding for all Streamlit entry points."""

from pathlib import Path

import streamlit as st

from utils.compliance import PUBLIC_HOME_URL


LOGO_PATH = Path(__file__).resolve().parent.parent / "logo_10.jpg"


def render_sidebar_navigation() -> None:
    """Show only the staff-facing operational pages in Streamlit navigation."""
    with st.sidebar:
        st.page_link("app.py", label="Welcome", icon="🏠")
        st.page_link("pages/01_Home.py", label="Study overview", icon="📋")
        st.page_link("pages/02_Dashboard.py", label="Dashboard", icon="📊")
        st.page_link("pages/03_Fitbit_Management.py", label="Device management", icon="⌚")
        st.page_link("pages/04_Alerts_Configuration.py", label="Alert configuration", icon="🔔")
        st.page_link("pages/07_OAuth_Connect.py", label="Connect wearable", icon="🔗")


def render_app_logo(*, show_in_page: bool = False, show_navigation: bool = True) -> None:
    """Render shared branding and, where appropriate, curated staff navigation."""
    if LOGO_PATH.is_file():
        st.logo(str(LOGO_PATH), size="large", link=PUBLIC_HOME_URL)
        if show_in_page:
            st.image(str(LOGO_PATH), width=520)
    if show_navigation:
        render_sidebar_navigation()
