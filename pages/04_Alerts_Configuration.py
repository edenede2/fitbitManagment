import streamlit as st
from view.alerts_config import alerts_config_page

# Page configuration
st.set_page_config(
    page_title="Alerts Configuration - Fitbit Management System",
    page_icon="🔔",
    layout="wide"
)

# Check authentication
if 'user_email' not in st.session_state:
    st.warning("Please log in from the main page to access this feature.")
    st.stop()

# Check role permissions (only Manager and Admin have access)
user_role = st.session_state.get('user_role', 'Guest')
if user_role not in ['Admin', 'manager']:
    st.warning("You don't have permission to access this page.")
    st.stop()

# Get data from session state
user_email = st.session_state.user_email
spreadsheet = st.session_state.get('spreadsheet', None)

# Display alerts configuration interface
alerts_config_page(user_email, spreadsheet)
