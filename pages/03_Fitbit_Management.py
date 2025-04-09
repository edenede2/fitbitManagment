import streamlit as st
from view.fitbit_management import load_fitbit_datatable

# Page configuration
st.set_page_config(
    page_title="Fitbit Management - Fitbit Management System",
    page_icon="⌚",
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
user_project = st.session_state.get('user_project', 'None')
spreadsheet = st.session_state.get('spreadsheet', None)

# Display Fitbit management interface
load_fitbit_datatable(user_email, user_role, user_project, spreadsheet)
