import streamlit as st
from view.dashboard import display_dashboard

# Page configuration
st.set_page_config(
    page_title="Dashboard - Fitbit Management System",
    page_icon="📊",
    layout="wide"
)

# Check authentication
if 'user_email' not in st.session_state:
    st.warning("Please log in from the main page to access this feature.")
    st.stop()

# Check role permissions (Student, Manager, Admin have access)
user_role = st.session_state.get('user_role', 'Guest')
if user_role == 'Guest':
    st.warning("You don't have permission to access this page.")
    st.stop()

# Get data from session state
user_email = st.session_state.user_email
user_project = st.session_state.get('user_project', 'None')
spreadsheet = st.session_state.get('spreadsheet', None)

# Display the dashboard
display_dashboard(user_email, user_role, user_project, spreadsheet)
