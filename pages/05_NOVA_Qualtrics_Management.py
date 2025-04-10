import streamlit as st
from view.nova_qualtrics_management import nova_qualtrics_management
from controllers.auth_controller import AuthenticationController

# Page configuration
st.set_page_config(
    page_title="NOVA Qualtrics Management - Fitbit Management System",
    page_icon="📋",
    layout="wide"
)

# Initialize authentication controller
auth_controller = AuthenticationController()
# Handle authentication in sidebar
auth_controller.render_auth_ui()

# Check authentication
if 'user_email' not in st.session_state:
    st.warning("Please log in from the main page to access this feature.")
    st.stop()

# Check role permissions
user_role = st.session_state.get('user_role', 'Guest')
user_project = st.session_state.get('user_project', 'None')

# Only show for NOVA managers or admin
if not ((user_project == 'nova' and user_role == 'Manager') or user_role == 'Admin'):
    st.warning("You don't have permission to access this page.")
    st.stop()

# Get data from session state
user_email = st.session_state.user_email
spreadsheet = st.session_state.get('spreadsheet', None)

# Display NOVA Qualtrics management interface
nova_qualtrics_management(user_email, user_role, user_project, spreadsheet)
