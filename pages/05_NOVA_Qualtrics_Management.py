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

# Safely check if user is logged in
try:
    is_streamlit_logged_in = st.user is not None and hasattr(st.user, 'is_logged_in') and st.user.is_logged_in
except Exception:
    is_streamlit_logged_in = False

is_logged_in = is_streamlit_logged_in or st.session_state.get('user_role') is not None

if is_logged_in:
    if is_streamlit_logged_in:
        # Check role permissions
        user_email = getattr(st.user, 'email', None)
        if user_email is None:
            st.error("Could not retrieve user email. Please refresh and try again.")
            st.stop()
        user_project = st.secrets.get(user_email.split('@')[0], 'None')
        if user_project is not None:
            user_project = user_project.split(',')[1]
        user_role = st.secrets.get(user_email.split('@')[0], 'Guest')
        if user_role != 'Guest':
            user_role = user_role.split(',')[0]

        if user_role not in ['Admin', 'Manager']:
            st.warning("You don't have permission to access this page.")
            st.stop()

        if 'spreadsheet' not in st.session_state:
            st.session_state.spreadsheet = auth_controller.get_spreadsheet()
        spreadsheet = st.session_state.get('spreadsheet', None)
        if user_project == 'nova' or user_role == 'Admin':
            # Display NOVA Qualtrics management interface
            nova_qualtrics_management(user_email, user_role, user_project, spreadsheet)
        else:
            st.warning("You don't have permission to access this page.")
            st.stop()
    elif st.session_state.get('user_email') == "guest@example.com":
        if 'demo_spreadsheet' not in st.session_state:
            st.session_state.demo_spreadsheet = auth_controller.get_demo_spreadsheet()
        demo_spreadsheet = st.session_state.get('demo_spreadsheet', None)

        nova_qualtrics_management(
            st.session_state.user_email,
            st.session_state.user_role,
            st.session_state.user_project,
            demo_spreadsheet
        )
        
    
    else:
        # Display the homepage content
        st.warning("Please log in via Google or as a Guest to visit this site.")
        st.stop()
else:
    # Display the homepage content
    st.warning("Please log in from the main page to access this feature.")
    st.stop()