import streamlit as st
from view.alerts_config import alerts_config_page
from controllers.auth_controller import AuthenticationController
# Page configuration
st.set_page_config(
    page_title="Alerts Configuration - Fitbit Management System",
    page_icon="🔔",
    layout="wide"
)
# Initialize authentication controller
auth_controller = AuthenticationController()
# Handle authentication in sidebar
auth_controller.render_auth_ui()
is_logged_in = st.experimental_user.is_logged_in or st.session_state.get('user_role') is not None

if is_logged_in:
    if st.experimental_user.is_logged_in:
        # Get data from session state
        user_email = st.experimental_user.email
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

        # Display alerts configuration interface
        alerts_config_page(user_email, spreadsheet, user_role, user_project)
    else:
        # Display the homepage content
        st.warning("Please log in from the main page to access this feature.")
        st.stop()
else:
    # Display the homepage content
    st.warning("Please log in from the main page to access this feature.")
    st.stop()