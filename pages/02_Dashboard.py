import streamlit as st
from view.dashboard import display_dashboard
from controllers.auth_controller import AuthenticationController
# Page configuration
st.set_page_config(
    page_title="Dashboard - Fitbit Management System",
    page_icon="📊",
    layout="wide"
)

# Initialize authentication controller
auth_controller = AuthenticationController()
# Handle authentication in sidebar
auth_controller.render_auth_ui()
is_logged_in = st.experimental_user.is_logged_in or st.session_state.get('user_role') is not None

if is_logged_in:
    if st.experimental_user.is_logged_in:
        # # Check authentication
        # if 'user_email' not in st.session_state:
        #     st.warning("Please log in from the main page to access this feature.")
        #     st.stop()

        # # Check role permissions (Student, Manager, Admin have access)
        # user_role = st.session_state.get('user_role', 'Guest')
        # if user_role == 'Guest':
        #     st.warning("You don't have permission to access this page.")
        #     st.stop()

        # Get data from session state
        user_email = st.experimental_user.email
        user_project = st.secrets.get(user_email.split('@')[0], 'None')
        if user_project is not None:
            user_project = user_project.split(',')[0]
        user_role = st.secrets.get(user_email.split('@')[0], 'Guest')
        if user_role != 'Guest':
            user_role = user_role.split(',')[1]

        if 'spreadsheet' not in st.session_state:
            st.session_state.spreadsheet = auth_controller.get_spreadsheet()
        spreadsheet = st.session_state.get('spreadsheet', None)

        st.write(st.experimental_user)
        # Display the dashboard
        display_dashboard(user_email, user_role, user_project, spreadsheet)
    else:
        # Display the homepage content
        st.warning("Please log in from the main page to access this feature.")
        st.stop()
else:
    # Display the homepage content
    st.warning("Please log in from the main page to access this feature.")
    st.stop()