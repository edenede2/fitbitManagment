import streamlit as st
from view.homepage import display_homepage
from Decorators.congrates import congrats
from controllers.auth_controller import AuthenticationController


# Page configuration
st.set_page_config(
    page_title="Home - Fitbit Management System",
    page_icon="🏠",
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
# user_role = st.session_state.get('user_role', 'Guest')

# if user_role == 'Guest':
#     congrats()
#     st.stop()
# Get data from session state
user_email = st.session_state.user_email
user_role = st.session_state.get('user_role', 'Guest')
user_project = st.session_state.get('user_project', 'None')
spreadsheet = st.session_state.get('spreadsheet', None)

if user_email is None:
    st.warning("Please log in from the main page to access this feature.")
    st.stop()
else:
    # Display the homepage content
    display_homepage(user_email, user_role, user_project, spreadsheet)
