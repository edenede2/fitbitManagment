import streamlit as st
from pathlib import Path

# Import controllers
from controllers.auth_controller import AuthenticationController
from controllers.user_controller import UserController
from controllers.project_controller import ProjectController
from pages.alerts_config import alerts_config_page
# Import views
from view.dashboard import display_dashboard
from view.homepage import display_homepage
# from view.alertConfig import display_alerts

# Import the fitbit management functionality
from pages.fitbit_management import load_fitbit_datatable
from pages.alerts_management import display_alerts_management
# Set up app configuration
st.set_page_config(
    page_title="Fitbit Management System",
    page_icon="💡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# def show_fitbit_management():
#     """Show the Fitbit management page with the current user's details."""
#     # Get current user details from session state
#     user_details = st.session_state.get('user_data', None)
    
#     # Check if user is logged in
#     if user_details is None:
#         st.error("You must be logged in to access this feature.")
#         return
    
#     # Load the fitbit management datatable with user details
#     load_fitbit_datatable(user_details)

def main():
    """Main application function"""
    # Initialize controllers - only when needed
    auth_controller = AuthenticationController()
    
    # Handle authentication in sidebar
    auth_controller.render_auth_ui()
    
    # Check if user is logged in (either through Streamlit auth or demo mode)
    is_logged_in = st.experimental_user.is_logged_in or st.session_state.get('user_role') is not None
    
    if is_logged_in:
        # Get user info - either from Streamlit auth or session state (for demo)
        if st.experimental_user.is_logged_in:
            user_email = st.experimental_user.email
            user_role = st.session_state.user_role
            user_project = st.session_state.user_project
        else:
            # Demo mode
            user_email = "demo@example.com"
            user_role = st.session_state.user_role
            user_project = st.session_state.user_project
        
        # Navigation options based on user role
        menu_options = ["Home", "Dashboard", "Fitbit Managment", "Alerts Configuration", "Settings", "About"]
        
        # Filter pages based on user role
        if user_role == "Student":
            menu_options = ["Home", "Dashboard", "About"]
        elif user_role == "Guest":
            menu_options = ["Home", "About"]

        if user_project == 'nova' and user_role in ['manager', 'Admin']:
            menu_options = ["Home", "Dashboard", "Fitbit Managment", "Alerts Configuration", "NOVA Qualtrics Managment" "Settings", "About"]

        selected_page = st.sidebar.radio("Navigation", menu_options)
        
        # Add Fitbit Device Management button for Admin and Manager roles
        if 'user_data' in st.session_state and st.session_state['user_data'].get('role', '').lower() in ['admin', 'manager']:
            if st.sidebar.button("Fitbit Device Management"):
                st.session_state['current_page'] = 'fitbit_management'

        # Display the selected page
        if selected_page == "Fitbit Managment":
            load_fitbit_datatable(user_email, user_role, user_project)
        elif selected_page == "Home":
            display_homepage(user_email, user_role, user_project)
        elif selected_page == "Dashboard":
            display_dashboard(user_email, user_role, user_project)
        elif selected_page == "Alerts Configuration":
            alerts_config_page(user_email)
        elif selected_page == "NOVA Qualtrics Managment":
            display_alerts_management(user_email, user_role, user_project)
        elif selected_page == "Reports":
            st.title("Reports")
            st.info("Reports functionality will be implemented here")
        elif selected_page == "Settings":
            st.title("Settings")
            st.info("Settings functionality will be implemented here")
        elif selected_page == "About":
            st.title("About")
            st.write("""
            ## Fitbit Management System
            
            This application allows monitoring and management of Fitbit devices across different projects.
            
            ### Features
            - Real-time device monitoring
            - Historical data analysis
            - User assignment and management
            - Battery and health tracking
            
            ### User Roles
            - **Admin:** Full access to all projects and features
            - **Manager:** Access to specific project data and settings
            - **Student:** Access to assigned watches only
            - **Guest:** Limited access to general information
            """)
        st.sidebar.button("Logout", on_click=auth_controller.logout_user)
    else:
        # Not logged in
        st.title("Welcome to the Fitbit Management System")
        st.write("Please log in to access the dashboard and features.")
        
        # Show login instructions
        st.info("Use the sidebar to log in or try a demo account.")

        st.sidebar.button("login with google", on_click=auth_controller.login_with_google)

if __name__ == "__main__":
    main()

