import streamlit as st
import pandas as pd
import os
from pathlib import Path
import datetime

# Import controllers
from controllers.auth_controller import AuthenticationController
from controllers.user_controller import UserController
from controllers.project_controller import ProjectController

# Import views
from view.dashboard import display_dashboard

# Set up app configuration
st.set_page_config(
    page_title="Fitbit Management System",
    page_icon="💡",
    layout="wide",
    initial_sidebar_state="expanded"
)

def main():
    """Main application function"""
    # Initialize controllers
    auth_controller = AuthenticationController()
    user_controller = UserController()
    project_controller = ProjectController()
    
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
        menu_options = ["Dashboard", "Reports", "Settings", "About"]
        
        # Filter pages based on user role
        if user_role == "Student":
            menu_options = ["Dashboard", "About"]
        elif user_role == "Guest":
            menu_options = ["About"]
        
        selected_page = st.sidebar.radio("Navigation", menu_options)
        
        # Display the selected page
        if selected_page == "Dashboard":
            display_dashboard(user_email, user_role, user_project)
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
    else:
        # Not logged in
        st.title("Welcome to the Fitbit Management System")
        st.write("Please log in to access the dashboard and features.")
        
        # Show login instructions
        st.info("Use the sidebar to log in or try a demo account.")

if __name__ == "__main__":
    main()

