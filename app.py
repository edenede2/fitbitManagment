import streamlit as st
from pathlib import Path

# Import controllers
from controllers.auth_controller import AuthenticationController
from controllers.user_controller import UserController

from utils.fitbit_callback import handle_fitbit_callback
from utils.google_health_callback import handle_google_health_callback
from utils.rate_limit_ui import show_rate_limit_notice

# Set up app configuration
st.set_page_config(
    page_title="Fitbit Management System",
    page_icon="💡",
    layout="wide",
    initial_sidebar_state="expanded"
)

def main():
    """Main application function - handles authentication and session state"""
    # Initialize authentication controller
    auth_controller = AuthenticationController()
    
    # 1) handle callback FIRST (no login required)
    if handle_google_health_callback(auth_controller):
        st.stop()
    if handle_fitbit_callback(auth_controller):
        st.stop()
    # Handle authentication in sidebar
    auth_controller.render_auth_ui()
    
    # Check if user is logged in (either through Streamlit auth or demo mode)
    try:
        is_streamlit_logged_in = st.user is not None and hasattr(st.user, 'is_logged_in') and st.user.is_logged_in
    except Exception:
        is_streamlit_logged_in = False
    
    is_logged_in = is_streamlit_logged_in or st.session_state.get('user_role') is not None
    
    if is_logged_in:
        try:
            # Store spreadsheet in session state
            if 'spreadsheet' not in st.session_state:
                st.session_state.spreadsheet = auth_controller.get_spreadsheet()

            if 'fibro_spreadsheet' not in st.session_state:
                st.session_state.fibro_spreadsheet = auth_controller.get_fibro_spreasheet()
            
            # Get user info - either from Streamlit auth or session state (for demo)
            if is_streamlit_logged_in:
                user_email = getattr(st.user, 'email', None)
                if user_email is None:
                    st.error("Could not retrieve user email. Please refresh and try again.")
                    st.stop()
                user_role, user_project = auth_controller.get_user_access(user_email)

                # user = UserController().get_user_by_email(user_email)
                
                # if user is not None:
                #     st.session_state.user_role = user.get('role', 'Guest')
                #     st.session_state.user_project = user.get('project', 'None')
                st.session_state.user_role = user_role
                st.session_state.user_project = user_project
                st.session_state.user_email = user_email
            else:
                # Demo mode
                st.session_state.user_email = "demo@example.com"
            
            # Add page descriptions
            st.sidebar.markdown("## App Pages")
            st.sidebar.markdown("""
            - **Homepage**: Overview of the active users and their projects
            - **Dashboard**: Overview of Fitbit activity and device stats
            - **Fitbit Management**: Manage Fitbit devices 
            - **Alerts Configuration**: Configure alerts for devices and EMA
            - **NOVA Qualtrics Management**: Manage buldog and Qualtrics data
            - **APPSHEET Management**: Manage AppSheet data
            """)
            
            # Add support information
            st.sidebar.markdown("---")
            st.sidebar.markdown("### Need Help?")
            st.sidebar.markdown("Contact support: edenede2@gmail.com")

            st.title("Welcome to the Fitbit Management System")
            st.write("You are logged in as: **{}**".format(st.session_state.user_email))
            st.write("Your role is: **{}**".format(st.session_state.user_role))
            st.write("Your project is: **{}**".format(st.session_state.user_project))
            st.write("You can now access the dashboard and features.")
            st.write("Use the sidebar to navigate through the app.")
            st.write("Click the 'Logout' button in the sidebar to log out.")
            st.write("If you encounter any issues, please contact support.")
            st.write("You can also use the sidebar to navigate through the app.")
            st.write("Click the 'Logout' button in the sidebar to log out.")
            
        except Exception as e:
            if not show_rate_limit_notice(
                e,
                provider="google_sheets",
                key="main_app",
                context="loading the app data",
            ):
                st.error(f"An error occurred: {e}")
    else:
        # Not logged in - show welcome screen
        st.title("Welcome to the Fitbit Management System")
        st.write("Please log in to access the dashboard and features.")
        
        # Show login instructions
        st.info("Use the sidebar to log in. Click the 'login with google' button to authenticate.")
        
        # Add page descriptions for non-logged in users
        st.markdown("## Features Available After Login:")
        st.markdown("""
        - **Dashboard**: Overview of Fitbit activity and stats
        - **User Management**: Manage user accounts and permissions
        - **Device Tracking**: Monitor Fitbit devices and sync status
        - **Data Analysis**: Analyze collected health and activity data
        - **Reports**: Generate and export reports
        """)
        
        # Add support information
        st.markdown("---")
        st.markdown("### Need Help?")
        st.markdown("Contact support: edenede2@gmail.com")

if __name__ == "__main__":
    main()
