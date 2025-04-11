import streamlit as st
from pathlib import Path
import time

# Import controllers
from controllers.auth_controller import AuthenticationController
from controllers.user_controller import UserController

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
    
    # Handle authentication in sidebar
    auth_controller.render_auth_ui()
    
    # Check if user is logged in (either through Streamlit auth or demo mode)
    is_logged_in = st.experimental_user.is_logged_in or st.session_state.get('user_role') is not None
    
    if is_logged_in:
        try:
            # Store spreadsheet in session state
            if 'spreadsheet' not in st.session_state:
                st.session_state.spreadsheet = auth_controller.get_spreadsheet()

            if 'fibro_spreadsheet' not in st.session_state:
                st.session_state.fibro_spreadsheet = auth_controller.get_fibro_spreasheet()
            
            # Get user info - either from Streamlit auth or session state (for demo)
            if st.experimental_user.is_logged_in:
                user_email = st.experimental_user.email
                user_project = st.secrets.get(user_email.split('@')[0], 'None')
                if user_project is not None:
                    user_project = user_project.split(',')[0]
                user_role = st.secrets.get(user_email.split('@')[0], 'Guest')
                if user_role != 'Guest':
                    user_role = user_role.split(',')[1].strip()

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
            
            # Display logout button
            st.sidebar.button("Logout", on_click=auth_controller.logout_user)
            st.sidebar.info("To log out, click the 'Logout' button above.")
            
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
            if "429" in str(e) or "Quota exceeded" in str(e):
                st.error("Google Sheets API rate limit exceeded. Please wait a moment and try again.")
                time.sleep(2)
            else:
                st.error(f"An error occurred: {e}")
    else:
        # Not logged in - show welcome screen
        st.title("Welcome to the Fitbit Management System")
        st.write("Please log in to access the dashboard and features.")
        
        # Show login instructions
        st.info("Use the sidebar to log in. Click the 'login with google' button to authenticate.")
        st.sidebar.button("login with google", on_click=auth_controller.login_with_google)
        
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

