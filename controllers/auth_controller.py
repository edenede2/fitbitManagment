import streamlit as st
from entity.Sheet import Spreadsheet, GoogleSheetsAdapter
from utils.sheets_cache import sheets_cache
import time
from model.config import get_secrets

class AuthenticationController:
    """Controller handling user authentication and authorization"""
    
    def __init__(self):
        """Initialize authentication controller"""
        self.main_spreadsheet = None
        self.fibro_spreadsheet = None
        
        # Initialize session state variables if they don't exist
        if 'user_email' not in st.session_state:
            st.session_state.user_email = None
        if 'user_role' not in st.session_state:
            st.session_state.user_role = None
        if 'user_project' not in st.session_state:
            st.session_state.user_project = None
        if 'user_data' not in st.session_state:
            st.session_state.user_data = None
    
    def render_auth_ui(self):
        """Render authentication UI in the sidebar"""
        with st.sidebar:
            st.title("👤 User Access")
            
            # Check if the user is authenticated through Streamlit or in demo mode
            is_logged_in = st.user.is_logged_in or st.session_state.get('user_role') is not None
            
            if is_logged_in:
                if st.user.is_logged_in:
                    st.write(f"Logged in as: {st.user.email}")
                    user_email = st.user.email
                    # Display user role information
                    user_role = st.secrets.get(st.user.email.split('@')[0], 'Guest')
                    user_project = st.secrets.get(f"{st.user.email.split('@')[0]}", 'None')
                else:
                    # For demo mode
                    st.write(f"Demo mode as: Guest")
                    user_email = st.session_state.get('user_email', 'guest@example.com')
                    user_role = 'Admin'
                    user_project = 'Admin'
                
                if user_role != 'Guest':
                    user_role = user_role.split(',')[0]
                

                # st.write(f"Project: {user_role.split(',')[1]}")
                # if user_project is not None:
                #     user_project = user_project.split(',')[1]
                st.session_state.user_email = user_email
                st.session_state.user_role = user_role
                st.session_state.user_project = user_project
                
                st.write(f"Role: {user_project}")
                st.write(f"Project: {user_role}")

                # Display logout button
                if st.button("Logout", key="logout_button"):
                    self.logout_user()
            else:
                # Demo login options
                st.subheader("Demo Login")
                
                if st.button("Guest Demo"):
                    self.demo_login("guest@example.com", "Guest", "Admin")
    

    @sheets_cache(timeout=300)
    def get_fibro_spreasheet(self):
        """Get or create the Fibro spreadsheet connection"""
        try:
            if not self.fibro_spreadsheet:
                # Use st.secrets to get the spreadsheet key
                spreadsheet_key = st.secrets.get("fibro_ema_sheet", "")
                self.fibro_spreadsheet = Spreadsheet(name="Fibro EMA Database", api_key=spreadsheet_key)
                GoogleSheetsAdapter.connect(self.fibro_spreadsheet)
            return self.fibro_spreadsheet
        except Exception as e:
            st.error(f"Error connecting to Fibro spreadsheet: {e}")
            # Add a delay to prevent rapid retries on rate limits
            if "429" in str(e) or "Quota exceeded" in str(e):
                time.sleep(2)
            return None
        
    @sheets_cache(timeout=300)
    def get_demo_ema_spreadsheet(self):
        """Get or create the demo Fibro spreadsheet connection"""
        try:
            if not self.fibro_spreadsheet:
                # Use st.secrets to get the spreadsheet key
                spreadsheet_key = st.secrets.get("demo_fibro", "")
                self.fibro_spreadsheet = Spreadsheet(name="Fibro EMA Database", api_key=spreadsheet_key)
                GoogleSheetsAdapter.connect(self.fibro_spreadsheet)
            return self.fibro_spreadsheet
        except Exception as e:
            st.error(f"Error connecting to demo Fibro spreadsheet: {e}")
            # Add a delay to prevent rapid retries on rate limits
            if "429" in str(e) or "Quota exceeded" in str(e):
                time.sleep(2)
            return None
        


    @sheets_cache(timeout=300)
    def get_spreadsheet(self):
        """Get or create the main spreadsheet connection"""
        try:
            if not self.main_spreadsheet:
                # Use st.secrets to get the spreadsheet key
                spreadsheet_key = st.secrets.get("spreadsheet_key", "")
                self.main_spreadsheet = Spreadsheet(name="Fitbit Database", api_key=spreadsheet_key)
                GoogleSheetsAdapter.connect(self.main_spreadsheet)
            return self.main_spreadsheet
        except Exception as e:
            st.error(f"Error connecting to spreadsheet: {e}")
            # Add a delay to prevent rapid retries on rate limits
            if "429" in str(e) or "Quota exceeded" in str(e):
                time.sleep(2)
            return None
    
    @sheets_cache(timeout=300)
    def get_demo_spreadsheet(self):
        """Get or create the demo spreadsheet connection"""
        try:
            if not self.main_spreadsheet:
                # Use st.secrets to get the spreadsheet key
                spreadsheet_key = st.secrets.get("demo_key", "")
                self.main_spreadsheet = Spreadsheet(name="Fitbit Database", api_key=spreadsheet_key)
                GoogleSheetsAdapter.connect(self.main_spreadsheet)
            return self.main_spreadsheet
        except Exception as e:
            st.error(f"Error connecting to spreadsheet: {e}")
            # Add a delay to prevent rapid retries on rate limits
            if "429" in str(e) or "Quota exceeded" in str(e):
                time.sleep(2)
            return None
    
    def get_user_details(self, user_email: str) -> tuple:
        """Get user details from spreadsheet"""
        if not self.main_spreadsheet:
            self.get_spreadsheet()
        
        # Get the user sheet
        try:
            user_sheet = self.main_spreadsheet.get_sheet("user", "user")
            users_data = user_sheet.data
            
            # Find user by email
            user_data = None
            for user in users_data:
                if user.get('email', '').lower() == user_email.lower():
                    user_data = user
                    break
            
            if user_data:
                # Extract user details
                user_role = user_data.get('role', 'Guest')
                user_projects = user_data.get('projects', [])
                if isinstance(user_projects, str):
                    user_projects = [p.strip() for p in user_projects.split(',')]
                
                # Default to first project in list or "None"
                user_project = user_projects[0] if user_projects else "None"
                
                return user_data, user_role, user_project
        except Exception as e:
            st.error(f"Error retrieving user details: {e}")
        
        # Default values if user not found
        return None, "Guest", "None"
    
    def login_with_google(self):
        """Redirect to Google login"""
        # This is a placeholder that will trigger Streamlit's built-in authentication
        st.login("google")
    
    def demo_login(self, email: str, role: str, project: str):
        """Set up a demo login with specified role and project"""
        st.session_state.user_email = email
        st.session_state.user_role = 'Admin'
        st.session_state.user_project = 'Admin'
        st.session_state.user_data = {
            'email': email,
            'role': 'Admin',
            'projects': ['Admin']
        }
        st.rerun()
    
    def logout_user(self):
        """Log out the current user"""
        # Clear session state
        for key in ['user_email', 'user_role', 'user_project', 'user_data']:
            if key in st.session_state:
                del st.session_state[key]
        
        # Use st.logout() directly as mentioned by user
        try:
            st.logout()
        except Exception as e:
            # Fallback in case the function isn't available in this Streamlit version
            st.warning("Could not perform automatic logout. Please refresh the page.")
            st.info("To completely log out, please use the logout option in the upper right menu.")
        
        st.rerun()
