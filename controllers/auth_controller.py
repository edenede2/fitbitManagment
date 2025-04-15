import streamlit as st
from entity.Sheet import Spreadsheet, GoogleSheetsAdapter
from utils.sheets_cache import sheets_cache
import time

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
            is_logged_in = st.experimental_user.is_logged_in or st.session_state.get('user_role') is not None
            
            if is_logged_in:
                if st.experimental_user.is_logged_in:
                    st.write(f"Logged in as: {st.experimental_user.email}")
                    user_email = st.experimental_user.email
                else:
                    # For demo mode
                    st.write(f"Demo mode as: {st.session_state.get('user_email', 'Guest')}")
                    user_email = st.session_state.get('user_email', 'demo@example.com')
                st.write(f"User email = {user_email}")
                st.write(f"User email from experimental_user = {st.experimental_user.email}")
                st.write(f"User from experimental_user = {st.experimental_user.email.split('@')[0]}")
                # Display user role information
                user_role = st.secrets.get(user_email.split('@')[0], 'Guest')
                if user_role != 'Guest':
                    user_role = user_role.split(',')[0]
                user_project = st.secrets.get(st.experimental_user.email.split('@')[0].strip(), 'None')
                st.write(f"User: {user_project}")
                st.write(f"Role: {user_project.split(',')[0]}")
                # st.write(f"Project: {user_role.split(',')[1]}")
                # if user_project is not None:
                #     user_project = user_project.split(',')[1]
                st.session_state.user_email = user_email
                st.session_state.user_role = user_role
                st.session_state.user_project = user_project
                
                st.write(f"Role: {user_project}")
                st.write(f"Project: {user_role}")
            else:
                # Demo login options
                st.subheader("Demo Login")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("Admin Demo"):
                        self.demo_login("admin@example.com", "Admin", "All")
                
                with col2:
                    if st.button("Manager Demo"):
                        self.demo_login("manager@example.com", "Manager", "nova")
                
                with col1:
                    if st.button("Student Demo"):
                        self.demo_login("student@example.com", "Student", "nova")
                
                with col2:
                    if st.button("Guest Demo"):
                        self.demo_login("guest@example.com", "Guest", "None")
    

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
        st.session_state.user_role = role
        st.session_state.user_project = project
        st.session_state.user_data = {
            'email': email,
            'role': role,
            'projects': [project] if project != "None" else []
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
