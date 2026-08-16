import streamlit as st
from entity.Sheet import Spreadsheet, GoogleSheetsAdapter
from utils.sheets_cache import sheets_cache
from utils.rate_limit_ui import show_rate_limit_notice
from utils.access_control import (
    AccessContext,
    clear_data_session_state,
    render_demo_banner,
    render_legal_links,
    require_real_data_access,
    resolve_access_context,
)
from utils.demo_data import create_demo_spreadsheet

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
        if 'demo_mode' not in st.session_state:
            st.session_state.demo_mode = False

    def get_user_access(self, user_email: str) -> tuple[str, str]:
        """Return (role, project) from Streamlit secrets for a user email."""
        username = user_email.split('@')[0]
        access_value = self._lookup_secret(username)
        if access_value == 'Guest':
            return 'Guest', 'None'

        parts = [part.strip() for part in str(access_value).split(',')]
        role = parts[0] if len(parts) > 0 and parts[0] else 'Guest'
        project = parts[1] if len(parts) > 1 and parts[1] else 'None'
        return role, project
    
    def _lookup_secret(self, email_prefix: str) -> str:
        """Lookup a user secret by email prefix, handling dotted keys (TOML nested tables) and case differences."""
        # For dotted prefixes like "anuta89.ap", TOML parses them as nested: st.secrets["anuta89"]["ap"]
        if '.' in email_prefix:
            parts = email_prefix.split('.', 1)
            try:
                table = st.secrets.get(parts[0], None)
                if table is not None and hasattr(table, 'get'):
                    val = table.get(parts[1], None)
                    if val is not None:
                        return val
            except Exception:
                pass
            # Also try case-insensitive nested lookup
            for key in st.secrets:
                if key.lower() == parts[0].lower():
                    table = st.secrets[key]
                    if hasattr(table, 'get'):
                        for sub_key in table:
                            if sub_key.lower() == parts[1].lower():
                                return table[sub_key]
        else:
            # Flat key lookup
            val = st.secrets.get(email_prefix, None)
            if val is not None:
                return val
            # Case-insensitive fallback
            for key in st.secrets:
                if key.lower() == email_prefix.lower():
                    result = st.secrets[key]
                    if isinstance(result, str):
                        return result
        return 'Guest'

    def get_access_context(self) -> AccessContext:
        return resolve_access_context(self.get_user_access)

    def render_auth_ui(self):
        """Render authentication UI in the sidebar"""
        context = self.get_access_context()
        with st.sidebar:
            st.title("👤 User Access")
            if context.is_authenticated:
                st.write(f"Logged in as: {context.email}")
                st.write(f"Role: {context.role}")
                st.write(f"Project: {context.project}")
                if st.button("Logout", key="logout_button"):
                    self.logout_user()
            elif context.is_guest:
                st.write("Demo mode as: Guest")
                st.write("Role: Guest")
                st.write("Project: Demo")
                render_demo_banner()
                if st.button("Exit demo", key="logout_button"):
                    self.logout_user()
            else:
                if st.button("Login with Google", key="google_login_button"):
                    self.login_with_google()
                st.subheader("Guest demonstration")
                st.caption("Explore fictional examples without accessing production data.")
                if st.button("Open read-only guest demo"):
                    self.demo_login()

            st.divider()
            render_legal_links()
    

    @sheets_cache(timeout=300)
    def get_fibro_spreasheet(self):
        """Get or create the Fibro spreadsheet connection"""
        try:
            require_real_data_access()
            if not self.fibro_spreadsheet:
                # Use st.secrets to get the spreadsheet key
                spreadsheet_key = st.secrets.get("fibro_ema_sheet", "")
                self.fibro_spreadsheet = Spreadsheet(name="Fibro EMA Database", api_key=spreadsheet_key)
                GoogleSheetsAdapter.connect(self.fibro_spreadsheet)
            return self.fibro_spreadsheet
        except Exception as e:
            if not show_rate_limit_notice(
                e,
                provider="google_sheets",
                key="fibro_spreadsheet",
                context="connecting to the Fibro spreadsheet",
            ):
                st.error(f"Error connecting to Fibro spreadsheet: {e}")
            return None
        
    @sheets_cache(timeout=300)
    def get_demo_ema_spreadsheet(self):
        """Get or create the demo Fibro spreadsheet connection"""
        return create_demo_spreadsheet()
        


    @sheets_cache(timeout=300)
    def get_spreadsheet(self):
        """Get or create the main spreadsheet connection"""
        try:
            require_real_data_access()
            if st.session_state.get("spreadsheet") is not None:
                self.main_spreadsheet = st.session_state.spreadsheet
                return self.main_spreadsheet

            if not self.main_spreadsheet:
                # Use st.secrets to get the spreadsheet key
                spreadsheet_key = st.secrets.get("spreadsheet_key", "")
                self.main_spreadsheet = Spreadsheet(name="Fitbit Database", api_key=spreadsheet_key)
                GoogleSheetsAdapter.connect(self.main_spreadsheet)
                st.session_state.spreadsheet = self.main_spreadsheet
            return self.main_spreadsheet
        except Exception as e:
            if not show_rate_limit_notice(
                e,
                provider="google_sheets",
                key="main_spreadsheet",
                context="connecting to the spreadsheet",
            ):
                st.error(f"Error connecting to spreadsheet: {e}")
            return None
    
    @sheets_cache(timeout=300)
    def get_demo_spreadsheet(self):
        """Get or create the demo spreadsheet connection"""
        return create_demo_spreadsheet()
    
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
    
    def demo_login(self):
        """Start the isolated guest demonstration."""
        email = "guest@example.invalid"
        clear_data_session_state()
        st.session_state.demo_mode = True
        st.session_state.user_email = email
        st.session_state.user_role = 'Guest'
        st.session_state.user_project = 'Demo'
        st.session_state.user_data = {
            'email': email,
            'role': 'Guest',
            'projects': ['Demo']
        }
        st.rerun()
    
    def logout_user(self):
        """Log out the current user"""
        st.session_state.clear()
        try:
            st.logout()
        except Exception:
            # Fallback in case the function isn't available in this Streamlit version
            st.rerun()
