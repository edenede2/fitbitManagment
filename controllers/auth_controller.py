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
    lookup_access_assignment,
    user_access_from_secrets,
)
from utils.demo_data import create_demo_spreadsheet
from utils.access_control import staff_identity_assignment, native_logout_principal, _user_claim
from utils.staff_sessions import create_staff_session, revoke_staff_sessions, revoke_session_owner, prepare_mfa_enrollment, confirm_mfa_enrollment, validate_staff_session


@st.fragment(run_every="60s")
def _check_open_staff_session():
    """A background check must not reset the user's inactivity clock."""
    session_id = st.session_state.get("staff_session_id")
    if not session_id:
        return
    names = ("is_logged_in", "email", "iss", "sub", "email_verified", "exp", "aud")
    try:
        actor = staff_identity_assignment(st.secrets, {name: _user_claim(st.user, name) for name in names})
        if not actor.can_read_real_data:
            raise PermissionError("Staff identity changed")
        validate_staff_session(actor, session_id, touch=False)
    except Exception:
        clear_data_session_state()
        st.rerun()

class AuthenticationController:
    """Controller handling user authentication and authorization"""
    
    def __init__(self):
        """Initialize authentication controller"""
        self.main_spreadsheet = None
        
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
        return user_access_from_secrets(st.secrets, user_email)
    
    def _lookup_secret(self, email_prefix: str) -> str:
        """Backward-compatible wrapper for direct prefix lookups."""
        return lookup_access_assignment(st.secrets, email_prefix)

    def get_access_context(self) -> AccessContext:
        return resolve_access_context(self.get_user_access)

    def render_auth_ui(self):
        """Render authentication UI in the sidebar"""
        context = self.get_access_context()
        if context.is_authenticated:
            _check_open_staff_session()
        with st.sidebar:
            st.title("👤 User Access")
            self.render_staff_mfa()
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

    def render_staff_mfa(self):
        """OIDC alone is not enough for access; challenge before any data load."""
        try:
            names = ("is_logged_in", "email", "iss", "sub", "email_verified", "exp", "aud", "amr", "auth_time")
            claims = {name: _user_claim(st.user, name) for name in names}
            actor = staff_identity_assignment(st.secrets, claims)
        except Exception:
            return
        if not actor.is_authenticated or self.get_access_context().is_authenticated:
            return
        st.caption("Complete staff security verification to access research data.")
        with st.form("staff_mfa_login"):
            code = st.text_input("Authenticator or recovery code", type="password", max_chars=128)
            submitted = st.form_submit_button("Verify staff session")
        if submitted:
            try:
                st.session_state.staff_session_id = create_staff_session(actor, claims, code=code)
                st.rerun()
            except Exception:
                st.error("Verification failed or setup is required. Contact an Admin.")
        with st.expander("Set up an authenticator"):
            approval = st.text_input("Admin-approved setup code", type="password", max_chars=128)
            if st.button("Begin approved setup"):
                try:
                    key = prepare_mfa_enrollment(actor, approval.strip())
                    # Display only to the bound account, never log or persist
                    # this plaintext key in Streamlit/session state.
                    st.code(key, language=None)
                    st.caption("Save this key in your authenticator now. Then confirm below.")
                except Exception:
                    st.error("Setup approval is invalid or expired.")
            confirmation = st.text_input("Authenticator confirmation", type="password", max_chars=6)
            if st.button("Confirm authenticator"):
                try:
                    recovery = confirm_mfa_enrollment(actor, confirmation.strip())
                    st.download_button("Save one-use recovery codes", "\n".join(recovery),
                                       file_name="admontracker-recovery-codes.txt", mime="text/plain")
                    st.success("Authenticator enrolled. Use its next code to verify your session.")
                except Exception:
                    st.error("Confirmation failed, expired or rate-limited.")
    

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
        if st.session_state.get("staff_session_id"):
            try:
                revoke_session_owner(st.session_state.staff_session_id)
            except Exception:
                st.error("Session revocation failed. Retry logout or contact an Admin.")
                return
        principal = st.session_state.get("principal_id")
        if not principal:
            try:
                names = ("is_logged_in", "email", "iss", "sub", "email_verified", "exp", "aud")
                principal = native_logout_principal({name: _user_claim(st.user, name) for name in names})
            except Exception:
                principal = None
        if principal:
            # If revocation fails, do not claim that other tabs/devices stopped.
            try:
                revoke_staff_sessions(principal)
            except Exception:
                st.error("Session revocation failed. Retry logout or contact an Admin.")
                return
        st.session_state.clear()
        try:
            st.logout()
        except Exception:
            # Fallback in case the function isn't available in this Streamlit version
            st.rerun()
