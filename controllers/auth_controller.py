import streamlit as st
from typing import Dict, Optional
import json
from entity.User import User, UserFactory, UserRepository, UserRole
from entity.Sheet import Spreadsheet, GoogleSheetsAdapter

class AuthenticationController:
    """Controller for handling authentication-related operations"""
    
    def __init__(self):
        """Initialize the authentication controller"""
        self._initialize_session_state()
        self.user_repo = UserRepository.get_instance()
        self.main_spreadsheet = None
    
    def _initialize_session_state(self):
        """Initialize session state for authentication"""
        if "authentication_status" not in st.session_state:
            st.session_state["authentication_status"] = None
            
        if "login" not in st.session_state:
            st.session_state["login"] = False
        
        if "user_role" not in st.session_state:
            st.session_state.user_role = None
        if "user_project" not in st.session_state:
            st.session_state.user_project = None
    
    def get_user_details(self, user_email: str) -> tuple:
        """
        Get user role and project from the spreadsheet.
        
        Args:
            user_email (str): User's email address
        
        Returns:
            tuple: (role, project)
        """
        try:
            if not self.main_spreadsheet:
                # Initialize the main spreadsheet if not already done
                spreadsheet_key = st.secrets.get("spreadsheet_key", "")
                self.main_spreadsheet = Spreadsheet(name="Fitbit Database", api_key=spreadsheet_key)
                GoogleSheetsAdapter.connect(self.main_spreadsheet)
                
            
            
            # Get user sheet
            user_sheet = self.main_spreadsheet.get_sheet("user", sheet_type="user")
            user_df = user_sheet.to_dataframe()
            
            # Find this user
            user_data = user_df[user_df['email'] == user_email]
            
            if len(user_data) > 0:
                role = user_data.iloc[0]['role']
                project = user_data.iloc[0]['project']
                return role, project
            else:
                # User not found, default to Guest
                return "Guest", ""
        except Exception as e:
            st.error(f"Error retrieving user details: {e}")
            return "Guest", ""
    
    def render_auth_ui(self):
        """Render the authentication UI in the sidebar"""
        # Display login state and buttons
        if st.experimental_user.is_logged_in: 
            user_email = st.experimental_user.email
            user_name = st.experimental_user.name
            
            st.sidebar.success(f"Logged in as: {user_name}")
            
            # Get and store user role and project if not already in session
            if not st.session_state.user_role or not st.session_state.user_project:
                role, project = self.get_user_details(user_email)
                st.session_state.user_role = role
                st.session_state.user_project = project
            
            # Display user info
            st.sidebar.info(f"Role: {st.session_state.user_role}")
            st.sidebar.info(f"Project: {st.session_state.user_project}")
        else:
            st.sidebar.info("You are not logged in")
            
            # For development/testing - demo buttons
            with st.sidebar.expander("Demo Login (Development Only)"):
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Admin Demo"):
                        st.session_state.user_role = "Admin"
                        st.session_state.user_project = "All Projects"
                        st.rerun()
                with col2:
                    if st.button("Manager Demo"):
                        st.session_state.user_role = "Manager"
                        st.session_state.user_project = "Project1"
                        st.rerun()
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Student Demo"):
                        st.session_state.user_role = "Student" 
                        st.session_state.user_project = "Project1"
                        st.rerun()
                with col2:
                    if st.button("Guest Demo"):
                        st.session_state.user_role = "Guest"
                        st.session_state.user_project = ""
                        st.rerun()
    
    def login_with_google(self):
        """Initiate Google OAuth login"""
        st.login("google")
    
    def login_as_guest(self):
        """Login as a guest user"""
        # Create a guest user if it doesn't exist in the repository
        guest_user = self.user_repo.get_by_name("Guest")
        if not guest_user:
            guest_user = UserFactory.create_guest()
            self.user_repo.add(guest_user)
        
        # Set session state
        st.session_state["username"] = "Guest"
        st.session_state["role"] = "guest"
        st.session_state["project"] = "Guest"
        st.session_state["login"] = True
    
    def logout_user(self):
        """Log out the current user"""
        st.session_state.user_role = None
        st.session_state.user_project = None
        st.session_state["login"] = False
        st.session_state["username"] = None
        st.session_state["email"] = None
        st.session_state["role"] = None
        st.session_state["project"] = None
        st.logout()
    
    def is_authenticated(self) -> bool:
        """Check if the user is authenticated"""
        return st.experimental_user.is_logged_in or st.session_state.get("login", False)  # Remove parentheses
    
    def get_current_user_info(self) -> Dict[str, str]:
        """Get the current user information"""
        if st.experimental_user.is_logged_in:
            email = st.experimental_user.get("email", "")
            return {
                "name": st.experimental_user.get("name", ""),
                "email": email,
                "username": email.split("@")[0] if email else ""
            }
        elif st.session_state.get("login", False):
            return {
                "name": st.session_state.get("username", "Guest"),
                "email": st.session_state.get("email", ""),
                "username": st.session_state.get("username", "Guest")
            }
        return {}
    
    def get_current_user(self) -> Optional[User]:
        """Get the current user entity"""
        user_info = self.get_current_user_info()
        if not user_info:
            return None
            
        # Try to find user by email first (for Google login)
        if user_info.get("email"):
            user = self.user_repo.get_by_email(user_info["email"])
            if user:
                return user
        
        # Fall back to finding by name
        return self.user_repo.get_by_name(user_info["name"])
