import streamlit as st
import pandas as pd
from typing import Dict, List, Any, Optional
from entity.User import User, UserRole

class HomeView:
    """View for the home page of the application"""
    
    def render_welcome(self, user_name: Optional[str] = None):
        """Render the welcome section of the home page"""
        if user_name:
            st.title(f"Welcome back, {user_name}!")
        else:
            st.title("Welcome to the Fitbit Management System")
        
        st.divider()
        st.write("What would you like to do today?")
    
    def render_guest_info(self):
        """Render information for guest users"""
        st.info("""
        You are currently browsing as a guest. Some features may be limited.
        Login with your Google account to access all features.
        """)
    
    def render_project_summary(self, project_data: Dict[str, Any]):
        """Render a summary of project information"""
        if not project_data:
            return
            
        st.subheader(f"Project: {project_data.get('name', 'Unknown')}")
        
        # Create columns for project stats
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Watches", project_data.get('watch_count', 0))
        
        with col2:
            st.metric("Students", project_data.get('student_count', 0))
        
        with col3:
            st.metric("Managers", project_data.get('manager_count', 0))
        
        with col4:
            st.metric("Status", project_data.get('status', 'Unknown'))
    
    def render_quick_stats(self, stats: Dict[str, Any]):
        """Render quick statistics on the home page"""
        st.subheader("Quick Stats")
        
        # Create columns for stats
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                "Active Watches", 
                stats.get('active_watches', 0),
                delta=stats.get('active_watches_delta', None)
            )
        
        with col2:
            st.metric(
                "Battery Alert", 
                stats.get('battery_alerts', 0),
                delta=stats.get('battery_alerts_delta', None),
                delta_color="inverse"
            )
        
        with col3:
            st.metric(
                "Sync Failures", 
                stats.get('sync_failures', 0),
                delta=stats.get('sync_failures_delta', None),
                delta_color="inverse"
            )
    
    def render_watch_table(self, watches: List[Dict[str, Any]]):
        """Render a table of watches"""
        if not watches:
            st.info("No watches found for this project.")
            return
            
        # Convert to DataFrame for display
        watch_df = pd.DataFrame(watches)
        
        # Format the DataFrame
        if 'battery_level' in watch_df.columns:
            watch_df['battery_level'] = watch_df['battery_level'].apply(
                lambda x: f"{x}%" if x is not None else "Unknown"
            )
        
        if 'is_active' in watch_df.columns:
            watch_df['status'] = watch_df['is_active'].apply(
                lambda x: "Active" if x else "Inactive"
            )
            watch_df = watch_df.drop(columns=['is_active'])
        
        # Display the table
        st.dataframe(
            watch_df,
            column_config={
                "name": "Watch Name",
                "battery_level": "Battery",
                "last_sync_time": "Last Sync",
                "assigned_to": "Assigned To",
                "status": "Status"
            },
            use_container_width=True
        )
    
    def render_user_role_specific_content(self, user: User):
        """Render content specific to the user's role"""
        if not user:
            return
            
        # Get the role value
        role = user.role.value if hasattr(user.role, 'value') else user.role
        
        if role == UserRole.ADMIN.value:
            st.subheader("Administrator Dashboard")
            st.write("As an administrator, you have access to all features of the system.")
            
            # Admin-specific metrics or shortcuts could go here
            admin_col1, admin_col2, admin_col3 = st.columns(3)
            with admin_col1:
                if st.button("User Management", use_container_width=True):
                    st.session_state["selected_page"] = "User Management"
            with admin_col2:
                if st.button("Project Management", use_container_width=True):
                    st.session_state["selected_page"] = "Project Management"
            with admin_col3:
                if st.button("System Settings", use_container_width=True):
                    st.session_state["selected_page"] = "System Settings"
                    
        elif role == UserRole.MANAGER.value:
            st.subheader("Project Manager Dashboard")
            st.write("As a project manager, you can manage your project, users, and watches.")
            
            # Manager-specific metrics or shortcuts
            manager_col1, manager_col2 = st.columns(2)
            with manager_col1:
                if st.button("Manage Watches", use_container_width=True):
                    st.session_state["selected_page"] = "Manage Watches"
            with manager_col2:
                if st.button("Manage Students", use_container_width=True):
                    st.session_state["selected_page"] = "Manage Students"
                    
        elif role == UserRole.STUDENT.value:
            st.subheader("Student Dashboard")
            st.write("As a student, you can view your assigned watch and data.")
            
            # Student-specific content
            if st.button("View My Watch Data", use_container_width=True):
                st.session_state["selected_page"] = "My Watch Data"
                
        elif role == UserRole.RESEARCHER.value:
            st.subheader("Researcher Dashboard")
            st.write("As a researcher, you can view data across all projects.")
            
            # Researcher-specific content
            if st.button("View Research Data", use_container_width=True):
                st.session_state["selected_page"] = "Research Data"
