import streamlit as st
import pandas as pd
from entity.Sheet import Spreadsheet, GoogleSheetsAdapter, SheetFactory
from typing import Dict, List, Any, Optional
import datetime
import polars as pl
from utils.health_oauth_clients import load_active_oauth_clients
from utils.rate_limit_ui import show_rate_limit_notice


def _google_oauth_client_label(row: dict) -> str:
    env = row.get("enviroment") or row.get("environment") or "staging"
    notes = row.get("notes") or row.get("client_id") or ""
    suffix = f" - {notes}" if notes else ""
    return f"{row.get('client_key', '')} ({env}){suffix}"


def _select_google_oauth_client_key(spreadsheet: Spreadsheet, key: str) -> str:
    try:
        clients = load_active_oauth_clients(spreadsheet, provider="google_health")
    except Exception as e:
        if not show_rate_limit_notice(
            e,
            provider="google_sheets",
            key="google_oauth_clients",
            context="loading Google Health OAuth clients",
        ):
            st.warning(f"Could not load Google Health OAuth clients: {e}")
        clients = []

    if not clients:
        st.warning("No active Google Health OAuth clients found in health_oauth_clients.")
        return st.text_input("OAuth client key", value="google_health_staging", key=key)

    selected = st.selectbox(
        "Google Cloud project / OAuth client",
        options=clients,
        format_func=_google_oauth_client_label,
        key=key,
    )
    return str(selected.get("client_key") or "")


def load_fitbit_datatable(user_email: str, user_role: str, user_project: str, spreadsheet: Spreadsheet) -> None:
    """
    Load and display the Fitbit datatable with role-specific permissions.
    
    Args:
        user_email: User's email
        user_role: User's role (admin, manager)
        user_project: User's project
        spreadsheet: Spreadsheet object for data access
    """
    st.title("Fitbit Devices Management")
    
    
    
    # Initialize spreadsheet connection
    try:
        with st.spinner("Loading data..."):
            # Get secrets for the spreadsheet key
            # secrets = st.secrets
            # spreadsheet_key = secrets.get("spreadsheet_key", "")
            
            # # Create and connect spreadsheet
            # spreadsheet = Spreadsheet(
            #     name="Fitbit Database",
            #     api_key=spreadsheet_key
            # )
            # GoogleSheetsAdapter.connect(spreadsheet)
            
            # Load fitbit and user sheets
            fitbit_sheet = spreadsheet.get_sheet("fitbit", "fitbit")
            user_sheet = spreadsheet.get_sheet("user", "user")
            
            if not fitbit_sheet.data:
                st.warning("No Fitbit devices found.")
                return
            
            # Convert to pandas DataFrame for easier manipulation
            original_fitbit_df = fitbit_sheet.to_dataframe(engine="polars")
            user_df = user_sheet.to_dataframe(engine="polars")
            
            # Filter based on user role
            if user_role == 'Manager':
                # user_projects = user_details.get('projects', [])
                # if isinstance(user_project, str):
                #     user_projects = [user_project]
                
                # st.sidebar.write(f"Your projects: {', '.join(user_project)}")
                
                # Filter devices by project
                fitbit_df = original_fitbit_df.filter(pl.col("project") == user_project)
                
                if fitbit_df.is_empty():
                    st.warning(f"No Fitbit devices found for your projects: {', '.join(user_project)}")
                    return
                
                # Also filter users by project for student assignment
                user_df = user_df.filter(pl.col("project")== user_project)
            
            # Display management interface based on role
            if user_role == 'Admin':
                fitbit_df = original_fitbit_df
                display_admin_interface(fitbit_df, user_df, fitbit_sheet, spreadsheet)
            else:  # manager
                display_manager_interface(fitbit_df, user_df, fitbit_sheet, spreadsheet, user_project,original_fitbit_df)
            
    except Exception as e:
        if not show_rate_limit_notice(
            e,
            provider="google_sheets",
            key="fitbit_management_devices",
            context="loading Fitbit devices",
        ):
            st.error(f"Error loading Fitbit devices: {str(e)}")
            st.exception(e)

def display_admin_interface(fitbit_df: pl.DataFrame, user_df: pl.DataFrame, 
                           fitbit_sheet: Any, spreadsheet: Spreadsheet) -> None:
    """Display admin interface with full control over Fitbit devices."""
    st.subheader("Admin View - All Fitbit Devices")
    
    # Add new device button
    if st.button("Add New Fitbit Device"):
        st.session_state.add_new_device = True
    
    # Form for adding new device
    if st.session_state.get('add_new_device', False):
        with st.form("new_device_form"):
            st.subheader("Add New Fitbit Device")
            
            # Get unique projects for dropdown - fixed to_list() method
            all_projects = sorted(fitbit_df.select("project").unique().get_column("project").to_list())
            
            new_project = st.selectbox("Project", all_projects)
            new_name = st.text_input("Device Name")
            new_token = st.text_input("Token")
            new_provider = st.selectbox(
                "Provider",
                options=["fitbit", "google_health"],
                format_func=lambda value: "Fitbit legacy" if value == "fitbit" else "Google Health",
            )
            new_oauth_client_key = ""
            if new_provider == "google_health":
                new_oauth_client_key = _select_google_oauth_client_key(
                    spreadsheet,
                    key="management_new_google_oauth_client_key",
                )
            new_user = st.text_input("User")
            new_is_active = st.checkbox("Is Active", value=True)
            new_current_student = st.text_input("Current Student (optional)")
            
            submitted = st.form_submit_button("Add Device")
            
            if submitted and new_name and new_project:
                # Create new device record
                new_device = {
                    "project": new_project,
                    "name": new_name,
                    "token": new_token,
                    "oauth_type": new_provider,
                    "provider": new_provider,
                    "oauth_client_key": new_oauth_client_key,
                    "auth_status": "not_connected",
                    "health_user_id": "",
                    "legacy_fitbit_user_id": "",
                    "last_successful_fetch_at": "",
                    "last_data_timestamp": "",
                    "last_auth_error": "",
                    "reauth_link": "",
                    "reauth_link_created_at": "",
                    "user": new_user,
                    "isActive": "TRUE" if new_is_active else "FALSE",
                    "currentStudent": new_current_student
                }
                
                # Add to the sheet data
                spreadsheet.update_sheet(
                    "fitbit",
                    new_device,
                    strategy="append"
                )
                
                # Save changes
                GoogleSheetsAdapter.save(spreadsheet, "fitbit")
                
                st.success(f"Added new device: {new_name}")
                st.session_state.add_new_device = False
                # st.experimental_rerun()
    
    # Display editable table for admin
    edited_df = display_editable_table(fitbit_df, user_df, is_admin=True)
    
    # Save changes button
    if st.button("Save Changes"):
        save_changes(edited_df, fitbit_sheet, spreadsheet)

def display_manager_interface(fitbit_df: pd.DataFrame, user_df: pd.DataFrame, 
                             fitbit_sheet: Any, spreadsheet: Spreadsheet, 
                             manager_projects: List[str], original_fitbit_df: pl.DataFrame) -> None:
    """Display manager interface with limited control over project devices."""
    st.subheader("Manager View - Your Project's Fitbit Devices")
    
    # Filter projects for the manager
    if isinstance(manager_projects, str):
        manager_projects = [manager_projects]
    
    # st.write(f"Managing projects: {', '.join(manager_projects)}")


    # Display editable table for manager
    edited_df = display_editable_table(fitbit_df, user_df, is_admin=False)
    original_fitbit_df = original_fitbit_df.to_pandas()
    edited_df = edited_df.to_pandas()
    original_fitbit_df = pd.concat([original_fitbit_df[original_fitbit_df['project'] != manager_projects[0]], edited_df])

    # Save changes button
    if st.button("Save Changes"):
        save_changes(original_fitbit_df, fitbit_sheet, spreadsheet)

def display_editable_table(fitbit_df: pl.DataFrame, user_df: pl.DataFrame, is_admin: bool = False) -> pl.DataFrame:
    """
    Display an editable datatable with role-appropriate permissions.
    
    Args:
        fitbit_df: DataFrame with Fitbit device data
        user_df: DataFrame with user data (for student assignment)
        is_admin: Whether the current user is an admin
        
    Returns:
        The edited DataFrame
    """
    # Create a copy to avoid modifying the original during editing
    edited_df = fitbit_df.clone().sort("project", "name")
    
    # Create a data editor with appropriate permissions
    edited_df = st.data_editor(
        edited_df,
        width="stretch",
        num_rows="dynamic" if is_admin else "fixed",
        column_config={
            "project": st.column_config.SelectboxColumn(
                "Project",
                help="The project this device belongs to",
                width="medium",
                # Fixed to_list() method for project options
                options=sorted(fitbit_df.select("project").unique().get_column("project").to_list()),
                disabled=not is_admin,  # Only admins can change project
            ),
            "name": st.column_config.TextColumn(
                "Device Name",
                help="The name of the Fitbit device",
                width="medium",
                disabled=not is_admin,  # Only admins can change name
            ),
            "token": st.column_config.TextColumn(
                "Legacy Fitbit Token",
                help="Legacy Fitbit API token. Google Health users store tokens in health_oauth_tokens.",
                width="large",
                disabled=not is_admin,  # Only admins can see/edit tokens
            ),
            "oauth_type": st.column_config.SelectboxColumn(
                "OAuth Type",
                help="Provider used for this watch",
                width="medium",
                options=["fitbit", "google_health", ""],
                disabled=not is_admin,
            ),
            "provider": st.column_config.SelectboxColumn(
                "Provider",
                help="Provider used for this watch",
                width="medium",
                options=["fitbit", "google_health", ""],
                disabled=not is_admin,
            ),
            "oauth_client_key": st.column_config.TextColumn(
                "OAuth Client Key",
                help="Key in health_oauth_clients for Google Health rows",
                width="medium",
                disabled=not is_admin,
            ),
            "auth_status": st.column_config.SelectboxColumn(
                "Auth Status",
                help="Current OAuth status",
                width="medium",
                options=["not_connected", "connected", "expired", "reauth_required", "revoked", "error", ""],
                disabled=not is_admin,
            ),
            "health_user_id": st.column_config.TextColumn(
                "Health User ID",
                help="Google Health user ID returned by OAuth identity",
                width="medium",
                disabled=True,
            ),
            "legacy_fitbit_user_id": st.column_config.TextColumn(
                "Legacy Fitbit User ID",
                help="Legacy Fitbit user ID returned by Google Health identity when available",
                width="medium",
                disabled=True,
            ),
            "last_auth_error": st.column_config.TextColumn(
                "Last Auth Error",
                help="Most recent OAuth or refresh error",
                width="large",
                disabled=True,
            ),
            "reauth_link": st.column_config.LinkColumn(
                "Reauth Link",
                help="Latest generated reauthorization link",
                width="large",
                disabled=True,
            ),
            "user": st.column_config.TextColumn(
                "User",
                help="User associated with this device",
                width="medium",
                disabled=not is_admin,  # Only admins can edit user
            ),
            "isActive": st.column_config.CheckboxColumn(
                "Is Active",
                help="Whether this device is currently active",
                width="small",
                disabled=False,  # Both managers and admins can toggle active status
            ),
            "currentStudent": st.column_config.SelectboxColumn(
                "Current Student",
                help="Student currently assigned to this device",
                width="medium",
                # Fixed to_list() method for student options
                options=sorted(user_df.select("name").unique().get_column("name").to_list() + [""]),
                disabled=False,  # Both managers and admins can assign students
            ),
        },
        hide_index=True,
    )
    
    return edited_df

def save_changes(edited_df: pl.DataFrame, fitbit_sheet: Any, spreadsheet: Spreadsheet) -> None:
    """Save changes made to the Fitbit devices table."""
    try:
        st.warning("⚠️ Please note: Changes may take 2-3 minutes to fully update in the cloud. Meanwhile, you can continue using the app.")
        # Update sheet data with edited DataFrame
        spreadsheet.update_sheet(
            "fitbit",
            edited_df,
            strategy="replace"  # Replace the entire sheet with the edited DataFrame
        )
        
        # Save changes to Google Sheets
        GoogleSheetsAdapter.save(spreadsheet, "fitbit")
        
        # Show success message
        st.success("Changes saved successfully!")
        
        # Add warning about cloud update delay
        
        
        # Add timestamp
        st.write(f"Last updated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        if not show_rate_limit_notice(
            e,
            provider="google_sheets",
            key="fitbit_management_save",
            context="saving Fitbit device changes",
        ):
            st.error(f"Error saving changes: {str(e)}")
            st.exception(e)

# Initialize session state variables if they don't exist
if 'add_new_device' not in st.session_state:
    st.session_state.add_new_device = False

# This page can be run directly for testing
# if __name__ == "__main__":
#     # Mock user details for testing
#     mock_user = {
#         "name": "Test Admin",
#         "role": "admin",
#         "projects": ["Project A", "Project B"]
#     }
#     load_fitbit_datatable(mock_user)
