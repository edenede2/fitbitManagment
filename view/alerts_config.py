import streamlit as st
import pandas as pd
from entity.Sheet import GoogleSheetsAdapter, SheetFactory, Spreadsheet
from model.config import get_secrets
import polars as pl
from datetime import date, timedelta
from st_aggrid import AgGrid
from st_aggrid.grid_options_builder import GridOptionsBuilder
from controllers.agGridHelper import aggrid_polars

def load_spreadsheet() -> Spreadsheet:
    """Load the spreadsheet with all configuration data"""
    secrets = get_secrets()
    spreadsheet = Spreadsheet(
        name="Fitbit Database",
        api_key=secrets.get("spreadsheet_key", "")
    )
    GoogleSheetsAdapter.connect(spreadsheet)
    return spreadsheet

def get_user_fitbit_config(spreadsheet:Spreadsheet, user_email, user_project):
    """Get Fitbit configuration for the current user"""
    # Get the fitbit alerts config sheet
    fitbit_config_sheet = spreadsheet.get_sheet("fitbit_alerts_config", "fitbit_alerts_config")
    fitbit_sheet_df = spreadsheet.get_sheet("fitbit", "fitbit").to_dataframe(engine="polars")
    
    # Convert to DataFrame for easier filtering
    config_df = fitbit_config_sheet.to_dataframe(engine="polars")
    
    # If empty, return default config
    if config_df.is_empty():
        return {
            'project': '',
            'currentSyncThr': 3,
            'totalSyncThr': 10,
            'currentHrThr': 3,
            'totalHrThr': 10,
            'currentSleepThr': 3,
            'totalSleepThr': 10,
            'currentStepsThr': 3,
            'totalStepsThr': 10,
            'batteryThr': 20,
            'manager': user_email,
            'email': user_email,
            'watch': '',
            'endDate': date.today() + timedelta(days=30)
        }
    
    # Filter by manager email
    user_config = config_df.filter(pl.col('manager') == user_email)
    
    # If user has no config, return default
    if user_config.is_empty():
        return {
            'project': '',
            'currentSyncThr': 3,
            'totalSyncThr': 10,
            'currentHrThr': 3,
            'totalHrThr': 10,
            'currentSleepThr': 3,
            'totalSleepThr': 10,
            'currentStepsThr': 3,
            'totalStepsThr': 10,
            'batteryThr': 20,
            'manager': user_email,
            'email': user_email,
            'watch': '',
            'endDate': date.today() + timedelta(days=30)
        }
    
    if user_project == 'Admin':
        watche_name_list = fitbit_sheet_df.select('name').unique().to_series().to_list()
    else:
        watche_name_list = fitbit_sheet_df.filter(pl.col('project') == user_project).select('name').unique().to_series().to_list()
    
    # Return the first config for this user
    return user_config.to_dicts()[0], sorted(watche_name_list)

def get_user_qualtrics_config(spreadsheet:Spreadsheet, user_email):
    """Get Qualtrics configuration for the current user"""
    # Get the qualtrics alerts config sheet
    # GoogleSheetsAdapter.connect(spreadsheet)
    qualtrics_config_sheet = spreadsheet.get_sheet("qualtrics_alerts_config", "qualtrics_alerts_config")

    # st.write(qualtrics_config_sheet)
    # Convert to DataFrame for easier filtering
    config_df = qualtrics_config_sheet.to_dataframe(engine="polars")
    # st.dataframe(config_df)
    # If empty, return default config
    if config_df.is_empty():
        return {
            'hoursThr': 48,
            'project': '',
            'manager': user_email
        }
    
    # Filter by manager email
    user_config = config_df.filter(pl.col('manager') == user_email)
    
    # If user has no config, return default
    if user_config.is_empty():
        return {
            'hoursThr': 48,
            'project': '',
            'manager': user_email
        }
    
    # Return the first config for this user
    return user_config.to_dicts()[0]

def save_fitbit_config(spreadsheet:Spreadsheet, config_data):
    """Save Fitbit configuration for the current user"""
    
    # Get the current configuration sheet
    fitbit_config_sheet = spreadsheet.get_sheet("fitbit_alerts_config", "fitbit_alerts_config")
    current_config_df = fitbit_config_sheet.to_dataframe(engine="polars")
    
    # Define the checks based on the specified criteria
    project = config_data.get('project', '')
    email = config_data.get('email', '')
    watch = config_data.get('watch', '')
    manager = config_data.get('manager', '')
    
    # Initialize a flag to track if we need to append or replace
    should_append = True
    
    if not current_config_df.is_empty():
        # Case 1: If no email and no watch, check for rows with the same project
        if not email and not watch and project:
            existing_rows = current_config_df.filter(pl.col('project') == project)
            if not existing_rows.is_empty():
                # Replace the existing rows with the same project
                current_config_df = current_config_df.filter(pl.col('project') != project)
                should_append = False
        
        # Case 2: If email specified but no watch, check for rows with the same project and email
        elif email and not watch and project:
            existing_rows = current_config_df.filter(
                (pl.col('project') == project) & (pl.col('email') == email)
            )
            if not existing_rows.is_empty():
                # Replace the existing rows with the same project and email
                current_config_df = current_config_df.filter(
                    ~((pl.col('project') == project) & (pl.col('email') == email))
                )
                should_append = False
        
        # Case 3: If watch, email and project all specified, check for exact match
        elif email and watch and project:
            existing_rows = current_config_df.filter(
                (pl.col('project') == project) & 
                (pl.col('email') == email) & 
                (pl.col('watch') == watch)
            )
            if not existing_rows.is_empty():
                # Replace the existing rows with the same project, email, and watch
                current_config_df = current_config_df.filter(
                    ~((pl.col('project') == project) & 
                      (pl.col('email') == email) & 
                      (pl.col('watch') == watch))
                )
                should_append = False
    
    # Convert the config_data to a DataFrame
    new_config_df = pl.DataFrame([config_data])
    
    # Append the new configuration or replace with updated configuration
    # Check if the misspelled column exists and rename it to match
    if "battaryThr" in current_config_df.columns and "batteryThr" in new_config_df.columns:
        current_config_df = current_config_df.rename({"battaryThr": "batteryThr"})
    elif "batteryThr" in current_config_df.columns and "battaryThr" in new_config_df.columns:
        new_config_df = new_config_df.rename({"battaryThr": "batteryThr"})
    
    # Ensure numeric columns are consistent types by converting to strings
    numeric_cols = ['currentSyncThr', 'totalSyncThr', 'currentHrThr', 'totalHrThr', 
                   'currentSleepThr', 'totalSleepThr', 'currentStepsThr', 'totalStepsThr', 
                   'batteryThr']
    
    # Convert numeric columns to string in both dataframes to ensure compatibility
    if not current_config_df.is_empty():
        for col in numeric_cols:
            if col in current_config_df.columns:
                current_config_df = current_config_df.with_columns(pl.col(col).cast(pl.Utf8))
    
    for col in numeric_cols:
        if col in new_config_df.columns:
            new_config_df = new_config_df.with_columns(pl.col(col).cast(pl.Utf8))
            
    if should_append:
        # Just append the new config
        if not current_config_df.is_empty():
            updated_df = pl.concat([current_config_df, new_config_df])
        else:
            updated_df = new_config_df
    else:
        # Add the new config after filtering out the old one
        updated_df = pl.concat([current_config_df, new_config_df])
    
    # Update the sheet with the new configuration
    spreadsheet.update_sheet("fitbit_alerts_config", updated_df, strategy="replace")
    GoogleSheetsAdapter.save(spreadsheet, "fitbit_alerts_config")
    
    return True

def save_qualtrics_config(spreadsheet:Spreadsheet, config_data):
    """Save Qualtrics configuration for the current user"""
    # Get the qualtrics alerts config sheet
    config_df = pl.DataFrame(config_data)

    spreadsheet.update_sheet("qualtrics_alerts_config", config_df, strategy="append")
    GoogleSheetsAdapter.save(spreadsheet, "qualtrics_alerts_config")

    return True

def get_user_appsheet_config(spreadsheet:Spreadsheet, user_email):
    """Get AppSheet configuration for the current user"""
    # Get the qualtrics alerts config sheet
    # GoogleSheetsAdapter.connect(spreadsheet)
    appsheet_config_sheet = spreadsheet.get_sheet("appsheet_alerts_config", "appsheet_alerts_config")

    # Convert to DataFrame for easier filtering
    config_df = appsheet_config_sheet.to_dataframe(engine="polars")
    
    # If empty, return default config
    if config_df.is_empty():
        return {
            'email': user_email,
            'user': '',
            'missingThr': 3
        }
    
    # Filter by manager email
    user_config = config_df.filter(pl.col('email') == user_email)
    
    # If user has no config, return default
    if user_config.is_empty():
        return {
            'email': user_email,
            'user': '',
            'missingThr': 3
        }
    
    # Return the config for this email
    return user_config

def save_appsheet_config(spreadsheet:Spreadsheet, config_data):
    """Save AppSheet configuration for the current user"""
    # Get the qualtrics alerts config sheet
    config_df = pl.DataFrame(config_data)

    spreadsheet.update_sheet("appsheet_alerts_config", config_df, strategy="append")
    GoogleSheetsAdapter.save(spreadsheet, "appsheet_alerts_config")

    return True

def get_fitbit_failures(spreadsheet:Spreadsheet, user_project):
    """Get Fitbit failures from the spreadsheet"""
    # Get the fitbit alerts config sheet
    # GoogleSheetsAdapter.connect(spreadsheet)
    fitbit_failures_sheet_df = spreadsheet.get_sheet("log", "log").to_dataframe(engine="polars")
    fitbit_failures_total_sheet_df = spreadsheet.get_sheet("FitbitLog", "log").to_dataframe(engine="polars")
    # Filter by project
    if user_project == 'Admin':
        project_fitbits = fitbit_failures_sheet_df
        # fitbit_failures_total_sheet_df = fitbit_failures_total_sheet_df
    else:
        # Filter by project
        project_fitbits = fitbit_failures_sheet_df.filter(pl.col('project') == user_project)

    # project_fitbits = fitbit_failures_sheet_df.filter(pl.col('project') == user_project)

    # If empty, return empty DataFrame
    if project_fitbits.is_empty():
        project_fitbits = pl.DataFrame(schema={
            'project': pl.Utf8,
            'name': pl.Utf8,
            'token': pl.Utf8,
            'user': pl.Utf8,
            'isActive': pl.Boolean,
            'currentStudent': pl.Utf8
        })
        return project_fitbits, fitbit_failures_total_sheet_df
    return project_fitbits, fitbit_failures_total_sheet_df

def get_fibro_users(spreadsheet:Spreadsheet):
    """Get Fibro users from the spreadsheet"""
    # Get the qualtrics alerts config sheet
    # GoogleSheetsAdapter.connect(spreadsheet)
    fibro_users_sheet = spreadsheet.get_sheet("fitbit", "fitbit")

    # Convert to DataFrame for easier filtering
    fibro_users_df = fibro_users_sheet.to_dataframe(engine="polars")
    # Filter by project
    fibro_users_df = fibro_users_df.filter(pl.col('project') == 'fibro')
    fibro_users_df = fibro_users_df.with_columns(
        pl.col("isActive").map_elements(
            lambda x: x if isinstance(x, bool) else (True if str(x).lower() == "true" else False)
        )
    )
    fibro_active_users_df = fibro_users_df.filter(pl.col('isActive') == True)


    # If empty, return empty DataFrame
    if fibro_users_df.is_empty():
        fibro_users_df = pl.DataFrame(schema={
            'project': pl.Utf8,
            'name': pl.Utf8,
            'token': pl.Utf8,
            'user': pl.Utf8,
            'isActive': pl.Boolean,
            'currentStudent': pl.Utf8
        })
        return fibro_users_df, fibro_users_df
    

    
        
                            
    # Return the first config for this user
    return fibro_users_df, fibro_active_users_df


def appsheet_config(spreadsheet:Spreadsheet,user_email):
    st.header("AppSheet Alerts Configuration")

    # Get current configuration
    appsheet_config = get_user_appsheet_config(spreadsheet, user_email)

    fitbit_fibro_table, fitbit_active_users = get_fibro_users(spreadsheet)

    # Get the list of users
    user_list = fitbit_fibro_table['user'].unique().to_list()

    # If user is inactive, disable the submission
    for user in user_list:
        with st.form(f"user_form_{user}"):
            st.write(f"User: {user}")
            if user not in fitbit_active_users['user'].to_list():
                st.warning("This user is inactive. You cannot edit their configuration.")
            else:
                # Check if appsheet_config is a DataFrame or dict
                if isinstance(appsheet_config, pl.DataFrame):
                    user_config = appsheet_config.filter(pl.col('user') == user)
                    missing_thr_value = user_config.get('missingThr', 3) if not user_config.is_empty() else 3
                else:
                    # It's a dictionary
                    missing_thr_value = appsheet_config.get('missingThr', 3)

                missing_thr = st.number_input("Missing Data Threshold",
                                            min_value=1, max_value=100,  # 1 hour to 1 week
                                            value=int(missing_thr_value))
                save_button = st.form_submit_button("Save Configuration")
                if save_button:
                    # Prepare data for saving
                    config_data = {
                        'email': user_email,
                        'user': user,
                        'missingThr': missing_thr
                    }
                    # Save the configuration
                    if save_appsheet_config(spreadsheet, config_data):
                        st.success(f"AppSheet alerts configuration for {user} saved successfully!")
                    else:
                        st.error("Failed to save configuration. Please try again.")

    # If no active users, show a message
    if fitbit_active_users.is_empty():
        st.warning("No active users found. Please check the user list.")

        
        

def qualtrics_config(spreadsheet:Spreadsheet, user_email):

    st.header("Qualtrics Alerts Configuration")
    
    # Get current configuration
    qualtrics_config = get_user_qualtrics_config(spreadsheet, user_email)
    
    # Create form for editing configuration
    with st.form("qualtrics_config_form"):
        project = st.text_input("Project", value=qualtrics_config.get('project', ''))
        
        hours_thr = st.number_input("Hours Threshold for Late Responses", 
                                    min_value=1, max_value=168,  # 1 hour to 1 week
                                    value=int(qualtrics_config.get('hoursThr', 48)))
        
        save_button = st.form_submit_button("Save Configuration")
        
        if save_button:
            # Prepare data for saving
            config_data = {
                'hoursThr': hours_thr,
                'project': project,
                'manager': user_email
            }
            
            # Save the configuration
            if save_qualtrics_config(spreadsheet, config_data):
                st.success("Qualtrics alerts configuration saved successfully!")
            else:
                st.error("Failed to save configuration. Please try again.")

def get_project_fitbit_configs(spreadsheet:Spreadsheet, user_project):
    """Get all Fitbit configurations for a specific project"""
    # Get the fitbit alerts config sheet
    fitbit_config_sheet = spreadsheet.get_sheet("fitbit_alerts_config", "fitbit_alerts_config")
    
    # Convert to DataFrame for easier filtering
    config_df = fitbit_config_sheet.to_dataframe(engine="polars")
    
    # If empty, return empty DataFrame
    if config_df.is_empty():
        return pl.DataFrame()
    
    # If Admin, return all configs, otherwise filter by project
    if user_project == 'Admin':
        return config_df
    else:
        # Filter by project
        return config_df.filter(pl.col('project') == user_project)

def get_project_qualtrics_configs(spreadsheet:Spreadsheet, user_project):
    """Get all Qualtrics configurations for a specific project"""
    # Get the qualtrics alerts config sheet
    qualtrics_config_sheet = spreadsheet.get_sheet("qualtrics_alerts_config", "qualtrics_alerts_config")
    
    # Convert to DataFrame for easier filtering
    config_df = qualtrics_config_sheet.to_dataframe(engine="polars")
    
    # If empty, return empty DataFrame
    if config_df.is_empty():
        return pl.DataFrame()
    
    # If Admin, return all configs, otherwise filter by project
    if user_project == 'Admin':
        return config_df
    else:
        # Filter by project
        return config_df.filter(pl.col('project') == user_project)

def display_fitbit_configs(configs_df):
    """Display a visualization of Fitbit alert configurations"""
    if configs_df.is_empty():
        st.info("No existing Fitbit alert configurations for this project.")
        return
    
    st.subheader("Current Fitbit Alert Configurations")
    
    # Create an expandable section for the configuration table
    with st.expander("View all configurations", expanded=True):
        # Create tabs for different views of the configuration
        tab_summary, tab_details = st.tabs(["Summary", "Detailed View"])
        
        with tab_summary:
            # Create a summary table with key information
            summary_cols = ['project', 'email', 'watch', 'batteryThr', 'endDate']
            if all(col in configs_df.columns for col in summary_cols):
                summary_df = configs_df.select(summary_cols)
                st.dataframe(summary_df, use_container_width=True)
            else:
                st.warning("Configuration data is missing expected columns")
        
        with tab_details:
            # Show the full configuration details
            st.dataframe(configs_df, use_container_width=True)
    
    # Add some visual charts if there are enough configurations
    if len(configs_df) > 1:
        st.subheader("Configuration Analysis")
        
        # Chart 1: Battery thresholds
        if 'batteryThr' in configs_df.columns:
            try:
                st.caption("Battery Level Thresholds")
                battery_data = configs_df.select(['email', 'batteryThr']).to_pandas()
                
                import plotly.express as px
                fig = px.bar(battery_data, x='email', y='batteryThr', 
                             labels={'batteryThr': 'Battery Threshold (%)', 'email': 'Recipient Email'},
                             title="Battery Alert Thresholds by Recipient")
                st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Error creating battery threshold chart: {e}")

def display_qualtrics_configs(configs_df):
    """Display a visualization of Qualtrics alert configurations"""
    if configs_df.is_empty():
        st.info("No existing Qualtrics alert configurations for this project.")
        return
    
    st.subheader("Current Qualtrics Alert Configurations")
    
    # Display the configuration table
    st.dataframe(configs_df, use_container_width=True)
    
    # Add a visualization if there are multiple configurations
    if len(configs_df) > 1 and 'hoursThr' in configs_df.columns:
        try:
            hours_data = configs_df.select(['manager', 'hoursThr']).to_pandas()
            
            import plotly.express as px
            fig = px.bar(hours_data, x='manager', y='hoursThr',
                         labels={'hoursThr': 'Hours Threshold', 'manager': 'Manager'},
                         title="Response Time Thresholds by Manager")
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error creating hours threshold chart: {e}")

def alerts_config_page(user_email, spreadsheet: Spreadsheet, user_role, user_project) -> None:
    """Main function for the alerts configuration page"""
    st.title("Alerts Configuration")
    
    # # Check if user is logged in and has appropriate role
    # if 'user_email' not in st.session_state or 'user_role' not in st.session_state:
    #     st.error("You must be logged in to view this page.")
    #     return
    
    # if st.session_state['user_role'] not in ['admin', 'manager']:
    #     st.error("You do not have permission to access this page.")
    #     return
    
    # Load data
    # user_email = st.session_state['user_email']
    # spreadsheet = load_spreadsheet()
    
    # Create tabs for the two configuration types
    tab1, tab2 = '', ''


    if user_project in ['nova', 'Admin']:
        if user_role in ['Manager', 'Admin']:
            tab1, tab2 = st.tabs(["Fitbit Alerts", "Qualtrics Alerts"])
    elif user_project in ['fibro', 'Admin']:
        tab1, tab2 = st.tabs(["Fitbit Alerts", "AppSheet Alerts"])
    else:
        tab1, tab2 = st.tabs(["Fitbit Alerts", ""])

    # Tab 1: Fitbit Alerts Configuration
    with tab1:
        st.header("Fitbit Alerts Configuration")
        
        # Add visualization of current configurations
        fitbit_configs = get_project_fitbit_configs(spreadsheet, user_project)
        display_fitbit_configs(fitbit_configs)
        st.subheader("Reset Fitbits Failures Counters")
        fitbit_failures, total_fitbit_df = get_fitbit_failures(spreadsheet, user_project)
        
        # Add a reset column for checkboxes
        fitbit_failures = fitbit_failures.with_columns(
            reset=pl.lit(False)
        )
        
        # Create a container to maintain state between rerenders
        if "reset_checkboxes" not in st.session_state:
            st.session_state.reset_checkboxes = {}
        
        # Pre-process the dataframe to use session state values
        if len(st.session_state.reset_checkboxes) > 0:
            # Create a function to check if a row should be marked as reset
            def set_reset_value(row):
                row_id = f"{row['name']}_{row.get('lastCheck', '')}"
                return st.session_state.reset_checkboxes.get(row_id, False)
                
            # Apply the function to update the reset column
            fitbit_failures = fitbit_failures.with_columns(
                reset=pl.struct(fitbit_failures.columns).map_elements(set_reset_value)
            )
        
        # Use the AgGrid with the preprocessed data
        edited_df, grid_response = aggrid_polars(fitbit_failures, bool_editable=True, key="fitbit_reset_grid")
        
        # After grid is rendered, update session state with any changes
        if grid_response.data is not None:
            st.write(grid_response.data)
            for row in grid_response.data:
                
                if isinstance(row, str):
                    # Handle the case where row is a string (e.g., row ID)
                    
                    row_id = row
                else:
                    # Handle the case where row is a dictionary
                    row_id = f"{row.get('name', '')}_{row.get('lastCheck', '')}"
                # row_id = f"{row.get('name', '')}_{row.get('lastCheck', '')}"
                st.session_state.reset_checkboxes[row_id] = row.get('reset', False)
        
        # Create checkbox alternatives - these will respond immediately
        with st.expander("Alternative selection method", expanded=True):
            st.write("If the grid checkboxes aren't responding, use these toggles instead:")
            
            # Get unique watches from the data
            unique_watches = fitbit_failures.select('name').unique().to_series().to_list()
            
            # Create columns for better layout
            cols = st.columns(3)
            
            # Create a checkbox for each watch
            for i, watch in enumerate(unique_watches):
                col_idx = i % 3
                checkbox_key = f"watch_{watch}_reset"
                
                # Pre-fill state from session state if available
                default = any(v for k, v in st.session_state.reset_checkboxes.items() if k.startswith(f"{watch}_"))
                
                # Create the checkbox
                reset_watch = cols[col_idx].checkbox(f"Reset {watch}", value=default, key=checkbox_key)
                
                # Update session state when checkbox changes
                if reset_watch:
                    # Find all rows for this watch
                    watch_rows = fitbit_failures.filter(pl.col('name') == watch)
                    
                    # Update session state for each row
                    for row in watch_rows.rows(named=True):
                        row_id = f"{row['name']}_{row.get('lastCheck', '')}"
                        st.session_state.reset_checkboxes[row_id] = True
        
        # Show the selections based on session state (this will always be accurate)
        reset_watches = [k.split('_')[0] for k, v in st.session_state.reset_checkboxes.items() if v]
        if reset_watches:
            st.write("Watches marked for reset:", ", ".join(set(reset_watches)))
        
        # Reset button now uses session state
        if st.button("Reset Fitbit Failures Counters"):
            reset_items = [(k.split('_')[0], k.split('_')[1]) for k, v in st.session_state.reset_checkboxes.items() if v]
            
            if len(reset_items) > 0:
                col_to_reset = [col for col in fitbit_failures.columns if col.startswith('Total')]
                # Reset the counters for the selected items
                for watch_name, last_check in reset_items:
                    for column in col_to_reset:
                        total_fitbit_df = total_fitbit_df.with_columns(
                            pl.when((pl.col('watchName') == watch_name) & 
                                   (pl.col('lastCheck') == last_check))
                            .then(pl.lit(0))
                            .otherwise(pl.col(column))
                            .alias(column)
                        )
                
                # Update the sheet with the new configuration
                spreadsheet.update_sheet("FitbitLog", total_fitbit_df, strategy="replace")
                GoogleSheetsAdapter.save(spreadsheet, "FitbitLog")
                st.success(f"Reset {len(reset_items)} fitbit failure counters successfully!")
                
                # Clear the session state after successful reset
                st.session_state.reset_checkboxes = {}
                
                # Force a rerun to refresh the grid with updated data
                st.rerun()
            else:
                st.warning("No watches selected for reset. Please check the boxes in the 'reset' column or use the toggles.")
        st.markdown("---")
        st.subheader("Create/Edit Configuration")
        
        # Get current configuration
        fitbit_config, watch_names = get_user_fitbit_config(spreadsheet, user_email, user_project)
        
        # Create form for editing configuration
        with st.form("fitbit_config_form"):
            project = st.text_input("Project", value=fitbit_config.get('project', ''))
            
            st.subheader("Sync Thresholds")
            current_sync_thr = st.number_input("Current Failed Sync Threshold", 
                                             min_value=1, max_value=100, 
                                             value=int(fitbit_config.get('currentSyncThr', 3)))
            total_sync_thr = st.number_input("Total Failed Sync Threshold", 
                                           min_value=1, max_value=1000, 
                                           value=int(fitbit_config.get('totalSyncThr', 10)))
            
            st.subheader("Heart Rate Thresholds")
            current_hr_thr = st.number_input("Current Failed HR Threshold", 
                                           min_value=1, max_value=100, 
                                           value=int(fitbit_config.get('currentHrThr', 3)))
            total_hr_thr = st.number_input("Total Failed HR Threshold", 
                                         min_value=1, max_value=1000, 
                                         value=int(fitbit_config.get('totalHrThr', 10)))
            
            st.subheader("Sleep Thresholds")
            current_sleep_thr = st.number_input("Current Failed Sleep Threshold", 
                                              min_value=1, max_value=100, 
                                              value=int(fitbit_config.get('currentSleepThr', 3)))
            total_sleep_thr = st.number_input("Total Failed Sleep Threshold", 
                                            min_value=1, max_value=1000, 
                                            value=int(fitbit_config.get('totalSleepThr', 10)))
            
            st.subheader("Steps Thresholds")
            current_steps_thr = st.number_input("Current Failed Steps Threshold", 
                                              min_value=1, max_value=100, 
                                              value=int(fitbit_config.get('currentStepsThr', 3)))
            total_steps_thr = st.number_input("Total Failed Steps Threshold", 
                                            min_value=1, max_value=1000, 
                                            value=int(fitbit_config.get('totalStepsThr', 10)))
            
            st.subheader("Battery Threshold")
            battery_thr = st.number_input("Battery Level Threshold (%)", 
                                        min_value=5, max_value=50, 
                                        value=int(fitbit_config.get('batteryThr', 20)))
            
            st.subheader("Recipient Email")
            st.write("This email will receive alerts when the thresholds are exceeded.")
            recipient_email = st.text_input("Recipient Email", value=user_email)

            st.subheader("(OPTIONAL) Watch Name")
            st.write("Select the specific watch name for which you want to set the alerts config.")
            watch_name = st.selectbox("Watch Name", options=["All the project."] + watch_names , index=0)

            st.subheader("End Date")
            st.write("This date will be used to stop the alerts.")
            end_date = st.date_input("End Date", value=date.today() + timedelta(days=30))
            
            save_button = st.form_submit_button("Save Configuration")
            
            if save_button:
                # Prepare data for saving
                config_data = {
                    'project': project,
                    'currentSyncThr': current_sync_thr,
                    'totalSyncThr': total_sync_thr,
                    'currentHrThr': current_hr_thr,
                    'totalHrThr': total_hr_thr,
                    'currentSleepThr': current_sleep_thr,
                    'totalSleepThr': total_sleep_thr,
                    'currentStepsThr': current_steps_thr,
                    'totalStepsThr': total_steps_thr,
                    'batteryThr': battery_thr,
                    'manager': user_email,
                    'email': recipient_email,
                    'watch': watch_name if watch_name != "All the project." else '',
                    'endDate': end_date.strftime("%Y-%m-%d")
                }
                
                # Save the configuration
                if save_fitbit_config(spreadsheet, config_data):
                    st.success("Fitbit alerts configuration saved successfully!")
                else:
                    st.error("Failed to save configuration. Please try again.")
    
    # Tab 2: Qualtrics Alerts Configuration
    with tab2:
        if user_role == 'Admin':
            user_project = st.selectbox("Select Project", ["fibro", "nova"])

        if user_project == 'fibro':
            appsheet_config(spreadsheet, user_email)
        elif user_project == 'nova':
            # Display AppSheet configuration
            
            
            st.header("Qualtrics Alerts Configuration")
            
            # Add visualization of current configurations
            qualtrics_configs = get_project_qualtrics_configs(spreadsheet, user_project)
            display_qualtrics_configs(qualtrics_configs)
            
            st.markdown("---")
            st.subheader("Create/Edit Configuration")
            
            # Get current configuration
            qualtrics_config = get_user_qualtrics_config(spreadsheet, user_email)
            
            # Create form for editing configuration
            with st.form("qualtrics_config_form"):
                project = st.text_input("Project", value=qualtrics_config.get('project', ''))
                
                hours_thr = st.number_input("Hours Threshold for Late Responses", 
                                        min_value=1, max_value=168,  # 1 hour to 1 week
                                        value=int(qualtrics_config.get('hoursThr', 48)))
                
                save_button = st.form_submit_button("Save Configuration")
                
                if save_button:
                    # Prepare data for saving
                    config_data = {
                        'hoursThr': hours_thr,
                        'project': project,
                        'manager': user_email
                    }
                    
                    # Save the configuration
                    if save_qualtrics_config(spreadsheet, config_data):
                        st.success("Qualtrics alerts configuration saved successfully!")
                    else:
                        st.error("Failed to save configuration. Please try again.")
        else:
            st.warning("You don't have permission to access this page.")
# # Run the page when this file is executed
# if __name__ == "__main__":
#     alerts_config_page(
