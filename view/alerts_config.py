import streamlit as st
import pandas as pd
from entity.Sheet import GoogleSheetsAdapter, SheetFactory, Spreadsheet
from model.config import get_secrets
import polars as pl
def load_spreadsheet() -> Spreadsheet:
    """Load the spreadsheet with all configuration data"""
    secrets = get_secrets()
    spreadsheet = Spreadsheet(
        name="Fitbit Database",
        api_key=secrets.get("spreadsheet_key", "")
    )
    GoogleSheetsAdapter.connect(spreadsheet)
    return spreadsheet

def get_user_fitbit_config(spreadsheet:Spreadsheet, user_email):
    """Get Fitbit configuration for the current user"""
    # Get the fitbit alerts config sheet
    fitbit_config_sheet = spreadsheet.get_sheet("fitbit_alerts_config", "fitbit_alerts_config")
    
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
            'manager': user_email
        }
    
    # Filter by manager email
    user_config = config_df.filter(config_df['manager'] == user_email)
    
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
            'manager': user_email
        }
    
    # Return the first config for this user
    return user_config.to_dicts()[0]

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

    # config_df = pl.DataFrame(config_data)
    spreadsheet.update_sheet("fitbit_alerts_config", config_data, strategy="append")
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
        pl.col("isActive").apply(
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
                appsheet_config = appsheet_config.filter(pl.col('user') == user)

                missing_thr = st.number_input("Missing Data Threshold",
                                            min_value=1, max_value=100,  # 1 hour to 1 week
                                            value=int(appsheet_config.get('missingThr', 3)))
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
        if user_role in ['manager', 'Admin']:
            tab1, tab2 = st.tabs(["Fitbit Alerts", "Qualtrics Alerts"])
    elif user_project in ['fibro', 'Admin']:
        tab1, tab2 = st.tabs(["Fitbit Alerts", "AppSheet Alerts"])
    else:
        tab1, tab2 = st.tabs(["Fitbit Alerts", "Qualtrics Alerts"])

    # Tab 1: Fitbit Alerts Configuration
    with tab1:
        st.header("Fitbit Alerts Configuration")
        
        # Get current configuration
        fitbit_config = get_user_fitbit_config(spreadsheet, user_email)
        
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
                    'manager': user_email
                }
                
                # Save the configuration
                if save_fitbit_config(spreadsheet, config_data):
                    st.success("Fitbit alerts configuration saved successfully!")
                else:
                    st.error("Failed to save configuration. Please try again.")
    
    # Tab 2: Qualtrics Alerts Configuration
    with tab2:

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

# # Run the page when this file is executed
# if __name__ == "__main__":
#     alerts_config_page(
