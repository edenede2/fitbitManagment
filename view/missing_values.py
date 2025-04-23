# import streamlit as st
# import pandas as pd
# from entity.Sheet import GoogleSheetsAdapter, SheetFactory, Spreadsheet
# from model.config import get_secrets
# import polars as pl
# from datetime import date, timedelta
# from st_aggrid import AgGrid, GridUpdateMode 
# from st_aggrid.grid_options_builder import GridOptionsBuilder
# from controllers.agGridHelper import aggrid_polars

# def load_spreadsheet() -> Spreadsheet:
#     """Load the spreadsheet with all configuration data"""
#     secrets = get_secrets()
#     spreadsheet = Spreadsheet(
#         name="Fitbit Database",
#         api_key=secrets.get("spreadsheet_key", "")
#     )
#     GoogleSheetsAdapter.connect(spreadsheet)
#     return spreadsheet


# def get_watches(spreadsheet: Spreadsheet, user_project: str) -> list:
#     """Get the list of watches for the given project"""
#     fitbit_sheet = spreadsheet.get_sheet("FitbitLog","log")
#     fitbit_df = fitbit_sheet.to_dataframe(engine='polars')
    
#     # Filter by project
#     if user_project != 'Admin':
#         fitbit_df = fitbit_df.filter(pl.col('project') == user_project)
    
#     # Get unique watch names
#     watch_names = fitbit_df.select('name').unique().to_series().to_list()
    
#     return watch_names


# # def present_missing_values(


# def missing_values_page(user_email, spreadsheet: Spreadsheet, user_role, user_project) -> None:
#     """Main function for the missing values analysis page"""
#     st.title("Missing Values Analysis")
    
    

#     tab1, tab2 = '', ''

#     watch_names = get_watches(spreadsheet, user_project)

#     if user_project in ['nova', 'Admin']:
#         if user_role in ['Manager', 'Admin']:
#             tab1, tab2 = st.tabs(["Fitbit Alerts", "Qualtrics Alerts"])
#     elif user_project in ['fibro', 'Admin']:
#         tab1, tab2 = st.tabs(["Fitbit Alerts", "AppSheet Alerts"])
#     else:
#         tab1, tab2 = st.tabs(["Fitbit Alerts", ""])

#     # Tab 1: Fitbit Alerts Configuration
#     with tab1:
#         st.header("Fitbit Alerts Configuration")
        
#         # Add visualization of current configurations
#         fitbit_configs = get_project_fitbit_configs(spreadsheet, user_project)
#         display_fitbit_configs(fitbit_configs)
#         # st.subheader("Reset Fitbits Failures Counters")
#         # fitbit_failures, total_fitbit_df = get_fitbit_failures(spreadsheet, user_project)
        
#         # # Add a reset column for checkboxes
#         # # First check which columns exist in the DataFrame
#         # available_columns = fitbit_failures.columns
        
#         # # Create a list to hold columns we want to select
#         # cols_to_select = []
        
#         # # Add any Total columns if they exist
#         # total_cols = [col for col in available_columns if col.startswith("Total")]
#         # # Add required columns if they exist
#         # for col_name in ["watchName", "lastCheck"]:
#         #     if col_name in available_columns:
#         #         cols_to_select.append(col_name)
#         # cols_to_select.extend(total_cols)
        
        
        
#         # # Select only the columns that exist
#         # if cols_to_select:
#         #     fitbit_failures = fitbit_failures.select(cols_to_select)
        
        
#         # # Create a container to maintain state between rerenders
#         # if "reset_checkboxes" not in st.session_state:
#         #     st.session_state.reset_checkboxes = {}
        
#         # # Pre-process the dataframe to use session state values
#         # if len(st.session_state.reset_checkboxes) > 0:
#         #     # Create a function to check if a row should be marked as reset
#         #     def set_reset_value(row):
#         #         row_id = f"{row['watchName']}_{row.get('lastCheck', '')}"
#         #         return st.session_state.reset_checkboxes.get(row_id, False)
                
#         #     # Apply the function to update the reset column
#         #     fitbit_failures = fitbit_failures.with_columns(
#         #         reset=pl.struct(fitbit_failures.columns).map_elements(set_reset_value)
#         #     )
        
#         # Use the AgGrid with the preprocessed data
#         # edited_df, grid_response = aggrid_polars(fitbit_failures, bool_editable=True, key="fitbit_reset_grid")
        
        


#         # # Create checkbox alternatives - these will respond immediately
#         # with st.expander("Reseting watches history", expanded=True):
#         #     gd = GridOptionsBuilder.from_dataframe(fitbit_failures.to_pandas())
#         #     gd.configure_default_column(editable=True, groupable=True)
#         #     gd.configure_selection(selection_mode="multiple", use_checkbox=True)
#         #     gd = gd.build()
#         #     grid_response = AgGrid(
#         #         fitbit_failures.to_pandas(),
#         #         gridOptions=gd,
#         #         update_mode=GridUpdateMode.SELECTION_CHANGED,
#         #         allow_unsafe_jscode=True,
#         #         height=500,
#         #         theme='fresh')
            
#         #     st.write("If you want to reset the total failures counts of some watches, please select them below.")
        
#         #     # Get unique watches from the data
#         #     unique_watches = fitbit_failures.select('watchName').unique().to_series().to_list()
            
#         #     # Create columns for better layout
#         #     cols = st.columns(3)
            
#         #     # Create a checkbox for each watch
#         #     for i, watch in enumerate(unique_watches):
#         #         col_idx = i % 3
#         #         checkbox_key = f"watch_{watch}_reset"
                
#         #         # Pre-fill state from session state if available
#         #         default = any(v for k, v in st.session_state.reset_checkboxes.items() if k.startswith(f"{watch}_"))
                
#         #         # Create the checkbox
#         #         reset_watch = cols[col_idx].checkbox(f"Reset {watch}", value=default, key=checkbox_key)
                
#         #         # Update session state when checkbox changes
#         #         if reset_watch:
#         #             # Find all rows for this watch
#         #             watch_rows = fitbit_failures.filter(pl.col('watchName') == watch)
                    
#         #             # Update session state for each row
#         #             for row in watch_rows.rows(named=True):
#         #                 row_id = f"{row['watchName']}_{row.get('lastCheck', '')}"
#         #                 st.session_state.reset_checkboxes[row_id] = True
#         #         st.write(st.session_state.reset_checkboxes)
        
#         # # Show the selections based on session state (this will always be accurate)
#         # reset_watches = [k.split('_')[0] for k, v in st.session_state.reset_checkboxes.items() if v]
#         # if reset_watches:
#         #     st.write("Watches marked for reset:", ", ".join(set(reset_watches)))
        
#         # # Reset button now uses session state
#         # if st.button("Reset Fitbit Failures Counters"):
#         #     reset_items = [(k.split('_')[0], k.split('_')[1]) for k, v in st.session_state.reset_checkboxes.items() if v]
            
#         #     if len(reset_items) > 0:
#         #         col_to_reset = [col for col in fitbit_failures.columns if col.startswith('Total')]
#         #         # Reset the counters for the selected items
#         #         for watch_name, last_check in reset_items:
#         #             for column in col_to_reset:
#         #                 total_fitbit_df = total_fitbit_df.with_columns(
#         #                     pl.when((pl.col('watchName') == watch_name) & 
#         #                            (pl.col('lastCheck') == last_check))
#         #                     .then(pl.lit(0))
#         #                     .otherwise(pl.col(column))
#         #                     .alias(column)
#         #                 )
                
#         #         # Update the sheet with the new configuration
#         #         spreadsheet.update_sheet("FitbitLog", total_fitbit_df, strategy="replace")
#         #         GoogleSheetsAdapter.save(spreadsheet, "FitbitLog")
#         #         st.success(f"Reset {len(reset_items)} fitbit failure counters successfully!")
                
#         #         # Clear the session state after successful reset
#         #         st.session_state.reset_checkboxes = {}
                
#         #         # Force a rerun to refresh the grid with updated data
#         #         st.rerun()
#         #     else:
#         #         st.warning("No watches selected for reset. Please check the boxes in the 'reset' column or use the toggles.")
#         st.markdown("---")
#         st.subheader("Create/Edit Configuration")
        
#         # Get current configuration
#         fitbit_config, watch_names = get_user_fitbit_config(spreadsheet, user_email, user_project)
        
#         # Create form for editing configuration
#         with st.form("fitbit_config_form"):
#             project = st.text_input("Project", value=fitbit_config.get('project', ''))
            
#             st.subheader("Sync Thresholds")
#             current_sync_thr = st.number_input("Current Failed Sync Threshold", 
#                                              min_value=1, max_value=100, 
#                                              value=int(fitbit_config.get('currentSyncThr', 3)))
#             total_sync_thr = st.number_input("Total Failed Sync Threshold", 
#                                            min_value=1, max_value=1000, 
#                                            value=int(fitbit_config.get('totalSyncThr', 10)))
            
#             st.subheader("Heart Rate Thresholds")
#             current_hr_thr = st.number_input("Current Failed HR Threshold", 
#                                            min_value=1, max_value=100, 
#                                            value=int(fitbit_config.get('currentHrThr', 3)))
#             total_hr_thr = st.number_input("Total Failed HR Threshold", 
#                                          min_value=1, max_value=1000, 
#                                          value=int(fitbit_config.get('totalHrThr', 10)))
            
#             st.subheader("Sleep Thresholds")
#             current_sleep_thr = st.number_input("Current Failed Sleep Threshold", 
#                                               min_value=1, max_value=100, 
#                                               value=int(fitbit_config.get('currentSleepThr', 3)))
#             total_sleep_thr = st.number_input("Total Failed Sleep Threshold", 
#                                             min_value=1, max_value=1000, 
#                                             value=int(fitbit_config.get('totalSleepThr', 10)))
            
#             st.subheader("Steps Thresholds")
#             current_steps_thr = st.number_input("Current Failed Steps Threshold", 
#                                               min_value=1, max_value=100, 
#                                               value=int(fitbit_config.get('currentStepsThr', 3)))
#             total_steps_thr = st.number_input("Total Failed Steps Threshold", 
#                                             min_value=1, max_value=1000, 
#                                             value=int(fitbit_config.get('totalStepsThr', 10)))
            
#             st.subheader("Battery Threshold")
#             battery_thr = st.number_input("Battery Level Threshold (%)", 
#                                         min_value=5, max_value=50, 
#                                         value=int(fitbit_config.get('batteryThr', 20)))
            
#             st.subheader("Recipient Email")
#             st.write("This email will receive alerts when the thresholds are exceeded.")
#             recipient_email = st.text_input("Recipient Email", value=user_email)

#             st.subheader("(OPTIONAL) Watch Name")
#             st.write("Select the specific watch name for which you want to set the alerts config.")
#             watch_name = st.selectbox("Watch Name", options=["All the project."] + watch_names , index=0)

#             st.subheader("End Date")
#             st.write("This date will be used to stop the alerts.")
#             end_date = st.date_input("End Date", value=date.today() + timedelta(days=30))
            
#             save_button = st.form_submit_button("Save Configuration")
            
#             if save_button:
#                 # Prepare data for saving
#                 config_data = {
#                     'project': project,
#                     'currentSyncThr': current_sync_thr,
#                     'totalSyncThr': total_sync_thr,
#                     'currentHrThr': current_hr_thr,
#                     'totalHrThr': total_hr_thr,
#                     'currentSleepThr': current_sleep_thr,
#                     'totalSleepThr': total_sleep_thr,
#                     'currentStepsThr': current_steps_thr,
#                     'totalStepsThr': total_steps_thr,
#                     'batteryThr': battery_thr,
#                     'manager': user_email,
#                     'email': recipient_email,
#                     'watch': watch_name if watch_name != "All the project." else '',
#                     'endDate': end_date.strftime("%Y-%m-%d")
#                 }
                
#                 # Save the configuration
#                 if save_fitbit_config(spreadsheet, config_data):
#                     st.success("Fitbit alerts configuration saved successfully!")
#                 else:
#                     st.error("Failed to save configuration. Please try again.")
    
#     # Tab 2: Qualtrics Alerts Configuration
#     with tab2:
#         if user_role == 'Admin':
#             user_project = st.selectbox("Select Project", ["fibro", "nova"])

#         if user_project == 'fibro':
#             appsheet_config(spreadsheet, user_email)
#         elif user_project == 'nova':
#             # Display AppSheet configuration
            
            
#             st.header("Qualtrics Alerts Configuration")
            
#             # Add visualization of current configurations
#             qualtrics_configs = get_project_qualtrics_configs(spreadsheet, user_project)
#             display_qualtrics_configs(qualtrics_configs)
            
#             st.markdown("---")
#             st.subheader("Create/Edit Configuration")
            
#             # Get current configuration
#             qualtrics_config = get_user_qualtrics_config(spreadsheet, user_email)
            
#             # Create form for editing configuration
#             with st.form("qualtrics_config_form"):
#                 project = st.text_input("Project", value=qualtrics_config.get('project', ''))
                
#                 hours_thr = st.number_input("Hours Threshold for Late Responses", 
#                                         min_value=1, max_value=168,  # 1 hour to 1 week
#                                         value=int(qualtrics_config.get('hoursThr', 48)))
                
#                 save_button = st.form_submit_button("Save Configuration")
                
#                 if save_button:
#                     # Prepare data for saving
#                     config_data = {
#                         'hoursThr': hours_thr,
#                         'project': project,
#                         'manager': user_email
#                     }
                    
#                     # Save the configuration
#                     if save_qualtrics_config(spreadsheet, config_data):
#                         st.success("Qualtrics alerts configuration saved successfully!")
#                     else:
#                         st.error("Failed to save configuration. Please try again.")
#         else:
#             st.warning("You don't have permission to access this page.")
# # # Run the page when this file is executed
# # if __name__ == "__main__":
# #     alerts_config_page(
