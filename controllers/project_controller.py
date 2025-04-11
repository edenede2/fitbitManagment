from typing import Dict, List, Optional
import pandas as pd
import streamlit as st
from entity.Sheet import Spreadsheet, GoogleSheetsAdapter
from entity.Watch import WatchFactory

class ProjectController:
    """Controller for project-related operations"""
    
    def __init__(self):
        """Initialize the project controller"""
        self.spreadsheet_key = st.secrets.get("spreadsheet_key", "")
        
    def get_all_projects(self) -> pd.DataFrame:
        """Get all projects from the spreadsheet"""
        try:
            # Create Spreadsheet instance
            spreadsheet = Spreadsheet(name="Fitbit Database", api_key=self.spreadsheet_key)
            GoogleSheetsAdapter.connect(spreadsheet)
            
            # Get project sheet
            project_sheet = spreadsheet.get_sheet("project", sheet_type="project")
            return project_sheet.to_dataframe()
        except Exception as e:
            print(f"Error getting projects: {e}")
            return pd.DataFrame()
    
    def get_project_by_name(self, name: str) -> Optional[Dict]:
        """Get a project by name"""
        projects_df = self.get_all_projects()
        if projects_df.empty:
            return None
            
        project_data = projects_df[projects_df['name'] == name]
        if len(project_data) > 0:
            return project_data.iloc[0].to_dict()
        return None
    
    def get_watches_for_project(self, project_name: str) -> pd.DataFrame:
        """Get watches for a specific project"""
        try:
            # Create Spreadsheet instance
            spreadsheet = Spreadsheet(name="Fitbit Database", api_key=self.spreadsheet_key)
            GoogleSheetsAdapter.connect(spreadsheet)
            
            # Get fitbit sheet
            fitbit_sheet = spreadsheet.get_sheet("fitbit", sheet_type="fitbit")
            fitbit_df = fitbit_sheet.to_dataframe()
            
            if project_name == "Admin":
                return fitbit_df
            else:
                # Filter for this project
                return fitbit_df[fitbit_df['project'] == project_name]
        except Exception as e:
            print(f"Error getting watches for project {project_name}: {e}")
            return pd.DataFrame()
    
    def get_watch_details(self, watch_name: str) -> Optional[Dict]:
        """Get detailed information about a specific watch"""
        try:
            # Create Spreadsheet instance
            spreadsheet = Spreadsheet(name="Fitbit Database", api_key=self.spreadsheet_key)
            GoogleSheetsAdapter.connect(spreadsheet)
            
            # Get fitbit sheet
            fitbit_sheet = spreadsheet.get_sheet("fitbit", sheet_type="fitbit")
            fitbit_df = fitbit_sheet.to_dataframe()
            
            # Get this watch's details
            watch_details = fitbit_df[fitbit_df['name'] == watch_name]
            
            # st.write(f"Watch details: {watch_details}")
            st.write(f"Watch details: {watch_details}")
            if len(watch_details) > 0:
                # Convert to dict for first row
                details = watch_details.iloc[0].to_dict()
                
                # Also get the latest log data
                log_sheet = spreadsheet.get_sheet("FitbitLog", sheet_type="log")
                log_df = log_sheet.to_dataframe()
                
                # Filter to this watch and get the most recent entry
                watch_logs = log_df[log_df['watchName'] == watch_name]
                if len(watch_logs) > 0:
                    # Sort by lastCheck (newest first) and get the first row
                    watch_logs = watch_logs.sort_values(by='lastCheck', ascending=False)
                    latest_log = watch_logs.iloc[0].to_dict()
                    
                    # Merge the details
                    details.update({
                        'lastSynced': latest_log.get('lastSynced', ''),
                        'lastBatteryLevel': latest_log.get('lastBattaryVal', ''),
                        'lastHeartRate': latest_log.get('lastHRVal', ''),
                        'lastSteps': latest_log.get('lastStepsVal', ''),
                        'lastSleepStart': latest_log.get('lastSleepStartDateTime', ''),
                        'lastSleepEnd': latest_log.get('lastSleepEndDateTime', ''),
                        'lastSleepDuration': latest_log.get('lastSleepDur', '')
                    })
                
                return details
            return None
        except Exception as e:
            print(f"Error getting details for watch {watch_name}: {e}")
            return None
            
    def get_watches_for_student(self, student_email: str) -> pd.DataFrame:
        """Get watches assigned to a specific student"""
        try:
            # Create Spreadsheet instance
            spreadsheet = Spreadsheet(name="Fitbit Database", api_key=self.spreadsheet_key)
            GoogleSheetsAdapter.connect(spreadsheet)
            
            # Get studentWatch sheet
            student_watch_sheet = spreadsheet.get_sheet("studentWatch", sheet_type="generic")
            student_watch_df = student_watch_sheet.to_dataframe()
            
            # Filter for this student
            student_watches = student_watch_df[student_watch_df['email'] == student_email]
            
            if student_watches.empty:
                return pd.DataFrame()
                
            # Get the watch names
            watch_names = student_watches['watch'].tolist()
            
            # Get full watch details from fitbit sheet
            fitbit_sheet = spreadsheet.get_sheet("fitbit", sheet_type="fitbit")
            fitbit_df = fitbit_sheet.to_dataframe()
            
            # Filter for these watches
            return fitbit_df[fitbit_df['name'].isin(watch_names)]
        except Exception as e:
            print(f"Error getting watches for student {student_email}: {e}")
            return pd.DataFrame()
