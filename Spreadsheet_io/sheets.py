import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import polars as pl
import datetime
import os

from model.config import get_secrets

class Spreadsheet:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Spreadsheet, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if hasattr(self, '_initialized') and getattr(self, '_initialized', False):
            return
            
        self._initialized = True
        self.client = self._get_client()
        self.spreadsheet = self._get_spreadsheet()
    
    @staticmethod
    @st.cache_resource
    def _get_client():
        secrets = get_secrets()

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/drive.file"
        ]
        
        credentials = Credentials.from_service_account_info(
            secrets["gcp_service_account"], scopes=scopes
        )
        
        return gspread.authorize(credentials)
    
    def _get_spreadsheet(self):
        spreadsheet_key = st.secrets["spreadsheet_key"]
        return self.client.open_by_key(spreadsheet_key)
    
    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls()
        return cls._instance
    
    @classmethod
    def get_user_details(cls):
        instance = cls.get_instance()
        return instance.spreadsheet.get_worksheet(0).get_all_records()
    
    @classmethod
    def get_project_details(cls):
        instance = cls.get_instance()
        return instance.spreadsheet.get_worksheet(1).get_all_records()

    @classmethod
    def get_spreadsheet(cls):
        instance = cls.get_instance()
        return instance.spreadsheet
    
    # @classmethod
    # def get

    # Add more methods for specific operations as needed
    # For example:
    @classmethod
    def update_user(cls, user_data, row_index):
        instance = cls.get_instance()
        worksheet = instance.spreadsheet.get_worksheet(0)
        # Implementation for updating user data
        
    @classmethod
    def add_user(cls, user_data):
        instance = cls.get_instance()
        worksheet = instance.spreadsheet.get_worksheet(0)
        # Implementation for adding a new user

    @classmethod
    def get_fitbits_details(cls):
        instance = cls.get_instance()
        return instance.spreadsheet.get_worksheet(2).get_all_records()

    @classmethod
    def get_fitbits_log(cls):
        instance = cls.get_instance()
        return instance.spreadsheet.get_worksheet(3).get_all_values()

    @classmethod
    def append_fitbits_log(cls, fitbits_data):
        instance = cls.get_instance()
        worksheet = instance.spreadsheet.get_worksheet(3)
        
        new_row = [
            fitbits_data["project"],
            fitbits_data["watchName"],
            fitbits_data["lastSynced"],
            fitbits_data["lastHR"],
            fitbits_data["lastHRVal"],
            fitbits_data["longestHRSeq"],
            fitbits_data["startActiveDate"],
            fitbits_data["isActive"],
            fitbits_data["endActiveDate"],
            fitbits_data["LastSleepStartDateTime"],
            fitbits_data["LastSleepEndDateTime"],
            fitbits_data["LastStepsMean"],
            fitbits_data["CurrentFailedSync"],
            fitbits_data["TotalFailedSync"],
            fitbits_data["CurrentFailedHR"],
            fitbits_data["TotalFailedHR"],
            fitbits_data["CurrentFailedSleep"],
            fitbits_data["TotalFailedSleep"],
            fitbits_data["CurrentFailedSteps"],
            fitbits_data["TotalFailedSteps"]
        ]
    
    def update_worksheet_3(self, data_list: list) -> None:
        """
        Updates worksheet 3 with the latest Fitbit data.
        Replaces all content in the worksheet with the new data.
        
        Args:
            data_list (list): List of dictionaries containing the watch data.
        """
        # Get the worksheet by index (4th worksheet)
        worksheet = self.spreadsheet.get_worksheet(3)  # 0-indexed, so 3 is the 4th worksheet
        
        if not data_list:
            print("No data to update in worksheet 3")
            return
            
        # Define the expected column order based on the sheet structure
        expected_columns = [
            "project", "watchName", "lastCheck", "lastSynced", "lastBattary", 
            "lastHR", "lastSleepStartDateTime", "lastSleepEndDateTime", "lastSteps", 
            "lastBattaryVal", "lastHRVal", "lastHRSeq", "lastSleepDur", "lastStepsVal",
            "CurrentFailedSync", "TotalFailedSync", "CurrentFailedHR", "TotalFailedHR",
            "CurrentFailedSleep", "TotalFailedSleep", "CurrentFailedSteps", "TotalFailedSteps", "ID"
        ]
            
        # Clear the current content
        worksheet.clear()
        
        # Add headers as the first row
        worksheet.append_row(expected_columns)
        
        # Add data rows
        for item in data_list:
            # Map data to expected columns format
            row_data = []
            for col in expected_columns:
                row_data.append(str(item.get(col, '')))
            
            worksheet.append_row(row_data)
        
        print(f"Updated worksheet 3 with {len(data_list)} records")


class fitbitLog:
    def __init__(self, project, watchName, lastSynced, lastHR, lastHRVal, longestHRSeq, startActiveDate, isActive,
                 endActiveDate, LastSleepStartDateTime, LastSleepEndDateTime, LastStepsMean,
                 CurrentFailedSync=0, TotalFailedSync=0,
                 CurrentFailedHR=0, TotalFailedHR=0,
                 CurrentFailedSleep=0, TotalFailedSleep=0,
                 CurrentFailedSteps=0, TotalFailedSteps=0):
        self.project = project
        self.watchName = watchName
        self.lastSynced = lastSynced
        self.lastHR = lastHR
        self.lastHRVal = lastHRVal
        self.longestHRSeq = longestHRSeq
        self.startActiveDate = startActiveDate
        self.isActive = isActive
        self.endActiveDate = endActiveDate
        self.LastSleepStartDateTime = LastSleepStartDateTime
        self.LastSleepEndDateTime = LastSleepEndDateTime
        self.LastStepsMean = LastStepsMean
        self.CurrentFailedSync = CurrentFailedSync
        self.TotalFailedSync = TotalFailedSync
        self.CurrentFailedHR = CurrentFailedHR
        self.TotalFailedHR = TotalFailedHR
        self.CurrentFailedSleep = CurrentFailedSleep
        self.TotalFailedSleep = TotalFailedSleep
        self.CurrentFailedSteps = CurrentFailedSteps
        self.TotalFailedSteps = TotalFailedSteps

    def __str__(self):
        return f"Fitbit Log: {self.project}, {self.watchName}, {self.lastSynced}, {self.lastHR}, {self.lastHRVal}, {self.longestHRSeq}, {self.startActiveDate}, {self.isActive}, {self.endActiveDate}, {self.LastSleepStartDateTime}, {self.LastSleepEndDateTime}, {self.LastStepsMean}"
    def __repr__(self):
        return f"Fitbit Log: {self.project}, {self.watchName}, {self.lastSynced}, {self.lastHR}, {self.lastHRVal}, {self.longestHRSeq}, {self.startActiveDate}, {self.isActive}, {self.endActiveDate}, {self.LastSleepStartDateTime}, {self.LastSleepEndDateTime}, {self.LastStepsMean}"
    def __eq__(self, value):
        if isinstance(value, fitbitLog):
            if self.project == value.project and self.watchName == value.watchName:
                return True
            else:
                return False
        else:
            return False
    def __ne__(self, value):
        if isinstance(value, fitbitLog):
            if self.project != value.project or self.watchName != value.watchName:
                return True
            else:
                return False
        else:
            return True
    def __hash__(self):
        return hash((self.project, self.watchName))
    def __len__(self):
        return len(self.__dict__)
    def __getitem__(self, key):
        return self.__dict__.get(key)
    


class serverLogFile:
    def __init__(self, project=None, watchName=None, syncedDateTime=None, battary=None, lastHR=None,
                 lastSteps=None, lastSleepStartDateTime=None, lastSleepEndDateTime=None, 
                 lastSleepDuration=None):
        """Initialize serverLogFile with optional parameters."""
        self.path = st.secrets.get("fitbit_log_path", "fitbit_log.csv")

    def __str__(self):
        return f"Server Log File: {self.path}"
        
    def __repr__(self):
        return f"Server Log File: {self.path}"
        
    def __eq__(self, value):
        if isinstance(value, serverLogFile):
            if self.path == value.path:
                return True
            else:
                return False
        else:
            return False
    def __ne__(self, value):
        if isinstance(value, serverLogFile):
            if self.path != value.path:
                return True
            else:
                return False
        else:
            return True
    def __hash__(self):
        return hash((self.path))
    def __len__(self):
        return len(self.__dict__)
    def __getitem__(self, key):
        return self.__dict__.get(key)
    def __setitem__(self, key, value):
        self.__dict__[key] = value
    def __delitem__(self, key):
        if key in self.__dict__:
            del self.__dict__[key]
        else:
            raise KeyError(f"Key '{key}' not found in the dictionary.")
    def __contains__(self, key):
        return key in self.__dict__
    def __iter__(self):
        return iter(self.__dict__)
    
    def get_path(self):
        return self.path
    
    def get_all(self):
        return self.__dict__
    def get_all_values(self):
        return list(self.__dict__.values())
    def get_all_keys(self):
        return list(self.__dict__.keys())
    def get_all_items(self):
        return list(self.__dict__.items())
    def get_all_values_as_string(self):
        return [str(value) for value in self.__dict__.values()]
    def get_all_keys_as_string(self):
        return [str(key) for key in self.__dict__.keys()]
    
    def update_fitbits_log(self, fitbit_data: pl.DataFrame) -> None:
        """
        Updates the Fitbit log file with new data.
        
        Args:
            fitbit_data (pl.DataFrame): DataFrame containing the watch data.
        """
        # Create directory if it doesn't exist
        log_dir = os.path.dirname(self.path)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        # Get current time for timestamp
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Define expected columns for the CSV
        expected_columns = [
            "project", "watchName", "lastCheck", "lastSynced", "lastBattary", 
            "lastHR", "lastSleepStartDateTime", "lastSleepEndDateTime", "lastSteps", 
            "lastBattaryVal", "lastHRVal", "lastHRSeq", "lastSleepDur", "lastStepsVal",
            "CurrentFailedSync", "TotalFailedSync", "CurrentFailedHR", "TotalFailedHR",
            "CurrentFailedSleep", "TotalFailedSleep", "CurrentFailedSteps", "TotalFailedSteps", "ID"
        ]
        
        # Initialize or load existing data
        if os.path.exists(self.path):
            try:
                df = pl.read_csv(self.path)
                # Ensure all columns exist
                for col in expected_columns:
                    if col not in df.columns:
                        df = df.with_columns(pl.lit(0).alias(col))
            except Exception as e:
                print(f"Error reading CSV file: {e}")
                df = pl.DataFrame({col: [] for col in expected_columns})
        else:
            df = pl.DataFrame({col: [] for col in expected_columns})
        
        # Process each row from the Fitbit data
        updated_data = []
        
        for row in fitbit_data.iter_rows(named=True):
            # Create watch ID for matching
            watch_id = f"{row.get('project', '')}-{row.get('name', '')}"
            
            # Check if watch is active
            is_active = str(row.get('isActive', '')).upper() != 'FALSE'
            if not is_active:
                # Skip processing inactive watches
                continue
                
            # Try to find existing data for this watch
            existing_row = None
            if not df.is_empty():
                filtered = df.filter(pl.col("ID") == watch_id)
                if not filtered.is_empty():
                    existing_row = filtered.row(0)
            
            # Get current failure counters or use defaults
            if existing_row:
                curr_failed_sync = int(existing_row[df.columns.index("CurrentFailedSync")] or 0)
                total_failed_sync = int(existing_row[df.columns.index("TotalFailedSync")] or 0)
                curr_failed_hr = int(existing_row[df.columns.index("CurrentFailedHR")] or 0)
                total_failed_hr = int(existing_row[df.columns.index("TotalFailedHR")] or 0)
                curr_failed_sleep = int(existing_row[df.columns.index("CurrentFailedSleep")] or 0)
                total_failed_sleep = int(existing_row[df.columns.index("TotalFailedSleep")] or 0)
                curr_failed_steps = int(existing_row[df.columns.index("CurrentFailedSteps")] or 0)
                total_failed_steps = int(existing_row[df.columns.index("TotalFailedSteps")] or 0)
            else:
                # Default values for new watches
                curr_failed_sync = 0
                total_failed_sync = 0
                curr_failed_hr = 0
                total_failed_hr = 0
                curr_failed_sleep = 0
                total_failed_sleep = 0
                curr_failed_steps = 0
                total_failed_steps = 0
            
            # Update failure counters based on data availability
            
            # Sync - consider failed if no sync date
            if not row.get("syncDate"):
                curr_failed_sync += 1
                total_failed_sync += 1
            else:
                curr_failed_sync = 0  # Reset current failures on success
            
            # Heart Rate - consider failed if HR is missing or empty
            if not row.get("HR"):
                curr_failed_hr += 1
                total_failed_hr += 1
            else:
                curr_failed_hr = 0  # Reset current failures on success
            
            # Sleep - consider failed if both sleep start and end times are missing
            if not row.get("sleep_start") or not row.get("sleep_end"):
                curr_failed_sleep += 1
                total_failed_sleep += 1
            else:
                curr_failed_sleep = 0  # Reset current failures on success
                
            # Steps - consider failed if steps data is missing
            if not row.get("steps"):
                curr_failed_steps += 1
                total_failed_steps += 1
            else:
                curr_failed_steps = 0  # Reset current failures on success
            
            # Map the data to the expected columns
            new_row = {
                "project": row.get("project", ""),
                "watchName": row.get("name", ""),
                "lastCheck": now,
                "lastSynced": row.get("syncDate", ""),
                "lastBattary": now if row.get("battery") else "",
                "lastHR": now if row.get("HR") else "",
                "lastSleepStartDateTime": row.get("sleep_start", ""),
                "lastSleepEndDateTime": row.get("sleep_end", ""),
                "lastSteps": now if row.get("steps") else "",
                "lastBattaryVal": row.get("battery", ""),
                "lastHRVal": row.get("HR", ""),
                "lastHRSeq": "",  # This needs to be calculated or determined elsewhere
                "lastSleepDur": row.get("sleep_duration", ""),
                "lastStepsVal": row.get("steps", ""),
                "CurrentFailedSync": curr_failed_sync,
                "TotalFailedSync": total_failed_sync,
                "CurrentFailedHR": curr_failed_hr,
                "TotalFailedHR": total_failed_hr,
                "CurrentFailedSleep": curr_failed_sleep,
                "TotalFailedSleep": total_failed_sleep,
                "CurrentFailedSteps": curr_failed_steps,
                "TotalFailedSteps": total_failed_steps,
                "ID": watch_id
            }
            
            updated_data.append(new_row)
        
        # Update the DataFrame with new data
        if updated_data:
            # Convert to DataFrame
            updated_df = pl.DataFrame(updated_data)
            
            # Merge with existing data - replace rows for watches in the update
            if not df.is_empty():
                # Get watch IDs from the updated data
                updated_ids = updated_df["ID"].to_list()
                
                # Filter out rows for watches that are in the update
                df_filtered = df.filter(~pl.col("ID").is_in(updated_ids))
                
                # Combine with updated data
                if not df_filtered.is_empty():
                    final_df = pl.concat([df_filtered, updated_df], how="vertical")
                else:
                    final_df = updated_df
            else:
                final_df = updated_df
            
            # Write to CSV
            final_df.write_csv(self.path)
            print(f"Updated log file with {len(updated_data)} records")
        else:
            print("No active watch data to update in log file")





