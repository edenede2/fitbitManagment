import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import polars as pl
import datetime
import os


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
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/drive.file"
        ]
        
        credentials = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"], scopes=scopes
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
        # return instance.spreadsheet.get_worksheet(3).get_all_records()
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
        # Get the worksheet by index (3rd worksheet)
        worksheet = self.spreadsheet.get_worksheet(2)  # 0-indexed, so 2 is the 3rd worksheet
        
        if not data_list:
            print("No data to update in worksheet 3")
            return
            
        # Clear the current content
        worksheet.clear()
        
        # Get the column headers from the first dictionary
        headers = list(data_list[0].keys())
        
        # Prepare the data rows
        rows = [headers]  # First row is headers
        for item in data_list:
            row = [item.get(header, '') for header in headers]
            rows.append(row)
        
        # Update the worksheet with all data at once
        worksheet.update(rows)
        
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
    def __init__(self, project, watchName, syncedDateTime, battary, lastHR,
                 lastSteps, lastSleepStartDateTime, lastSleepEndDateTime, 
                 lastSleepDuration):
        # self.project = project
        # self.watchName = watchName
        # self.syncedDateTime = syncedDateTime
        # self.battary = battary
        # self.lastHR = lastHR
        # self.lastSteps = lastSteps
        # self.lastSleepStartDateTime = lastSleepStartDateTime
        # self.lastSleepEndDateTime = lastSleepEndDateTime
        # self.lastSteps = lastSteps
        # self.lastSleepDuration = lastSleepDuration
        self.path = st.secrets["fitbit_log_path"]

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
    
    # def get_project(self):
    #     return self.project
    # def get_watchName(self):
    #     return self.watchName
    # def get_syncedDateTime(self):
    #     return self.syncedDateTime
    # def get_battary(self):
    #     return self.battary
    # def get_lastHR(self):
    #     return self.lastHR
    # def get_lastSteps(self):
    #     return self.lastSteps
    # def get_lastSleepStartDateTime(self):
    #     return self.lastSleepStartDateTime
    # def get_lastSleepEndDateTime(self):
    #     return self.lastSleepEndDateTime
    # def get_lastSleepDuration(self):
    #     return self.lastSleepDuration
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
    
    # def update_fitbits_log(self, fitbit_data: pl.DataFrame) -> None:
    #     # Update the log with the new data
    #     if os.path.exists(self.path):
    #         # If the file exists, read it
    #         df = pl.read_csv(self.path)
    #     else:
    #         # If the file doesn't exist, create a new DataFrame
    #         df = pl.DataFrame({
    #             "project": [],
    #             "watchName": [],
    #             "syncedDateTime": [],
    #             "battary": [],
    #             "lastHR": [],
    #             "lastSteps": [],
    #             "lastSleepStartDateTime": [],
    #             "lastSleepEndDateTime": [],
    #             "lastSleepDuration": [],
    #             "lastHRDateTime": [],
    #             "lastStepsDateTime": [],
    #             "lastSleepDateTime": [],
    #             "isActive": [],
    #             "currentFailedSync": [],
    #             "totalFailedSync": [],
    #             "currentFailedHR": [],
    #             "totalFailedHR": [],
    #             "currentFailedSleep": [],
    #             "totalFailedSleep": [],
    #             "currentFailedSteps": [],
    #             "totalFailedSteps": []
    #         })

    #     for row in fitbit_data.iter_rows(named=True):
    #         if row["isActive"] == "FALSE":
    #             continue
    #         current_failed_sync = df.filter(pl.col("project") == row["project"]).filter(pl.col("watchName") == row["name"]).select(pl.col("CurrentFailedSync")).to_numpy()[0][0]
    #         new_row = {
    #             "project": row["project"],
    #             "watchName": row["name"],
    #             "syncedDateTime": row["syncDate"],
    #             "battary": row["battery"],
    #             "lastHR": row["HR"],
    #             "lastSteps": row["steps"],
    #             "lastSleepStartDateTime": row["sleep_start"],
    #             "lastSleepEndDateTime": row["sleep_end"],
    #             "lastSleepDuration": row["sleep_duration"],
    #             "lastHRDateTime": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") if row["HR"] else df["lastHRDateTime"].max(),
    #             "lastStepsDateTime": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") if row["steps"] else df["lastStepsDateTime"].max(),
    #             "lastSleepDateTime": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") if row["sleep_start"] else df["lastSleepDateTime"].max(),
    #             "isActive": row["isActive"],
    #             "curr

    #         }
    #         df = df.vstack(pl.DataFrame(new_row))

    #     df.write_csv(self.path)
        # Append the new data to the log file





