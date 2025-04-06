from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any, Callable, Type, Union, Set
from collections import defaultdict
import pandas as pd
import polars as pl
from abc import ABC, abstractmethod
import datetime
import os
import gspread
from google.oauth2.service_account import Credentials
import uuid
import streamlit as st
from entity.Watch import Watch, WatchFactory  # Remove FitbitAPI as it doesn't exist
import traceback  # Add import for traceback

# Import needed functions from model
try:
    from model.config import get_secrets
except ImportError:
    # Fallback function if model.config is not available
    def get_secrets():
        try:
            return st.secrets
        except:
            # Return empty dict if neither is available
            return {"gcp_service_account": {}, "spreadsheet_key": ""}

# =====================================================================
# ==================== ENTITY LAYER SHEET CLASSES =====================
# =====================================================================

@dataclass
class Sheet:
    """
    Sheet entity class.

    Attributes:
        name (str): The name of the sheet.
        data (dict): The data associated with the sheet.
    """
    name: str
    data: dict = field(default_factory=dict)
    
    def to_dataframe(self, engine: str = 'pandas') -> Union[pd.DataFrame, pl.DataFrame]:
        """Convert sheet data to a dataframe"""
        if engine == 'pandas':
            return pd.DataFrame(self.data)
        elif engine == 'polars':
            return pl.DataFrame(self.data)
        else:
            raise ValueError(f"Unsupported dataframe engine: {engine}")
    
    def from_dataframe(self, df: Union[pd.DataFrame, pl.DataFrame]) -> None:
        """Update sheet data from a dataframe"""
        if isinstance(df, pd.DataFrame):
            self.data = df.to_dict(orient='records')
        elif isinstance(df, pl.DataFrame):
            self.data = df.to_dicts()
        else:
            raise ValueError(f"Unsupported dataframe type: {type(df)}")


# Strategy pattern for different update operations
class UpdateStrategy(ABC):
    @abstractmethod
    def update(self, sheet: Sheet, new_data: dict) -> None:
        pass

class AppendStrategy(UpdateStrategy):
    def update(self, sheet: Sheet, new_data: dict) -> None:
        if isinstance(sheet.data, list):
            sheet.data.append(new_data)
        else:
            sheet.data = [sheet.data, new_data]

class ReplaceStrategy(UpdateStrategy):
    def update(self, sheet: Sheet, new_data: dict) -> None:
        sheet.data = new_data

class MergeStrategy(UpdateStrategy):
    def update(self, sheet: Sheet, new_data: dict) -> None:
        if isinstance(sheet.data, dict) and isinstance(new_data, dict):
            sheet.data.update(new_data)
        else:
            raise ValueError("Both current and new data must be dictionaries for merge strategy")


# Schema validator for sheets
class SheetSchema:
    def __init__(self, columns: List[str], required_columns: List[str] = None):
        self.columns = columns
        self.required_columns = required_columns or []
    
    def validate(self, data: dict) -> bool:
        """Validate data against schema"""
        if isinstance(data, list) and data:
            # Check first item if it's a list of records
            first_item = data[0]
            return all(col in first_item for col in self.required_columns)
        elif isinstance(data, dict):
            return all(col in data for col in self.required_columns)
        return False


# Specialized sheet types using factory pattern
class SheetFactory:
    @staticmethod
    def create_sheet(sheet_type: str, name: str, **kwargs) -> Sheet:
        sheet_types = {
            'user': UserSheet,
            'project': ProjectSheet,
            'fitbit': FitbitSheet,
            'log': LogSheet,
            'bulldog': BulldogSheet,
            'qualtrics_nova': QualtricsNovaSheet,
            'fitbit_alerts_config': FitbitAlertsConfig,
            'qualtrics_alert_config': QualtricsAlertConfig,
            'late_nums': LateNums,
            'suspicious_nums': SuspiciousNums,
            'student_fitbit': FitbitStudent,
            'chats': ChatsSheet,
            'generic': Sheet
        }
        
        if sheet_type not in sheet_types:
            raise ValueError(f"Unknown sheet type: {sheet_type}")
        
        return sheet_types[sheet_type](name=name, **kwargs)


@dataclass
class UserSheet(Sheet):
    """Sheet for storing user data"""
    schema: SheetSchema = field(default_factory=lambda: SheetSchema(
        columns=['id', 'name', 'email', 'last_login','role', 'projects'],
        required_columns=['name', 'role']
    ))


@dataclass
class ProjectSheet(Sheet):
    """Sheet for storing project data"""
    schema: SheetSchema = field(default_factory=lambda: SheetSchema(
        columns=['id', 'name'],
        required_columns=['name']
    ))


@dataclass
class FitbitSheet(Sheet):
    """Sheet for storing Fitbit device data"""
    schema: SheetSchema = field(default_factory=lambda: SheetSchema(
        columns=['project', 'name', 'token', 'user','isActive','currentStudent'],
        required_columns=['project', 'name']
    ))


@dataclass
class LogSheet(Sheet):
    """Sheet for storing log data"""
    schema: SheetSchema = field(default_factory=lambda: SheetSchema(
        columns=['projec', 'watchName',	'lastCheck', 'lastSynced',
                'lastBattary', 'lastHR', 'lastSleepStartDateTime',	'lastSleepEndDateTime',
                'lastSteps','lastBattaryVal','lastHRVal', 'lastHRSeq',
                'lastSleepDur',	'lastStepsVal',	'CurrentFailedSync', 'TotalFailedSync'
                'CurrentFailedHR',	'TotalFailedHR', 'CurrentFailedSleep', 'TotalFailedSleep',
                'CurrentFailedSteps', 'TotalFailedSteps','CurrentFailedBattary', 'TotalFailedBattary',
                  'ID'],
        required_columns=['timestamp', 'event']
    ))


@dataclass
class BulldogSheet(Sheet):
    """Sheet for storing bulldog data"""
    schema: SheetSchema = field(default_factory=lambda: SheetSchema(
        columns=['שם',	'נייד',	'קטגוריה ( לא חובה )',	'סטטוס שליחה',	'זמן שליחה'],
        required_columns=['שם', 'נייד']
    ))

@dataclass
class QualtricsNovaSheet(Sheet):
    """Sheet for storing Qualtrics Nova data"""
    schema: SheetSchema = field(default_factory=lambda: SheetSchema(
        columns=['num','currentDate','startDate','endDate','status','finished'],
        required_columns=['num', 'currentDate']
    ))

@dataclass
class FitbitAlertsConfig(Sheet):
    """Sheet for storing Fitbit alerts configuration"""
    schema: SheetSchema = field(default_factory=lambda: SheetSchema(
        columns=['project','currentSyncThr', 'totalSyncThr', 'currentHrThr', 'totalHrThr',
                'currentSleepThr', 'totalSleepThr', 'currentStepsThr', 'totalStepsThr', 'batteryThr',
                'manager'],
        required_columns=['project', 'watchName']
    ))

@dataclass
class QualtricsAlertConfig(Sheet):
    """Sheet for storing Qualtrics alerts configuration"""
    schema: SheetSchema = field(default_factory=lambda: SheetSchema(
        columns=['hoursThr', 'project', 'manager'],
        required_columns=['hoursThr', 'project']
    ))

@dataclass
class LateNums(Sheet):
    """Sheet for storing late numbers"""
    schema: SheetSchema = field(default_factory=lambda: SheetSchema(
        columns=['nums', 'sentTime', 'hoursLate', 'lastUpdated'],
        required_columns=['nums', 'sentTime']
    ))

@dataclass
class SuspiciousNums(Sheet):
    """Sheet for storing suspicious numbers"""
    schema: SheetSchema = field(default_factory=lambda: SheetSchema(
        columns=['nums', 'filledTime', 'lastUpdated'],
        required_columns=['nums', 'filledTime']
    ))

@dataclass
class FitbitStudent(Sheet):
    """Sheet for storing pairing Fitbit devices with students"""
    schema: SheetSchema = field(default_factory=lambda: SheetSchema(
        columns=['email', 'watch'],
        required_columns=['email', 'watch']
    ))

@dataclass
class ChatsSheet(Sheet):
    """Sheet for storing chat data"""
    schema: SheetSchema = field(default_factory=lambda: SheetSchema(
        columns=['watchName', 'user', 'content', 'timestamp'],
        required_columns=['watchName', 'user']
    ))



# Enhanced Spreadsheet (Entity version)
@dataclass
class Spreadsheet:
    """
    Spreadsheet entity class.

    Attributes:
        name (str): The name of the spreadsheet.
        api_key (str): The API key associated with the spreadsheet.
        sheets (Dict): Dictionary of sheets indexed by name.
    """
    name: str
    api_key: str
    sheets: Dict[str, Sheet] = field(default_factory=dict)
    _gspread_connection = None
    
    def get_sheet(self, name: str, sheet_type: str = 'generic') -> Sheet:
        """Get a sheet by name, creating it if it doesn't exist"""
        if name not in self.sheets:
            self.sheets[name] = SheetFactory.create_sheet(sheet_type, name)
        return self.sheets[name]
    
    def update_sheet(self, name: str, data: Union[dict, pd.DataFrame, pl.DataFrame], 
                     strategy: str = 'replace') -> None:
        """Update a sheet with new data using the specified strategy"""
        sheet = self.get_sheet(name)
        
        # Convert dataframe to dict if needed
        if isinstance(data, (pd.DataFrame, pl.DataFrame)):
            if isinstance(data, pd.DataFrame):
                data = data.to_dict(orient='records')
            else:  # polars
                data = data.to_dicts()
        
        # Apply the selected update strategy
        strategies = {
            'append': AppendStrategy(),
            'replace': ReplaceStrategy(),
            'merge': MergeStrategy()
        }
        
        if strategy not in strategies:
            raise ValueError(f"Unknown update strategy: {strategy}")
        
        strategies[strategy].update(sheet, data)
    
    def sheet_to_dataframe(self, name: str, engine: str = 'pandas') -> Union[pd.DataFrame, pl.DataFrame]:
        """Convert a sheet to a dataframe"""
        sheet = self.get_sheet(name)
        return sheet.to_dataframe(engine)
    
    def get_gspread_connection(self):
        """Get the gspread connection for this spreadsheet"""
        if not self._gspread_connection:
            # Initialize connection
            sheets_api = SheetsAPI.get_instance()
            self._gspread_connection = sheets_api.client.open_by_key(self.api_key)
        return self._gspread_connection

# =====================================================================
# ==================== GOOGLE SHEETS API LAYER ========================
# =====================================================================

class SheetsAPI:
    """Singleton class for accessing the Google Sheets API"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SheetsAPI, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if getattr(self, '_initialized', False):
            return
            
        self._initialized = True
        self.client = self._get_client()
        self._spreadsheets = {}
    
    @staticmethod
    @st.cache_resource
    def _get_client():
        """Get a Google Sheets API client with proper authentication"""
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
    
    def open_spreadsheet(self, key: str):
        """Open a spreadsheet by key"""
        if key not in self._spreadsheets:
            self._spreadsheets[key] = self.client.open_by_key(key)
        return self._spreadsheets[key]
    
    @classmethod
    def get_instance(cls):
        """Get the singleton instance"""
        if cls._instance is None:
            cls()
        return cls._instance


class GoogleSheetsAdapter:
    """Adapter for connecting entity layer Spreadsheet with Google Sheets API"""
    
    @staticmethod
    def connect(spreadsheet: Spreadsheet) -> Spreadsheet:
        """Connect the entity Spreadsheet with the actual Google Sheets API"""
        # Get API instance
        sheets_api = SheetsAPI.get_instance()
        
        # Use the client to fetch the actual spreadsheet
        google_spreadsheet = sheets_api.open_spreadsheet(spreadsheet.api_key)
        
        # Map worksheets to Sheet objects
        for worksheet in google_spreadsheet.worksheets():
            sheet_name = worksheet.title
            if r'שליחה לרשימת תפוצה' in sheet_name:
                sheet_name = 'bulldog'
            # White list of sheet names
            sheets_names = [
                "user", "project", "fitbit", "log", "bulldog", "qualtrics_nova", "FitbitLog",
                "fitbit_alerts_config", "qualtrics_alert_config", "late_nums", "suspicious_nums",
                "qualtrics_nova", "student_fitbit", "chats"
            ]
            if sheet_name not in sheets_names:
                continue
                
            try:
                # For bulldog sheet with duplicate headers, use a custom extraction
                if sheet_name == 'bulldog':
                    # Get all values including headers
                    all_values = worksheet.get_all_values()
                    if len(all_values) > 0:
                        # Get the first 5 columns only
                        headers = all_values[0][:5]
                        # Extract records (skip header row)
                        records = []
                        for row in all_values[1:]:
                            if any(row[:5]):  # Skip empty rows
                                record = {headers[i]: row[i] if i < len(row) else "" 
                                        for i in range(len(headers))}
                                records.append(record)
                else:
                    # For other sheets, try the normal approach
                    records = worksheet.get_all_records()
            except Exception as e:
                print(f"Error getting records from {sheet_name}: {e}")
                # Fallback for any sheet with problematic headers
                try:
                    all_values = worksheet.get_all_values()
                    if len(all_values) > 0:
                        headers = all_values[0]
                        # Create unique headers
                        unique_headers = []
                        seen = {}
                        for h in headers:
                            if h in seen:
                                seen[h] += 1
                                unique_headers.append(f"{h}_{seen[h]}")
                            else:
                                seen[h] = 0
                                unique_headers.append(h)
                        
                        # Get records with unique headers
                        records = worksheet.get_all_records(expected_headers=unique_headers)
                    else:
                        records = []
                except Exception as e2:
                    print(f"Fallback also failed for {sheet_name}: {e2}")
                    records = []
            
            # Determine sheet type based on content or name
            sheet_type = 'generic'
            if 'user' in sheet_name.lower():
                sheet_type = 'user'
            elif 'project' in sheet_name.lower():
                sheet_type = 'project'
            elif 'fitbit' in sheet_name.lower():
                sheet_type = 'fitbit'
            elif 'log' in sheet_name.lower():
                sheet_type = 'log'
            elif sheet_name == 'bulldog':
                sheet_type = 'bulldog'
            elif 'qualtrics' in sheet_name.lower():
                sheet_type = 'qualtrics_nova'
            elif 'fitbitlog' in sheet_name.lower():
                sheet_type = 'log'
            elif 'fitbit_alerts_config' in sheet_name.lower():
                sheet_type = 'fitbit_alerts_config'
            elif 'qualtrics_alert_config' in sheet_name.lower():
                sheet_type = 'qualtrics_alert_config'
            elif 'late_nums' in sheet_name.lower():
                sheet_type = 'late_nums'
            elif 'suspicious_nums' in sheet_name.lower():
                sheet_type = 'suspicious_nums'
            elif 'student_fitbit' in sheet_name.lower():
                sheet_type = 'student_fitbit'
            elif 'qualtrics_nova' in sheet_name.lower():
                sheet_type = 'qualtrics_nova'
            
            # Create and populate the sheet
            sheet = SheetFactory.create_sheet(sheet_type, sheet_name)
            sheet.data = records
            spreadsheet.sheets[sheet_name] = sheet
        
        # Store connection for future use
        spreadsheet._gspread_connection = google_spreadsheet
        
        return spreadsheet
    
    @staticmethod
    def save(spreadsheet: Spreadsheet, sheet_name: str = None):
        """Save changes back to Google Sheets"""
        # Get the Google Sheets connection
        google_spreadsheet = spreadsheet.get_gspread_connection()
        
        # If a specific sheet is provided, only update that one
        if sheet_name:
            if sheet_name in spreadsheet.sheets:
                sheet = spreadsheet.sheets[sheet_name]
                
                try:
                    # Get or create worksheet
                    try:
                        worksheet = google_spreadsheet.worksheet(sheet_name)
                    except gspread.exceptions.WorksheetNotFound:
                        worksheet = google_spreadsheet.add_worksheet(
                            title=sheet_name, rows=1, cols=10
                        )
                    
                    # Clear and update
                    worksheet.clear()
                    if sheet.data:
                        if isinstance(sheet.data, list) and sheet.data:
                            # Get headers from first item
                            headers = list(sheet.data[0].keys())
                            worksheet.append_row(headers)
                            
                            # Add all rows
                            for item in sheet.data:
                                row = [item.get(header, '') for header in headers]
                                worksheet.append_row(row)
                except Exception as e:
                    print(f"Error saving sheet {sheet_name}: {e}")
        else:
            # Update all sheets
            for sheet_name, sheet in spreadsheet.sheets.items():
                try:
                    # Get or create worksheet
                    try:
                        worksheet = google_spreadsheet.worksheet(sheet_name)
                    except gspread.exceptions.WorksheetNotFound:
                        worksheet = google_spreadsheet.add_worksheet(
                            title=sheet_name, rows=1, cols=10
                        )
                    
                    # Clear and update
                    worksheet.clear()
                    if sheet.data:
                        if isinstance(sheet.data, list) and sheet.data:
                            # Get headers from first item
                            headers = list(sheet.data[0].keys())
                            worksheet.append_row(headers)
                            
                            # Add all rows
                            for item in sheet.data:
                                row = [item.get(header, '') for header in headers]
                                worksheet.append_row(row)
                except Exception as e:
                    print(f"Error saving sheet {sheet_name}: {e}")

# =====================================================================
# ================ LEGACY COMPATIBILITY LAYER ========================
# =====================================================================

class LegacySpreadsheetManager:
    """Singleton class for maintaining legacy compatibility with Spreadsheet_io.sheets"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(LegacySpreadsheetManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if getattr(self, '_initialized', False):
            return
            
        self._initialized = True
        self.sheets_api = SheetsAPI.get_instance()
        self._spreadsheet_key = get_secrets().get("spreadsheet_key", "")
        self._spreadsheet = None
        self._entity_spreadsheet = None
        
    def get_spreadsheet(self):
        """Get the Google Spreadsheet object"""
        if not self._spreadsheet:
            self._spreadsheet = self.sheets_api.open_spreadsheet(self._spreadsheet_key)
        return self._spreadsheet
    
    def get_entity_spreadsheet(self):
        """Get the entity layer Spreadsheet object"""
        if not self._entity_spreadsheet:
            self._entity_spreadsheet = Spreadsheet(
                name="Fitbit Database",
                api_key=self._spreadsheet_key
            )
            GoogleSheetsAdapter.connect(self._entity_spreadsheet)
        return self._entity_spreadsheet
    
    def get_worksheet(self, index):
        """Get a worksheet by index"""
        return self.get_spreadsheet().get_worksheet(index)
    
    def get_worksheet_by_name(self, name):
        """Get a worksheet by name"""
        try:
            return self.get_spreadsheet().worksheet(name)
        except gspread.exceptions.WorksheetNotFound:
            return None
    
    def get_all_records(self, index):
        """Get all records from a worksheet by index"""
        return self.get_worksheet(index).get_all_records()
    
    def get_user_details(self):
        """Get user details from the first worksheet"""
        return self.get_all_records(0)
    
    def get_project_details(self):
        """Get project details from the second worksheet"""
        return self.get_all_records(1)
    
    def get_fitbits_details(self):
        """Get fitbit details from the third worksheet"""
        return self.get_all_records(2)
    
    def get_fitbits_log(self):
        """Get fitbit log from the fourth worksheet"""
        return self.get_worksheet(3).get_all_values()
    
    def append_to_worksheet_3(self, data_list):
        """Append data to worksheet 3 with the latest Fitbit data"""
        if not data_list:
            print("No data to append in worksheet 3")
            return
            
        worksheet = self.get_worksheet(3)
        
        # Define the expected column order
        expected_columns = [
            "project", "watchName", "lastCheck", "lastSynced", "lastBattary", 
            "lastHR", "lastSleepStartDateTime", "lastSleepEndDateTime", "lastSteps", 
            "lastBattaryVal", "lastHRVal", "lastHRSeq", "lastSleepDur", "lastStepsVal",
            "CurrentFailedSync", "TotalFailedSync", "CurrentFailedHR", "TotalFailedHR",
            "CurrentFailedSleep", "TotalFailedSleep", "CurrentFailedSteps", "TotalFailedSteps", "ID"
        ]
            
        # Add data rows
        for item in data_list:
            # Map data to expected columns format
            row_data = []
            for col in expected_columns:
                row_data.append(str(item.get(col, '')))
            
            worksheet.append_row(row_data)
        
        print(f"Appended {len(data_list)} records to worksheet 3")
        
        # Also update entity layer if possible
        try:
            entity_spreadsheet = self.get_entity_spreadsheet()
            log_sheet = entity_spreadsheet.get_sheet("FitbitLog", "log")
            if isinstance(log_sheet.data, list):
                log_sheet.data.extend(data_list)
            else:
                log_sheet.data = data_list
            GoogleSheetsAdapter.save(entity_spreadsheet, "FitbitLog")
        except Exception as e:
            print(f"Error updating entity layer: {e}")

    @classmethod
    def get_instance(cls):
        """Get the singleton instance"""
        if cls._instance is None:
            cls()
        return cls._instance


# Define Legacy API for backward compatibility
class LegacySpreadsheet:
    """Legacy API compatible with Spreadsheet_io.sheets.Spreadsheet"""
    _instance = None
    
    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = LegacySpreadsheetManager.get_instance()
        return cls._instance
    
    @classmethod
    def get_client(cls):
        instance = cls.get_instance()
        return instance.sheets_api.client
    
    @classmethod
    def get_user_details(cls):
        instance = cls.get_instance()
        return instance.get_user_details()
    
    @classmethod
    def get_project_details(cls):
        instance = cls.get_instance()
        return instance.get_project_details()
    
    @classmethod
    def get_spreadsheet(cls):
        instance = cls.get_instance()
        return instance.get_spreadsheet()
    
    @classmethod
    def get_fitbits_details(cls):
        instance = cls.get_instance()
        return instance.get_fitbits_details()
    
    @classmethod
    def get_fitbits_log(cls):
        instance = cls.get_instance()
        return instance.get_fitbits_log()
    
    @classmethod
    def append_to_worksheet_3(cls, data_list):
        instance = cls.get_instance()
        return instance.append_to_worksheet_3(data_list)
    
    @classmethod
    def get_entity_spreadsheet(cls):
        instance = cls.get_instance()
        return instance.get_entity_spreadsheet()


class FitbitLog:
    """Legacy FitbitLog class for compatibility"""
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
        return f"Fitbit Log: {self.project}, {self.watchName}"
    
    def __repr__(self):
        return self.__str__()
    
    def __eq__(self, value):
        if isinstance(value, FitbitLog):
            return self.project == value.project and self.watchName == value.watchName
        return False
    
    def __ne__(self, value):
        return not self.__eq__(value)
    
    def __hash__(self):
        return hash((self.project, self.watchName))
    
    def __getitem__(self, key):
        return self.__dict__.get(key)


class ServerLogFile:
    """Legacy ServerLogFile class for compatibility"""
    def __init__(self, path=None):
        """Initialize serverLogFile with optional parameters."""
        self.path = path or get_secrets().get("fitbit_log_path", "fitbit_log.csv")

    def __str__(self):
        return f"Server Log File: {self.path}"
        
    def __repr__(self):
        return f"Server Log File: {self.path}"
        
    def __eq__(self, value):
        if isinstance(value, ServerLogFile):
            if self.path == value.path:
                return True
            else:
                return False
        else:
            return False
            
    def __ne__(self, value):
        if isinstance(value, ServerLogFile):
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
    
    def update_fitbits_log(self, fitbit_data: pl.DataFrame) -> bool:
        """
        Updates the Fitbit log files and sheets.
        - CSV file: Always append (full history)
        - "log" sheet: Replace with latest records only (one per watch)
        - "FitbitLog" sheet: Always append (full history)
        
        Args:
            fitbit_data (pl.DataFrame): DataFrame containing the watch data.
            
        Returns:
            bool: True if update succeeded, False otherwise
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
            "CurrentFailedSleep", "TotalFailedSleep", "CurrentFailedSteps", "TotalFailedSteps",
            "CurrentFailedBattary", "TotalFailedBattary", "ID"
        ]
        
        try:
            # Initialize or load existing data
            if os.path.exists(self.path):
                try:
                    existing_df = pl.read_csv(self.path)
                    # Print debug info
                    print(f"Existing log file schema: {existing_df.schema}")
                    # Ensure all expected columns exist
                    for col in expected_columns:
                        if col not in existing_df.columns:
                            existing_df = existing_df.with_columns(pl.lit("").alias(col))
                    
                    # Convert all columns to strings (Utf8) to avoid type mismatches
                    existing_df = existing_df.select([
                        pl.col(col).cast(pl.Utf8) for col in existing_df.columns
                    ])
                except Exception as e:
                    print(f"Error reading existing log file: {e}")
                    existing_df = pl.DataFrame({col: [] for col in expected_columns})
            else:
                existing_df = pl.DataFrame({col: [] for col in expected_columns})
            
            # Process each row from the Fitbit data
            new_log_entries = []
            latest_entries_by_watch = {}  # Dictionary to store latest entry per watch
            
            for row in fitbit_data.iter_rows(named=True):
                # Create watch ID for matching
                watch_id = f"{row.get('project', '')}-{row.get('name', '')}"
                
                # Check if watch is active
                is_active = str(row.get('isActive', '')).upper() != 'FALSE'
                if not is_active:
                    # Skip processing inactive watches
                    continue
                
                # Create a Watch object and update data via API
                try:
                    # Convert row data to a dict for the factory
                    watch_data = {key: value for key, value in row.items()}
                    
                    # Create Watch object using the factory
                    watch = WatchFactory.create_from_details(watch_data)
                    
                    # Update device information via Fitbit API
                    print(f"Fetching latest data from Fitbit API for watch {watch.name}...")
                    # Use the proper API method to update watch data
                    watch.update_device_info()
                    
                    # Check if essential attributes were updated
                    if hasattr(watch, 'last_sync_time') and watch.last_sync_time:
                        # Update row with the fresh data from API using the appropriate getters
                        row = {
                            **row,
                            "battery": watch.battery_level if hasattr(watch, 'battery_level') else "",
                            "HR": watch.get_current_hourly_HR() if hasattr(watch, 'get_current_hourly_HR') else "",
                            "syncDate": watch.last_sync_time.isoformat() if hasattr(watch, 'last_sync_time') and watch.last_sync_time else "",
                            "sleep_start": watch.get_last_sleep_start_end()[0] if hasattr(watch, 'get_last_sleep_start_end') else "",
                            "sleep_end": watch.get_last_sleep_start_end()[1] if hasattr(watch, 'get_last_sleep_start_end') else "",
                            "sleep_duration": watch.get_last_sleep_duration() if hasattr(watch, 'get_last_sleep_duration') else "",
                            "steps": watch.get_current_hourly_steps() if hasattr(watch, 'get_current_hourly_steps') else ""
                        }
                        print(f"Successfully updated data for watch {watch.name} from Fitbit API")
                    else:
                        print(f"Failed to update data for watch {watch.name} from Fitbit API, using existing data")
                except Exception as e:
                    print(f"Error creating/updating watch {row.get('name', '')} via API: {e}")
                    # Continue with existing data
                
                # Build the log entry
                log_entry = {
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
                    "lastHRSeq": self._calculate_hr_sequence(row),
                    "lastSleepDur": row.get("sleep_duration", ""),
                    "lastStepsVal": row.get("steps", ""),
                    "CurrentFailedSync": 0,  
                    "TotalFailedSync": 0,
                    "CurrentFailedHR": 0,
                    "TotalFailedHR": 0,
                    "CurrentFailedSleep": 0,
                    "TotalFailedSleep": 0,
                    "CurrentFailedSteps": 0,
                    "TotalFailedSteps": 0,
                    "CurrentFailedBattary": 0,
                    "TotalFailedBattary": 0,
                    "ID": watch_id
                }
                
                # Add to the list of all entries
                new_log_entries.append(log_entry)
                
                # Keep track of latest entry for each unique watch ID
                latest_entries_by_watch[watch_id] = log_entry
            
            # For "log" sheet - Use REPLACE strategy (latest records only - one per watch)
            if latest_entries_by_watch:
                try:
                    # Get the legacy spreadsheet manager
                    manager = LegacySpreadsheetManager.get_instance()
                    
                    # Get the worksheet for the log sheet
                    worksheet = manager.get_worksheet(3)  # Worksheet 3 is the log sheet
                    
                    # Clear the current content (replace strategy)
                    worksheet.clear()
                    
                    # Define the expected column order
                    expected_columns = [
                        "project", "watchName", "lastCheck", "lastSynced", "lastBattary", 
                        "lastHR", "lastSleepStartDateTime", "lastSleepEndDateTime", "lastSteps", 
                        "lastBattaryVal", "lastHRVal", "lastHRSeq", "lastSleepDur", "lastStepsVal",
                        "CurrentFailedSync", "TotalFailedSync", "CurrentFailedHR", "TotalFailedHR",
                        "CurrentFailedSleep", "TotalFailedSleep", "CurrentFailedSteps", "TotalFailedSteps",
                        "CurrentFailedBattary", "TotalFailedBattary", "ID"
                    ]
                    
                    # Add headers as the first row
                    worksheet.append_row(expected_columns)
                    
                    # Add only the latest entry for each watch
                    latest_entries = list(latest_entries_by_watch.values())
                    for item in latest_entries:
                        # Map data to expected columns format
                        row_data = []
                        for col in expected_columns:
                            row_data.append(str(item.get(col, '')))
                        
                        worksheet.append_row(row_data)
                    
                    print(f"Replaced log sheet with {len(latest_entries)} latest records (one per watch)")
                except Exception as e:
                    print(f"Error updating log sheet: {e}")
                    print(f"Error details: {traceback.format_exc()}")
            
            # For CSV file - Always APPEND (keep full history)
            if new_log_entries:
                # Convert new entries to DataFrame with explicit string types
                new_entries_df = pl.DataFrame(new_log_entries)
                print(f"New entries schema: {new_entries_df.schema}")
                
                # Convert all columns to strings to avoid type issues
                new_entries_df = new_entries_df.select([
                    pl.col(col).cast(pl.Utf8) for col in new_entries_df.columns
                ])
                
                # Make sure both DataFrames have the same columns
                # Get common columns
                common_cols = list(set(existing_df.columns).intersection(set(new_entries_df.columns)))
                if len(common_cols) < len(existing_df.columns) or len(common_cols) < len(new_entries_df.columns):
                    print(f"Warning: Column mismatch: existing has {existing_df.columns}, new has {new_entries_df.columns}")
                    # Select only common columns
                    existing_df = existing_df.select(common_cols)
                    new_entries_df = new_entries_df.select(common_cols)
                
                # ALWAYS append to existing CSV (never replace)
                try:
                    final_df = pl.concat([existing_df, new_entries_df], how="vertical")
                    final_df.write_csv(self.path)
                    print(f"Appended {len(new_log_entries)} records to log file (total: {len(final_df)})")
                except Exception as e:
                    print(f"Error during CSV concatenation: {e}")
                    print(f"Existing types: {[existing_df[col].dtype for col in common_cols]}")
                    print(f"New types: {[new_entries_df[col].dtype for col in common_cols]}")
                    # Fall back to overwriting if append fails
                    print("Falling back to creating new CSV file")
                    new_entries_df.write_csv(self.path)
                    print(f"Created new log file with {len(new_entries_df)} records")
            else:
                print("No active watch data to append to log file")
            
            # For "FitbitLog" sheet - Always APPEND (keep full history)
            try:
                # Get entity spreadsheet
                entity_sp = LegacySpreadsheetManager.get_instance().get_entity_spreadsheet()
                
                # First make sure the FitbitLog sheet exists
                if "FitbitLog" not in entity_sp.sheets:
                    print("Creating new FitbitLog sheet since it doesn't exist")
                    entity_sp.get_sheet("FitbitLog", "log")
                
                log_sheet = entity_sp.sheets["FitbitLog"]
                
                # Initialize data if needed
                if not hasattr(log_sheet, 'data') or log_sheet.data is None:
                    log_sheet.data = []
                
                # Make sure we have a list to append to
                if not isinstance(log_sheet.data, list):
                    print(f"Converting log_sheet.data from {type(log_sheet.data)} to list")
                    try:
                        log_sheet.data = [log_sheet.data] if log_sheet.data else []
                    except Exception as e:
                        print(f"Error converting to list: {e}")
                        log_sheet.data = []
                
                # Debug info before append
                original_count = len(log_sheet.data) if log_sheet.data else 0
                print(f"FitbitLog sheet before append: {original_count} records")
                
                # Ensure all entries in new_log_entries have string values only
                for entry in new_log_entries:
                    for key, value in list(entry.items()):
                        # Convert all values to strings to avoid type issues
                        entry[key] = str(value) if value is not None else ""
                
                # Append new records
                log_sheet.data.extend(new_log_entries)
                
                # Debug info after append
                new_count = len(log_sheet.data)
                print(f"FitbitLog sheet after append: {new_count} records (+{new_count - original_count})")
                
                # Make sure all data is string-typed
                if log_sheet.data:
                    print(f"First record sample types: " + 
                          ", ".join(f"{k}: {type(v)}" for k, v in list(log_sheet.data[0].items())[:5]))
                
                # Explicitly save the sheet
                print("Saving FitbitLog sheet...")
                GoogleSheetsAdapter.save(entity_sp, "FitbitLog")
                print(f"Successfully appended {len(new_log_entries)} records to FitbitLog sheet")
            except Exception as e:
                print(f"Error updating FitbitLog sheet: {e}")
                print(f"Error details: {traceback.format_exc()}")
            
            return True
        except Exception as e:
            print(f"Error in update_fitbits_log: {e}")
            print(f"Full error details: {traceback.format_exc()}")
            return False
    
    def _calculate_hr_sequence(self, row_data):
        """Calculate heart rate sequence information from the data"""
        return ""
    
    def get_summary_statistics(self):
        """Get summary statistics about watch failures"""
        try:
            if os.path.exists(self.path):
                df = pl.read_csv(self.path)
                if df.is_empty():
                    return {}
                
                return {
                    "total_watches": len(df),
                    "sync_failures": df.filter(pl.col("CurrentFailedSync") > 0).height,
                    "hr_failures": df.filter(pl.col("CurrentFailedHR") > 0).height,
                    "sleep_failures": df.filter(pl.col("CurrentFailedSleep") > 0).height,
                    "steps_failures": df.filter(pl.col("CurrentFailedSteps") > 0).height,
                    "total_failures": df.filter(
                        (pl.col("CurrentFailedSync") > 0) |
                        (pl.col("CurrentFailedHR") > 0) |
                        (pl.col("CurrentFailedSleep") > 0) |
                        (pl.col("CurrentFailedSteps") > 0)
                    ).height
                }
        except Exception as e:
            print(f"Error getting summary statistics: {e}")
        
        return {}

# Create aliases for backward compatibility
serverLogFile = ServerLogFile
Spreadsheet_Legacy = LegacySpreadsheet
fitbitLog = FitbitLog



# Add this class to your file
class AlertAnalyzer:
    """Analyzes WhatsApp alerts and message statuses."""
    
    @staticmethod
    def analyze_whatsapp_messages(bulldog_sheet, alert_sheet, hours_threshold=48):
        """
        Analyze WhatsApp messages for status and identify suspicious numbers.
        
        Args:
            bulldog_sheet: Sheet containing WhatsApp message data
            alert_sheet: Sheet containing patient alert data
            hours_threshold: Hours to consider for recent messages
            
        Returns:
            tuple: (recent_messages DataFrame, suspicious_numbers DataFrame)
        """
        import datetime
        import polars as pl
        
        # Convert bulldog sheet to DataFrame with proper column names
        sheet_df = bulldog_sheet.to_dataframe(engine="polars").with_columns(
            pl.col('שם').alias('name'),
            pl.col('נייד').alias('phone'),
            pl.col('קטגוריה ( לא חובה )').alias('category'),
            pl.col('סטטוס שליחה').alias('status'),
            pl.col('זמן שליחה').alias('time')
        ).select(
            pl.col('phone'),
            pl.col('category'),
            pl.col('status'),
            pl.col('time')
        )
        
        # Clean phone numbers
        sheet_df = sheet_df.with_columns(
            pl.col('phone').str.slice(-9).alias('phone')
        )
        
        # Convert alert sheet to DataFrame with cleaned phone numbers
        alert_df = alert_sheet.to_dataframe(engine="polars").with_columns(
            pl.col('num').cast(pl.String).replace('-', '').replace(' ', '').alias('phone')
        )
        
        # Convert time column to datetime
        try:
            sheet_df = sheet_df.with_columns(
                pl.col('time').str.strptime(pl.Datetime, format="%d/%m/%Y %H:%M").alias('datetime')
            )
        except Exception as e:
            print(f"Error converting time to datetime: {e}")
            # Add a fallback datetime column
            sheet_df = sheet_df.with_columns(
                pl.lit(datetime.datetime.now()).alias('datetime')
            )
        
        # Get current time
        now = datetime.datetime.now()
        
        # Filter successfully sent messages
        sent_messages = sheet_df.filter(pl.col('status') == 'נשלח בהצלחה')
        
        # Calculate hours since message was sent
        sent_messages = sent_messages.with_columns(
            ((now - pl.col('datetime')).dt.total_hours()).alias('hours_since_sent')
        )
        
        # Filter recent messages within threshold
        recent_messages = sent_messages.filter(pl.col('hours_since_sent') <= hours_threshold)
        
        # Calculate hours left until threshold
        recent_messages = recent_messages.with_columns(
            (hours_threshold - pl.col('hours_since_sent')).alias('hours_left')
        )
        
        # Get list of all phones that received messages
        contacted_phones = sent_messages.select('phone').unique()
        
        # Find phones in alert_df that are not in sheet_df (suspicious numbers)
        if 'phone' in alert_df.columns:
            alert_phones = alert_df.select('phone','endDate').unique()
            suspicious_phones = alert_phones.filter(
                ~pl.col('phone').is_in(contacted_phones.get_column('phone'))
            )
        else:
            suspicious_phones = pl.DataFrame(schema={'phone': pl.String})
        
        return recent_messages, suspicious_phones
        
    @staticmethod
    def generate_alert_report(recent_messages, suspicious_numbers):
        """
        Generate a formatted report from alert analysis.
        
        Args:
            recent_messages: DataFrame of recent messages
            suspicious_numbers: DataFrame of suspicious numbers
            
        Returns:
            str: Formatted report text
        """
        report = []
        
        report.append(f"==== WhatsApp Alert Report ====")
        report.append(f"Generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        report.append(f"== Recent Messages ==")
        report.append(f"Total: {recent_messages.height} messages")
        
        if recent_messages.height > 0:
            report.append("\nTop 10 recent messages:")
            for row in recent_messages.select(['name', 'phone', 'time', 'hours_left']).head(10).iter_rows(named=True):
                report.append(f"- {row['name']} ({row['phone']}): Sent {row['time']}, {row['hours_left']:.1f} hours left")
        
        report.append("")
        report.append(f"== Suspicious Numbers ==")
        report.append(f"Total: {suspicious_numbers.height} numbers")
        
        if suspicious_numbers.height > 0:
            report.append("\nSuspicious numbers (top 10):")
            for row in suspicious_numbers.head(10).iter_rows(named=True):
                report.append(f"- {row['phone']}")
        
        return "\n".join(report)