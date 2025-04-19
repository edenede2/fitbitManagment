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
from utils.sheets_cache import sheets_cache  # Import sheets_cache

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
            'EMA': QualtricsNovaSheet,
            'fitbit_alerts_config': FitbitAlertsConfig,
            'qualtrics_alerts_config': QualtricsAlertConfig,
            'late_nums': LateNums,
            'suspicious_nums': SuspiciousNums,
            'student_fitbit': FitbitStudent,
            'chats': ChatsSheet,
            'for_analysis': FibroEMASheet,
            'appsheet_config': AppSheetConfig,
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
class AppSheetConfig(Sheet):
    """Sheet for storing AppSheet configuration data"""
    schema: SheetSchema = field(default_factory=lambda: SheetSchema(
        columns=['email', 'user', 'missingThr'],
        required_columns=['email', 'user']
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
        columns=['project', 'watchName',	'lastCheck', 'lastSynced',
                'lastBattary', 'lastHR', 'lastSleepStartDateTime',	'lastSleepEndDateTime',
                'lastSteps','lastBattaryVal','lastHRVal', 'lastHRSeq',
                'lastSleepDur',	'lastStepsVal',	'CurrentFailedSync', 'TotalFailedSync'
                'CurrentFailedHR',	'TotalFailedHR', 'CurrentFailedSleep', 'TotalFailedSleep',
                'CurrentFailedSteps', 'TotalFailedSteps','CurrentFailedBattary', 'TotalFailedBattary',
                  'ID'],
        required_columns=['project', 'ID','lastCheck']
    ))


@dataclass
class BulldogSheet(Sheet):
    """Sheet for storing bulldog data"""
    schema: SheetSchema = field(default_factory=lambda: SheetSchema(
        columns=['שם',	'נייד',	'קטגוריה ( לא חובה )',	'סטטוס שליחה',	'זמן שליחה'],
        required_columns=['שם', 'נייד']
    ))

@dataclass
class FibroEMASheet(Sheet):
    """Sheet for storing Fibro EMA data"""
    schema: SheetSchema = field(default_factory=lambda: SheetSchema(
        columns=['User Id', 'KEY', 'Date Time','העריכי כמה שעות ישנת אתמול בלילה?',
                'העריכי מספרית את איכות השינה שלך (1- שינה גרועה, 10- שינה מעולה)',
                r'"עד כמה את מרגישה רגועה עכשיו אחרי השינה בלילה?(1- כלל לא רגועה, 10 רגועה מאוד)"',
                r'"מהי רמת הכאב שלך כרגע?(1- אין כאב, 10- הכאב החמור ביותר)"',r'איפה כואב לך?',
                'אחר:','באיזו יד כואב לך?', 'באיזו רגל כואב לך?',
                'מלבד כאב, אילו סימפטומים את חשה כרגע בנוסף? (ניתן להוסיף)',
                'דרגי עד כמה את מרגישה כרגע לחוצה  (5= כל הזמן, 4= רוב הזמן, 3= חלק מהזמן, 2= מעט מהזמן, 1= כלל לא)',
                'דרגי עד כמה את מרגישה כרגע חסרת תקווה  (5= כל הזמן, 4= רוב הזמן, 3= חלק מהזמן, 2= מעט מהזמן, 1= כלל לא)',
                'דרגי עד כמה את מרגישה כרגע חסרת מנוחה או חסרת שקט?  (5= כל הזמן, 4= רוב הזמן, 3= חלק מהזמן, 2= מעט מהזמן, 1= כלל לא)',
                'דרגי עד כמה את מרגישה כרגע מדוכאת ששום דבר לא יכול לשמח אותך?  (5= כל הזמן, 4= רוב הזמן, 3= חלק מהזמן, 2= מעט מהזמן, 1= כלל לא)',
                'דרגי עד כמה את מרגישה כרגע שכל דבר מחייב מאמץ?  (5= כל הזמן, 4= רוב הזמן, 3= חלק מהזמן, 2= מעט מהזמן, 1= כלל לא)',
                'דרגי עד כמה את מרגישה כרגע חסרת ערך?  (5= כל הזמן, 4= רוב הזמן, 3= חלק מהזמן, 2= מעט מהזמן, 1= כלל לא)','EDA אנא בצעי כעת בעזרת הצמיד מדידת',
                'איפה את נמצאת כרגע:','אחר -','עם מי את נמצאת כרגע:','"דרגי את היום שלך: רמת כאב כללית(1- ללא כאב, 10- הכאב הגרוע ביותר)"', '"דרגי את היום שלך: רמת תפקוד כללית(1- גרועה, 10- מעולה)"',
                'איזו תרופה לקחת היום ,באיזה מינון ומתי?', 'מלבד תרופות, האם ניסית דבר מה נוסף במטרה להרגיע את הכאב? אם כן, אנא פרטי', 'סכמי את היום שלך בכמה מילים אם תרצי האם יש משהו נוסף לגבי היום שחשוב שנדע'],
        required_columns=['User Id', 'KEY']
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
                'manager','email', 'watch', 'endDate'],
        required_columns=['project', 'manager']
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
        columns=['nums', 'sentTime', 'hoursLate', 'lastUpdated', 'accepted'],
        required_columns=['nums', 'sentTime']
    ))

@dataclass
class SuspiciousNums(Sheet):
    """Sheet for storing suspicious numbers"""
    schema: SheetSchema = field(default_factory=lambda: SheetSchema(
        columns=['nums', 'filledTime', 'lastUpdated', 'accepted'],
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
    
    def get_sheet(self, name: str, sheet_type: str = 'generic', refresh = False) -> Sheet:
        """Get a sheet by name, creating it if it doesn't exist"""
        if refresh:
            GoogleSheetsAdapter.connect(self)
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
    def get_all_reords(spreadsheet: Spreadsheet, name: str) -> Sheet:
        """Get a sheet by name from the entity layer"""
        sheets_api = SheetsAPI.get_instance()
        google_spreadsheet = sheets_api.open_spreadsheet(spreadsheet.api_key)
        try:
            worksheet = google_spreadsheet.worksheet(name)
            records = worksheet.get_all_records()
        except gspread.exceptions.WorksheetNotFound:
            print(f"Worksheet {name} not found in spreadsheet {spreadsheet.name}")
            records = []
        return records
    
    @staticmethod
    def get_row(spreadsheet: Spreadsheet, name: str, *keys, **row) -> Optional[dict]:
        """Get a row from a sheet by keys"""
        sheet_api = SheetsAPI.get_instance()
        google_spreadsheet = sheet_api.open_spreadsheet(spreadsheet.api_key)
        try:
            worksheet = google_spreadsheet.worksheet(name)
            records = worksheet.get_all_records()
            for record in records:
                if all(record.get(key) == row[key] for key in keys):
                    return record
        except gspread.exceptions.WorksheetNotFound:
            print(f"Worksheet {name} not found in spreadsheet {spreadsheet.name}")
    
    @staticmethod
    def get_rows(spreadsheet: Spreadsheet, name: str, *keys, **row) -> List[dict]:
        """Get rows from a sheet by keys"""
        sheet_api = SheetsAPI.get_instance()
        google_spreadsheet = sheet_api.open_spreadsheet(spreadsheet.api_key)
        try:
            worksheet = google_spreadsheet.worksheet(name)
            records = worksheet.get_all_records()
            result = []
            for record in records:
                if all(record.get(key) == row[key] for key in keys):
                    result.append(record)
            return result
        except gspread.exceptions.WorksheetNotFound:
            print(f"Worksheet {name} not found in spreadsheet {spreadsheet.name}")
            return []
    
    @staticmethod
    def update_row(spreadsheet: Spreadsheet, name: str, **on) -> None:
        """Update a row in a sheet by ID. on is a dictionary of column names and values"""
        sheet_api = SheetsAPI.get_instance()
        google_spreadsheet = sheet_api.open_spreadsheet(spreadsheet.api_key)
        try:
            worksheet = google_spreadsheet.worksheet(name)
            records = worksheet.get_all_records()
            for record in records:
                if all(record.get(key) == on[key] for key in on):
                    # Update the record with new values
                    for key, value in on.items():
                        worksheet.update_cell(record['row'], record['col'], value)
                    break
        except gspread.exceptions.WorksheetNotFound:
            print(f"Worksheet {name} not found in spreadsheet {spreadsheet.name}")
            return None
        except Exception as e:
            print(f"Error updating row in {name}: {e}")
            return None
    @staticmethod
    def update_rows(spreadsheet: Spreadsheet, name: str, **on) -> None:
        """Update rows in a sheet by ID. on is a dictionary of column names and values"""
        sheet_api = SheetsAPI.get_instance()
        google_spreadsheet = sheet_api.open_spreadsheet(spreadsheet.api_key)
        try:
            worksheet = google_spreadsheet.worksheet(name)
            records = worksheet.get_all_records()
            for record in records:
                if all(record.get(key) == on[key] for key in on):
                    # Update the record with new values
                    for key, value in on.items():
                        worksheet.update_cell(record['row'], record['col'], value)
        except gspread.exceptions.WorksheetNotFound:
            print(f"Worksheet {name} not found in spreadsheet {spreadsheet.name}")
            return None
        except Exception as e:
            print(f"Error updating row in {name}: {e}")
            return None
        return None
    
    @staticmethod
    def append_rows(spreadsheet: Spreadsheet, name: str, data: List[dict]) -> None:
        """Append rows to a sheet"""
        sheet_api = SheetsAPI.get_instance()
        google_spreadsheet = sheet_api.open_spreadsheet(spreadsheet.api_key)
        try:
            worksheet = google_spreadsheet.worksheet(name)
            for record in data:
                worksheet.append_row(list(record.values()))
        except gspread.exceptions.WorksheetNotFound:
            print(f"Worksheet {name} not found in spreadsheet {spreadsheet.name}")
            return None
        except Exception as e:
            print(f"Error appending rows to {name}: {e}")
            return None
        return None
    

    @staticmethod
    def delete_row(spreadsheet: Spreadsheet, name: str, **on) -> None:
        """Delete a row in a sheet by ID. on is a dictionary of column names and values"""
        sheet_api = SheetsAPI.get_instance()
        google_spreadsheet = sheet_api.open_spreadsheet(spreadsheet.api_key)
        try:
            worksheet = google_spreadsheet.worksheet(name)
            records = worksheet.get_all_records()
            for record in records:
                if all(record.get(key) == on[key] for key in on):
                    # Delete the row
                    worksheet.delete_rows(record['row'])
                    break
        except gspread.exceptions.WorksheetNotFound:
            print(f"Worksheet {name} not found in spreadsheet {spreadsheet.name}")
            return None
        except Exception as e:
            print(f"Error deleting row in {name}: {e}")
            return None
        return None
    

                

    @staticmethod
    # @sheets_cache(timeout=300)  # Cache for 5 minutes
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
                "user", "project", "fitbit", "log", "bulldog", "EMA", "FitbitLog",
                "fitbit_alerts_config", "qualtrics_alerts_config", "late_nums", "suspicious_nums",
                "EMA", "student_fitbit", "chats", "for_analysis", "appsheet_alerts_config"
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
                sheet_type = 'EMA'
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
            elif 'EMA' in sheet_name.lower():
                sheet_type = 'EMA'
            elif 'qualtrics_nova' in sheet_name.lower():
                sheet_type = 'EMA'
            elif 'fibroema' in sheet_name.lower():
                sheet_type = 'fibroEMA'
            elif 'for_analysis' in sheet_name.lower():
                sheet_type = 'for_analysis'
            
            # Create and populate the sheet
            sheet = SheetFactory.create_sheet(sheet_type, sheet_name)
            sheet.data = records
            spreadsheet.sheets[sheet_name] = sheet
        
        # Store connection for future use
        spreadsheet._gspread_connection = google_spreadsheet
        
        return spreadsheet
    
    @staticmethod
    # @sheets_cache(timeout=300)  # Cache for 5 minutes
    def get_worksheet_data(worksheet_id):
        """Get worksheet data by ID"""
        # Implementation for fetching worksheet data
        pass
    
    @staticmethod
    def save(spreadsheet: Spreadsheet, sheet_name: str = None, mode: str = 'auto'):
        """
        Save changes back to Google Sheets.
        
        Args:
            spreadsheet: Spreadsheet entity to save
            sheet_name: Optional specific sheet to save
            mode: Save mode - 'auto' (detect best approach), 'append' (add new records),
                 'rewrite' (clear and rewrite), 'update' (update existing + append new)
        """
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
                        
                        # Choose appropriate save strategy based on sheet type and mode
                        is_append_only = sheet_name.lower() in ['fitbitlog', 'log', 'chats']
                        detected_mode = 'append' if is_append_only else 'update'
                        save_mode = mode if mode != 'auto' else detected_mode
                        
                        # Make sure we have valid data
                        if not sheet.data or not isinstance(sheet.data, list) or not sheet.data:
                            print(f"No data to save for sheet {sheet_name}")
                            return
                        
                        # Make sure headers are in list format (not dict_keys, which is not JSON serializable)
                        if isinstance(sheet.data[0], dict):
                            headers = list(sheet.data[0].keys())
                        elif isinstance(sheet.data[0], list):
                            # Convert dict_keys to a proper list
                            headers = list(sheet.data[0][0].keys()) if sheet.data[0] else []
                        else:
                            print(f"Unexpected data format in sheet {sheet_name}")
                            return
                            
                        # Verify headers are valid
                        if not headers:
                            print(f"No headers found for sheet {sheet_name}")
                            return
                        
                        # Different save strategies
                        if save_mode == 'rewrite':
                            # Full rewrite - clear and add all data
                            print(f"Using REWRITE strategy for {sheet_name}")
                            worksheet.clear()
                            worksheet.append_row(headers)
                            
                            # Add all rows in batches for better performance
                            batch_size = 100  # Google Sheets allows up to 100 rows in a batch
                            all_rows = []
                            i = 0
                            for item in sheet.data:
                                if isinstance(item, list):
                                    item = item[i] 
                                    i += 1 
                                row = [item.get(header, '') for header in headers]
                                all_rows.append(row)
                            
                            # Send in batches
                            for i in range(0, len(all_rows), batch_size):
                                batch = all_rows[i:i+batch_size]
                                worksheet.append_rows(batch)
                                print(f"Saved batch {i//batch_size + 1}/{(len(all_rows)-1)//batch_size + 1}")
                        
                        elif save_mode == 'append':
                            # Append-only strategy - add only new records
                            print(f"Using APPEND strategy for {sheet_name}")
                            
                            # Make sure headers match
                            existing_headers = worksheet.row_values(1)
                            if existing_headers != headers:
                                # If headers don't match, we need to handle it (could be a schema change)
                                print(f"Headers mismatch in {sheet_name}: existing={existing_headers}, new={headers}")
                                
                                # Check if this is a new sheet with no data
                                all_values = worksheet.get_all_values()
                                if len(all_values) <= 1:  # Only header row or empty
                                    # Rewrite headers for new/empty sheet
                                    worksheet.clear()
                                    worksheet.append_row(headers)
                                else:
                                    # For existing data with schema change, fall back to rewrite
                                    print(f"Schema change detected, falling back to rewrite")
                                    save_mode = 'rewrite'
                                    # Call the method again with rewrite mode
                                    GoogleSheetsAdapter.save(spreadsheet, sheet_name, 'rewrite')
                                    return
                            
                            # Get existing data for comparison (if we need it)
                            if sheet_name.lower() == 'fitbitlog':
                                # For FitbitLog, we know we just want to append all data
                                # Find the last row with data
                                try:
                                    last_row = len(worksheet.get_all_values())
                                    if last_row <= 1:  # Only header or empty
                                        start_row = 1  # Start after header
                                    else:
                                        start_row = last_row
                                        
                                    # Add all new rows
                                    batch_size = 100
                                    all_rows = []
                                    
                                    for item in sheet.data:
                                        row = [item.get(header, '') for header in headers]
                                        all_rows.append(row)
                                    
                                    # Send in batches
                                    for i in range(0, len(all_rows), batch_size):
                                        batch = all_rows[i:i+batch_size]
                                        worksheet.append_rows(batch)
                                        print(f"Appended batch {i//batch_size + 1}/{(len(all_rows)-1)//batch_size + 1}")
                                except Exception as e:
                                    print(f"Error during append: {e}")
                                    print(traceback.format_exc())
                            else:
                                # For other sheets, check what's already there and only add new
                                existing_data = worksheet.get_all_records()
                                
                                # Find records that don't exist yet
                                # Use a simple hash-based approach for comparison
                                existing_hashes = {GoogleSheetsAdapter._hash_record(record) for record in existing_data}
                                new_records = []
                                
                                for item in sheet.data:
                                    if isinstance(item, list):
                                        for i in range(len(item)):
                                            item_hash = GoogleSheetsAdapter._hash_record(item[i])
                                            if item_hash not in existing_hashes:
                                                new_records.append(item[i])
                                    else:
                                        # If item is a dict, hash the whole dict
                                        item_hash = GoogleSheetsAdapter._hash_record(item)
                                        if item_hash not in existing_hashes:
                                            new_records.append(item)
                                    # item_hash = GoogleSheetsAdapter._hash_record(item)
                                    # if item_hash not in existing_hashes:
                                    #     new_records.append(item)
                                
                                print(f"Found {len(new_records)} new records to append")
                                
                                # Add new records in batches
                                if new_records:
                                    batch_size = 100
                                    all_rows = []
                                    
                                    for item in new_records:
                                        row = [item.get(header, '') for header in headers]
                                        all_rows.append(row)
                                    
                                    # Send in batches
                                    for i in range(0, len(all_rows), batch_size):
                                        batch = all_rows[i:i+batch_size]
                                        worksheet.append_rows(batch)
                                        print(f"Appended batch {i//batch_size + 1}/{(len(all_rows)-1)//batch_size + 1}")
                        
                        elif save_mode == 'update':
                            # Update strategy - update existing records and add new ones
                            print(f"Using UPDATE strategy for {sheet_name}")
                            
                            # Get all existing data
                            try:
                                existing_data = worksheet.get_all_records()
                                
                                if not existing_data:
                                    # Sheet exists but is empty, just write all data
                                    worksheet.append_row(headers)
                                    
                                    # Add all rows in batches
                                    batch_size = 100
                                    all_rows = []
                                    
                                    for item in sheet.data:
                                        row = [item.get(header, '') for header in headers]
                                        all_rows.append(row)
                                    
                                    # Send in batches
                                    for i in range(0, len(all_rows), batch_size):
                                        batch = all_rows[i:i+batch_size]
                                        worksheet.append_rows(batch)
                                        print(f"Saved batch {i//batch_size + 1}/{(len(all_rows)-1)//batch_size + 1}")
                                else:
                                    # Determine primary key field(s) based on sheet type
                                    id_fields = ['id']  # Default - use 'id' as primary key
                                    if 'fitbit' in sheet_name.lower():
                                        id_fields = ['name', 'project']
                                    elif 'log' in sheet_name.lower():
                                        id_fields = ['project', 'watchName']
                                    elif 'student_fitbit' in sheet_name.lower():
                                        id_fields = ['email', 'watch']
                                    
                                    # Build index of existing data by primary key(s)
                                    existing_index = {}
                                    for idx, record in enumerate(existing_data):
                                        # Create a composite key from the id fields
                                        key = tuple(str(record.get(field, '')) for field in id_fields)
                                        existing_index[key] = (idx + 2, record)  # +2 for 1-based index and header row
                                    
                                    # Track what to update and what to add
                                    to_update = []  # (row_idx, column_idxs, values)
                                    to_add = []     # New rows to append
                                    
                                    # Process all records
                                    for item in sheet.data:
                                        # Create key to check if record exists
                                        key = tuple(str(item.get(field, '')) for field in id_fields)
                                        
                                        if key in existing_index:
                                            # Record exists - check for changes
                                            row_idx, old_record = existing_index[key]
                                            
                                            # Compare fields and collect changes
                                            changes = []
                                            for col_idx, header in enumerate(headers, 1):  # 1-based column index
                                                new_val = item.get(header, '')
                                                old_val = old_record.get(header, '')
                                                if str(new_val) != str(old_val):
                                                    changes.append((col_idx, header, new_val))
                                            
                                            if changes:
                                                # Group changes by row for efficient updates
                                                to_update.append((row_idx, changes))
                                        else:
                                            # New record - add to append list
                                            to_add.append(item)
                                    
                                    # First, update existing records (use batch updates)
                                    if to_update:
                                        print(f"Updating {len(to_update)} existing records")
                                        
                                        # Group updates into batches for efficiency
                                        batch_size = 30  # Limit batch size for updates
                                        for i in range(0, len(to_update), batch_size):
                                            batch = to_update[i:i+batch_size]
                                            
                                            # Create batch update request
                                            batch_updates = []
                                            for row_idx, changes in batch:
                                                for col_idx, header, value in changes:
                                                    batch_updates.append({
                                                        'range': f'{worksheet.title}!{GoogleSheetsAdapter._col_num_to_letter(col_idx)}{row_idx}',
                                                        'values': [[value]]
                                                    })
                                            
                                            # Apply batch update if not empty
                                            if batch_updates:
                                                try:
                                                    google_spreadsheet.values_batch_update({'data': batch_updates, 'valueInputOption': 'RAW'})
                                                    print(f"Updated batch {i//batch_size + 1}/{(len(to_update)-1)//batch_size + 1}")
                                                except Exception as e:
                                                    print(f"Error in batch update: {e}")
                                    
                                    # Second, add new records
                                    if to_add:
                                        print(f"Adding {len(to_add)} new records")
                                        
                                        # Prepare data for batch append
                                        all_rows = []
                                        for item in to_add:
                                            row = [item.get(header, '') for header in headers]
                                            all_rows.append(row)
                                        
                                        # Send in batches
                                        batch_size = 100
                                        for i in range(0, len(all_rows), batch_size):
                                            batch = all_rows[i:i+batch_size]
                                            worksheet.append_rows(batch)
                                            print(f"Appended batch {i//batch_size + 1}/{(len(all_rows)-1)//batch_size + 1}")
                                    
                                    if not to_update and not to_add:
                                        print(f"No changes detected for sheet {sheet_name}")
                            
                            except Exception as e:
                                print(f"Error in update strategy: {e}")
                                print(traceback.format_exc())
                                # Fall back to rewrite if update fails
                                print(f"Falling back to rewrite strategy")
                                GoogleSheetsAdapter.save(spreadsheet, sheet_name, 'rewrite')
                    
                    except gspread.exceptions.WorksheetNotFound:
                        # For new worksheets, create it and use full write
                        print(f"Creating new worksheet {sheet_name}")
                        worksheet = google_spreadsheet.add_worksheet(
                            title=sheet_name, rows=1, cols=max(10, len(headers) if 'headers' in locals() else 10)
                        )
                        
                        # Initialize new worksheet
                        if sheet.data and isinstance(sheet.data, list) and sheet.data:
                            headers = list(sheet.data[0].keys())
                            worksheet.append_row(headers)
                            
                            # Prepare data for batch update
                            rows = []
                            for item in sheet.data:
                                row = [item.get(header, '') for header in headers]
                                rows.append(row)
                            
                            # Use batch update for efficiency
                            if rows:
                                # Send in batches
                                batch_size = 100
                                for i in range(0, len(rows), batch_size):
                                    batch = rows[i:i+batch_size]
                                    worksheet.append_rows(batch)
                                    print(f"Saved batch {i//batch_size + 1}/{(len(rows)-1)//batch_size + 1}")
                    
                except Exception as e:
                    print(f"Error saving sheet {sheet_name}: {e}")
                    print(f"Error details: {traceback.format_exc()}")
                    print(f"Failed operation: Saving data to worksheet '{sheet_name}'")
                    
                    # Try a simple retry once with rewrite strategy
                    try:
                        print(f"Attempting to retry saving sheet {sheet_name} with rewrite strategy...")
                        
                        # Use rewrite strategy for retry
                        GoogleSheetsAdapter.save(spreadsheet, sheet_name, 'rewrite')
                    except Exception as retry_error:
                        print(f"Retry also failed for sheet {sheet_name}: {retry_error}")
                        print(f"Retry error details: {traceback.format_exc()}")
        else:
            # Update all sheets
            for sheet_name in spreadsheet.sheets:
                GoogleSheetsAdapter.save(spreadsheet, sheet_name, mode)

    @staticmethod
    def _hash_record(record):
        """Create a simple hash of a record for comparison"""
        # Convert to string and hash
        record_str = str(sorted(record.items()))
        return hash(record_str)
        
    @staticmethod
    def _col_num_to_letter(col_num):
        """Convert column number (1-based) to column letter (A, B, C, ..., AA, AB, etc.)"""
        letters = ""
        while col_num > 0:
            col_num, remainder = divmod(col_num - 1, 26)
            letters = chr(65 + remainder) + letters
        return letters

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
    
    def update_fitbits_log(self, spreadsheet:Spreadsheet, fitbit_data: pl.DataFrame, reset_total_for_watches=None) -> bool:
        """
        Updates the Fitbit log files and sheets.
        - CSV file: Always append (full history)
        - "log" sheet: Replace with latest records only (one per watch)
        - "FitbitLog" sheet: Always append (full history)
        
        Args:
            fitbit_data (pl.DataFrame): DataFrame containing the watch data.
            reset_total_for_watches (list, optional): List of watch IDs that became inactive
                and should have their total failures reset.
                
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
        
        # Convert reset_total_for_watches to a set for faster lookups
        reset_watches = set(reset_total_for_watches or [])
        
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
            
            # Get previous log entries to keep track of failure counters
            # Create a map of watch ID to most recent log entry
            previous_log_entries = {}
            if not existing_df.is_empty():
                # Get the most recent entry for each watch
                for watch_id in existing_df.select("ID").unique().to_series():
                    watch_entries = existing_df.filter(pl.col("ID") == watch_id)
                    # Sort by lastCheck to get the most recent entry
                    if "lastCheck" in watch_entries.columns:
                        watch_entries = watch_entries.sort(pl.col("lastCheck"), descending=True)
                    # Get the first (most recent) entry as a dictionary
                    if not watch_entries.is_empty():
                        previous_log_entries[watch_id] = watch_entries.row(0, named=True)
            
            # Process each row from the Fitbit data
            new_log_entries = []
            latest_entries_by_watch = {}  # Dictionary to store latest entry per watch
            
            for row in fitbit_data.iter_rows(named=True):
                # Create watch ID for matching - try to use the same logic as in hourly_data_collection
                watch_id = str(row.get('id', row.get('deviceId', '')))
                if not watch_id and 'project' in row and 'name' in row:
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
                
                # Get previous log entry if available to keep track of failure counters
                prev_entry = previous_log_entries.get(watch_id, {})
                
                # Initialize failure counters with previous values or 0
                current_failed_sync = int(prev_entry.get("CurrentFailedSync", 0) or 0)
                total_failed_sync = int(prev_entry.get("TotalFailedSync", 0) or 0)
                current_failed_hr = int(prev_entry.get("CurrentFailedHR", 0) or 0)
                total_failed_hr = int(prev_entry.get("TotalFailedHR", 0) or 0)
                current_failed_sleep = int(prev_entry.get("CurrentFailedSleep", 0) or 0)
                total_failed_sleep = int(prev_entry.get("TotalFailedSleep", 0) or 0)
                current_failed_steps = int(prev_entry.get("CurrentFailedSteps", 0) or 0)
                total_failed_steps = int(prev_entry.get("TotalFailedSteps", 0) or 0)
                current_failed_battery = int(prev_entry.get("CurrentFailedBattary", 0) or 0)
                total_failed_battery = int(prev_entry.get("TotalFailedBattary", 0) or 0)
                
                # Reset total failures if this watch is in the reset list
                if watch_id in reset_watches:
                    print(f"Resetting total failure counters for watch {row.get('name', '')} (ID: {watch_id})")
                    total_failed_sync = 0
                    total_failed_hr = 0
                    total_failed_sleep = 0
                    total_failed_steps = 0
                    total_failed_battery = 0
                
                # Track success/failure for each data type
                sync_success = bool(row.get("syncDate"))
                hr_success = bool(row.get("HR"))
                sleep_success = bool(row.get("sleep_start"))
                steps_success = bool(row.get("steps"))
                battery_success = bool(row.get("battery"))
                
                # Update failure counters based on success/failure
                if sync_success:
                    current_failed_sync = 0  # Reset current count on success
                else:
                    current_failed_sync += 1
                    total_failed_sync += 1
                
                if hr_success:
                    current_failed_hr = 0  # Reset current count on success
                else:
                    current_failed_hr += 1
                    total_failed_hr += 1
                
                if sleep_success:
                    current_failed_sleep = 0  # Reset current count on success
                else:
                    current_failed_sleep += 1
                    total_failed_sleep += 1
                
                if steps_success:
                    current_failed_steps = 0  # Reset current count on success
                else:
                    current_failed_steps += 1
                    total_failed_steps += 1
                
                if battery_success:
                    current_failed_battery = 0  # Reset current count on success
                else:
                    current_failed_battery += 1
                    total_failed_battery += 1
                
                # Build the log entry with updated failure counters
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
                    "CurrentFailedSync": current_failed_sync,
                    "TotalFailedSync": total_failed_sync,
                    "CurrentFailedHR": current_failed_hr,
                    "TotalFailedHR": total_failed_hr,
                    "CurrentFailedSleep": current_failed_sleep,
                    "TotalFailedSleep": total_failed_sleep,
                    "CurrentFailedSteps": current_failed_steps,
                    "TotalFailedSteps": total_failed_steps,
                    "CurrentFailedBattary": current_failed_battery,
                    "TotalFailedBattary": total_failed_battery,
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
                    new_df = pl.DataFrame(list(latest_entries_by_watch.values()))

                    spreadsheet.update_sheet("log", new_df, strategy="replace")
                    # Use our improved save method with rewrite mode for this sheet
                    GoogleSheetsAdapter.save(spreadsheet, "log", mode="rewrite")
                    
                    print(f"Replaced log sheet with {len(latest_entries_by_watch)} latest records (one per watch)")
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
                
                # Update the FitbitLog sheet with only the NEW log entries
                # Instead of extending the data, we use our updated save method with append mode
                if new_log_entries:
                    # Create a new DataFrame with just the new entries
                    fitbit_log_df = pl.DataFrame(new_log_entries)
                    
                    # Update the sheet with only the new data
                    entity_sp.update_sheet("FitbitLog", fitbit_log_df, strategy="append")
                    
                    # Use our new save method with append mode to efficiently add only new records
                    print("Saving only new records to FitbitLog sheet using append mode...")
                    GoogleSheetsAdapter.save(entity_sp, "FitbitLog", mode="append")
                    print(f"Successfully appended {len(new_log_entries)} records to FitbitLog sheet")
                else:
                    print("No new data to append to FitbitLog sheet")
            except Exception as e:
                print(f"Error updating FitbitLog sheet: {e}")
                print(f"Error details: {traceback.format_exc()}")
                
            return True
        except Exception as e:
            print(f"Error in update_fitbits_log: {e}")
            print(f"Full error details: {traceback.format_exc()}")
            return False
    
    def prepare_log_entries(self, spreadsheet: Spreadsheet, fitbit_data: pl.DataFrame, reset_total_for_watches=None) -> List[dict]:
        """
        Process Fitbit data and prepare log entries without saving them.
        
        Args:
            spreadsheet (Spreadsheet): The spreadsheet to reference for previous entries
            fitbit_data (pl.DataFrame): DataFrame containing the watch data
            reset_total_for_watches (list, optional): List of watch IDs to reset counters for
            
        Returns:
            List[dict]: List of new log entries
        """
        # Get current time for timestamp
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Define expected columns for entries
        expected_columns = [
            "project", "watchName", "lastCheck", "lastSynced", "lastBattary", 
            "lastHR", "lastSleepStartDateTime", "lastSleepEndDateTime", "lastSteps", 
            "lastBattaryVal", "lastHRVal", "lastHRSeq", "lastSleepDur", "lastStepsVal",
            "CurrentFailedSync", "TotalFailedSync", "CurrentFailedHR", "TotalFailedHR",
            "CurrentFailedSleep", "TotalFailedSleep", "CurrentFailedSteps", "TotalFailedSteps",
            "CurrentFailedBattary", "TotalFailedBattary", "ID"
        ]
        
        # Convert reset_total_for_watches to a set for faster lookups
        reset_watches = set(reset_total_for_watches or [])
        
        # Get the FitbitLog sheet to check previous entries
        previous_log_entries = {}
        try:
            if "FitbitLog" in spreadsheet.sheets:
                log_sheet = spreadsheet.get_sheet("FitbitLog", "log")
                log_df = log_sheet.to_dataframe(engine="polars")
                
                if not log_df.is_empty() and "ID" in log_df.columns:
                    # Get the most recent entry for each watch
                    for watch_id in log_df.select("ID").unique().to_series():
                        watch_entries = log_df.filter(pl.col("ID") == watch_id)
                        # Sort by lastCheck to get the most recent entry
                        if "lastCheck" in watch_entries.columns:
                            watch_entries = watch_entries.sort(pl.col("lastCheck"), descending=True)
                        # Get the first (most recent) entry as a dictionary
                        if not watch_entries.is_empty():
                            previous_log_entries[watch_id] = watch_entries.row(0, named=True)
        except Exception as e:
            print(f"Error getting previous log entries: {e}")
            print(traceback.format_exc())
        
        # Process each row from the Fitbit data
        new_log_entries = []
        
        for row in fitbit_data.iter_rows(named=True):
            # Create watch ID for matching
            watch_id = str(row.get('id', row.get('deviceId', '')))
            if not watch_id and 'project' in row and 'name' in row:
                watch_id = f"{row.get('project', '')}-{row.get('name', '')}"
            
            # Skip inactive watches
            is_active = str(row.get('isActive', '')).upper() != 'FALSE'
            if not is_active:
                continue
            
            # Try to update watch data via API
            try:
                # Convert row data to a dict for the factory
                watch_data = {key: value for key, value in row.items()}
                
                # Create Watch object using the factory
                watch = WatchFactory.create_from_details(watch_data)
                
                # Update device information via Fitbit API
                print(f"Fetching latest data from Fitbit API for watch {watch.name}...")
                watch.update_device_info()
                
                # Check if essential attributes were updated
                if hasattr(watch, 'last_sync_time') and watch.last_sync_time:
                    # Update row with the fresh data from API
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
            
            # Get previous log entry if available to keep track of failure counters
            prev_entry = previous_log_entries.get(watch_id, {})
            
            # Initialize failure counters with previous values or 0
            current_failed_sync = int(prev_entry.get("CurrentFailedSync", 0) or 0)
            total_failed_sync = int(prev_entry.get("TotalFailedSync", 0) or 0)
            current_failed_hr = int(prev_entry.get("CurrentFailedHR", 0) or 0)
            total_failed_hr = int(prev_entry.get("TotalFailedHR", 0) or 0)
            current_failed_sleep = int(prev_entry.get("CurrentFailedSleep", 0) or 0)
            total_failed_sleep = int(prev_entry.get("TotalFailedSleep", 0) or 0)
            current_failed_steps = int(prev_entry.get("CurrentFailedSteps", 0) or 0)
            total_failed_steps = int(prev_entry.get("TotalFailedSteps", 0) or 0)
            current_failed_battery = int(prev_entry.get("CurrentFailedBattary", 0) or 0)
            total_failed_battery = int(prev_entry.get("TotalFailedBattary", 0) or 0)
            
            # Reset total failures if needed
            if watch_id in reset_watches:
                print(f"Resetting total failure counters for watch {row.get('name', '')} (ID: {watch_id})")
                total_failed_sync = 0
                total_failed_hr = 0
                total_failed_sleep = 0
                total_failed_steps = 0
                total_failed_battery = 0
            
            # Track success/failure for each data type
            sync_success = bool(row.get("syncDate"))
            hr_success = bool(row.get("HR"))
            sleep_success = bool(row.get("sleep_start"))
            steps_success = bool(row.get("steps"))
            battery_success = bool(row.get("battery"))
            
            # Update counters based on success/failure
            if sync_success:
                current_failed_sync = 0
            else:
                current_failed_sync += 1
                total_failed_sync += 1
            
            if hr_success:
                current_failed_hr = 0
            else:
                current_failed_hr += 1
                total_failed_hr += 1
            
            if sleep_success:
                current_failed_sleep = 0
            else:
                current_failed_sleep += 1
                total_failed_sleep += 1
            
            if steps_success:
                current_failed_steps = 0
            else:
                current_failed_steps += 1
                total_failed_steps += 1
            
            if battery_success:
                current_failed_battery = 0
            else:
                current_failed_battery += 1
                total_failed_battery += 1
            
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
                "CurrentFailedSync": current_failed_sync,
                "TotalFailedSync": total_failed_sync,
                "CurrentFailedHR": current_failed_hr,
                "TotalFailedHR": total_failed_hr,
                "CurrentFailedSleep": current_failed_sleep,
                "TotalFailedSleep": total_failed_sleep,
                "CurrentFailedSteps": current_failed_steps,
                "TotalFailedSteps": total_failed_steps,
                "CurrentFailedBattary": current_failed_battery,
                "TotalFailedBattary": total_failed_battery,
                "ID": watch_id
            }
            
            # Add to the list of entries
            new_log_entries.append(log_entry)
        
        return new_log_entries

    def update_log_sheet(self, spreadsheet: Spreadsheet, fitbit_data: pl.DataFrame, reset_total_for_watches=None) -> bool:
        """
        Updates only the log sheet with latest data (replacement strategy).
        
        Args:
            spreadsheet (Spreadsheet): The spreadsheet to update
            fitbit_data (pl.DataFrame): DataFrame containing the watch data
            reset_total_for_watches (list, optional): List of watch IDs to reset counters for
            
        Returns:
            bool: True if update succeeded, False otherwise
        """
        try:
            # Generate log entries
            new_log_entries = self.prepare_log_entries(spreadsheet, fitbit_data, reset_total_for_watches)
            
            if not new_log_entries:
                print("No active watch data to update log sheet")
                return True
                
            # For log sheet, we keep only the latest entry per watch
            latest_entries_by_watch = {}
            for entry in new_log_entries:
                watch_id = entry.get('ID', '')
                if watch_id:
                    latest_entries_by_watch[watch_id] = entry
            
            # Update the log sheet with the latest entries (replace strategy)
            log_df = pl.DataFrame(list(latest_entries_by_watch.values()))
            spreadsheet.update_sheet("log", log_df, strategy="replace")
            GoogleSheetsAdapter.save(spreadsheet, "log", mode="rewrite")
            
            print(f"Updated log sheet with {len(latest_entries_by_watch)} latest entries (one per watch)")
            return True
            
        except Exception as e:
            print(f"Error updating log sheet: {e}")
            print(f"Error details: {traceback.format_exc()}")
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
    def analyze_whatsapp_messages(bulldog_sheet:Sheet, alert_sheet:Sheet, hours_threshold=48):
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
                pl.col('time').str.strptime(pl.Datetime, format="%d/%m/%Y %H:%M", strict=False).alias('datetime')
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
            for row in recent_messages.select([ 'phone', 'time', 'hours_left']).head(10).iter_rows(named=True):
                report.append(f"-({row['phone']}): Sent {row['time']}, {row['hours_left']:.1f} hours left")
        
        report.append("")
        report.append(f"== Suspicious Numbers ==")
        report.append(f"Total: {suspicious_numbers.height} numbers")
        
        if suspicious_numbers.height > 0:
            report.append("\nSuspicious numbers (top 10):")
            for row in suspicious_numbers.head(10).iter_rows(named=True):
                report.append(f"- {row['phone']}")
        
        return "\n".join(report)