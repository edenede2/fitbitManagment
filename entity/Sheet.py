from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any, Callable, Type, Union
from collections import defaultdict
import pandas as pd
import polars as pl
from abc import ABC, abstractmethod

# Base Sheet class definition
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
            'generic': Sheet
        }
        
        if sheet_type not in sheet_types:
            raise ValueError(f"Unknown sheet type: {sheet_type}")
        
        return sheet_types[sheet_type](name=name, **kwargs)


@dataclass
class UserSheet(Sheet):
    """Sheet for storing user data"""
    schema: SheetSchema = field(default_factory=lambda: SheetSchema(
        columns=['id', 'name', 'email', 'role'],
        required_columns=['id', 'name']
    ))


@dataclass
class ProjectSheet(Sheet):
    """Sheet for storing project data"""
    schema: SheetSchema = field(default_factory=lambda: SheetSchema(
        columns=['id', 'name', 'description', 'start_date', 'end_date', 'status'],
        required_columns=['id', 'name']
    ))


@dataclass
class FitbitSheet(Sheet):
    """Sheet for storing Fitbit device data"""
    schema: SheetSchema = field(default_factory=lambda: SheetSchema(
        columns=['project', 'watchName', 'lastSynced', 'lastHR', 'lastHRVal', 'isActive'],
        required_columns=['project', 'watchName']
    ))


@dataclass
class LogSheet(Sheet):
    """Sheet for storing log data"""
    schema: SheetSchema = field(default_factory=lambda: SheetSchema(
        columns=['timestamp', 'event', 'details'],
        required_columns=['timestamp', 'event']
    ))


# Enhanced Spreadsheet with improved defaultdict handling
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


# Adapter for connecting with the Spreadsheet_io implementation
class GoogleSheetsAdapter:
    @staticmethod
    def connect(spreadsheet: Spreadsheet):
        """Connect the entity Spreadsheet with the actual Google Sheets API"""
        from Spreadsheet_io.sheets import Spreadsheet as APISpreadsheet
        
        # Get the client from the singleton Spreadsheet
        client = APISpreadsheet.get_client()
        
        # Use the client to fetch the actual spreadsheet
        google_spreadsheet = client.open_by_key(spreadsheet.api_key)
        
        # Map worksheets to Sheet objects
        for i, worksheet in enumerate(google_spreadsheet.worksheets()):
            sheet_name = worksheet.title
            records = worksheet.get_all_records()
            
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
            
            # Create and populate the sheet
            sheet = SheetFactory.create_sheet(sheet_type, sheet_name)
            sheet.data = records
            spreadsheet.sheets[sheet_name] = sheet
        
        return spreadsheet
    
    @staticmethod
    def save(spreadsheet: Spreadsheet, sheet_name: str = None):
        """Save changes back to Google Sheets"""
        from Spreadsheet_io.sheets import Spreadsheet as APISpreadsheet
        
        # Get the Google Sheets client
        api_spreadsheet = APISpreadsheet.get_instance().spreadsheet
        
        # If a specific sheet is provided, only update that one
        if sheet_name:
            if sheet_name in spreadsheet.sheets:
                sheet = spreadsheet.sheets[sheet_name]
                worksheet = api_spreadsheet.worksheet(sheet_name)
                
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
        else:
            # Update all sheets
            for sheet_name, sheet in spreadsheet.sheets.items():
                try:
                    worksheet = api_spreadsheet.worksheet(sheet_name)
                    
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
                except:
                    # Skip sheets that don't exist in the actual spreadsheet
                    pass



