# Compatibility layer - define a direct alias instead of importing from entity.Sheet
# This breaks the circular dependency

# Define a simple Spreadsheet class that matches the expected interface
class Spreadsheet:
    _instance = None
    
    @classmethod
    def get_instance(cls):
        from entity.Sheet import LegacySpreadsheet
        if cls._instance is None:
            cls._instance = LegacySpreadsheet.get_instance()
        return cls._instance
    
    @classmethod
    def get_client(cls):
        return cls.get_instance().get_client()
    
    @classmethod
    def get_user_details(cls):
        return cls.get_instance().get_user_details()
    
    @classmethod
    def get_project_details(cls):
        return cls.get_instance().get_project_details()
    
    @classmethod
    def get_spreadsheet(cls):
        return cls.get_instance().get_spreadsheet()
    
    @classmethod
    def get_fitbits_details(cls):
        return cls.get_instance().get_fitbits_details()
    
    @classmethod
    def get_fitbits_log(cls):
        return cls.get_instance().get_fitbits_log()
    
    @classmethod
    def update_worksheet_3(cls, data_list):
        return cls.get_instance().update_worksheet_3(data_list)
    
    @classmethod
    def get_entity_spreadsheet(cls):
        return cls.get_instance().get_entity_spreadsheet()

# Add a warning about deprecation
import warnings

warnings.warn(
    "The Spreadsheet_io module is deprecated and will be removed in a future version. "
    "Please use entity.Sheet instead.",
    DeprecationWarning,
    stacklevel=2
)
