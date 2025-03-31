import streamlit as st
import gspread
from google.oauth2.service_account import Credentials

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
