from typing import Dict, List, Optional
import pandas as pd
from entity.Sheet import Spreadsheet, GoogleSheetsAdapter
import streamlit as st
class UserController:
    """Controller for user-related operations"""
    
    def __init__(self):
        """Initialize the user controller"""
        self.spreadsheet_key = st.secrets.get("spreadsheet_key", "")
        
    def get_all_users(self) -> pd.DataFrame:
        """Get all users from the spreadsheet"""
        try:
            # Create Spreadsheet instance
            spreadsheet = Spreadsheet(name="Fitbit Database", api_key=self.spreadsheet_key)
            GoogleSheetsAdapter.connect(spreadsheet)
            
            # Get user sheet
            user_sheet = spreadsheet.get_sheet("user", sheet_type="user")
            return user_sheet.to_dataframe()
        except Exception as e:
            print(f"Error getting users: {e}")
            return pd.DataFrame()
    
    def get_user_by_email(self, email: str) -> Optional[Dict]:
        """Get a user by email"""
        users_df = self.get_all_users()
        if users_df.empty:
            return None
            
        user_data = users_df[users_df['email'] == email]
        if len(user_data) > 0:
            return user_data.iloc[0].to_dict()
        return None
    
    def get_users_by_role(self, role: str) -> pd.DataFrame:
        """Get users by role"""
        users_df = self.get_all_users()
        if users_df.empty:
            return pd.DataFrame()
            
        return users_df[users_df['role'] == role]
    
    def get_users_by_project(self, project: str) -> pd.DataFrame:
        """Get users associated with a project"""
        users_df = self.get_all_users()
        if users_df.empty:
            return pd.DataFrame()
            
        # Filter users whose projects field contains the specified project
        # This assumes projects may be stored as a comma-separated list
        return users_df[users_df['projects'].str.contains(project, na=False)]
