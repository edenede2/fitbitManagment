import pickle
from pathlib import Path
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
# import pymongo as pm
from collections import defaultdict
import streamlit as st
import json

import streamlit_authenticator as stauth
# import streamlit_authenticator as stauth


# scopes = ["https://www.googleapis.com/auth/spreadsheets"]

# credentials = Credentials.from_service_account_info(
#     st.secrets["gcp_service_account"], scopes=scopes
# )

# client = gspread.authorize(credentials)

# # Open the Google Sheet
# spreadsheet = client.open_by_key("1HLJ9zzOCfaoTQHFHVcCKhLzqGkZZzOc8Z5jF4Q-iN_I")
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.file"
]

credentials = Credentials.from_service_account_info(
    st.secrets["gcp_service_account"], scopes=scopes
)

client = gspread.authorize(credentials)

# Add error handling
try:
    spreadsheet = client.open_by_key('1yCWeGfmGP5-RmZzlDspHA2ayJSFoIzar1TjhIaqtP_o')
    # names = spreadsheet.worksheet('users').get_all_records()
except PermissionError:
    st.error("Service account doesn't have permission to access this spreadsheet")
except Exception as e:
    st.error(f"An error occurred: {str(e)}")

names = [row['username'] for row in spreadsheet.get_worksheet(0).get_all_records()]
passwords = [row['pass'] for row in spreadsheet.get_worksheet(0).get_all_records()]

hashed_passwords = stauth.Hasher().hash_list(passwords)

users_dict = defaultdict(dict)
credentials = {'usernames': {}}

for i, row in enumerate(spreadsheet.get_worksheet(0).get_all_records()):
    username = row['username']
    credentials['usernames'][username] = username
    credentials['usernames'][username] = dict({'password': None, 'email': None, 'name': None})
    credentials['usernames'][username]['password'] = hashed_passwords[i]
    credentials['usernames'][username]['email'] = row['email']
    credentials['usernames'][username]['name'] = username

file_path = Path(__file__).parent / "users.json"

with file_path.open("w") as file:
    json.dump(credentials['usernames'], file, indent=4)

