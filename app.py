import pickle
import streamlit as st
from pathlib import Path
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import streamlit_authenticator as stauth
from collections import defaultdict
from pymongo import MongoClient
import json
# import cProfiler

from Decorators.congrates import congrats
from Menu.menu import getMenu
from Spreadsheet_io.sheets import Spreadsheet


st.set_page_config(
    page_title="Generate Key",
    page_icon="🔑",
    layout="wide",
)


def load_data():
    users_dict = defaultdict(dict)
    credentials = {'usernames': {}}
    # spreadsheet = get_spreadsheet(client)

    if 'role' not in st.session_state:
        st.session_state['role'] = None
    if 'project' not in st.session_state:
        st.session_state['project'] = None

    sp = Spreadsheet.get_instance()
    user_details = sp.get_user_details()

    hashed_passwords = st.secrets.app_users.passwords
    


    for i,row in enumerate(user_details):
        username = row['username']
        credentials['usernames'][username] = username
        credentials['usernames'][username] = dict({'password': None, 'email': None, 'name': None})
        credentials['usernames'][username]['password'] = hashed_passwords[i]
        credentials['usernames'][username]['email'] = row['email']
        credentials['usernames'][username]['name'] = username
        users_dict[username]['role'] = row['role']
        users_dict[username]['project'] = row['project']
        

    return users_dict, credentials


def login_user(credentials):
    # authenticator = stauth.Authenticate(
    #     credentials,
    #     cookie_name = "fitbit_users",
    #     cookie_key = "abcdef",
    #     cookie_expiry_days=30,
    #     auto_hash=True,
    #     api_key=st.secrets["api_key"]
    # )
    # try:
    #     authenticator.login(location="sidebar")
    # except Exception as e:
        # st.error(e)
    if "username" not in st.session_state:
        st.session_state["username"] = None
    if "email" not in st.session_state:
        st.session_state["email"] = None
    if "role" not in st.session_state:
        st.session_state["role"] = None
    if "login" not in st.session_state:
        st.session_state["login"] = False if 'is_logged_in' not in st.session_state else st.session_state['is_logged_in']

    # st.json(st.session_state)

    if  "is_logged_in" not in st.experimental_user:
        if st.experimental_user["is_logged_in"] == False:
            if st.sidebar.button("Login with Google"):
            
                st.login('google')

    if 'email' in st.experimental_user.keys():
        if st.experimental_user['email'] == 'test@example.com':
            st.warning('Please login with your Google account')
        else:    
            st.json(st.experimental_user)
            if st.experimental_user.is_logged_in:
                
                logout_button = st.sidebar.button("Logout")
                if logout_button:
                    st.session_state["login"] = False
                    st.logout()
                    

            elif st.experimental_user.is_logged_in is None:
                st.warning('Please enter your username and password')
    

def main():
    
    users_dict, credentials = load_data()
    login_user(credentials)
    if "authentication_status" not in st.session_state:
        st.session_state["authentication_status"] = None

    if  "is_logged_in" not in st.experimental_user:
        st.write("Or continue as guest")
        st.title("Home Page")
        congrate = congrats().format('Guest')
        st.session_state["role"] = 'Guest'
        st.session_state["project"] = 'Guest'
        st.title(congrate)
        st.divider()
        st.write("Welcome to the fitbit management system")
        st.write("What would you like to do today?")
        projectAc, choice = getMenu(
            'Guest'
        )
    else:
        if st.experimental_user["is_logged_in"] == None or False:
            # st.title("Home Page")
            # st.write("Welcome to the fitbit management system")
            # st.write("Please login open the sidebar to access the app")
            # projectAc, choice = getMenu(
            #     'Guest'
            # )
            st.write("Or continue as guest")
            st.title("Home Page")
            congrate = congrats().format('Guest')
            st.session_state["role"] = 'Guest'
            st.session_state["project"] = 'Guest'
            st.title(congrate)
            st.divider()
            st.write("Welcome to the fitbit management system")
            st.write("What would you like to do today?")
            projectAc, choice = getMenu(
                'Guest'
            )
            pass
        else:
            congrate = congrats().format(st.experimental_user['name'])
            st.session_state["role"] = users_dict[st.experimental_user['name']]['role']
            st.session_state["project"] = users_dict[st.experimental_user['name']]['project']
            st.title(congrate)
            st.divider()

            st.write("Welcome to the fitbit management system")
            st.write("What would you like to do today?")
            
            # Get the spreadsheet client for the menu
            spreadsheet = Spreadsheet.get_instance()
            
            projectAc, choice = getMenu(
                users_dict[st.experimental_user["name"]]['role'],
            )

            if choice == "Dashboard":
                pass
            elif choice == "Alert Management":
                pass
            elif choice == "Project Management":
                pass
            elif choice == "Generate QR":
                pass
            elif choice == "Statistics":
                pass
            elif choice == "About":
                pass




if __name__ == "__main__":
    main()
    # cProfiler.run("main()") 

