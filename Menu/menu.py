import streamlit as st

from Spreadsheet_io.sheets import Spreadsheet

def getMenu(role):
    sp = Spreadsheet.get_instance()
    projects = sp.get_project_details()

    project_row = [project['project'] for project in projects if project['project'] == project]
    if project_row:
        project_row = project_row[0]
    else:
        project_row = None

    if role == 'Admin':
        menu = ['Homepage','Dashboard', 'Alert Management', 'Project Management', 'Generate QR', 'Statistics', 'About']
    elif role == 'Manager':
        menu = ['Homepage','Dashboard', 'Alert Management', 'Project Management', 'Generate QR', 'About']
    elif role == 'Student':
        menu = ['Homepage','Dashboard', 'Alert Management', 'Generate QR', 'About']
    else:
        menu = ['Homepage','About']

    choice = st.sidebar.selectbox(
        "Menu",
        menu,
        index=0,
        key="menu",
        label_visibility="collapsed",
    )
    return project_row, choice