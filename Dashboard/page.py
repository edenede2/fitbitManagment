import streamlit as st

from Spreadsheet_io.sheets import Spreadsheet
from entity.Watch import Watch


def getBoardForWatch(watch: Watch):
    pass


def presentDashbord(project, role, name):
    st.title("Dashboard")

    sp = Spreadsheet.get_instance()
    watches_details = sp.get_watch_details()




    



# def main():
#     pass


# if __name__ == "__main__":
#     main()