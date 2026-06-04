#!/usr/bin/env python3
import requests
import pickle
from pathlib import Path
import pandas as pd
import numpy as np
import plotly.express as px
from io import BytesIO
import base64
import matplotlib.pyplot as plt
import datetime
from datetime import time
from datetime import datetime, timedelta
import json
import zipfile
from tempfile import TemporaryDirectory
from zipfile import ZipFile, ZIP_DEFLATED
import os
import time as ti
import polars as pl
import sys
import traceback
import argparse
from dotenv import load_dotenv

# Add project root to Python path if necessary
project_root = str(Path(__file__).parent)
if project_root not in sys.path:
    sys.path.append(project_root)

# Import entity components directly
from entity.Sheet import Spreadsheet, GoogleSheetsAdapter

# Configuration
FITBIT_ZIP_SAVE_PATH = Path("/media/psylab-6028/DATA1/FitbitData")
DOWNLOAD_TRACKING_FILE = Path(project_root) / "data" / "download_tracking.csv"
INITIAL_DOWNLOAD_DAYS = 1  # Number of days to download for first-time watches (24 hours)

global FILES_DICT
FILES_DICT = {'csv': [], 'json': []}

# Data type configurations for different endpoints
DATA_TYPE_CONFIGS = {
    'sleep': {
        'endpoint': 'Sleep',
        'resolution': 'monthly',  # Sleep data can be retrieved in 30-day chunks
        'max_days_per_request': 30,
        'folder': 'Sleep'
    },
    'steps': {
        'endpoint': 'Steps',
        'resolution': 'daily',
        'max_days_per_request': 1,
        'folder': 'Physical Activity'
    },
    'heart_rate': {
        'endpoint': 'Heart Rate Intraday',
        'resolution': 'daily',
        'max_days_per_request': 1,
        'folder': 'Physical Activity'
    },
    'hrv': {
        'endpoint': 'HRV Daily',
        'resolution': 'daily',
        'max_days_per_request': 1,
        'folder': 'Sleep'
    },
    'temperature': {
        'endpoint': 'Sleep temp skin',
        'resolution': 'monthly',
        'max_days_per_request': 30,
        'folder': 'Sleep'
    },
    'breathing_rate': {
        'endpoint': 'Breathing Rate',
        'resolution': 'monthly',
        'max_days_per_request': 30,
        'folder': 'Sleep'
    },
    'calories': {
        'endpoint': 'Activity intraday',
        'resolution': 'daily',
        'max_days_per_request': 1,
        'folder': 'Physical Activity'
    }
}


def load_download_tracking():
    """
    Load download tracking data from CSV file.
    This tracks the last download datetime for each watch and data type.

    Returns:
        pd.DataFrame: DataFrame with columns [watch_name, data_type, last_download_datetime]
    """
    if DOWNLOAD_TRACKING_FILE.exists():
        try:
            df = pd.read_csv(DOWNLOAD_TRACKING_FILE)

            # Handle legacy column name migration (date -> datetime)
            if 'last_download_date' in df.columns and 'last_download_datetime' not in df.columns:
                print("Migrating legacy download tracking format from date to datetime...")
                # Convert date strings to datetime strings (assume midnight)
                df['last_download_datetime'] = df['last_download_date'] + ' 00:00:00'
                df = df.drop('last_download_date', axis=1)
                # Save the migrated version
                save_download_tracking(df)

            # Ensure required columns exist
            required_columns = ['watch_name', 'data_type', 'last_download_datetime']
            for col in required_columns:
                if col not in df.columns:
                    df[col] = None
            return df
        except Exception as e:
            print(f"Error loading download tracking file: {e}")
            return pd.DataFrame(columns=['watch_name', 'data_type', 'last_download_datetime'])
    else:
        # Create initial tracking file
        return pd.DataFrame(columns=['watch_name', 'data_type', 'last_download_datetime'])

def save_download_tracking(tracking_df):
    """
    Save download tracking data to CSV file.

    Args:
        tracking_df (pd.DataFrame): DataFrame with download tracking data
    """
    try:
        # Create directory if it doesn't exist
        DOWNLOAD_TRACKING_FILE.parent.mkdir(parents=True, exist_ok=True)
        tracking_df.to_csv(DOWNLOAD_TRACKING_FILE, index=False)
        print(f"Download tracking saved to {DOWNLOAD_TRACKING_FILE}")
    except Exception as e:
        print(f"Error saving download tracking file: {e}")

def get_last_download_date(tracking_df, watch_name, data_type):
    """
    Get the last download datetime for a specific watch and data type.

    Args:
        tracking_df (pd.DataFrame): Download tracking DataFrame
        watch_name (str): Name of the watch
        data_type (str): Type of data (sleep, steps, etc.)

    Returns:
        datetime.datetime or None: Last download datetime or None if never downloaded
    """
    mask = (tracking_df['watch_name'] == watch_name) & (tracking_df['data_type'] == data_type)
    matches = tracking_df[mask]

    if len(matches) > 0 and pd.notna(matches.iloc[0]['last_download_datetime']):
        try:
            return pd.to_datetime(matches.iloc[0]['last_download_datetime'])
        except:
            return None
    return None

def update_download_tracking(tracking_df, watch_name, data_type, download_datetime):
    """
    Update download tracking for a specific watch and data type.

    Args:
        tracking_df (pd.DataFrame): Download tracking DataFrame
        watch_name (str): Name of the watch
        data_type (str): Type of data
        download_datetime (datetime.datetime): Datetime of download

    Returns:
        pd.DataFrame: Updated tracking DataFrame
    """
    mask = (tracking_df['watch_name'] == watch_name) & (tracking_df['data_type'] == data_type)

    # Convert datetime to string for storage
    datetime_str = download_datetime.strftime('%Y-%m-%d %H:%M:%S')

    if mask.any():
        # Update existing record
        tracking_df.loc[mask, 'last_download_datetime'] = datetime_str
    else:
        # Add new record
        new_row = pd.DataFrame({
            'watch_name': [watch_name],
            'data_type': [data_type],
            'last_download_datetime': [datetime_str]
        })
        tracking_df = pd.concat([tracking_df, new_row], ignore_index=True)

    return tracking_df

def get_active_watches():
    """
    Get active watches from the Google Sheets spreadsheet.
    Uses the same logic as run_data_collection.py

    Returns:
        pl.DataFrame: DataFrame containing active watch details with token information
    """
    try:
        # Load environment variables
        load_dotenv()

        # Get spreadsheet key from environment
        spreadsheet_key = os.getenv("SPREADSHEET_KEY")
        if not spreadsheet_key:
            raise ValueError("SPREADSHEET_KEY not found in environment variables")

        # Create spreadsheet instance
        spreadsheet = Spreadsheet(name="FitbitData", api_key=spreadsheet_key)
        GoogleSheetsAdapter.connect(spreadsheet)

        # Get the fitbit sheet
        fitbit_sheet = spreadsheet.get_sheet("fitbit", sheet_type="fitbit")

        # Convert to DataFrame and filter for active watches
        df = fitbit_sheet.to_dataframe(engine="pandas")

        # Filter for active watches (isActive != 'FALSE').
        # Google Sheets can return mixed bool/string values, so normalize first.
        if 'isActive' in df.columns:
            active_mask = df['isActive'].fillna('').astype(str).str.strip().str.upper() != 'FALSE'
            active_watches = df[active_mask].copy()
        else:
            active_watches = df.copy()

        print(f"Found {len(active_watches)} active watches")

        # Convert to polars DataFrame for return
        return pl.from_pandas(active_watches)

    except Exception as e:
        print(f"Error retrieving active watches: {e}")
        print(traceback.format_exc())
        return pl.DataFrame()

def calculate_date_ranges(start_date, end_date, max_days_per_request):
    """
    Calculate date ranges for API requests to respect rate limits.

    Args:
        start_date (datetime.date): Start date
        end_date (datetime.date): End date
        max_days_per_request (int): Maximum days per API request

    Returns:
        List[tuple]: List of (start_date, end_date) tuples
    """
    date_ranges = []
    current_start = start_date

    while current_start <= end_date:
        current_end = min(current_start + timedelta(days=max_days_per_request - 1), end_date)
        date_ranges.append((current_start, current_end))
        current_start = current_end + timedelta(days=1)

    return date_ranges

def getTempFiles(path, start_date, end_date, token):
    date_range = pd.date_range(start_date, end_date)

    SleepPath = Path(path).joinpath('Sleep')
    if not SleepPath.exists():
        SleepPath.mkdir()

    responses = []

    date_ranges_dict = {}

    if len(date_range) >= 30:
        if len(date_range) >= 60:
            start_date = date_range[0]
            end_date = date_range[30]
            date_range_1 = [start_date, end_date]

            start_date_1 = date_range[31]
            end_date_1 = date_range[60]
            date_range_2 = [start_date_1, end_date_1]

            start_date_2 = date_range[61]
            end_date_2 = date_range[-1]
            date_range_3 = [start_date_2, end_date_2]

            date_ranges_dict = {1: date_range_1, 2: date_range_2, 3: date_range_3}

        else:
            # Generate start and end dates that have a maximum of 30 days between them
            start_date = date_range[0]
            end_date = date_range[30]
            date_range_1 = [start_date, end_date]

            start_date_1 = date_range[31]
            end_date_1 = date_range[-1]
            date_range_2 = [start_date_1, end_date_1]

            date_ranges_dict = {1: date_range_1, 2: date_range_2}
    else:
        start_date = date_range[0]
        end_date = date_range[-1]
        date_ranges_dict = {1: [start_date, end_date]}

    for key, date_range in date_ranges_dict.items():
        start_date = date_range[0].strftime('%Y-%m-%d')
        end_date = date_range[1].strftime('%Y-%m-%d')

        url = f'https://api.fitbit.com/1/user/-/temp/skin/date/{start_date}/{end_date}.json'
        headers = {
            'Authorization': 'Bearer ' + token,
        }

        response = requests.get(url, headers=headers)

        responses.append(response)

        if response.status_code == 200:
            response = response.json()
            # create a csv file with the data
            for item in response['tempSkin']:
                timestamp = f"{item['dateTime']} 00:00"
                dailyTempDF = pd.DataFrame({'recorded_time': [timestamp] , 'temperature': [item['value']['nightlyRelative']], 'sensor_type': 'UNKNOWN'}, index=[0])
                dailyTempDF.to_csv(SleepPath.joinpath('Device Temperature - ' + item['dateTime'] + ' API.csv'), index=False)
                with open(SleepPath.joinpath('Device Temperature - ' + item['dateTime'] + ' API.csv'), 'w') as newFTemp:
                    FILES_DICT['csv'].append(newFTemp)


    return responses

def getHRVfiles(path, start_date, end_date, token):
    date_range = pd.date_range(start_date, end_date)

    SleepPath = Path(path).joinpath('Sleep')
    if not SleepPath.exists():
        SleepPath.mkdir()

    responses = []

    for date in date_range:
        newDfHRV = pd.DataFrame({'timestamp': [], 'rmssd': [], 'coverage': [], 'high_frequency': [], 'low_frequency': []})

        date = date.strftime('%Y-%m-%d')
        url = f'https://api.fitbit.com/1/user/-/hrv/date/{date}/all.json'
        headers = {
            'Authorization': 'Bearer ' + token,
        }

        response = requests.get(url, headers=headers)

        responses.append(response)

        if response.status_code == 200:
            response = response.json()
            # create a csv file with the data
            for item in response['hrv']:
                date = item['dateTime']
                for minute in item['minutes']:
                    timestamp = minute['minute'].replace('T', ' ')
                    timestamp = pd.to_datetime(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                    rmssd = minute['value']['rmssd']
                    coverage = minute['value']['coverage']
                    highFrequency = minute['value']['hf']
                    lowFrequency = minute['value']['lf']
                    new_row = pd.DataFrame({'timestamp': [timestamp], 'rmssd': [rmssd], 'coverage': [coverage], 'high_frequency': [highFrequency], 'low_frequency': [lowFrequency]})
                    newDfHRV = pd.concat([newDfHRV, new_row], ignore_index=True)
        else:
            if response.status_code == 403:
                print(f'Error 403: Forbidden - {date} from {path}')
            # print(response.json())
        newDfHRV.to_csv(SleepPath.joinpath('Heart Rate Variability Details - ' + date + '.csv'), index=False)
        with open(SleepPath.joinpath('Heart Rate Variability Details - ' + date + '.csv'), 'w') as newFHRV:
            FILES_DICT['csv'].append(newFHRV)

    return responses

def getfilesHR(path, start_date, end_date, token):
    date_range = pd.date_range(start_date, end_date)

    responses = []

    PhysicalActivityPath = Path(path).joinpath('Physical Activity')
    if not PhysicalActivityPath.exists():
        PhysicalActivityPath.mkdir()

    for date in date_range:
        date = date.strftime('%Y-%m-%d')
        url = f'https://api.fitbit.com/1.2/user/-/activities/heart/date/{date}/1d/1sec/time/00:00/23:59.json'
        headers = {
            'Authorization': 'Bearer ' + token,
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            response = response.json()
            responses.append(response)

            # open the json file
            if 'Request failed with status code 502' in response:
                continue
            # take the date from the json file
            date = response['activities-heart'][0]['dateTime']
            # create a new file
            newFile = 'api-heart_rate-' + date + '.json'
            # write the data to the new file [{"dateTime": "${date} ${data.activities-heart-intraday.dataset.time}", "value": {"bpm": ${data.activities-heart-intraday.dataset.value}, "confidence": 2}}]
            with open(PhysicalActivityPath.joinpath(newFile), 'w') as newFHR:
                newFHR.write('[{\n')
                for item in response['activities-heart-intraday']['dataset']:
                    newFHR.write('"dateTime": "' + date + ' ' + item['time'] + '", "value": {"bpm": ' + str(
                        item['value']) + ', "confidence": 2}\n')
                    newFHR.write('},{\n')

                newFHR.write('}]')

                FILES_DICT['json'].append(newFHR)

    return responses

def getfilesSteps(path, start_date, end_date, token):
    date_range = pd.date_range(start_date, end_date)

    steps_df = pd.DataFrame(columns=['timestamp', 'steps'])

    responses = []

    PhysicalActivityPath = Path(path).joinpath('Physical Activity')
    if not PhysicalActivityPath.exists():
        PhysicalActivityPath.mkdir()

    for date in date_range:
        date = date.strftime('%Y-%m-%d')
        url = f'https://api.fitbit.com/1/user/-/activities/steps/date/{date}/1d/1min/time/00:00/23:59.json'
        headers = {
            'Authorization': 'Bearer ' + token,
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            response = response.json()
            responses.append(response)

            # open the json file
            if 'Request failed with status code 502' in response:
                continue
            # take the date from the json file
            date = response['activities-steps'][0]['dateTime']
            # create a new file
            newFile = 'api-steps-' + date + '.json'
            # write the data to the new file [{"dateTime": "${date} ${data.activities-heart-intraday.dataset.time}", "value": {"bpm": ${data.activities-heart-intraday.dataset.value}, "confidence": 2}}]
            with open(PhysicalActivityPath.joinpath(newFile), 'w') as newF:
                newF.write('[{\n')
                for item in response['activities-steps-intraday']['dataset']:
                    newF.write('"dateTime": "' + date + ' ' + item['time'] + '", "value": ' + str(item['value']))
                    newF.write('\n},{\n')
                    new_row = pd.DataFrame({'timestamp': [date + ' ' + item['time']], 'steps': [item['value']]})
                    steps_df = pd.concat([steps_df, new_row], ignore_index=True)
                newF.write('\n}]')
                FILES_DICT['json'].append(newF)

    steps_df.to_csv(PhysicalActivityPath.joinpath('steps.csv'), index=False)

    return responses

def generateRespiratoryRateCSV(path, start_date, end_date, token):
    date_range = pd.date_range(start_date, end_date)


    responses = []

    SleepPath = Path(path).joinpath('Sleep')
    if not SleepPath.exists():
        SleepPath.mkdir()
    date_ranges_dict = {}

    if len(date_range) >= 30:
        if len(date_range) >= 60:
            start_date = date_range[0]
            end_date = date_range[30]
            date_range_1 = [start_date, end_date]

            start_date_1 = date_range[31]
            end_date_1 = date_range[60]
            date_range_2 = [start_date_1, end_date_1]

            start_date_2 = date_range[61]
            end_date_2 = date_range[-1]
            date_range_3 = [start_date_2, end_date_2]

            date_ranges_dict = {1: date_range_1, 2: date_range_2, 3: date_range_3}

        else:
            # Generate start and end dates that have a maximum of 30 days between them
            start_date = date_range[0]
            end_date = date_range[30]
            date_range_1 = [start_date, end_date]

            start_date_1 = date_range[31]
            end_date_1 = date_range[-1]
            date_range_2 = [start_date_1, end_date_1]

            date_ranges_dict = {1: date_range_1, 2: date_range_2}
    else:
        start_date = date_range[0]
        end_date = date_range[-1]
        date_ranges_dict = {1: [start_date, end_date]}

    for key, date_range in date_ranges_dict.items():
        start_date = date_range[0].strftime('%Y-%m-%d')
        end_date = date_range[1].strftime('%Y-%m-%d')
        url = f'https://api.fitbit.com/1/user/-/br/date/{start_date}/{end_date}/all.json'
        headers = {
            'Authorization': 'Bearer ' + token,
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            response = response.json()
            responses.append(response)

            # open the json file
            if 'Request failed with status code 502' in response:
                continue
            for item in response['br']:
                timestamp = f"{item['dateTime']} 0:00:00"
                dailyRespiratoryRateDF = pd.DataFrame({'timestamp': [timestamp] , 'daily_respiratory_rate': [item['value']['fullSleepSummary']['breathingRate']]}, index=[0])
                dailyRespiratoryRateDF.to_csv(SleepPath.joinpath('Daily Respiratory Rate Summary - ' + item['dateTime'] + '.csv'), index=False)
                with open(SleepPath.joinpath('Daily Respiratory Rate Summary - ' + item['dateTime'] + '.csv'), 'w') as newF:
                    FILES_DICT['csv'].append(newF)

    return responses

def getCalories(path, start_date, end_date, token):
    date_range = pd.date_range(start_date, end_date)

    responses = []

    calories_df = pd.DataFrame(columns=['timestamp', 'calories'])

    PhysicalActivityPath = Path(path).joinpath('Physical Activity')
    if not PhysicalActivityPath.exists():
        PhysicalActivityPath.mkdir()

    for date in date_range:
        date = date.strftime('%Y-%m-%d')
        url = f'https://api.fitbit.com/1/user/-/activities/calories/date/{date}/1d/1min/time/00:00/23:59.json'
        headers = {
            'Authorization': 'Bearer ' + token,
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            response = response.json()
            responses.append(response)

            # open the json file
            if 'Request failed with status code 502' in response:
                continue
            # take the date from the json file
            date = response['activities-calories'][0]['dateTime']
            # create a new file
            newFile = 'calories-' + date + '.json'
            # write the data to the new file [{"dateTime": "${date} ${data.activities-heart-intraday.dataset.time}", "value": {"bpm": ${data.activities-heart-intraday.dataset.value}, "confidence": 2}}]
            with open(PhysicalActivityPath.joinpath(newFile), 'w') as newF:
                newF.write('[{\n')
                for item in response['activities-calories-intraday']['dataset']:
                    newF.write('"dateTime": "' + date + ' ' + item['time'] + '", "value": ' + str(item['value']))
                    newF.write('\n},{\n')
                    new_row = pd.DataFrame({'timestamp': [date + ' ' + item['time']], 'calories': [item['value']]})
                    calories_df = pd.concat([calories_df, new_row], ignore_index=True)
                newF.write('\n}]')
                FILES_DICT['json'].append(newF)

    calories_df.to_csv(PhysicalActivityPath.joinpath('calories.csv'), index=False)

def getSleepfiles(path, start_date, end_date, token):
    date_range = pd.date_range(start_date, end_date)

    responses = []

    SleepPath = Path(path).joinpath('Sleep')
    if not SleepPath.exists():
        SleepPath.mkdir()

    date_ranges_dict = {}

    if len(date_range) >= 30:
        if len(date_range) >= 60:
            start_date = date_range[0]
            end_date = date_range[30]
            date_range_1 = [start_date, end_date]

            start_date_1 = date_range[31]
            end_date_1 = date_range[60]
            date_range_2 = [start_date_1, end_date_1]

            start_date_2 = date_range[61]
            end_date_2 = date_range[-1]
            date_range_3 = [start_date_2, end_date_2]

            date_ranges_dict = {1: date_range_1, 2: date_range_2, 3: date_range_3}

        else:
            # Generate start and end dates that have a maximum of 30 days between them
            start_date = date_range[0]
            end_date = date_range[30]
            date_range_1 = [start_date, end_date]

            start_date_1 = date_range[31]
            end_date_1 = date_range[-1]
            date_range_2 = [start_date_1, end_date_1]

            date_ranges_dict = {1: date_range_1, 2: date_range_2}
    else:
        start_date = date_range[0]
        end_date = date_range[-1]
        date_ranges_dict = {1: [start_date, end_date]}

    for key, date_range in date_ranges_dict.items():
        start_date = date_range[0].strftime('%Y-%m-%d')
        end_date = date_range[1].strftime('%Y-%m-%d')
        url = f'https://api.fitbit.com/1.1/user/-/sleep/date/{start_date}/{end_date}.json'
        headers = {
            'Authorization': 'Bearer ' + token,
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            response = response.json()
            responses.append(response)

            # open the json file

            if 'Request failed with status code 502' in response:
                continue
            if '492' in response:
                print(response)
                continue
            # save the data to a json file
            with open(SleepPath.joinpath('sleep-' + start_date + '.json'), 'w') as newF:
                json.dump(response, newF)
                FILES_DICT['json'].append(newF)

    return responses


def saveAsZIP(path, token, start_date, end_date,sub_name):
    with TemporaryDirectory() as temp_dir:
        # Nova131_token = 'eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiIyM1JXVEQiLCJzdWIiOiJCVDRHUEIiLCJpc3MiOiJGaXRiaXQiLCJ0eXAiOiJhY2Nlc3NfdG9rZW4iLCJzY29wZXMiOiJyc29jIHJzZXQgcm94eSBycHJvIHJudXQgcnNsZSByYWN0IHJyZXMgcmxvYyByd2VpIHJociBydGVtIiwiZXhwIjoxNzQ1MjQyMjYyLCJpYXQiOjE3MTM3MDYyNjZ9.wmP3VhhaoxoGxUEqALN284VW--DQpR7Tum37CdHLX7I'
        # Nova131_start_date = '2024-03-07'
        # Nova131_end_date = '2024-04-21'

        temp_dir = Path(temp_dir)
        sub_temp_path = temp_dir.joinpath(sub_name)
        if not sub_temp_path.exists():
            sub_temp_path.mkdir()
        sub_temp_path.joinpath('Sleep').mkdir()
        sub_temp_path.joinpath('Physical Activity').mkdir()
        sub_temp_path.joinpath('Stress').mkdir()

        sleep_responses_nova = getSleepfiles(sub_temp_path, start_date, end_date, token)
        steps_responses_nova = getfilesSteps(sub_temp_path, start_date, end_date, token)
        HR_responses_nova = getfilesHR(sub_temp_path, start_date, end_date, token)
        Respiratory_responses_nova = generateRespiratoryRateCSV(sub_temp_path, start_date, end_date, token)
        HRV_responses_nova = getHRVfiles(sub_temp_path, start_date, end_date, token)
        Temp_responses_nova = getTempFiles(sub_temp_path, start_date, end_date, token)

        # Create a ZipFile Object
        with ZipFile(path.joinpath(f'{sub_name}.zip'), 'w', compression=ZIP_DEFLATED) as zipObj:
            for folderName, subfolders, filenames in os.walk(temp_dir):
                for filename in filenames:
                    filePath = os.path.join(folderName, filename)
                    zipObj.write(filePath, os.path.relpath(filePath, temp_dir))


def clean_steps(physical_activity_path):
    # read the steps and calories dataframes that generated from the API
    steps_df = pd.read_csv(physical_activity_path.joinpath('Physical Activity').joinpath('steps.csv'))
    steps_df['timestamp'] = pd.to_datetime(steps_df['timestamp'])

    # read the calories dataframe that generated from the API
    calories_df = pd.read_csv(physical_activity_path.joinpath('Physical Activity').joinpath('calories.csv'))
    calories_df['timestamp'] = pd.to_datetime(calories_df['timestamp'])

    # merge the two dataframes on the timestamp column
    merged_df = pd.merge(steps_df, calories_df, on='timestamp', how='outer')

    # save the merged dataframe to a csv file
    merged_df.to_csv(physical_activity_path.joinpath('Physical Activity').joinpath('merged.csv'), index=False)

    min_calories_per_date = {}


    merged_df['date'] = merged_df['timestamp'].dt.date

    for date in merged_df['date'].unique():
        min_calories_per_date[date] = merged_df[merged_df['date'] == date]['calories'].min()

    for index, row in merged_df.iterrows():
        date = row['date']
        if row['calories'] == min_calories_per_date[date] and row['steps'] == 0:
            merged_df.at[index, 'steps'] = np.nan
            merged_df.at[index, 'timestamp'] = np.nan



    # get new steps json file
    new_steps_df = pd.DataFrame(columns=['timestamp', 'steps'])
    for index, row in merged_df.iterrows():
        new_row = pd.DataFrame({'timestamp': [row['timestamp']], 'steps': [row['steps']]})
        new_steps_df = pd.concat([new_steps_df, new_row], ignore_index=True)

    # save the new steps json file
    new_steps = []
    for index, row in new_steps_df.iterrows():
        if pd.notna(row['timestamp']) and pd.notna(row['steps']):
            new_steps.append({
                "dateTime": row['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                "value": row['steps']
            })

    with open(physical_activity_path.joinpath('api_steps-2024-04-27.json'), 'w') as newF:
        newF.write('[\n')
        newF.write(',\n'.join([str(step).replace("'", '"') for step in new_steps]))
        newF.write('\n]')


def append_to_existing_zip(existing_zip_path, new_data_path):
    """
    Append new data to an existing ZIP file or create a new one.

    Args:
        existing_zip_path (Path): Path to existing ZIP file
        new_data_path (Path): Path to new data directory to add
    """
    try:
        if existing_zip_path.exists():
            # Extract existing ZIP to temporary directory
            with TemporaryDirectory() as temp_extract_dir:
                temp_extract_path = Path(temp_extract_dir)

                # Extract existing ZIP
                with ZipFile(existing_zip_path, 'r') as existing_zip:
                    existing_zip.extractall(temp_extract_path)

                # Copy new data to extracted directory
                import shutil
                for item in new_data_path.rglob('*'):
                    if item.is_file():
                        # Calculate relative path and target path
                        rel_path = item.relative_to(new_data_path)
                        target_path = temp_extract_path / rel_path

                        # Create directory if needed
                        target_path.parent.mkdir(parents=True, exist_ok=True)

                        # Copy or overwrite file
                        shutil.copy2(item, target_path)

                # Create new ZIP with merged content
                with ZipFile(existing_zip_path, 'w', compression=ZIP_DEFLATED) as new_zip:
                    for file_path in temp_extract_path.rglob('*'):
                        if file_path.is_file():
                            arc_path = file_path.relative_to(temp_extract_path)
                            new_zip.write(file_path, arc_path)

            print(f"Appended new data to existing ZIP: {existing_zip_path}")
        else:
            # Create new ZIP file
            with ZipFile(existing_zip_path, 'w', compression=ZIP_DEFLATED) as new_zip:
                for file_path in new_data_path.rglob('*'):
                    if file_path.is_file():
                        arc_path = file_path.relative_to(new_data_path)
                        new_zip.write(file_path, arc_path)
            print(f"Created new ZIP file: {existing_zip_path}")

    except Exception as e:
        print(f"Error managing ZIP file {existing_zip_path}: {e}")
        print(traceback.format_exc())

def download_watch_data(watch_name, token, data_type, start_date, end_date):
    """
    Download data for a specific watch and data type.

    Args:
        watch_name (str): Name of the watch
        token (str): Fitbit API token
        data_type (str): Type of data to download
        start_date (datetime.date): Start date for data
        end_date (datetime.date): End date for data

    Returns:
        tuple: (temp_dir_object, temp_path) or (None, None) if failed
    """
    try:
        config = DATA_TYPE_CONFIGS.get(data_type)
        if not config:
            print(f"Unknown data type: {data_type}")
            return None, None

        # Create temporary directory for this download
        temp_dir = TemporaryDirectory()
        temp_path = Path(temp_dir.name)

        # Create subdirectories
        watch_path = temp_path / watch_name
        watch_path.mkdir(parents=True, exist_ok=True)

        folder_path = watch_path / config['folder']
        folder_path.mkdir(parents=True, exist_ok=True)

        if data_type == 'sleep':
            folder_path.mkdir(parents=True, exist_ok=True)

        # Calculate date ranges to respect API limits
        date_ranges = calculate_date_ranges(start_date, end_date, config['max_days_per_request'])

        success = False
        for range_start, range_end in date_ranges:
            try:
                if data_type == 'sleep':
                    responses = getSleepfiles(watch_path, range_start.strftime('%Y-%m-%d'),
                                            range_end.strftime('%Y-%m-%d'), token)
                elif data_type == 'steps':
                    responses = getfilesSteps(watch_path, range_start.strftime('%Y-%m-%d'),
                                            range_end.strftime('%Y-%m-%d'), token)
                elif data_type == 'heart_rate':
                    responses = getfilesHR(watch_path, range_start.strftime('%Y-%m-%d'),
                                          range_end.strftime('%Y-%m-%d'), token)
                elif data_type == 'hrv':
                    responses = getHRVfiles(watch_path, range_start.strftime('%Y-%m-%d'),
                                           range_end.strftime('%Y-%m-%d'), token)
                elif data_type == 'temperature':
                    responses = getTempFiles(watch_path, range_start.strftime('%Y-%m-%d'),
                                            range_end.strftime('%Y-%m-%d'), token)
                elif data_type == 'breathing_rate':
                    responses = generateRespiratoryRateCSV(watch_path, range_start.strftime('%Y-%m-%d'),
                                                          range_end.strftime('%Y-%m-%d'), token)
                elif data_type == 'calories':
                    responses = getCalories(watch_path, range_start.strftime('%Y-%m-%d'),
                                          range_end.strftime('%Y-%m-%d'), token)

                # Check if any response was successful
                # Since responses contain processed JSON data (not HTTP response objects),
                # we check for non-empty responses with valid data structure and no error messages
                print(f"  Responses received: {len(responses) if responses else 0}")
                if responses:
                    print(f"  Sample response keys: {list(responses[0].keys()) if isinstance(responses[0], dict) else 'Not a dict'}")

                if responses and any(
                    isinstance(r, dict) and
                    len(r) > 0 and
                    'Request failed with status code' not in str(r) and
                    '492' not in str(r)
                    for r in responses
                ):
                    success = True
                    print(f"  ✓ Successfully downloaded {data_type} data for {watch_name}")
                else:
                    print(f"  ✗ No valid data received for {data_type}")
                    if responses:
                        print(f"    Response sample: {str(responses[0])[:200]}...")

                # Small delay to respect rate limits
                ti.sleep(1)

            except Exception as e:
                print(f"Error downloading {data_type} data for {watch_name} from {range_start} to {range_end}: {e}")
                continue

        if success:
            return temp_dir, temp_path
        else:
            temp_dir.cleanup()
            return None, None

    except Exception as e:
        print(f"Error in download_watch_data for {watch_name}, {data_type}: {e}")
        print(traceback.format_exc())
        return None, None

def process_watch_data_downloads(watch_row, tracking_df, initial_days=None):
    """
    Process data downloads for a single watch.

    Args:
        watch_row (dict): Watch information from the spreadsheet
        tracking_df (pd.DataFrame): Download tracking DataFrame
        initial_days (int, optional): Number of days for initial download. If None, uses INITIAL_DOWNLOAD_DAYS

    Returns:
        pd.DataFrame: Updated tracking DataFrame
    """
    watch_name = watch_row.get('name', '')
    token = watch_row.get('token', '')

    if not watch_name or not token:
        print(f"Missing name or token for watch: {watch_row}")
        return tracking_df

    print(f"Processing downloads for watch: {watch_name}")

    # Create watch-specific ZIP path
    zip_path = FITBIT_ZIP_SAVE_PATH / f"{watch_name}_fitbit_data.zip"

    current_date = datetime.now().date()

    # Use provided initial_days or default configuration
    if initial_days is None:
        initial_days = INITIAL_DOWNLOAD_DAYS

    # Process each data type
    for data_type in DATA_TYPE_CONFIGS.keys():
        try:
            # Get last download datetime for this data type
            last_download_dt = get_last_download_date(tracking_df, watch_name, data_type)

            if last_download_dt is None:
                # First time download - download from last N days only to minimize API usage
                start_date = current_date - timedelta(days=initial_days)
                print(f"First download for {watch_name} {data_type}: from {start_date} to {current_date} ({initial_days}d initial)")
            else:
                # Incremental download - download from last download date + 1 to now
                # Convert datetime to date for date range calculation
                last_download_date = last_download_dt.date()
                start_date = last_download_date + timedelta(days=1)
                print(f"Incremental download for {watch_name} {data_type}: from {start_date} to {current_date}")

            # Skip if no new data to download
            if start_date > current_date:
                print(f"No new data to download for {watch_name} {data_type}")
                continue

            # Download the data
            temp_dir, temp_data_path = download_watch_data(watch_name, token, data_type, start_date, current_date)

            if temp_data_path and temp_data_path.exists():
                # Ensure ZIP save directory exists
                FITBIT_ZIP_SAVE_PATH.mkdir(parents=True, exist_ok=True)

                # Append to existing ZIP or create new one
                append_to_existing_zip(zip_path, temp_data_path)

                # Update tracking with current datetime
                current_datetime = datetime.now()
                tracking_df = update_download_tracking(tracking_df, watch_name, data_type, current_datetime)

                print(f"Successfully downloaded and saved {data_type} data for {watch_name}")

                # Clean up temporary directory after successful processing
                if temp_dir:
                    temp_dir.cleanup()
            else:
                print(f"Failed to download {data_type} data for {watch_name}")
                # temp_dir is already cleaned up in download_watch_data on failure

        except Exception as e:
            print(f"Error processing {data_type} for {watch_name}: {e}")
            print(traceback.format_exc())
            continue

    return tracking_df

def hourly_fitbit_data_collection(initial_days=None):
    """
    Main function for hourly Fitbit data collection.
    This function runs every hour via cronjob.

    Args:
        initial_days (int, optional): Number of days for initial download. If None, uses INITIAL_DOWNLOAD_DAYS
    """
    print(f"[{datetime.now()}] Starting hourly Fitbit data collection...")

    if initial_days is not None:
        print(f"Using custom initial download period: {initial_days} days")
    else:
        print(f"Using default initial download period: {INITIAL_DOWNLOAD_DAYS} days")

    try:
        # Load download tracking
        tracking_df = load_download_tracking()
        print(f"Loaded download tracking with {len(tracking_df)} records")

        # Get active watches from spreadsheet
        active_watches_df = get_active_watches()

        if active_watches_df.is_empty():
            print("No active watches found")
            return

        print(f"Found {len(active_watches_df)} active watches")

        # Process each active watch
        for watch_row in active_watches_df.iter_rows(named=True):
            try:
                tracking_df = process_watch_data_downloads(watch_row, tracking_df, initial_days)

                # Save tracking after each watch to preserve progress
                save_download_tracking(tracking_df)

                # Small delay between watches to respect rate limits
                ti.sleep(2)

            except Exception as e:
                print(f"Error processing watch {watch_row.get('name', 'unknown')}: {e}")
                print(traceback.format_exc())
                continue

        # Final save of tracking data
        save_download_tracking(tracking_df)

        print(f"[{datetime.now()}] Hourly Fitbit data collection completed successfully")

    except Exception as e:
        print(f"Error during hourly Fitbit data collection: {e}")
        print(traceback.format_exc())

def main():
    """Entry point function for cronjob execution"""
    parser = argparse.ArgumentParser(description='Fitbit Data Organizer - Downloads data from active watches')
    parser.add_argument('--initial-days', type=int, default=None,
                        help=f'Number of days to download for first-time watches (default: {INITIAL_DOWNLOAD_DAYS})')
    parser.add_argument('--test-mode', action='store_true',
                        help='Run in test mode with minimal downloads')

    args = parser.parse_args()

    try:
        # Set initial days based on arguments
        initial_days = args.initial_days

        # In test mode, use very small download window
        if args.test_mode:
            initial_days = 1  # Only 1 day for testing
            print("Running in TEST MODE - downloading minimal data")

        hourly_fitbit_data_collection(initial_days)
    except Exception as e:
        print(f"[{datetime.now()}] Error during Fitbit data collection process:")
        print(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()

# ===== OLD CODE BELOW FOR REFERENCE (NOT EXECUTED) =====
# The code below is kept for reference but not executed in the new system
