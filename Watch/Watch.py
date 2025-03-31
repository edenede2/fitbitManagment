from Spreadsheet_io.sheets import Spreadsheet
import requests
import json
import datetime

BASE_URL2 = "https://api.fitbit.com/1.2/user/-/"
BASE_URL = "https://api.fitbit.com/1/user/-/"
HEADERS = {"Accept": "application/json",
            "Authorization": "Bearer {}"}
URL_DICT = {
    'Sleep': "https://api.fitbit.com/1.2/user/-/sleep/date/{}/{}.json", # BASE_URL2, start_date, end_date
    'Steps': "https://api.fitbit.com/1.2/user/-/activities/steps/date/{}/{}.json", # BASE_URL2, start_date, end_date
    'Steps Intraday': "https://api.fitbit.com/1/user/-/activities/steps/date/{}/1d/1min/time/{}/{}.json", # BASE_URL, start_date, start_time, end_time
    'Sleep Levels': "https://api.fitbit.com/1.2/user/-/sleep/date/{}/{}.json", # BASE_URL2, start_date, end_date
    'Heart Rate Intraday': "https://api.fitbit.com/1.2/user/-/activities/heart/date/{}/1d/1sec/time/{}/{}.json", # BASE_URL2, start_date, start_time, end_time
    'Heart Rate': "https://api.fitbit.com/1/user/-/activities/heart/date/{}/{}.json", # BASE_URL, start_date, end_date
    'HRV Daily': "https://api.fitbit.com/1/user/-/hrv/date/{}/{}/all.json", # BASE_URL, start_date, end_date
    'HRV Intraday': "{}",
    'Sleep temp skin': "https://api.fitbit.com/1/user/-/temp/skin/date/{}/{}.json", # BASE_URL, start_date, end_date
    'Sleep temp': "https://api.fitbit.com/1/user/-/temp/core/date/{}.json", # BASE_URL, start_date
    'Daily RMSSD': "https://api.fitbit.com/1.2/user/-/hrv/date/{}/all.json", # BASE_URL2, start_date
    'ECG': 'https://api.fitbit.com/1.2/user/-/ecg/list.json?{} asc {} {}', # BASE_URL2, start_date, limit, offset
    'Breathing Rate': 'https://api.fitbit.com/1/user/-/br/date/{}/{}.json', # BASE_URL, start_date, end_date
    'device': 'https://api.fitbit.com/1.2/user/-/devices.json', # BASE_URL2
    'Activity_Time_Series': 'https://api.fitbit.com/1/user/-/spo2/date/{}/{}/all.json', # BASE_URL, start_date, end_date
    'Activity intraday': 'https://api.fitbit.com/1/user/-/activities/{}/date/{}/1m/time/{}/{}.json' # BASE_URL, start_date, start_time, end_time
}
    
SP = Spreadsheet.get_instance()

class Watch:
    def __init__(self,details=None):
        self.name = details.get('name', None) if details else None
        self.project = details.get('project', None) if details else None
        self.token = details.get('token', None) if details else None
        self.user = details.get('user', None) if details else ''
        self.header = get_headers(self.token)
        self.current_student = None

    def __str__(self):
        return f"Fitbit Watch: {self.name}, Project: {self.project}, User: {self.user}, Token: {self.token}"
    
    def __repr__(self):
        return f"Fitbit Watch: {self.name}, Project: {self.project}, User: {self.user}, Token: {self.token}"
    
    def __eq__(self, value):
        if isinstance(value, Watch):
            if self.name == value.name and self.project == value.project:
                return True
            else:
                return False
        else:
            return False
        
    def __ne__(self, value):
        if isinstance(value, Watch):
            if self.name != value.name or self.project != value.project:
                return True
            else:
                return False
        else:
            return True
        

    def get_name(self):
        return self.name
    
    def get_project(self):
        return self.project
    
    def get_token(self):
        return self.token
    
    def get_user(self):
        return self.user
    
    def get_header(self):
        return self.header
    
    def get_current_hourly_HR(self):
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        current_time = datetime.datetime.now()
        current_time_str = current_time.strftime("%H:%M")
        hour_ago = current_time - datetime.timedelta(hours=1)
        hour_ago_str = hour_ago.strftime("%H:%M")
        ihr_endpoint = URL_DICT["Heart Rate Intraday"].format(current_date, hour_ago_str, current_time_str)
        response = requests.get(ihr_endpoint, headers=self.header)
        if response.status_code == 200:
            data = response.json()
            if data['activities-heart-intraday']['dataset']:
                heart_rate = data['activities-heart-intraday']['dataset'][-1]['value']
                return heart_rate
            else:
                return None
        else:
            print(f"Error: {response.status_code}")
            return None
        
    def get_current_hourly_steps(self):
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        current_time = datetime.datetime.now()
        current_time_str = current_time.strftime("%H:%M")
        hour_ago = current_time - datetime.timedelta(hours=1)
        hour_ago_str = hour_ago.strftime("%H:%M")
        ihr_endpoint = URL_DICT["Steps Intraday"].format(current_date, hour_ago_str, current_time_str)
        response = requests.get(ihr_endpoint, headers=self.header)
        if response.status_code == 200:
            data = response.json()
            if data['activities-steps-intraday']['dataset']:
                steps = data['activities-steps-intraday']['dataset'][-1]['value']
                return steps
            else:
                return None
        else:
            print(f"Error: {response.status_code}")
            return None
        
    def get_current_battery(self):
        device_endpoint = URL_DICT["device"]
        response = requests.get(device_endpoint, headers=self.header)
        if response.status_code == 200:
            data = response.json()
            if data:
                battery_level = data[0]['batteryLevel']
                return battery_level
            else:
                return None
        else:
            print(f"Error: {response.status_code}")
            return None
        
    
    def get_last_sleep_start_end(self):
        previous_date = datetime.datetime.now() - datetime.timedelta(days=1)
        previous_date_str = previous_date.strftime("%Y-%m-%d")
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")

        sleep_endpoint = URL_DICT["Sleep"].format(previous_date_str, current_date)
        response = requests.get(sleep_endpoint, headers=self.header)

        if response.status_code == 200:
            data = response.json()
            if data['sleep']:
                sleep_start = data['sleep'][0]['startTime']
                sleep_end = data['sleep'][0]['endTime']
                return sleep_start, sleep_end
            else:
                return None, None
        else:
            print(f"Error: {response.status_code}")
            return None, None
        
    def get_last_sleep_duration(self):
        last_sleep_start, last_sleep_end = self.get_last_sleep_start_end()
    
        if last_sleep_start and last_sleep_end:
            start_time = datetime.datetime.strptime(last_sleep_start, "%Y-%m-%dT%H:%M:%S.%fZ")
            end_time = datetime.datetime.strptime(last_sleep_end, "%Y-%m-%dT%H:%M:%S.%fZ")
            duration = end_time - start_time
            return duration.total_seconds() / 3600
        else:
            return None
        
    
    



def get_headers(token):
    headers = HEADERS
    headers['Authorization'] = f"Bearer {token}"
    return headers
    

