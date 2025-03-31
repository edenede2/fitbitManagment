from Spreadsheet_io.sheets import Spreadsheet


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
    



def get_headers(token):
    headers = HEADERS
    headers['Authorization'] = f"Bearer {token}"
    return headers
    

