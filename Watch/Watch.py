from Spreadsheet_io.sheets import Spreadsheet


BASE_URL2 = "https://api.fitbit.com/1.2/user/-/"
BASE_URL = "https://api.fitbit.com/1/user/-/"
HEADERS = {"Accept": "application/json",
            "Authorization": "Bearer {}"}
URL_DICT = {
    'Sleep': "{}sleep/date/{}/{}.json", # BASE_URL2, start_date, end_date
    'Steps': "{}activities/steps/date/{}/{}.json", # BASE_URL2, start_date, end_date
    'Steps Intraday': "{}activities/steps/date/{}/1d/1min/time/{}/{}.json", # BASE_URL, start_date, start_time, end_time
    'Sleep Levels': "{}sleep/date/{}/{}.json", # BASE_URL2, start_date, end_date
    'Heart Rate Intraday': "{}activities/heart/date/{}/1d/1sec/time/{}/{}.json", # BASE_URL2, start_date, start_time, end_time
    'Heart Rate': "{}activities/heart/date/{}/{}.json", # BASE_URL, start_date, end_date
    'HRV Daily': "{}hrv/date/{}/{}/all.json", # BASE_URL, start_date, end_date
    'HRV Intraday': "{}",
    'Sleep temp skin': "{}temp/skin/date/{}/{}.json", # BASE_URL, start_date, end_date
    'Sleep temp': "{}temp/core/date/{}.json", # BASE_URL, start_date
    'Daily RMSSD': "{}hrv/date/{}/all.json", # BASE_URL2, start_date
    'ECG': '{}ecg/list.json?{} asc {} {}', # BASE_URL2, start_date, limit, offset
    'Breathing Rate': '{}br/date/{}/{}.json', # BASE_URL, start_date, end_date
    'device': '{}devices.json', # BASE_URL2
    'Activity_Time_Series': '{}spo2/date/{}/{}/all.json', # BASE_URL, start_date, end_date
    'Activity intraday': '{}activities/{}/date/{}/1m/time/{}/{}.json' # BASE_URL, start_date, start_time, end_time
}
    
SP = Spreadsheet.get_instance()

class Watch:
    def __init__(self):
        self.name = None
        self.project = None
        self.token = None
        self.students = []
        self.current_student = None
        self.start_date = None
        self.end_date = None
        self.start_time = None
        self.end_time = None

    def set_name(self, name):
        self.name = name
        return
    
    def set_details(self):
        fitbit_records = SP.get_fitbits_details()
        self.token = 