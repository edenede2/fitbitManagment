import requests
import json
import datetime
import pandas as pd
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any, Union, Callable, TypeVar, TYPE_CHECKING
from enum import Enum
from abc import ABC, abstractmethod
from entity.User import User
import streamlit as st
# Use string references for Project to avoid circular imports
if TYPE_CHECKING:
    from entity.Project import Project

# Constants - preserve existing URL definitions
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


# ===== Data Types and Enums =====

class DataType(Enum):
    """Enum for different types of Fitbit data"""
    HEART_RATE = "heart_rate"
    STEPS = "steps"
    SLEEP = "sleep"
    BATTERY = "battery"
    DEVICE_INFO = "device_info"
    HRV = "hrv"
    SPO2 = "spo2"
    TEMPERATURE = "temperature"
    ECG = "ecg"
    BREATHING_RATE = "breathing_rate"
    ACTIVITY = "activity"

class TimeRange(Enum):
    """Enum for different time ranges for data retrieval"""
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    CUSTOM = "custom"

# ===== API Request Builder Pattern =====

class RequestBuilder:
    """Builder pattern for creating API requests"""
    
    def __init__(self, endpoint_type: str, token: str):
        self.endpoint_type = endpoint_type
        if hasattr(token, 'item') and callable(token.item):
            self.token = token.item()
        elif isinstance(token, str):
            self.token = token
        else:
            self.token = str(token)
        self.headers = get_headers(self.token)
        self.params = {}
        self.url = None
        self.date_format = "%Y-%m-%d"
        self.time_format = "%H:%M"
        
    def with_date_range(self, start_date: Union[datetime.datetime, str], end_date: Union[datetime.datetime, str] = None) -> 'RequestBuilder':
        """Set date range for the request with proper formatting"""
        # st.write(f"start_date: {start_date}")
        # st.write(f"start_date type: {type(start_date)}")
        if isinstance(start_date, datetime.datetime):
            self.params['start_date'] = start_date.strftime(self.date_format)
        elif isinstance(start_date, str):
            self.params['start_date'] = start_date
        elif isinstance(start_date, datetime.date):
            self.params['start_date'] = start_date.strftime(self.date_format)
            
        if end_date:
            if isinstance(end_date, datetime.datetime):
                self.params['end_date'] = end_date.strftime(self.date_format)
            elif isinstance(end_date, str):
                self.params['end_date'] = end_date
        else:
            self.params['end_date'] = datetime.datetime.now().strftime(self.date_format)
            
        return self
    
    def with_time_range(self, start_time: Union[datetime.time, datetime.datetime, str], end_time: Union[datetime.time, datetime.datetime, str] = None) -> 'RequestBuilder':
        """Set time range for the request with proper formatting"""
        if isinstance(start_time, datetime.time):
            self.params['start_time'] = start_time.strftime(self.time_format)
        elif isinstance(start_time, datetime.datetime):
            self.params['start_time'] = start_time.time().strftime(self.time_format)
        elif isinstance(start_time, str):
            self.params['start_time'] = start_time
            
        if end_time:
            if isinstance(end_time, datetime.time):
                self.params['end_time'] = end_time.strftime(self.time_format)
            elif isinstance(end_time, datetime.datetime):
                self.params['end_time'] = end_time.time().strftime(self.time_format)
            elif isinstance(end_time, str):
                self.params['end_time'] = end_time
        else:
            self.params['end_time'] = datetime.datetime.now().time().strftime(self.time_format)
            
        return self
    
    def with_detail_level(self, detail_level: str) -> 'RequestBuilder':
        self.params['detail_level'] = detail_level
        return self
    
    def with_limit(self, limit: int) -> 'RequestBuilder':
        self.params['limit'] = limit
        return self
    
    def with_activity_type(self, activity_type: str) -> 'RequestBuilder':
        """Set activity type for activity-related endpoints"""
        self.params['activity_type'] = activity_type
        return self
    
    def is_intraday_endpoint(self) -> bool:
        intraday_endpoints = [
            'Heart Rate Intraday', 
            'Steps Intraday', 
            'Activity intraday'
        ]
        return self.endpoint_type in intraday_endpoints
    
    def split_date_range_for_intraday(self) -> List[Dict]:
        if not self.is_intraday_endpoint():
            return [self.params]
            
        if 'start_date' not in self.params or 'end_date' not in self.params:
            return [self.params]
            
        try:
            start_date = datetime.datetime.strptime(self.params['start_date'], self.date_format)
            end_date = datetime.datetime.strptime(self.params['end_date'], self.date_format)
        except ValueError:
            return [self.params]
            
        if start_date == end_date:
            return [self.params]
            
        day_requests = []
        current_date = start_date
        
        while current_date <= end_date:
            day_params = self.params.copy()
            day_params['start_date'] = current_date.strftime(self.date_format)
            day_params['end_date'] = current_date.strftime(self.date_format)
            
            day_requests.append(day_params)
            
            current_date += datetime.timedelta(days=1)
            
        return day_requests
        
    def build(self) -> Dict:
        url_template = URL_DICT.get(self.endpoint_type)
        if not url_template:
            raise ValueError(f"Unknown endpoint type: {self.endpoint_type}")
            
        if self.endpoint_type == 'Sleep' or self.endpoint_type == 'Sleep Levels':
            self.url = url_template.format(self.params.get('start_date'), self.params.get('end_date'))
        elif self.endpoint_type == 'Steps':
            self.url = url_template.format(self.params.get('start_date'), self.params.get('end_date'))
        elif self.endpoint_type == 'Heart Rate':
            self.url = url_template.format(self.params.get('start_date'), self.params.get('end_date'))
        elif self.endpoint_type == 'Heart Rate Intraday':
            self.url = url_template.format(
                self.params.get('start_date'),
                self.params.get('start_time'),
                self.params.get('end_time')
            )
        elif self.endpoint_type == 'Steps Intraday':
            self.url = url_template.format(
                self.params.get('start_date'),
                self.params.get('start_time'),
                self.params.get('end_time')
            )
        elif self.endpoint_type == 'device':
            self.url = url_template
        
        # st.write(f"Request URL: {self.url}")
        if self.is_intraday_endpoint() and 'start_date' in self.params and 'end_date' in self.params:
            day_params = self.split_date_range_for_intraday()
            
            if len(day_params) > 1:
                return {
                    'multiday': True,
                    'day_params': day_params,
                    'headers': self.headers,
                    'endpoint_type': self.endpoint_type
                }
        
        return {
            'url': self.url,
            'headers': self.headers,
            'params': self.params
        }

# ===== Data Processing Strategy Pattern =====

class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Dict) -> Any:
        pass
    
    def to_dataframe(self, data: Any) -> pd.DataFrame:
        pass

class HeartRateProcessor(DataProcessor):
    """Processor for heart rate data"""
    
    def process(self, data: Dict) -> List[Dict]:
        """Process heart rate data from API response"""
        if not data or 'activities-heart-intraday' not in data:
            return []
            
        dataset = data['activities-heart-intraday'].get('dataset', [])
        
        date_str = None
        if 'activities-heart' in data and data['activities-heart']:
            try:
                date_str = data['activities-heart'][0].get('dateTime')
            except (IndexError, KeyError):
                date_str = None
        
        for item in dataset:
            if 'time' in item:
                item['original_time'] = item['time']
                
            if 'datetime' in item:
                item['date_time'] = item['datetime']
            elif 'time' in item and date_str:
                item['date_time'] = f"{date_str}T{item['time']}"
            elif 'time' in item:
                item['date_time'] = item['time']
        
        return dataset
    
    def to_dataframe(self, data: Any) -> pd.DataFrame:
        """Convert heart rate data to DataFrame with proper datetime handling"""
        if not data:
            return pd.DataFrame()
            
        df = pd.DataFrame(data)
        
        if 'date_time' in df.columns:
            try:
                df['datetime'] = pd.to_datetime(df['date_time'])
            except:
                try:
                    df['datetime'] = pd.to_datetime(df['date_time'], format='%H:%M:%S')
                except:
                    df['datetime'] = df['date_time']
        elif 'time' in df.columns:
            if 'datetime' in data[0] and data[0]['datetime']:
                df['datetime'] = pd.to_datetime(df['datetime'])
            else:
                df['datetime'] = pd.to_datetime(df['time'], format='%H:%M:%S')
            
        return df

class StepsProcessor(DataProcessor):
    """Processor for steps data"""
    
    def process(self, data: Dict) -> List[Dict]:
        """Process steps data from API response"""
        if not data or 'activities-steps-intraday' not in data:
            return []
            
        dataset = data['activities-steps-intraday'].get('dataset', [])
        
        date_str = None
        if 'activities-steps' in data and data['activities-steps']:
            try:
                date_str = data['activities-steps'][0].get('dateTime')
            except (IndexError, KeyError):
                date_str = None
        
        for item in dataset:
            if 'time' in item:
                item['original_time'] = item['time']
                
            if 'datetime' in item:
                item['date_time'] = item['datetime']
            elif 'time' in item and date_str:
                item['date_time'] = f"{date_str}T{item['time']}"
            elif 'time' in item:
                item['date_time'] = item['time']
        
        return dataset
    
    def to_dataframe(self, data: Any) -> pd.DataFrame:
        """Convert steps data to DataFrame with proper datetime handling"""
        if not data:
            return pd.DataFrame()
            
        df = pd.DataFrame(data)
        
        if 'date_time' in df.columns:
            try:
                df['datetime'] = pd.to_datetime(df['date_time'])
            except:
                try:
                    df['datetime'] = pd.to_datetime(df['date_time'], format='%H:%M:%S')
                except:
                    df['datetime'] = df['date_time']
        elif 'time' in df.columns:
            if 'datetime' in data[0] and data[0]['datetime']:
                df['datetime'] = pd.to_datetime(df['datetime'])
            else:
                df['datetime'] = pd.to_datetime(df['time'], format='%H:%M:%S')
            
        return df

class SleepProcessor(DataProcessor):
    def process(self, data: Dict) -> List[Dict]:
        if not data or 'sleep' not in data:
            return []
            
        sleep_data = data['sleep']
        processed_data = []
        
        for sleep_record in sleep_data:
            processed_record = {
                'start_time': sleep_record.get('startTime'),
                'end_time': sleep_record.get('endTime'),
                'duration': sleep_record.get('duration'),
                'efficiency': sleep_record.get('efficiency'),
                'main_sleep': sleep_record.get('isMainSleep'),
                'minutes_asleep': sleep_record.get('minutesAsleep'),
                'minutes_awake': sleep_record.get('minutesAwake')
            }
            
            if 'levels' in sleep_record and 'summary' in sleep_record['levels']:
                summary = sleep_record['levels']['summary']
                for stage, stage_data in summary.items():
                    if isinstance(stage_data, dict) and 'minutes' in stage_data:
                        processed_record[f'{stage}_minutes'] = stage_data['minutes']
            
            processed_data.append(processed_record)
            
        return processed_data
    
    def to_dataframe(self, data: Any) -> pd.DataFrame:
        if not data:
            return pd.DataFrame()
            
        df = pd.DataFrame(data)
        
        for col in ['start_time', 'end_time']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
                
        return df

class DeviceProcessor(DataProcessor):
    def process(self, data: Dict) -> List[Dict]:
        if not data or not isinstance(data, list):
            return []
            
        processed_data = []
        for device in data:
            processed_device = {
                'id': device.get('id'),
                'device_version': device.get('deviceVersion'),
                'battery_level': device.get('batteryLevel'),
                'battery_state': device.get('batteryState'),
                'last_sync_time': device.get('lastSyncTime'),
                'type': device.get('type'),
                'mac': device.get('mac', 'N/A')
            }
            processed_data.append(processed_device)
            
        return processed_data
    
    def to_dataframe(self, data: Any) -> pd.DataFrame:
        if not data:
            return pd.DataFrame()
            
        df = pd.DataFrame(data)
        
        if 'last_sync_time' in df.columns:
            df['last_sync_time'] = pd.to_datetime(df['last_sync_time'])
            
        return df

# ===== Factory Pattern for Processors =====

class ProcessorFactory:
    @staticmethod
    def get_processor(data_type: Union[str, DataType]) -> DataProcessor:
        if isinstance(data_type, DataType):
            data_type = data_type.value
            
        processors = {
            DataType.HEART_RATE.value: HeartRateProcessor(),
            DataType.STEPS.value: StepsProcessor(),
            DataType.SLEEP.value: SleepProcessor(),
            DataType.DEVICE_INFO.value: DeviceProcessor(),
        }
        
        endpoint_to_data_type = {
            'Heart Rate': DataType.HEART_RATE.value,
            'Heart Rate Intraday': DataType.HEART_RATE.value,
            'Steps': DataType.STEPS.value,
            'Steps Intraday': DataType.STEPS.value,
            'Sleep': DataType.SLEEP.value,
            'Sleep Levels': DataType.SLEEP.value,
            'device': DataType.DEVICE_INFO.value,
        }
        
        if data_type in endpoint_to_data_type:
            data_type = endpoint_to_data_type[data_type]
            
        return processors.get(data_type, HeartRateProcessor())

# ===== Watch-Student Association Manager =====

@dataclass
class WatchAssignment:
    watch: 'Watch'
    student: User
    assigned_date: datetime.datetime = field(default_factory=datetime.datetime.now)
    is_active: bool = True
    
    def unassign(self):
        self.is_active = False

class WatchAssignmentManager:
    def __init__(self):
        self.assignments: List[WatchAssignment] = []
    
    def assign_watch(self, watch: 'Watch', student: User) -> WatchAssignment:
        self.unassign_watch(watch)
        
        assignment = WatchAssignment(watch=watch, student=student)
        self.assignments.append(assignment)
        
        watch.current_student = student
        if watch.previous_student != student:
            watch.previous_student = watch.current_student
            
        return assignment
    
    def unassign_watch(self, watch: 'Watch') -> None:
        for assignment in self.assignments:
            if assignment.watch == watch and assignment.is_active:
                assignment.unassign()
                
                watch.previous_student = watch.current_student
                watch.current_student = None
                break
    
    def get_student_watches(self, student: User) -> List['Watch']:
        return [
            assignment.watch
            for assignment in self.assignments
            if assignment.student == student and assignment.is_active
        ]
    
    def get_watch_history(self, watch: 'Watch') -> List[WatchAssignment]:
        return [
            assignment
            for assignment in self.assignments
            if assignment.watch == watch
        ]
    
    def get_student_assignment_history(self, student: User) -> List[WatchAssignment]:
        """Get assignment history for a specific student"""
        return [
            assignment
            for assignment in self.assignments
            if assignment.student == student
        ]

# ===== Enhanced Watch Class =====

@dataclass
class Watch:
    """Enhanced Watch entity class using design patterns"""
    name: str
    project: str  
    token: str
    header: Dict = field(init=False, default_factory=dict)
    current_student: Optional[User] = None
    previous_student: Optional[User] = None
    is_active: bool = True
    last_sync_time: Optional[datetime.datetime] = None
    battery_level: Optional[int] = None
    # Add data caching
    _cached_data: Dict[str, Dict] = field(default_factory=dict)
    
    def __post_init__(self):
        """Initialize after the dataclass initialization"""
        token_value = self.token

        self.header = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token_value}"
        }
    
    def __eq__(self, other):
        """Equal comparison based on watch name and project"""
        if not isinstance(other, Watch):
            return False
        return self.name == other.name and self.project == other.project
    
    def __hash__(self):
        """Hash for using in dictionaries and sets"""
        return hash((self.name, self.project))
    
    def fetch_data(self, endpoint_type: str, force_fetch: bool = False, **kwargs) -> Dict:
        """Fetch data from Fitbit API using the Builder pattern"""
        # Return cached data if available and force_fetch is False
        cache_key = f"{endpoint_type}_{json.dumps(kwargs, default=str)}"
        if not force_fetch and cache_key in self._cached_data:
            return self._cached_data[cache_key]
            
        builder = RequestBuilder(endpoint_type, self.token)
        
        # Apply request parameters based on kwargs
        if 'start_date' in kwargs:
            end_date = kwargs.get('end_date', None)
            builder.with_date_range(kwargs['start_date'], end_date)
            
        if 'start_time' in kwargs:
            end_time = kwargs.get('end_time', None)
            builder.with_time_range(kwargs['start_time'], end_time)
            
        if 'detail_level' in kwargs:
            builder.with_detail_level(kwargs['detail_level'])
            
        if 'limit' in kwargs:
            builder.with_limit(kwargs['limit'])
        
        # st.write(f"builder.params: {builder.params}")
        # Build the request
        request = builder.build()
        
        # Handle multi-day intraday request if needed
        if isinstance(request, dict) and request.get('multiday', False):
            result = self.handle_multiday_request(request)
            if result:
                self._cached_data[cache_key] = result
            return result
        
        # Execute the request
        response = requests.get(request['url'], headers=request['headers'])
        
        if response.status_code != 200:
            print(f"Error fetching {endpoint_type} data: {response.status_code}")
            return {}
        
        result = response.json()
        # Cache the result
        if result:
            self._cached_data[cache_key] = result
            
        return result
    
    def handle_multiday_request(self, multiday_request: Dict) -> Dict:
        """
        Handle a multi-day intraday request by splitting it into multiple daily requests
        and combining the results.
        
        Args:
            multiday_request (Dict): Multi-day request parameters
            
        Returns:
            Dict: Combined results from all daily requests
        """
        endpoint_type = multiday_request.get('endpoint_type')
        headers = multiday_request.get('headers')
        day_params = multiday_request.get('day_params', [])
        
        if not day_params:
            return {}
            
        combined_results = {}
        
        if 'Heart Rate Intraday' in endpoint_type:
            combined_results = {
                'activities-heart': [],
                'activities-heart-intraday': {'dataset': []}
            }
        elif 'Steps Intraday' in endpoint_type:
            combined_results = {
                'activities-steps': [],
                'activities-steps-intraday': {'dataset': []}
            }
        else:
            pass
            
        for params in day_params:
            date_str = params.get('start_date')
            
            url_template = URL_DICT.get(endpoint_type)
            if not url_template:
                continue
                
            if endpoint_type == 'Heart Rate Intraday':
                url = url_template.format(
                    params.get('start_date'),
                    params.get('start_time'),
                    params.get('end_time')
                )
            elif endpoint_type == 'Steps Intraday':
                url = url_template.format(
                    params.get('start_date'),
                    params.get('start_time'),
                    params.get('end_time')
                )
            else:
                continue
                
            response = requests.get(url, headers=headers)
            
            if response.status_code != 200:
                print(f"Error fetching {endpoint_type} data for {params.get('start_date')}: {response.status_code}")
                continue
                
            day_result = response.json()
            
            if 'Heart Rate Intraday' in endpoint_type:
                if 'activities-heart' in day_result:
                    combined_results['activities-heart'].extend(day_result['activities-heart'])
                    
                if 'activities-heart-intraday' in day_result and 'dataset' in day_result['activities-heart-intraday']:
                    for data_point in day_result['activities-heart-intraday']['dataset']:
                        if 'time' in data_point:
                            data_point['original_time'] = data_point['time']
                            data_point['datetime'] = f"{date_str}T{data_point['time']}"
                    
                    if not combined_results['activities-heart-intraday'].get('datasetInterval'):
                        combined_results['activities-heart-intraday']['datasetInterval'] = \
                            day_result['activities-heart-intraday'].get('datasetInterval')
                    
                    if not combined_results['activities-heart-intraday'].get('datasetType'):
                        combined_results['activities-heart-intraday']['datasetType'] = \
                            day_result['activities-heart-intraday'].get('datasetType')
                    
                    combined_results['activities-heart-intraday']['dataset'].extend(
                        day_result['activities-heart-intraday']['dataset']
                    )
                    
            elif 'Steps Intraday' in endpoint_type:
                if 'activities-steps' in day_result:
                    combined_results['activities-steps'].extend(day_result['activities-steps'])
                    
                if 'activities-steps-intraday' in day_result and 'dataset' in day_result['activities-steps-intraday']:
                    for data_point in day_result['activities-steps-intraday']['dataset']:
                        if 'time' in data_point:
                            data_point['original_time'] = data_point['time']
                            data_point['datetime'] = f"{date_str}T{data_point['time']}"
                    
                    if not combined_results['activities-steps-intraday'].get('datasetInterval'):
                        combined_results['activities-steps-intraday']['datasetInterval'] = \
                            day_result['activities-steps-intraday'].get('datasetInterval')
                    
                    if not combined_results['activities-steps-intraday'].get('datasetType'):
                        combined_results['activities-steps-intraday']['datasetType'] = \
                            day_result['activities-steps-intraday'].get('datasetType')
                    
                    combined_results['activities-steps-intraday']['dataset'].extend(
                        day_result['activities-steps-intraday']['dataset']
                    )
            else:
                if not combined_results and day_result:
                    combined_results = {key: [] for key in day_result.keys()}
                
                for key, value in day_result.items():
                    if isinstance(value, list) and key in combined_results:
                        combined_results[key].extend(value)
        
        if 'Heart Rate Intraday' in endpoint_type and 'activities-heart-intraday' in combined_results:
            try:
                combined_results['activities-heart-intraday']['dataset'] = sorted(
                    combined_results['activities-heart-intraday']['dataset'],
                    key=lambda x: x.get('datetime', '')
                )
            except Exception as e:
                print(f"Error sorting heart rate dataset: {e}")
                
        elif 'Steps Intraday' in endpoint_type and 'activities-steps-intraday' in combined_results:
            try:
                combined_results['activities-steps-intraday']['dataset'] = sorted(
                    combined_results['activities-steps-intraday']['dataset'],
                    key=lambda x: x.get('datetime', '')
                )
            except Exception as e:
                print(f"Error sorting steps dataset: {e}")
        
        return combined_results
    
    def process_data(self, endpoint_type: str, data: Dict) -> Any:
        """Process data using the appropriate processor"""
        processor = ProcessorFactory.get_processor(endpoint_type)
        return processor.process(data)
    
    def get_data_as_dataframe(self, endpoint_type: str, data: Dict = None, force_fetch: bool = False, **kwargs) -> pd.DataFrame:
        """Get data as a pandas DataFrame"""
        if data is None:
            data = self.fetch_data(endpoint_type, force_fetch=force_fetch, **kwargs)
        
        processor = ProcessorFactory.get_processor(endpoint_type)
        processed_data = processor.process(data)
        return processor.to_dataframe(processed_data)
    
    def update_device_info(self, force_fetch: bool = False) -> None:
        """Update device information (battery, sync time, etc.)"""
        data = self.fetch_data('device', force_fetch=force_fetch)
        
        if data and isinstance(data, list) and len(data) > 0:
            device = data[0]  # Get the first device
            self.battery_level = device.get('batteryLevel')
            sync_time = device.get('lastSyncTime')
            if sync_time:
                try:
                    self.last_sync_time = datetime.datetime.strptime(
                        sync_time, "%Y-%m-%dT%H:%M:%S.%f"
                    )
                except ValueError:
                    try:
                        self.last_sync_time = datetime.datetime.strptime(
                            sync_time, "%Y-%m-%dT%H:%M:%S.%fZ"
                        )
                    except ValueError:
                        print(f"Could not parse sync time: {sync_time}")
    
    def get_current_hourly_HR(self, force_fetch: bool = False) -> Optional[int]:
        """Get the current hourly heart rate (convenience method)"""
        current_date = datetime.datetime.now()
        hour_ago = current_date - datetime.timedelta(hours=1)
        
        data = self.fetch_data(
            'Heart Rate Intraday',
            force_fetch=force_fetch,
            start_date=current_date,
            start_time=hour_ago,
            end_time=current_date
        )
        
        processed_data = self.process_data('Heart Rate Intraday', data)
        
        if processed_data and len(processed_data) > 0:
            return processed_data[-1]['value']
        return None
    
    def get_current_hourly_steps(self, force_fetch: bool = False) -> Optional[int]:
        """Get the current hourly steps (convenience method)"""
        current_date = datetime.datetime.now()
        hours_ago = current_date - datetime.timedelta(hours=6)
        
        data = self.fetch_data(
            'Steps Intraday',
            force_fetch=force_fetch,
            start_date=current_date,
            start_time=hours_ago,
            end_time=current_date
        )
        
        processed_data = self.process_data('Steps Intraday', data)
        
        if processed_data and len(processed_data) > 0:
            for step_data in reversed(processed_data):
                if step_data['value'] > 0:
                    return step_data['value']
        return None
    
    def get_current_battery(self, force_fetch: bool = False) -> Optional[int]:
        """Get the current battery level (convenience method)"""
        if force_fetch:
            self.update_device_info(force_fetch=True)
        return self.battery_level
    
    def get_last_sleep_start_end(self, force_fetch: bool = False) -> tuple:
        """Get the last sleep start and end times (convenience method)"""
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        today = datetime.datetime.now()
        
        data = self.fetch_data(
            'Sleep',
            force_fetch=force_fetch,
            start_date=yesterday,
            end_date=today
        )
        
        processed_data = self.process_data('Sleep', data)
        
        if processed_data and len(processed_data) > 0:
            sleep_record = processed_data[0]
            return sleep_record.get('start_time'), sleep_record.get('end_time')
        return None, None
    
    def get_last_sleep_duration(self, force_fetch: bool = False) -> Optional[float]:
        """Get the last sleep duration in hours (convenience method)"""
        start_time, end_time = self.get_last_sleep_start_end(force_fetch=force_fetch)
        if not start_time or not end_time:
            return None
        try:
            for fmt in ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"]:
                try:
                    start_dt = datetime.datetime.strptime(start_time, fmt)
                    end_dt = datetime.datetime.strptime(end_time, fmt)
                    duration = (end_dt - start_dt).total_seconds() / 3600
                    return duration
                except ValueError:
                    continue
            
            print(f"Could not parse sleep times after trying multiple formats")
            return None
        except Exception as e:
            print(f"Error calculating sleep duration: {e}")
            return None
            
    def clear_cache(self) -> None:
        """Clear the cached data"""
        self._cached_data = {}

# ===== Watch Factory =====

class WatchFactory:
    @staticmethod
    def create_from_details(details: Dict) -> Watch:
        """Factory for creating Watch objects"""
        name = details.get('name')
        project_name = details.get('project')
        token = details.get('token')
        
        if not all([name, project_name, token]):
            raise ValueError("Missing required watch details: name, project, or token")
        
        watch = Watch(
            name=name,
            project=project_name,
            token=token
        )
        
        if 'isActive' in details:
            watch.is_active = details['isActive']
        
        if 'batteryLevel' in details:
            watch.battery_level = details['batteryLevel']
        
        if 'lastSyncTime' in details:
            try:
                watch.last_sync_time = datetime.datetime.strptime(
                    details['lastSyncTime'], 
                    "%Y-%m-%dT%H:%M:%S.%fZ"
                )
            except (ValueError, TypeError):
                pass
        
        user_name = details.get('user')
        if user_name:
            student = User(
                name=user_name, 
                role="student",
                projects=[project_name]
            )
            watch.current_student = student
        
        return watch
    
    @staticmethod
    def create_from_spreadsheet(spreadsheet_data: List[Dict]) -> List[Watch]:
        """Create a list of Watch objects from spreadsheet data"""
        watches = []
        
        for row in spreadsheet_data:
            try:
                watch = WatchFactory.create_from_details(row)
                watches.append(watch)
            except Exception as e:
                print(f"Error creating watch from row: {e}")
                continue
                
        return watches

# ===== Utility Functions =====

def get_headers(token: str) -> Dict:
    """Get HTTP headers for Fitbit API requests"""
    headers = HEADERS.copy()
    headers['Authorization'] = headers['Authorization'].format(token)
    return headers

def get_activity(project: str) -> bool:
    """Check if a project is active"""
    return True  # Replace with actual logic based on your system


