from Spreadsheet_io.sheets import Spreadsheet
import requests
import json
import datetime
import pandas as pd
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any, Union, Callable, TypeVar
from enum import Enum
from abc import ABC, abstractmethod
from entity.User import User
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

# Singleton for spreadsheet instance
SP = Spreadsheet.get_instance()

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
        self.token = token
        self.headers = get_headers(token)
        self.params = {}
        self.url = None
        self.date_format = "%Y-%m-%d"
        self.time_format = "%H:%M"
        
    def with_date_range(self, start_date: datetime.datetime, end_date: datetime.datetime = None) -> 'RequestBuilder':
        """Add date range to the request"""
        self.params['start_date'] = start_date.strftime(self.date_format)
        if end_date:
            self.params['end_date'] = end_date.strftime(self.date_format)
        else:
            self.params['end_date'] = datetime.datetime.now().strftime(self.date_format)
        return self
    
    def with_time_range(self, start_time: datetime.datetime, end_time: datetime.datetime = None) -> 'RequestBuilder':
        """Add time range to the request"""
        self.params['start_time'] = start_time.strftime(self.time_format)
        if end_time:
            self.params['end_time'] = end_time.strftime(self.time_format)
        else:
            self.params['end_time'] = datetime.datetime.now().strftime(self.time_format)
        return self
    
    def with_detail_level(self, detail_level: str) -> 'RequestBuilder':
        """Add detail level to the request (e.g., 1sec, 1min)"""
        self.params['detail_level'] = detail_level
        return self
    
    def with_limit(self, limit: int) -> 'RequestBuilder':
        """Add limit to the request"""
        self.params['limit'] = limit
        return self
        
    def build(self) -> Dict:
        """Build the request object"""
        url_template = URL_DICT.get(self.endpoint_type)
        if not url_template:
            raise ValueError(f"Unknown endpoint type: {self.endpoint_type}")
            
        # Build URL based on the endpoint type and parameters
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
            
        return {
            'url': self.url,
            'headers': self.headers,
            'params': self.params
        }

# ===== Data Processing Strategy Pattern =====

class DataProcessor(ABC):
    """Abstract class for data processors using Strategy pattern"""
    
    @abstractmethod
    def process(self, data: Dict) -> Any:
        """Process the API response data"""
        pass
    
    def to_dataframe(self, data: Any) -> pd.DataFrame:
        """Convert processed data to a pandas DataFrame"""
        pass

class HeartRateProcessor(DataProcessor):
    """Processor for heart rate data"""
    
    def process(self, data: Dict) -> List[Dict]:
        """Process heart rate data from API response"""
        if not data or 'activities-heart-intraday' not in data:
            return []
            
        dataset = data['activities-heart-intraday'].get('dataset', [])
        return dataset
    
    def to_dataframe(self, data: Any) -> pd.DataFrame:
        """Convert heart rate data to DataFrame"""
        if not data:
            return pd.DataFrame()
            
        df = pd.DataFrame(data)
        if 'time' in df.columns:
            df['datetime'] = pd.to_datetime(df['time'])
        return df

class StepsProcessor(DataProcessor):
    """Processor for steps data"""
    
    def process(self, data: Dict) -> List[Dict]:
        """Process steps data from API response"""
        if not data or 'activities-steps-intraday' not in data:
            return []
            
        dataset = data['activities-steps-intraday'].get('dataset', [])
        return dataset
    
    def to_dataframe(self, data: Any) -> pd.DataFrame:
        """Convert steps data to DataFrame"""
        if not data:
            return pd.DataFrame()
            
        df = pd.DataFrame(data)
        if 'time' in df.columns:
            df['datetime'] = pd.to_datetime(df['time'])
        return df

class SleepProcessor(DataProcessor):
    """Processor for sleep data"""
    
    def process(self, data: Dict) -> List[Dict]:
        """Process sleep data from API response"""
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
            
            # Add sleep stages if available
            if 'levels' in sleep_record and 'summary' in sleep_record['levels']:
                summary = sleep_record['levels']['summary']
                for stage, stage_data in summary.items():
                    if isinstance(stage_data, dict) and 'minutes' in stage_data:
                        processed_record[f'{stage}_minutes'] = stage_data['minutes']
            
            processed_data.append(processed_record)
            
        return processed_data
    
    def to_dataframe(self, data: Any) -> pd.DataFrame:
        """Convert sleep data to DataFrame"""
        if not data:
            return pd.DataFrame()
            
        df = pd.DataFrame(data)
        
        # Convert datetime strings to datetime objects
        for col in ['start_time', 'end_time']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
                
        return df

class DeviceProcessor(DataProcessor):
    """Processor for device data"""
    
    def process(self, data: Dict) -> List[Dict]:
        """Process device data from API response"""
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
                'mac': device.get('mac', 'N/A')  # Some devices may not have MAC
            }
            processed_data.append(processed_device)
            
        return processed_data
    
    def to_dataframe(self, data: Any) -> pd.DataFrame:
        """Convert device data to DataFrame"""
        if not data:
            return pd.DataFrame()
            
        df = pd.DataFrame(data)
        
        # Convert sync time to datetime
        if 'last_sync_time' in df.columns:
            df['last_sync_time'] = pd.to_datetime(df['last_sync_time'])
            
        return df

# ===== Factory Pattern for Processors =====

class ProcessorFactory:
    """Factory for creating data processors"""
    
    @staticmethod
    def get_processor(data_type: Union[str, DataType]) -> DataProcessor:
        """Get the appropriate processor for the data type"""
        if isinstance(data_type, DataType):
            data_type = data_type.value
            
        processors = {
            DataType.HEART_RATE.value: HeartRateProcessor(),
            DataType.STEPS.value: StepsProcessor(),
            DataType.SLEEP.value: SleepProcessor(),
            DataType.DEVICE_INFO.value: DeviceProcessor(),
            # Add more processors as needed
        }
        
        # Map endpoint types to data types
        endpoint_to_data_type = {
            'Heart Rate': DataType.HEART_RATE.value,
            'Heart Rate Intraday': DataType.HEART_RATE.value,
            'Steps': DataType.STEPS.value,
            'Steps Intraday': DataType.STEPS.value,
            'Sleep': DataType.SLEEP.value,
            'Sleep Levels': DataType.SLEEP.value,
            'device': DataType.DEVICE_INFO.value,
        }
        
        # If an endpoint type is provided, convert it to a data type
        if data_type in endpoint_to_data_type:
            data_type = endpoint_to_data_type[data_type]
            
        return processors.get(data_type, HeartRateProcessor())  # Default to HeartRateProcessor

# ===== Watch-Student Association Manager =====

@dataclass
class WatchAssignment:
    """Class to represent a watch assignment to a student"""
    watch: 'Watch'
    student: User
    assigned_date: datetime.datetime = field(default_factory=datetime.datetime.now)
    is_active: bool = True
    
    def unassign(self):
        """Unassign the watch from the student"""
        self.is_active = False

class WatchAssignmentManager:
    """Manager for watch-student associations"""
    
    def __init__(self):
        self.assignments: List[WatchAssignment] = []
    
    def assign_watch(self, watch: 'Watch', student: User) -> WatchAssignment:
        """Assign a watch to a student"""
        # First, unassign this watch from any current student
        self.unassign_watch(watch)
        
        # Create new assignment
        assignment = WatchAssignment(watch=watch, student=student)
        self.assignments.append(assignment)
        
        # Update the watch's current student
        watch.current_student = student
        if watch.previous_student != student:
            watch.previous_student = watch.current_student
            
        return assignment
    
    def unassign_watch(self, watch: 'Watch') -> None:
        """Unassign a watch from its current student"""
        # Find current assignment
        for assignment in self.assignments:
            if assignment.watch == watch and assignment.is_active:
                assignment.unassign()
                
                # Update the watch
                watch.previous_student = watch.current_student
                watch.current_student = None
                break
    
    def get_student_watches(self, student: User) -> List['Watch']:
        """Get all watches assigned to a student"""
        return [
            assignment.watch
            for assignment in self.assignments
            if assignment.student == student and assignment.is_active
        ]
    
    def get_watch_history(self, watch: 'Watch') -> List[WatchAssignment]:
        """Get assignment history for a watch"""
        return [
            assignment
            for assignment in self.assignments
            if assignment.watch == watch
        ]
    
    def get_student_assignment_history(self, student: User) -> List[WatchAssignment]:
        """Get assignment history for a student"""
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
    project: Project
    token: str
    header: Dict = field(init=False, default_factory=dict)
    current_student: Optional[User] = None
    previous_student: Optional[User] = None
    is_active: bool = True
    last_sync_time: Optional[datetime.datetime] = None
    battery_level: Optional[int] = None
    
    def __post_init__(self):
        """Initialize after the dataclass initialization"""
        self.header = get_headers(self.token)
        
    def __eq__(self, other):
        """Equal comparison based on watch name and project"""
        if not isinstance(other, Watch):
            return False
        return self.name == other.name and self.project == other.project
    
    def __hash__(self):
        """Hash for using in dictionaries and sets"""
        return hash((self.name, self.project))
    
    def fetch_data(self, endpoint_type: str, **kwargs) -> Dict:
        """Fetch data from Fitbit API using the Builder pattern"""
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
            
        # Build the request
        request = builder.build()
        
        # Execute the request
        response = requests.get(request['url'], headers=request['headers'])
        
        if response.status_code != 200:
            print(f"Error fetching {endpoint_type} data: {response.status_code}")
            return {}
            
        return response.json()
    
    def process_data(self, endpoint_type: str, data: Dict) -> Any:
        """Process data using the appropriate processor"""
        processor = ProcessorFactory.get_processor(endpoint_type)
        return processor.process(data)
    
    def get_data_as_dataframe(self, endpoint_type: str, data: Dict = None, **kwargs) -> pd.DataFrame:
        """Get data as a pandas DataFrame"""
        # Fetch data if not provided
        if data is None:
            data = self.fetch_data(endpoint_type, **kwargs)
            
        # Process the data
        processor = ProcessorFactory.get_processor(endpoint_type)
        processed_data = processor.process(data)
        
        # Convert to DataFrame
        return processor.to_dataframe(processed_data)
    
    def update_device_info(self) -> None:
        """Update device information (battery, sync time, etc.)"""
        data = self.fetch_data('device')
        
        if data and isinstance(data, list) and len(data) > 0:
            device = data[0]  # Get the first device
            self.battery_level = device.get('batteryLevel')
            
            # Convert sync time to datetime
            sync_time = device.get('lastSyncTime')
            if sync_time:
                try:
                    self.last_sync_time = datetime.datetime.strptime(
                        sync_time, "%Y-%m-%dT%H:%M:%S.%f"
                    )
                except ValueError:
                    try:
                        # Try alternative format
                        self.last_sync_time = datetime.datetime.strptime(
                            sync_time, "%Y-%m-%dT%H:%M:%S.%fZ"
                        )
                    except ValueError:
                        print(f"Could not parse sync time: {sync_time}")
    
    def get_current_hourly_HR(self) -> Optional[int]:
        """Get the current hourly heart rate (convenience method)"""
        current_date = datetime.datetime.now()
        hour_ago = current_date - datetime.timedelta(hours=1)
        
        data = self.fetch_data(
            'Heart Rate Intraday',
            start_date=current_date,
            start_time=hour_ago,
            end_time=current_date
        )
        
        processed_data = self.process_data('Heart Rate Intraday', data)
        
        if processed_data and len(processed_data) > 0:
            return processed_data[-1]['value']
        return None
    
    def get_current_hourly_steps(self) -> Optional[int]:
        """Get the current hourly steps (convenience method)"""
        current_date = datetime.datetime.now()
        hours_ago = current_date - datetime.timedelta(hours=6)
        
        data = self.fetch_data(
            'Steps Intraday',
            start_date=current_date,
            start_time=hours_ago,
            end_time=current_date
        )
        
        processed_data = self.process_data('Steps Intraday', data)
        
        if processed_data and len(processed_data) > 0:
            # Find the latest non-zero steps value
            for step_data in reversed(processed_data):
                if step_data['value'] > 0:
                    return step_data['value']
        return None
    
    def get_current_battery(self) -> Optional[int]:
        """Get the current battery level (convenience method)"""
        self.update_device_info()
        return self.battery_level
    
    def get_last_sleep_start_end(self) -> tuple:
        """Get the last sleep start and end times (convenience method)"""
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        today = datetime.datetime.now()
        
        data = self.fetch_data(
            'Sleep',
            start_date=yesterday,
            end_date=today
        )
        
        processed_data = self.process_data('Sleep', data)
        
        if processed_data and len(processed_data) > 0:
            sleep_record = processed_data[0]
            return sleep_record.get('start_time'), sleep_record.get('end_time')
        return None, None
    
    def get_last_sleep_duration(self) -> Optional[float]:
        """Get the last sleep duration in hours (convenience method)"""
        start_time, end_time = self.get_last_sleep_start_end()
        
        if not start_time or not end_time:
            return None
        
        try:
            # Try different datetime formats
            for fmt in ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"]:
                try:
                    start_dt = datetime.datetime.strptime(start_time, fmt)
                    end_dt = datetime.datetime.strptime(end_time, fmt)
                    duration = (end_dt - start_dt).total_seconds() / 3600  # Convert to hours
                    return duration
                except ValueError:
                    continue
            
            print(f"Could not parse sleep times after trying multiple formats")
            return None
        except Exception as e:
            print(f"Error calculating sleep duration: {e}")
            return None

# ===== Watch Factory =====

class WatchFactory:
    """Factory for creating Watch objects"""
    
    @staticmethod
    def create_from_details(details: Dict) -> Watch:
        """Create a Watch from a dictionary of details"""
        # Basic required fields
        name = details.get('name')
        project_name = details.get('project')
        token = details.get('token')
        
        if not all([name, project_name, token]):
            raise ValueError("Missing required watch details: name, project, or token")
        
        # Create a project if only the name is provided
        project = details.get('project_obj')
        if not project and project_name:
            from entity.Project import Project
            project = Project(name=project_name)
        
        # Create the watch
        watch = Watch(
            name=name,
            project=project,
            token=token
        )
        
        # Set optional attributes
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
        
        # Set current student if provided
        user_name = details.get('user')
        if user_name:
            from entity.User import User
            student = User(
                name=user_name, 
                project=project_name, 
                role="student"
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

def get_activity(project: Project) -> bool:
    """Check if a project is active"""
    # This is a placeholder - implement actual logic based on your system
    return True


