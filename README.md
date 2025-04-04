# Fitbit Management System

A comprehensive system for managing Fitbit device data, user information, and project details using Google Sheets as a database backend.

## Architecture Overview

This system uses Google Spreadsheets as a database, with specialized entity classes that provide structured access to the data. It implements several design patterns to ensure flexibility, maintainability, and scalability.

### Core Components

- **Spreadsheet Management**: Handles connections to Google Sheets API
- **Entity Layer**: Provides object representations of spreadsheet data
- **Data Processing**: Converts between spreadsheet data and Pandas/Polars DataFrames

## Design Patterns

The system implements several design patterns:

1. **Singleton Pattern**: Used in `Spreadsheet_io.sheets.Spreadsheet` to ensure a single connection to Google Sheets API
2. **Factory Pattern**: In `entity.Sheet.SheetFactory` to create appropriate sheet objects based on type
3. **Strategy Pattern**: Through `UpdateStrategy` classes to customize how data is updated in sheets
4. **Adapter Pattern**: Via `GoogleSheetsAdapter` to connect entity classes with the Google Sheets API
5. **Data Validation**: Through `SheetSchema` to enforce data structure
6. **Repository Pattern**: Used in `UserRepository` and `ProjectRepository` for efficient data management
7. **Observer Pattern**: Used in `Project` and `User` classes to notify listeners of changes

## Entity Layer

The entity layer provides object-oriented access to the data:

### Spreadsheet Class (Sheet.py)

Represents a Google Spreadsheet containing multiple sheets:

```python
spreadsheet = Spreadsheet(
    name="My Database",
    api_key="your_google_spreadsheet_key"
)
```

### Sheet Types

Specialized sheet classes with appropriate schemas:

- `Sheet`: Generic sheet
- `UserSheet`: For user data
- `ProjectSheet`: For project data
- `FitbitSheet`: For Fitbit device data
- `LogSheet`: For log data

### User Management (User.py)

The system includes a comprehensive user management system with role-based permissions:

#### User Entity

The `User` class represents a person in the system with specific roles and permissions:

```python
user = User(
    name="John Smith",
    role=UserRole.MANAGER,
    email="john@example.com"
)
```

#### User Roles and Permissions

The system supports multiple user roles with specific permissions:

- **Admin**: Has access to all projects and all permissions
- **Manager**: Can manage one project and its users/watches
- **Student**: Can view data for their assigned project
- **Researcher**: Can read data across projects but not modify

```python
# Check if a user has a specific permission
if user.has_permission(Permission.MANAGE_WATCHES, project_id):
    # Allow watch management
```

#### User Factory

The `UserFactory` simplifies creating users with appropriate roles:

```python
# Create different types of users
admin = UserFactory.create_admin("Admin Name", "admin@example.com")
manager = UserFactory.create_manager("Manager Name", project_id, "manager@example.com")
student = UserFactory.create_student("Student Name", project_id, "student@example.com")
```

#### User Repository

The `UserRepository` provides a centralized store for user objects with efficient lookup:

```python
# Get the repository singleton
user_repo = UserRepository.get_instance()

# Add a user
user_repo.add(user)

# Find users by various criteria
admin_users = user_repo.get_by_role(UserRole.ADMIN)
project_users = user_repo.get_by_project(project_id)
```

### Project Management (Project.py)

The system provides comprehensive project management with rich relationships:

#### Project Entity

The `Project` class represents a research project with associated users, watches, and spreadsheets:

```python
project = Project(
    name="Sleep Research Study",
    description="A study of sleep patterns in college students",
    status=ProjectStatus.ACTIVE
)
```

#### Project Status

Projects can have different statuses to track their lifecycle:

- **Planning**: Project is in planning phase
- **Active**: Project is currently running
- **Paused**: Project is temporarily paused
- **Completed**: Project has been completed
- **Archived**: Project is archived for historical reference

#### Project-Spreadsheet Association

Projects can be associated with multiple spreadsheets, with specific sheets relevant to each project:

```python
# Add a spreadsheet to a project
project.add_spreadsheet(spreadsheet, sheets=["Students", "Watches", "SleepData"])

# Add a specific sheet to a project's spreadsheet
project.add_sheet_to_spreadsheet(spreadsheet_id, "HeartRateData")
```

#### Project-User Association

Projects have different types of users with different roles:

```python
# Add users to a project
project.add_manager(manager_id)
project.add_student(student_id)
project.add_admin(admin_id)

# Get users by role
managers = project.get_managers()
students = project.get_students()
```

#### Project Repository

The `ProjectRepository` provides centralized storage and efficient lookup of projects:

```python
# Get the repository singleton
project_repo = ProjectRepository.get_instance()

# Add a project
project_repo.add(project)

# Find projects by various criteria
active_projects = project_repo.get_by_status(ProjectStatus.ACTIVE)
user_projects = project_repo.get_by_user(user_id)
spreadsheet_projects = project_repo.get_by_spreadsheet(spreadsheet_id)
```

#### Project UI Adapter

The `ProjectUIAdapter` helps transform project data for display in the UI with appropriate permission filtering:

```python
# Get project summary for display
project_summary = ProjectUIAdapter.get_project_summary(project)

# Get user information for a project, organized by role
project_users = ProjectUIAdapter.get_project_users(project)

# Get watch information for a project
project_watches = ProjectUIAdapter.get_project_watches(project)

# Filter data based on user permissions
filtered_data = ProjectUIAdapter.filter_project_data_for_user(user, project_data)
```

## Fitbit Watch Management (Watch.py)

The system includes a comprehensive framework for managing Fitbit watches, retrieving data via the Fitbit API, and associating watches with students.

### Design Patterns in Watch.py

1. **Builder Pattern**: `RequestBuilder` creates API requests with different parameters
2. **Strategy Pattern**: `DataProcessor` classes handle different types of Fitbit data
3. **Factory Pattern**: `WatchFactory` and `ProcessorFactory` simplify object creation
4. **Singleton Pattern**: Reuses spreadsheet connection across the application
5. **Manager Pattern**: `WatchAssignmentManager` manages student-watch associations

### Watch Entity

The core `Watch` class represents a Fitbit device:

```python
watch = Watch(
    name="FitbitSense123",
    project=project_obj,
    token="your_fitbit_api_token"
)
```

### Data Types

The system supports various Fitbit data types through the `DataType` enum:

- `HEART_RATE`: Heart rate measurements
- `STEPS`: Step counts
- `SLEEP`: Sleep data and stages
- `BATTERY`: Battery level information
- `DEVICE_INFO`: Device details and status

### API Request Construction

The `RequestBuilder` helps construct API requests with proper parameters:

```python
# Build a request for heart rate data
builder = RequestBuilder('Heart Rate Intraday', watch.token)
request = builder.with_date_range(start_date, end_date) \
                .with_time_range(start_time, end_time) \
                .build()
```

### Data Processing

Each data type has a specialized processor to extract and transform API responses:

```python
# Process heart rate data
processor = ProcessorFactory.get_processor(DataType.HEART_RATE)
processed_data = processor.process(api_response)
dataframe = processor.to_dataframe(processed_data)
```

### Student-Watch Pairing

The `WatchAssignmentManager` handles pairing students with watches:

```python
# Create assignment manager
assignment_manager = WatchAssignmentManager()

# Assign a watch to a student
assignment_manager.assign_watch(watch, student)

# Get all watches for a student
student_watches = assignment_manager.get_student_watches(student)

# Get assignment history for a watch
watch_history = assignment_manager.get_watch_history(watch)
```

### Simplified Data Retrieval

The Watch class provides convenience methods for common operations:

```python
# Get current heart rate
hr = watch.get_current_hourly_HR()

# Get battery level
battery = watch.get_current_battery()

# Get sleep duration
sleep_duration = watch.get_last_sleep_duration()
```

### Creating DataFrame for Visualization

The Watch class makes it easy to get data in DataFrame format for dashboards:

```python
# Get heart rate data as DataFrame
hr_df = watch.get_data_as_dataframe(
    'Heart Rate Intraday',
    start_date=datetime.datetime.now(),
    start_time=datetime.datetime.now() - datetime.timedelta(hours=6),
    end_time=datetime.datetime.now()
)

# Now you can easily plot this with any visualization library
import plotly.express as px
fig = px.line(hr_df, x='datetime', y='value', title="Heart Rate")
```

### Creating Watches from Spreadsheet Data

The `WatchFactory` simplifies creating Watch objects from spreadsheet data:

```python
# Get watch data from spreadsheet
spreadsheet = Spreadsheet.get_instance()
watches_data = spreadsheet.get_worksheet("Fitbits").get_all_records()

# Create Watch objects
watches = WatchFactory.create_from_spreadsheet(watches_data)
```

## Google Sheets Integration

The `GoogleSheetsAdapter` connects entity classes with the actual Google Sheets API:

```python
# Connect to actual Google Sheets
spreadsheet = GoogleSheetsAdapter.connect(spreadsheet)

# Save changes back to Google Sheets
GoogleSheetsAdapter.save(spreadsheet, sheet_name="Users")
```

## Update Strategies

Three update strategies are available:

1. **AppendStrategy**: Add new data to existing data
2. **ReplaceStrategy**: Replace existing data with new data
3. **MergeStrategy**: Merge new data with existing data (for dictionaries)

```python
# Update a sheet with new data using append strategy
spreadsheet.update_sheet("Users", new_data, strategy="append")
```

## Server Log Integration (Spreadsheet_io/sheets.py)

The system includes integration with server logs for tracking Fitbit data:

### Spreadsheet Singleton

The `Spreadsheet` class is implemented as a singleton to ensure a single connection to Google Sheets:

```python
# Get the spreadsheet instance
spreadsheet = Spreadsheet.get_instance()

# Access spreadsheet data
users = spreadsheet.get_user_details()
projects = spreadsheet.get_project_details()
fitbits = spreadsheet.get_fitbits_details()
```

### Log Management

The `serverLogFile` class manages logging of Fitbit data to a CSV file:

```python
from Spreadsheet_io.sheets import serverLogFile

# Initialize log file
log_file = serverLogFile()

# Update log with Fitbit data
log_file.update_fitbits_log(fitbit_data_df)
```

### Fitbit Log Format

The system tracks comprehensive log information for each Fitbit device:

- Basic info: project, watch name, last sync time
- Health metrics: heart rate, sleep, steps
- Diagnostics: battery level, failure counters
- Timestamps for last successful data points

## Data Validation

Each sheet type includes a schema for data validation:

```python
# Validate data against schema
user_sheet = SheetFactory.create_sheet("user", "Users")
is_valid = user_sheet.schema.validate(data)

if is_valid:
    user_sheet.data = data
else:
    print("Invalid data format for user sheet")
```

## Complete System Integration

All components work together to provide a comprehensive system:

```python
# 1. Set up the core repositories
user_repo = UserRepository.get_instance()
project_repo = ProjectRepository.get_instance()

# 2. Create a project
project = ProjectFactory.create_project("Sleep Study")
project_repo.add(project)

# 3. Create and add users
manager = UserFactory.create_manager("Research Lead", project.id)
user_repo.add(manager)
project.add_manager(manager.id)

student = UserFactory.create_student("Participant 1", project.id)
user_repo.add(student)
project.add_student(student.id)

# 4. Set up spreadsheet access
spreadsheet = Spreadsheet(name="Sleep Data", api_key="your_key")
project.add_spreadsheet(spreadsheet, ["Participants", "SleepData", "HeartRate"])

# 5. Create and assign watches
watch = WatchFactory.create_from_details({
    "name": "Watch001",
    "project": project.name,
    "token": "fitbit_token"
})
project.add_watch(watch)

# 6. Assign watch to student
assignment_manager = WatchAssignmentManager()
assignment_manager.assign_watch(watch, student)

# 7. Retrieve and process data
hr_data = watch.fetch_data("Heart Rate Intraday", 
                          start_date=datetime.datetime.now() - datetime.timedelta(days=1),
                          start_time=datetime.datetime.now() - datetime.timedelta(hours=8),
                          end_time=datetime.datetime.now())

# 8. Convert to DataFrame for visualization
hr_df = watch.get_data_as_dataframe("Heart Rate Intraday", hr_data)

# 9. Update sheet with new data
spreadsheet.update_sheet("HeartRate", hr_df, strategy="append")

# 10. Save data back to Google Sheets
GoogleSheetsAdapter.save(spreadsheet, "HeartRate")
```
