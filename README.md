# Fitbit Management System

A comprehensive system for managing Fitbit device data, user information, and project details using Google Sheets as a database backend.

## Architecture Overview

This system uses Google Spreadsheets as a database, with specialized entity classes that provide structured access to the data. It implements several design patterns to ensure flexibility, maintainability, and scalability.

### Core Components

- **Spreadsheet Management**: Handles connections to Google Sheets API
- **Entity Layer**: Provides object representations of spreadsheet data
- **Data Processing**: Converts between spreadsheet data and Pandas/Polars DataFrames
- **Server Integration**: Automated data collection via cron jobs
- **Web Interface**: Streamlit-based UI with role-based access control

## Design Patterns

The system implements several design patterns:

1. **Singleton Pattern**: Used in `Spreadsheet_io.sheets.Spreadsheet` to ensure a single connection to Google Sheets API
2. **Factory Pattern**: In `entity.Sheet.SheetFactory` to create appropriate sheet objects based on type
3. **Strategy Pattern**: Through `UpdateStrategy` classes to customize how data is updated in sheets
4. **Adapter Pattern**: Via `GoogleSheetsAdapter` to connect entity classes with the Google Sheets API
5. **Data Validation**: Through `SheetSchema` to enforce data structure
6. **Repository Pattern**: Used in `UserRepository` and `ProjectRepository` for efficient data management
7. **Observer Pattern**: Used in `Project` and `User` classes to notify listeners of changes
8. **Builder Pattern**: In `RequestBuilder` for creating API requests with different parameters
9. **Entity-Control-Boundary (ECB)**: Architectural pattern used to organize the application

## Google Sheets Integration Guide

This section provides detailed instructions on connecting Google Sheets to your application and using the Sheet classes.

### Setting Up Google Sheets API

Before you can connect to Google Sheets, you need to set up the Google API:

1. **Create a Google Cloud Project**:
   - Go to the [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project or select an existing one
   - Enable the Google Sheets API and Google Drive API

2. **Create Service Account Credentials**:
   - In the Google Cloud Console, go to "APIs & Services" > "Credentials"
   - Click "Create Credentials" > "Service Account"
   - Download the JSON key file
   - Store this file securely

3. **Configure Streamlit Secrets**:
   - Create a `.streamlit/secrets.toml` file with:
   ```toml
   [gcp_service_account]
   type = "service_account"
   project_id = "your-project-id"
   private_key_id = "your-private-key-id"
   private_key = "your-private-key"
   client_email = "your-client-email"
   client_id = "your-client-id"
   auth_uri = "https://accounts.google.com/o/oauth2/auth"
   token_uri = "https://oauth2.googleapis.com/token"
   auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
   client_x509_cert_url = "your-cert-url"

   spreadsheet_key = "your-spreadsheet-id"
   fitbit_log_path = "path/to/fitbit_log.csv"
   ```

4. **Share Spreadsheet with Service Account**:
   - Share your Google Spreadsheet with the service account email (client_email in your JSON key)
   - Grant editor permissions

### Connecting to Google Sheets

There are two ways to connect to Google Sheets in the application:

#### 1. Using the Enhanced Entity Sheet Classes

```python
from entity.Sheet import Spreadsheet, GoogleSheetsAdapter, SheetFactory

# Create a spreadsheet instance
spreadsheet = Spreadsheet(
    name="Fitbit Database",
    api_key="your-spreadsheet-id-from-url"
)

# Connect to Google Sheets (loads all worksheets)
GoogleSheetsAdapter.connect(spreadsheet)

# Get a specific sheet with proper typing
user_sheet = spreadsheet.get_sheet("Users", sheet_type="user")

# Access data in the sheet
all_users = user_sheet.data  # List of user records as dictionaries
```

#### 2. Using the Legacy Spreadsheet Access (via Compatibility Layer)

```python
from entity.Sheet import LegacySpreadsheet as Spreadsheet

# Get singleton instance
spreadsheet = Spreadsheet.get_instance()

# Access worksheets by index
user_details = spreadsheet.get_user_details()  # First worksheet (index 0)
project_details = spreadsheet.get_project_details()  # Second worksheet (index 1)
fitbit_details = spreadsheet.get_fitbits_details()  # Third worksheet (index 2)
```

### Detailed Example: Connecting to User Sheet for Authentication

This example shows how to integrate Google Sheets with user authentication:

```python
from entity.Sheet import Spreadsheet, GoogleSheetsAdapter
from entity.User import UserFactory, UserRepository

def load_users_from_sheet():
    # Step 1: Connect to the spreadsheet
    spreadsheet = Spreadsheet(
        name="Fitbit Database",
        api_key="your-spreadsheet-id"  # From your secrets
    )
    GoogleSheetsAdapter.connect(spreadsheet)
    
    # Step 2: Get the Users sheet
    user_sheet = spreadsheet.get_sheet("Users", sheet_type="user")
    
    # Step 3: Transform sheet data to User objects
    user_repo = UserRepository.get_instance()
    
    for user_data in user_sheet.data:
        # Create user from sheet data
        user = UserFactory.create_from_dict(user_data)
        
        # Add to repository for efficient lookup
        user_repo.add(user)
    
    return user_repo

def authenticate_user(email, password):
    # Get the user repository (populated from sheet)
    user_repo = load_users_from_sheet()
    
    # Find user by email
    user = user_repo.get_by_email(email)
    
    if user and verify_password(user, password):
        # Update last login time
        user.update_last_login()
        
        # Save changes back to sheet
        save_user_to_sheet(user)
        
        return user
    
    return None

def save_user_to_sheet(user):
    # Get the spreadsheet connection
    spreadsheet = Spreadsheet(
        name="Fitbit Database",
        api_key="your-spreadsheet-id"
    )
    GoogleSheetsAdapter.connect(spreadsheet)
    
    # Get the Users sheet
    user_sheet = spreadsheet.get_sheet("Users", sheet_type="user")
    
    # Find and update the user record
    for i, user_record in enumerate(user_sheet.data):
        if user_record.get("email") == user.email:
            # Update the record
            user_sheet.data[i] = user.to_dict()
            break
    
    # Save changes back to Google Sheets
    GoogleSheetsAdapter.save(spreadsheet, "Users")
```

### Google Sheets Schema for Users

For the authentication system to work properly, your Users sheet should have the following structure:

| id | name | email | role | last_login | projects |
|----|------|-------|------|------------|----------|
| uuid-1 | John Smith | john@example.com | admin | 2023-08-15T10:30:00 | project1,project2 |
| uuid-2 | Jane Doe | jane@example.com | manager | 2023-08-14T14:22:00 | project1 |
| uuid-3 | Student1 | student1@example.com | student | 2023-08-10T09:15:00 | project1 |

### Streamlined Google Sheets Integration in Controllers

The application architecture includes controllers that handle Google Sheets integration for you:

```python
from controllers.user_controller import UserController

# Initialize the controller
user_controller = UserController()

# Get all users (automatically loads from sheet)
all_users = user_controller.get_all_users()

# Find user by email (for login)
user = user_controller.get_user_by_email("john@example.com")

# Get users by role
admin_users = user_controller.get_users_by_role("admin")
```

### Working with Multiple Sheet Types

The system supports different sheet types with appropriate schemas:

```python
# UserSheet - for user data
user_sheet = spreadsheet.get_sheet("Users", sheet_type="user")

# ProjectSheet - for project data
project_sheet = spreadsheet.get_sheet("Projects", sheet_type="project")

# FitbitSheet - for Fitbit device data
fitbit_sheet = spreadsheet.get_sheet("Devices", sheet_type="fitbit")

# LogSheet - for log data
log_sheet = spreadsheet.get_sheet("Logs", sheet_type="log")
```

### Update Strategies

When updating sheet data, you can choose from three strategies:

```python
# Append new data to existing data
spreadsheet.update_sheet("Users", new_user, strategy="append")

# Replace existing data with new data
spreadsheet.update_sheet("Logs", log_data, strategy="replace")

# Merge new data with existing data (for dictionaries)
spreadsheet.update_sheet("Settings", settings, strategy="merge")
```

### Converting Between Sheets and DataFrames

The system makes it easy to work with both Pandas and Polars DataFrames:

```python
# Convert sheet to DataFrame
pandas_df = user_sheet.to_dataframe(engine="pandas")
polars_df = user_sheet.to_dataframe(engine="polars")

# Update sheet from DataFrame
user_sheet.from_dataframe(updated_df)
```

### Automating Sheet Updates with Cron Jobs

For automating data collection and sheet updates:

1. **Set up secrets.json for server use**:
   Create a `model/secrets.json` file with the same credentials as your Streamlit secrets.
   
2. **Run the data collection script**:
   ```bash
   python run_data_collection.py
   ```
   
3. **Set up a cron job** (Linux/Mac):
   ```
   # Run every hour
   0 * * * * /path/to/python /path/to/run_data_collection.py
   
   # Run every 15 minutes
   */15 * * * * /path/to/python /path/to/run_data_collection.py
   ```

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
- **Guest**: Limited access for exploring the system

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

## Server Integration and Automation

The system includes robust server-side components for automated data collection and processing.

### Cron-Based Data Collection

A scheduled task system collects Fitbit data at regular intervals:

```python
# Run the data collection process
python run_data_collection.py
```

Command-line options for flexibility:
