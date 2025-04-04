from dataclasses import dataclass, field
from typing import Optional, List, Dict, Set, Any, Union
from collections import defaultdict
import uuid
from abc import ABC, abstractmethod
from enum import Enum

from entity.User import User, UserRole, Observer, Subject, UserRepository, Permission
from entity.Watch import Watch, WatchAssignmentManager, WatchFactory
from entity.Sheet import Sheet, Spreadsheet, SheetFactory

class ProjectStatus(str, Enum):
    """Enum for project status"""
    PLANNING = "planning"
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    ARCHIVED = "archived"

@dataclass
class ProjectSpreadsheet:
    """Class to represent association between project and spreadsheet"""
    project_id: str
    spreadsheet_id: str
    sheets: List[str] = field(default_factory=list)
    permissions: Dict[str, bool] = field(default_factory=dict)

@dataclass
class Project(Subject):
    """
    Enhanced Project entity class using design patterns.

    Attributes:
        name (str): The name of the project.
        id (str): Unique identifier for the project.
        status (ProjectStatus): Current status of the project.
        description (str): Description of the project.
        spreadsheets (Dict): Mapping of spreadsheet IDs to ProjectSpreadsheet objects.
        watches (Dict): Mapping of watch IDs to Watch objects.
        managers (Set): Set of user IDs who manage this project.
        students (Set): Set of user IDs who are students in this project.
        admins (Set): Set of user IDs who are admins with access to this project.
        observers (List): List of observers watching this project.
    """
    name: str
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: ProjectStatus = ProjectStatus.ACTIVE
    description: str = ""
    spreadsheets: Dict[str, ProjectSpreadsheet] = field(default_factory=dict)
    watches: Dict[str, Watch] = field(default_factory=dict)
    managers: Set[str] = field(default_factory=set)
    students: Set[str] = field(default_factory=set)
    admins: Set[str] = field(default_factory=set)
    _observers: List[Observer] = field(default_factory=list)
    
    def __post_init__(self):
        """Post initialization processing"""
        # Convert string status to enum if needed
        if isinstance(self.status, str):
            try:
                self.status = ProjectStatus(self.status.lower())
            except ValueError:
                # Default to active if status isn't recognized
                self.status = ProjectStatus.ACTIVE
    
    # Spreadsheet management
    def add_spreadsheet(self, spreadsheet: Spreadsheet, sheets: List[str] = None) -> None:
        """Add a spreadsheet to this project"""
        if spreadsheet.api_key not in self.spreadsheets:
            self.spreadsheets[spreadsheet.api_key] = ProjectSpreadsheet(
                project_id=self.id,
                spreadsheet_id=spreadsheet.api_key,
                sheets=sheets or []
            )
            self.notify("spreadsheet_added", spreadsheet_id=spreadsheet.api_key)
    
    def remove_spreadsheet(self, spreadsheet_id: str) -> None:
        """Remove a spreadsheet from this project"""
        if spreadsheet_id in self.spreadsheets:
            del self.spreadsheets[spreadsheet_id]
            self.notify("spreadsheet_removed", spreadsheet_id=spreadsheet_id)
    
    def get_spreadsheets(self) -> List[ProjectSpreadsheet]:
        """Get all spreadsheets associated with this project"""
        return list(self.spreadsheets.values())
    
    def add_sheet_to_spreadsheet(self, spreadsheet_id: str, sheet_name: str) -> None:
        """Add a sheet to a spreadsheet in this project"""
        if spreadsheet_id in self.spreadsheets:
            if sheet_name not in self.spreadsheets[spreadsheet_id].sheets:
                self.spreadsheets[spreadsheet_id].sheets.append(sheet_name)
                self.notify("sheet_added", spreadsheet_id=spreadsheet_id, sheet_name=sheet_name)
    
    def remove_sheet_from_spreadsheet(self, spreadsheet_id: str, sheet_name: str) -> None:
        """Remove a sheet from a spreadsheet in this project"""
        if spreadsheet_id in self.spreadsheets:
            if sheet_name in self.spreadsheets[spreadsheet_id].sheets:
                self.spreadsheets[spreadsheet_id].sheets.remove(sheet_name)
                self.notify("sheet_removed", spreadsheet_id=spreadsheet_id, sheet_name=sheet_name)
    
    # Watch management
    def add_watch(self, watch: Watch) -> None:
        """Add a watch to this project"""
        self.watches[watch.name] = watch
        self.notify("watch_added", watch_name=watch.name)
    
    def remove_watch(self, watch_name: str) -> None:
        """Remove a watch from this project"""
        if watch_name in self.watches:
            del self.watches[watch_name]
            self.notify("watch_removed", watch_name=watch_name)
    
    def get_watches(self) -> List[Watch]:
        """Get all watches in this project"""
        return list(self.watches.values())
    
    def get_active_watches(self) -> List[Watch]:
        """Get all active watches in this project"""
        return [watch for watch in self.watches.values() if watch.is_active]
    
    # User management
    def add_manager(self, user_id: str) -> None:
        """Add a manager to this project"""
        self.managers.add(user_id)
        
        # Update user's project association
        user_repo = UserRepository.get_instance()
        user = user_repo.get_by_id(user_id)
        if user:
            user.add_project(self.id)
        
        self.notify("manager_added", user_id=user_id)
    
    def remove_manager(self, user_id: str) -> None:
        """Remove a manager from this project"""
        if user_id in self.managers:
            self.managers.remove(user_id)
            
            # Update user's project association
            user_repo = UserRepository.get_instance()
            user = user_repo.get_by_id(user_id)
            if user:
                user.remove_project(self.id)
            
            self.notify("manager_removed", user_id=user_id)
    
    def add_student(self, user_id: str) -> None:
        """Add a student to this project"""
        self.students.add(user_id)
        
        # Update user's project association
        user_repo = UserRepository.get_instance()
        user = user_repo.get_by_id(user_id)
        if user:
            user.add_project(self.id)
        
        self.notify("student_added", user_id=user_id)
    
    def remove_student(self, user_id: str) -> None:
        """Remove a student from this project"""
        if user_id in self.students:
            self.students.remove(user_id)
            
            # Update user's project association
            user_repo = UserRepository.get_instance()
            user = user_repo.get_by_id(user_id)
            if user:
                user.remove_project(self.id)
            
            self.notify("student_removed", user_id=user_id)
    
    def add_admin(self, user_id: str) -> None:
        """Add an admin to this project"""
        self.admins.add(user_id)
        self.notify("admin_added", user_id=user_id)
    
    def remove_admin(self, user_id: str) -> None:
        """Remove an admin from this project"""
        if user_id in self.admins:
            self.admins.remove(user_id)
            self.notify("admin_removed", user_id=user_id)
    
    def get_managers(self) -> List[User]:
        """Get all managers for this project"""
        user_repo = UserRepository.get_instance()
        return [user_repo.get_by_id(user_id) for user_id in self.managers if user_repo.get_by_id(user_id)]
    
    def get_students(self) -> List[User]:
        """Get all students for this project"""
        user_repo = UserRepository.get_instance()
        return [user_repo.get_by_id(user_id) for user_id in self.students if user_repo.get_by_id(user_id)]
    
    def get_admins(self) -> List[User]:
        """Get all admins with access to this project"""
        user_repo = UserRepository.get_instance()
        return [user_repo.get_by_id(user_id) for user_id in self.admins if user_repo.get_by_id(user_id)]
    
    def get_all_users(self) -> List[User]:
        """Get all users associated with this project"""
        user_ids = self.managers.union(self.students).union(self.admins)
        user_repo = UserRepository.get_instance()
        return [user_repo.get_by_id(user_id) for user_id in user_ids if user_repo.get_by_id(user_id)]
    
    # Observer pattern implementation
    def attach(self, observer: Observer) -> None:
        """Attach an observer to this project"""
        if observer not in self._observers:
            self._observers.append(observer)
    
    def detach(self, observer: Observer) -> None:
        """Detach an observer from this project"""
        if observer in self._observers:
            self._observers.remove(observer)
    
    def notify(self, event_type: str, **data) -> None:
        """Notify all observers about an event"""
        for observer in self._observers:
            observer.update(self, event_type=event_type, **data)
    
    # String representation
    def __str__(self) -> str:
        return f"{self.name} ({self.status})"
    
    def __repr__(self) -> str:
        return f"Project(name='{self.name}', id='{self.id}', status='{self.status}')"


# Factory pattern for creating projects
class ProjectFactory:
    """Factory for creating projects"""
    
    @staticmethod
    def create_project(name: str, description: str = "", status: str = "active") -> Project:
        """Create a new project"""
        return Project(
            name=name,
            description=description,
            status=status
        )
    
    @staticmethod
    def create_from_dict(data: Dict[str, Any]) -> Project:
        """Create a project from a dictionary (e.g., from spreadsheet data)"""
        project = Project(
            name=data.get('name', ''),
            description=data.get('description', ''),
            status=data.get('status', 'active')
        )
        
        # Add relationships if specified
        if 'managers' in data and isinstance(data['managers'], list):
            for manager_id in data['managers']:
                project.add_manager(manager_id)
        
        if 'students' in data and isinstance(data['students'], list):
            for student_id in data['students']:
                project.add_student(student_id)
        
        if 'admins' in data and isinstance(data['admins'], list):
            for admin_id in data['admins']:
                project.add_admin(admin_id)
        
        return project


# Project repository for managing projects
class ProjectRepository:
    """Repository for storing and retrieving projects"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ProjectRepository, cls).__new__(cls)
            cls._instance._projects = {}
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if getattr(self, '_initialized', False):
            return
        self._initialized = True
        self._projects = {}  # Dictionary of projects by ID
        self._name_index = {}  # Index of projects by name
        self._status_index = {}  # Index of projects by status
        self._user_index = {}  # Index of projects by user ID
        self._spreadsheet_index = {}  # Index of projects by spreadsheet ID
    
    def add(self, project: Project) -> None:
        """Add a project to the repository"""
        # Store project by ID
        self._projects[project.id] = project
        
        # Update indices
        self._name_index[project.name] = project.id
        
        status_key = project.status.value if hasattr(project.status, 'value') else project.status
        if status_key not in self._status_index:
            self._status_index[status_key] = set()
        self._status_index[status_key].add(project.id)
        
        # Index by users
        user_ids = project.managers.union(project.students).union(project.admins)
        for user_id in user_ids:
            if user_id not in self._user_index:
                self._user_index[user_id] = set()
            self._user_index[user_id].add(project.id)
        
        # Index by spreadsheets
        for spreadsheet_id in project.spreadsheets:
            if spreadsheet_id not in self._spreadsheet_index:
                self._spreadsheet_index[spreadsheet_id] = set()
            self._spreadsheet_index[spreadsheet_id].add(project.id)
    
    def remove(self, project_id: str) -> None:
        """Remove a project from the repository"""
        if project_id not in self._projects:
            return
        
        project = self._projects[project_id]
        
        # Remove from indices
        if project.name in self._name_index:
            del self._name_index[project.name]
        
        status_key = project.status.value if hasattr(project.status, 'value') else project.status
        if status_key in self._status_index and project_id in self._status_index[status_key]:
            self._status_index[status_key].remove(project_id)
        
        # Remove from user index
        user_ids = project.managers.union(project.students).union(project.admins)
        for user_id in user_ids:
            if user_id in self._user_index and project_id in self._user_index[user_id]:
                self._user_index[user_id].remove(project_id)
        
        # Remove from spreadsheet index
        for spreadsheet_id in project.spreadsheets:
            if spreadsheet_id in self._spreadsheet_index and project_id in self._spreadsheet_index[spreadsheet_id]:
                self._spreadsheet_index[spreadsheet_id].remove(project_id)
        
        # Remove from projects dictionary
        del self._projects[project_id]
    
    def get_by_id(self, project_id: str) -> Optional[Project]:
        """Get a project by ID"""
        return self._projects.get(project_id)
    
    def get_by_name(self, name: str) -> Optional[Project]:
        """Get a project by name"""
        project_id = self._name_index.get(name)
        if project_id:
            return self._projects.get(project_id)
        return None
    
    def get_by_status(self, status: ProjectStatus) -> List[Project]:
        """Get all projects with a specific status"""
        status_key = status.value if hasattr(status, 'value') else status
        project_ids = self._status_index.get(status_key, set())
        return [self._projects[project_id] for project_id in project_ids if project_id in self._projects]
    
    def get_by_user(self, user_id: str) -> List[Project]:
        """Get all projects associated with a specific user"""
        project_ids = self._user_index.get(user_id, set())
        return [self._projects[project_id] for project_id in project_ids if project_id in self._projects]
    
    def get_by_spreadsheet(self, spreadsheet_id: str) -> List[Project]:
        """Get all projects associated with a specific spreadsheet"""
        project_ids = self._spreadsheet_index.get(spreadsheet_id, set())
        return [self._projects[project_id] for project_id in project_ids if project_id in self._projects]
    
    def get_all(self) -> List[Project]:
        """Get all projects"""
        return list(self._projects.values())
    
    @staticmethod
    def get_instance() -> 'ProjectRepository':
        """Get the singleton instance of the repository"""
        if ProjectRepository._instance is None:
            ProjectRepository()
        return ProjectRepository._instance


# Create UI adapter for projects and users
class ProjectUIAdapter:
    """Adapter for using projects in the UI"""
    
    @staticmethod
    def get_project_summary(project: Project) -> Dict[str, Any]:
        """Get a summary of a project for the UI"""
        return {
            'id': project.id,
            'name': project.name,
            'status': project.status.value if hasattr(project.status, 'value') else project.status,
            'description': project.description,
            'manager_count': len(project.managers),
            'student_count': len(project.students),
            'watch_count': len(project.watches),
            'spreadsheet_count': len(project.spreadsheets)
        }
    
    @staticmethod
    def get_project_users(project: Project) -> Dict[str, List[Dict[str, Any]]]:
        """Get user information for a project, organized by role"""
        user_repo = UserRepository.get_instance()
        
        managers = []
        for user_id in project.managers:
            user = user_repo.get_by_id(user_id)
            if user:
                managers.append({
                    'id': user.id,
                    'name': user.name,
                    'email': user.email,
                    'last_login': user.last_login
                })
        
        students = []
        for user_id in project.students:
            user = user_repo.get_by_id(user_id)
            if user:
                students.append({
                    'id': user.id,
                    'name': user.name,
                    'email': user.email,
                    'last_login': user.last_login
                })
        
        admins = []
        for user_id in project.admins:
            user = user_repo.get_by_id(user_id)
            if user:
                admins.append({
                    'id': user.id,
                    'name': user.name,
                    'email': user.email,
                    'last_login': user.last_login
                })
        
        return {
            'managers': managers,
            'students': students,
            'admins': admins
        }
    
    @staticmethod
    def get_project_watches(project: Project) -> List[Dict[str, Any]]:
        """Get watch information for a project"""
        watches = []
        for watch in project.watches.values():
            watches.append({
                'name': watch.name,
                'is_active': watch.is_active,
                'battery_level': watch.battery_level,
                'last_sync_time': watch.last_sync_time.isoformat() if watch.last_sync_time else None,
                'assigned_to': watch.current_student.name if watch.current_student else None
            })
        return watches
    
    @staticmethod
    def filter_project_data_for_user(user: User, project_data: Dict[str, Any]) -> Dict[str, Any]:
        """Filter project data based on user permissions"""
        if user.has_permission(Permission.ADMIN_ALL):
            # Admin sees everything
            return project_data
        
        if user.has_permission(Permission.READ_PROJECT, project_data.get('id')):
            # User can read the project
            result = project_data.copy()
            
            # Check if user can manage other users
            if not user.has_permission(Permission.MANAGE_USERS, project_data.get('id')):
                # Remove sensitive user information
                if 'users' in result:
                    for role in result['users']:
                        for u in result['users'][role]:
                            if 'email' in u:
                                del u['email']
            
            return result
        
        # User doesn't have permission, return minimal info
        return {
            'id': project_data.get('id'),
            'name': project_data.get('name'),
            'status': project_data.get('status')
        }
