from dataclasses import dataclass, field
from typing import Optional, List, Dict, Set, Any, Union
from enum import Enum
import uuid
from abc import ABC, abstractmethod
import datetime
# from entity.Sheet import GoogleSheetsAdapter, Spreadsheet

class UserRole(Enum):
    """Enum for user roles"""
    ADMIN = "admin"
    MANAGER = "manager"
    STUDENT = "student"
    RESEARCHER = "researcher"
    GUEST = "guest"

class Permission(Enum):
    """Enum for user permissions"""
    READ_PROJECT = "read_project"
    WRITE_PROJECT = "write_project"
    MANAGE_USERS = "manage_users"
    MANAGE_WATCHES = "manage_watches"
    READ_ALL_PROJECTS = "read_all_projects"


# Observer pattern interfaces
class Observer(ABC):
    @abstractmethod
    def update(self, subject: Any, *args, **kwargs) -> None:
        """Update method called when observed subject changes"""
        pass

class Subject(ABC):
    @abstractmethod
    def attach(self, observer: Observer) -> None:
        """Attach an observer to this subject"""
        pass
    
    @abstractmethod
    def detach(self, observer: Observer) -> None:
        """Detach an observer from this subject"""
        pass
    
    @abstractmethod
    def notify(self, *args, **kwargs) -> None:
        """Notify all observers about an event"""
        pass

@dataclass
class User(Subject):
    """
    Enhanced User entity class using design patterns.

    Attributes:
        name (str): The name of the user.
        role (UserRole): The role of the user.
        id (str): Unique identifier for the user.
        email (str): The email of the user.
        last_login (str): The last login time of the user.
        projects (List): Projects associated with the user.
        permission_cache (Dict): Cached permissions for quick access.
        observers (List): Observers watching this user.
    """
    name: str
    role: str
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    email: Optional[str] = None
    last_login: Optional[str] = None
    projects: List[str] = field(default_factory=list)
    permission_cache: Dict[str, bool] = field(default_factory=dict)
    _observers: List[Observer] = field(default_factory=list)
    
    def __post_init__(self):
        """Post initialization processing"""
        # Convert string role to enum if needed
        if isinstance(self.role, str):
            try:
                self.role = UserRole(self.role.lower())
            except ValueError:
                # Default to student if role isn't recognized
                self.role = UserRole.STUDENT
                
        
    
    def has_permission(self, permission: Permission, project_id: Optional[str] = None) -> bool:
        """Check if user has a specific permission, optionally for a specific project"""
        # Check cache first
        cache_key = f"{permission.value}:{project_id or 'global'}"
        if cache_key in self.permission_cache:
            return self.permission_cache[cache_key]
        
        # Calculate permission based on role
        has_perm = False
        
        role_value = self.role.value if hasattr(self.role, 'value') else self.role
        
        if role_value == UserRole.ADMIN.value:
            # Admins have all permissions
            has_perm = True
        elif role_value == UserRole.MANAGER.value:
            # Managers have all permissions for their projects
            if permission in [Permission.READ_PROJECT, Permission.WRITE_PROJECT, 
                            Permission.MANAGE_WATCHES]:
                has_perm = project_id in self.projects
            elif permission == Permission.MANAGE_USERS:
                # Managers can only manage students in their projects
                has_perm = project_id in self.projects
        elif role_value == UserRole.STUDENT.value:
            # Students can only read their project
            if permission == Permission.READ_PROJECT:
                has_perm = project_id in self.projects
        elif role_value == UserRole.RESEARCHER.value:
            # Researchers can read all projects but not modify
            if permission == Permission.READ_PROJECT:
                has_perm = True
            elif permission == Permission.READ_ALL_PROJECTS:
                has_perm = True
        
        # Cache the result
        self.permission_cache[cache_key] = has_perm
        return has_perm
    
    def add_project(self, project_id: str) -> None:
        """Add a project to this user"""
        if project_id not in self.projects:
            role_value = self.role.value if hasattr(self.role, 'value') else self.role
            
            # Handle specific role constraints
            if role_value == UserRole.MANAGER.value and len(self.projects) >= 1:
                # Managers can only have one project, replace the existing one
                self.projects = [project_id]
            elif role_value == UserRole.STUDENT.value and len(self.projects) >= 1:
                # Students can only have one project, replace the existing one
                self.projects = [project_id]
            else:
                self.projects.append(project_id)
            
            # Clear permission cache as it may have changed
            self.permission_cache.clear()
            
            # Notify observers
            self.notify("project_added", project_id=project_id)
    
    def remove_project(self, project_id: str) -> None:
        """Remove a project from this user"""
        if project_id in self.projects:
            self.projects.remove(project_id)
            
            # Clear permission cache as it may have changed
            self.permission_cache.clear()
            
            # Notify observers
            self.notify("project_removed", project_id=project_id)
    
    def update_last_login(self) -> None:
        """Update the last login timestamp"""
        self.last_login = datetime.datetime.now().isoformat()
    
    # Observer pattern implementation
    def attach(self, observer: Observer) -> None:
        """Attach an observer to this user"""
        if observer not in self._observers:
            self._observers.append(observer)
    
    def detach(self, observer: Observer) -> None:
        """Detach an observer from this user"""
        if observer in self._observers:
            self._observers.remove(observer)
    
    def notify(self, event_type: str, **data) -> None:
        """Notify all observers about an event"""
        for observer in self._observers:
            observer.update(self, event_type=event_type, **data)

    # String representation
    def __str__(self) -> str:
        role_value = self.role.value if hasattr(self.role, 'value') else self.role
        return f"{self.name} ({role_value})"
    
    def __repr__(self) -> str:
        role_value = self.role.value if hasattr(self.role, 'value') else self.role
        return f"User(name='{self.name}', role='{role_value}', projects={self.projects})"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert user to dictionary for serialization"""
        role_value = self.role.value if hasattr(self.role, 'value') else self.role
        return {
            'id': self.id,
            'name': self.name,
            'role': role_value,
            'email': self.email,
            'last_login': self.last_login,
            'projects': self.projects
        }


# Factory pattern for creating users
class UserFactory:
    """Factory for creating different types of users"""
    
    @staticmethod
    def create_admin(name: str, email: Optional[str] = None) -> User:
        """Create an admin user"""
        return User(
            name=name,
            role=UserRole.ADMIN.value,
            email=email
        )
    
    @staticmethod
    def create_manager(name: str, project_id: str, email: Optional[str] = None) -> User:
        """Create a manager user for a specific project"""
        user = User(
            name=name,
            role=UserRole.MANAGER.value,
            email=email
        )
        user.add_project(project_id)
        return user
    
    @staticmethod
    def create_student(name: str, project_id: str, email: Optional[str] = None) -> User:
        """Create a student user for a specific project"""
        user = User(
            name=name,
            role=UserRole.STUDENT.value,
            email=email
        )
        user.add_project(project_id)
        return user
    
    @staticmethod
    def create_researcher(name: str, email: Optional[str] = None) -> User:
        """Create a researcher user with read access to all projects"""
        return User(
            name=name,
            role=UserRole.RESEARCHER.value,
            email=email
        )
    
    @staticmethod
    def create_guest() -> User:
        """Create a guest user with minimal permissions"""
        return User(
            name="Guest",
            role=UserRole.GUEST.value
        )
    
    @staticmethod
    def create_from_dict(data: Dict[str, Any]) -> User:
        """Create a user from a dictionary (e.g., from spreadsheet data)"""
        # Handle username vs name in spreadsheet data
        name = data.get('name', data.get('username', ''))
        
        user = User(
            name=name,
            role=data.get('role', 'student'),
            email=data.get('email')
        )
        
        # Add projects if specified
        if 'project' in data:
            user.add_project(data['project'])
        elif 'projects' in data and isinstance(data['projects'], list):
            for project in data['projects']:
                user.add_project(project)
        
        return user


# User repository for managing users
class UserRepository:
    """Repository for storing and retrieving users"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(UserRepository, cls).__new__(cls)
            cls._instance._users = {}
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if getattr(self, '_initialized', False):
            return
        self._initialized = True
        self._users = {}  # Dictionary of users by ID
        self._name_index = {}  # Index of users by name
        self._email_index = {}  # Index of users by email
        self._role_index = {}  # Index of users by role
        self._project_index = {}  # Index of users by project
    
    def add(self, user: User) -> None:
        """Add a user to the repository"""
        # Store user by ID
        self._users[user.id] = user
        
        # Update indices
        self._name_index[user.name] = user.id
        
        if user.email:
            self._email_index[user.email.lower()] = user.id
        
        role_key = user.role.value if hasattr(user.role, 'value') else user.role
        if role_key not in self._role_index:
            self._role_index[role_key] = set()
        self._role_index[role_key].add(user.id)
        
        for project_id in user.projects:
            if project_id not in self._project_index:
                self._project_index[project_id] = set()
            self._project_index[project_id].add(user.id)
    
    def remove(self, user_id: str) -> None:
        """Remove a user from the repository"""
        if user_id not in self._users:
            return
        
        user = self._users[user_id]
        
        # Remove from indices
        if user.name in self._name_index:
            del self._name_index[user.name]
        
        if user.email and user.email.lower() in self._email_index:
            del self._email_index[user.email.lower()]
        
        role_key = user.role.value if hasattr(user.role, 'value') else user.role
        if role_key in self._role_index and user_id in self._role_index[role_key]:
            self._role_index[role_key].remove(user_id)
        
        for project_id in user.projects:
            if project_id in self._project_index and user_id in self._project_index[project_id]:
                self._project_index[project_id].remove(user_id)
        
        # Remove from users dictionary
        del self._users[user_id]
    
    def get_by_id(self, user_id: str) -> Optional[User]:
        """Get a user by ID"""
        return self._users.get(user_id)
    
    def get_by_name(self, name: str) -> Optional[User]:
        """Get a user by name"""
        user_id = self._name_index.get(name)
        if user_id:
            return self._users.get(user_id)
        return None
    
    def get_by_email(self, email: str) -> Optional[User]:
        """Get a user by email"""
        if not email:
            return None
        user_id = self._email_index.get(email.lower())
        if user_id:
            return self._users.get(user_id)
        return None
    
    def get_by_role(self, role: Union[UserRole, str]) -> List[User]:
        """Get all users with a specific role"""
        role_key = role.value if hasattr(role, 'value') else role
        user_ids = self._role_index.get(role_key, set())
        return [self._users[user_id] for user_id in user_ids if user_id in self._users]
    
    def get_by_project(self, project_id: str) -> List[User]:
        """Get all users associated with a specific project"""
        user_ids = self._project_index.get(project_id, set())
        return [self._users[user_id] for user_id in user_ids if user_id in self._users]
    
    def get_managers_by_project(self, project_id: str) -> List[User]:
        """Get all managers for a specific project"""
        project_users = self.get_by_project(project_id)
        return [user for user in project_users 
                if (hasattr(user.role, 'value') and user.role.value == UserRole.MANAGER.value) 
                or user.role == UserRole.MANAGER.value]
    
    def get_students_by_project(self, project_id: str) -> List[User]:
        """Get all students for a specific project"""
        project_users = self.get_by_project(project_id)
        return [user for user in project_users 
                if (hasattr(user.role, 'value') and user.role.value == UserRole.STUDENT.value) 
                or user.role == UserRole.STUDENT.value]
    
    def get_all(self) -> List[User]:
        """Get all users"""
        return list(self._users.values())
    
    @staticmethod
    def get_instance() -> 'UserRepository':
        """Get the singleton instance of the repository"""
        if UserRepository._instance is None:
            UserRepository()
        return UserRepository._instance
