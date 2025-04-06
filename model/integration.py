"""
Integration module to connect legacy spreadsheet data with enhanced entity layer.
This bridges the gap between cron job data collection and the new application architecture.
"""
from typing import List, Dict, Any, Optional
import datetime

from Spreadsheet_io.sheets import Spreadsheet as LegacySpreadsheet
from entity.Sheet import Spreadsheet as EntitySpreadsheet, GoogleSheetsAdapter
from entity.Project import Project, ProjectRepository, ProjectFactory
from entity.User import User, UserRepository, UserFactory
from entity.Watch import Watch, WatchFactory, WatchAssignmentManager


def sync_watches_to_entity_layer() -> None:
    """
    Synchronizes watches from the legacy spreadsheet into the enhanced entity layer.
    This ensures that cron-collected data is available in the entity system.
    """
    # Get data from legacy spreadsheet
    sp_legacy = LegacySpreadsheet.get_instance()
    watch_details = sp_legacy.get_fitbits_details()
    
    # Get project repository
    project_repo = ProjectRepository.get_instance()
    user_repo = UserRepository.get_instance()
    
    # Create projects if they don't exist
    project_names = set(watch["project"] for watch in watch_details if "project" in watch)
    projects = {}
    
    for project_name in project_names:
        # Look for existing project
        project = project_repo.get_by_name(project_name)
        
        if not project:
            # Create new project
            project = ProjectFactory.create_project(
                name=project_name, 
                description=f"Project for {project_name}",
                status="active"
            )
            project_repo.add(project)
        
        projects[project_name] = project
    
    # Create watches and add to projects
    skipped_watches = 0
    for watch_detail in watch_details:
        project_name = watch_detail.get("project")
        if not project_name or project_name not in projects:
            skipped_watches += 1
            continue
            
        # Validate required fields for watch creation
        required_fields = ["name", "project", "token"]
        if not all(field in watch_detail for field in required_fields):
            missing_fields = [field for field in required_fields if field not in watch_detail]
            print(f"Skipping watch: missing required fields: {', '.join(missing_fields)}")
            skipped_watches += 1
            continue
            
        # Get project
        project = projects[project_name]
        
        # Create watch using the enhanced WatchFactory
        try:
            watch = WatchFactory.create_from_details(watch_detail)
            
            # Add watch to project if it doesn't exist
            if watch.name not in project.watches:
                project.add_watch(watch)
                
                # If watch has a user assigned, create/update the user and student relationship
                user_name = watch_detail.get("user")
                if user_name:
                    # Find or create user
                    user = user_repo.get_by_name(user_name)
                    if not user:
                        user = UserFactory.create_student(user_name, project.id)
                        user_repo.add(user)
                        
                    # Add user to project as a student if not already there
                    if user.id not in project.students:
                        project.add_student(user.id)
                    
                    # Assign watch to student
                    watch_manager = WatchAssignmentManager()
                    watch_manager.assign_watch(watch, user)
        except ValueError as e:
            print(f"Error creating watch: {e}")
            skipped_watches += 1
    
    print(f"Synchronized {len(watch_details) - skipped_watches} watches to entity layer")
    print(f"Skipped {skipped_watches} watches due to missing or invalid data")
    print(f"Created/updated {len(projects)} projects")


def get_entity_watches() -> List[Watch]:
    """
    Get all watches from the entity layer.
    
    Returns:
        List[Watch]: List of all watches from all projects
    """
    # Get all projects
    project_repo = ProjectRepository.get_instance()
    projects = project_repo.get_all()
    
    # Collect all watches
    watches = []
    for project in projects:
        watches.extend(project.get_watches())
    
    return watches


def update_entity_watch_data() -> None:
    """
    Updates watch data in the entity layer from the most recent Fitbit API data.
    This should be run after the cron job has collected new data.
    """
    # Get all watches from entity layer
    watches = get_entity_watches()
    
    # Update device info for each watch
    for watch in watches:
        try:
            watch.update_device_info()
            print(f"Updated device info for watch {watch.name}")
        except Exception as e:
            print(f"Error updating device info for watch {watch.name}: {e}")
    
    # Save updated watch data to spreadsheet
    try:
        # Get spreadsheet
        sp_legacy = LegacySpreadsheet.get_instance()
        entity_spreadsheet = sp_legacy.get_entity_spreadsheet()
        
        # Get watch data in a format suitable for the spreadsheet
        watch_data = []
        for watch in watches:
            if watch.is_active:
                watch_data.append({
                    "name": watch.name,
                    "project": watch.project,
                    "battery_level": watch.battery_level,
                    "last_sync_time": watch.last_sync_time.isoformat() if watch.last_sync_time else None,
                    "is_active": watch.is_active,
                    "assigned_to": watch.current_student.name if watch.current_student else None
                })
        
        # Update FitbitData sheet in entity spreadsheet
        if watch_data:
            import pandas as pd
            df = pd.DataFrame(watch_data)
            entity_spreadsheet.update_sheet("FitbitData", df, strategy="replace")
            
            # Save changes
            GoogleSheetsAdapter.save(entity_spreadsheet, "FitbitData")
            
            print(f"Saved {len(watch_data)} watches to entity spreadsheet")
        else:
            print("No active watches to save")
    except Exception as e:
        print(f"Error saving watch data to spreadsheet: {e}")


if __name__ == "__main__":
    # Sync watches from legacy spreadsheet to entity layer
    sync_watches_to_entity_layer()
    
    # Update watch data in entity layer
    update_entity_watch_data()
