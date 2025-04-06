import threading
import queue
import time
import streamlit as st
from entity.Sheet import Spreadsheet, GoogleSheetsAdapter

class AsyncSheetsManager:
    """
    A utility class to handle asynchronous updates to Google Sheets.
    This prevents blocking the main thread when saving to Google Sheets.
    """
    _instance = None
    _lock = threading.Lock()
    
    @classmethod
    def get_instance(cls):
        """Singleton pattern to ensure only one background worker exists"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = AsyncSheetsManager()
        return cls._instance
    
    def __init__(self):
        """Initialize the async sheets manager with a queue and worker thread"""
        self.message_queue = queue.Queue()
        self.worker_thread = None
        self.running = False
        self.sheet_connection = None
        self.debug_messages = []
    
    def connect(self, sheet_name, api_key):
        """Establish the Google Sheets connection"""
        try:
            self.sheet_connection = Spreadsheet(name=sheet_name, api_key=api_key)
            GoogleSheetsAdapter.connect(self.sheet_connection)
            return True
        except Exception as e:
            self.debug_messages.append(f"Connection error: {str(e)}")
            return False
    
    def start_worker(self):
        """Start the background worker thread if not already running"""
        if not self.running:
            self.running = True
            self.worker_thread = threading.Thread(target=self._process_queue)
            self.worker_thread.daemon = True  # Thread will exit when main program exits
            self.worker_thread.start()
    
    def _process_queue(self):
        """Background thread that processes the message queue"""
        while self.running:
            try:
                # Get a batch of messages (up to 10) to process at once
                messages = []
                try:
                    # Always get at least one message (blocking)
                    messages.append(self.message_queue.get(block=True, timeout=5))
                    
                    # Try to get more messages (non-blocking)
                    for _ in range(9):  # Up to 9 more messages (total 10)
                        try:
                            messages.append(self.message_queue.get(block=False))
                        except queue.Empty:
                            break
                except queue.Empty:
                    # If queue is empty after waiting, just continue the loop
                    continue
                
                if not messages:
                    continue
                
                # Process this batch
                self._save_to_sheet(messages)
                
                # Mark all processed messages as done
                for _ in range(len(messages)):
                    self.message_queue.task_done()
                    
            except Exception as e:
                self.debug_messages.append(f"Worker thread error: {str(e)}")
                time.sleep(1)  # Prevent tight loop if errors
    
    def _save_to_sheet(self, messages):
        """Save a batch of messages to the Google Sheet"""
        if not self.sheet_connection:
            self.debug_messages.append("No sheet connection established")
            return
        
        try:
            # Ensure 'chats' sheet exists
            try:
                self.sheet_connection.get_sheet("chats", sheet_type="chats")
            except:
                # Sheet will be created on first update
                pass
            
            # Add each message to the sheet
            for msg in messages:
                self.sheet_connection.update_sheet("chats", msg, strategy="append")
            
            # Save all changes at once
            GoogleSheetsAdapter.save(self.sheet_connection)
            self.debug_messages.append(f"Successfully saved {len(messages)} messages")
            
        except Exception as e:
            self.debug_messages.append(f"Sheet save error: {str(e)}")
            # Put messages back in queue for retry
            for msg in messages:
                self.message_queue.put(msg)
    
    def add_message(self, message):
        """Add a message to the processing queue"""
        if not self.running:
            self.start_worker()
        self.message_queue.put(message)
        return True
    
    def get_debug_info(self):
        """Get the last few debug messages"""
        # Return last 5 debug messages
        return self.debug_messages[-5:] if self.debug_messages else []
    
    def shutdown(self):
        """Shut down the worker thread (called when app exits)"""
        self.running = False
        if self.worker_thread:
            self.worker_thread.join(timeout=1)
