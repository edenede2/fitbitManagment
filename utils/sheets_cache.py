import time
from functools import wraps
import streamlit as st

def sheets_cache(timeout=300):
    """
    Decorator to cache Google Sheets API responses to reduce API calls.
    
    Args:
        timeout (int): Cache timeout in seconds
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Create a cache key based on function name and arguments
            cache_key = f"sheets_cache_{func.__name__}_{str(args)}_{str(kwargs)}"
            
            # Check if we have a cached result
            if cache_key in st.session_state:
                cache_time, cached_result = st.session_state[cache_key]
                # Check if the cache is still valid
                if time.time() - cache_time < timeout:
                    return cached_result
            
            # No valid cache, call the function
            result = func(*args, **kwargs)
            
            # Store the result in cache
            st.session_state[cache_key] = (time.time(), result)
            
            return result
        return wrapper
    return decorator
