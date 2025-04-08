import time
from functools import wraps
import streamlit as st
import inspect

def sheets_cache(timeout=300):
    """
    Decorator to cache Google Sheets API responses to reduce API calls.
    
    Args:
        timeout (int): Cache timeout in seconds
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Get function signature
            sig = inspect.signature(func)
            param_names = list(sig.parameters.keys())
            
            # Create a cache key based on function name only
            # Skip hashing arguments that might not be hashable
            cache_key = f"sheets_cache_{func.__name__}"
            
            # Add only hashable arguments to the cache key
            try:
                # Try to create a hash of primitive arguments
                for i, arg in enumerate(args):
                    if i < len(param_names):
                        param_name = param_names[i]
                        # Skip parameters that start with underscore
                        if not param_name.startswith('_'):
                            try:
                                hash(arg)  # Test if hashable
                                cache_key += f"_{param_name}={arg}"
                            except:
                                cache_key += f"_{param_name}=unhashable"
                
                # Add hashable keyword arguments
                for key, value in kwargs.items():
                    if not key.startswith('_'):
                        try:
                            hash(value)  # Test if hashable
                            cache_key += f"_{key}={value}"
                        except:
                            cache_key += f"_{key}=unhashable"
            except:
                # If anything goes wrong with hashing, use a simpler key
                cache_key = f"sheets_cache_{func.__name__}_simple"
            
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
