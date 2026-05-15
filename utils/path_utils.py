"""
Path utility functions for the project.

This module provides helper functions for working with file paths,
project structure, and configuration file locations.
"""
import os
import logging

logger = logging.getLogger(__name__)


def find_project_root(marker_dir: str = 'configs') -> str:
    """
    Finds the project root directory by walking up from current working directory
    until it finds the marker directory (default: 'configs').
    
    Args:
        marker_dir (str): Directory name to search for that indicates project root.
                         Defaults to 'configs'.
    
    Returns:
        str: Absolute path to project root directory.
        
    Raises:
        FileNotFoundError: If project root cannot be found within 10 levels up.
        
    Example:
        >>> root = find_project_root()
        >>> config_path = os.path.join(root, 'configs', 'generic.json')
    """
    current = os.getcwd()
    
    # Walk up the directory tree
    for _ in range(10):  # Limit depth to avoid infinite loop
        if os.path.exists(os.path.join(current, marker_dir)):
            logger.debug(f"Found project root at: {current}")
            return current
        parent = os.path.dirname(current)
        if parent == current:  # Reached filesystem root
            break
        current = parent
    
    # If not found, raise error
    raise FileNotFoundError(
        f"Could not find project root. Make sure '{marker_dir}' directory exists in project root."
    )


def get_config_path(filename: str, subdirectory: str = 'configs') -> str:
    """
    Gets the absolute path to a configuration file in the project.
    
    Args:
        filename (str): Name of the config file (e.g., 'generic.json')
        subdirectory (str): Subdirectory within project root. Defaults to 'configs'.
        
    Returns:
        str: Absolute path to the configuration file.
        
    Example:
        >>> generic_config = get_config_path('generic.json')
        >>> entity_config = get_config_path('deputados.json')
    """
    root = find_project_root()
    return os.path.join(root, subdirectory, filename)


def make_absolute_path(path: str, base_dir: str = None) -> str:
    """
    Converts a relative path to absolute, using project root as base if not specified.
    
    Args:
        path (str): Path to convert (can be absolute or relative)
        base_dir (str, optional): Base directory for relative paths. 
                                 If None, uses project root.
    
    Returns:
        str: Absolute path.
        
    Example:
        >>> abs_path = make_absolute_path('configs/deputados.json')
    """
    if os.path.isabs(path):
        return path
    
    if base_dir is None:
        base_dir = find_project_root()
    
    return os.path.join(base_dir, path)
