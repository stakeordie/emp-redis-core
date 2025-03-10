import logging
import os
import sys

def setup_logging():
    """Set up basic logging configuration"""
    # Get the root logger
    root_logger = logging.getLogger()
    
    # Clear any existing handlers to avoid duplicate logs
    if root_logger.handlers:
        for handler in root_logger.handlers:
            root_logger.removeHandler(handler)
    
    # Default level from environment or DEBUG for development
    log_level_name = os.environ.get("LOG_LEVEL", "DEBUG")
    log_level = getattr(logging, log_level_name.upper(), logging.DEBUG)
    
    # Set root logger level
    root_logger.setLevel(log_level)
    
    # Create console handler with detailed formatting
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    
    # Detailed format for development
    log_format = "%(asctime)s - %(levelname)s - %(name)s:%(lineno)d - %(message)s"
    formatter = logging.Formatter(log_format)
    console_handler.setFormatter(formatter)
    
    # Add handler to root logger
    root_logger.addHandler(console_handler)
    
    return root_logger

# Initialize logging
logger = setup_logging()