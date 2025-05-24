#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Logging utility for news scrapers
Creates separate log files for each script with rotating file handler
"""
import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime

# Set up constants
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
MAX_LOG_SIZE = 10 * 1024 * 1024  # 10 MB
BACKUP_COUNT = 5  # Keep 5 backup files

# Create log directory if it doesn't exist
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

def get_logger(script_name, level=logging.INFO):
    """
    Get a logger with rotating file handler
    
    Args:
        script_name: Name of the script (used for log file name)
        level: Logging level
        
    Returns:
        Logger instance
    """
    # Remove .py extension if present
    if script_name.endswith('.py'):
        script_name = script_name[:-3]
    
    # Create logger
    logger = logging.getLogger(script_name)
    logger.setLevel(level)
    
    # Clear existing handlers to avoid duplicates
    if logger.handlers:
        logger.handlers.clear()
    
    # Create log file path
    log_file = os.path.join(LOG_DIR, f"{script_name}.log")
    
    # Create handlers
    file_handler = RotatingFileHandler(
        log_file, 
        maxBytes=MAX_LOG_SIZE, 
        backupCount=BACKUP_COUNT
    )
    console_handler = logging.StreamHandler()
    
    # Set levels
    file_handler.setLevel(level)
    console_handler.setLevel(level)
    
    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Add formatters to handlers
    file_handler.setFormatter(file_formatter)
    console_handler.setFormatter(console_formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def log_start(logger, script_name, test_mode=False):
    """Log script start with banner"""
    mode = "TEST MODE" if test_mode else "PRODUCTION MODE"
    logger.info(f"{'='*20} STARTING {script_name} [{mode}] {'='*20}")
    logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
def log_end(logger, script_name, success=True):
    """Log script end with banner"""
    status = "SUCCESSFUL" if success else "WITH ERRORS"
    logger.info(f"{'='*20} ENDING {script_name} [{status}] {'='*20}")
    logger.info(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    # Test the logger
    test_logger = get_logger("logger_test")
    log_start(test_logger, "Logger Test", test_mode=True)
    test_logger.debug("This is a debug message")
    test_logger.info("This is an info message")
    test_logger.warning("This is a warning message")
    test_logger.error("This is an error message")
    test_logger.critical("This is a critical message")
    log_end(test_logger, "Logger Test")
    
    print(f"Log file created at: {os.path.join(LOG_DIR, 'logger_test.log')}")