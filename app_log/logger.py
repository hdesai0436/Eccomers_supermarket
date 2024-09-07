import logging
import os
from datetime import datetime



def setup_logger(script_file, log_level="info"):
    """
    Set up the logger to create logs under a folder structure like 'logs/<script_name>/<log_level>/<date>.log'.
    :param script_file: The current script file name (__file__).
    :param log_level: The logging level (e.g., 'error', 'info').
    :return: Configured logger object.
    """
    # Get the script name (without the extension) dynamically
    script_name = os.path.splitext(os.path.basename(script_file))[0]

    # Get the current date in a specific format (for example: '09_07_2024')
    current_date = datetime.now().strftime('%m_%d_%Y')

    # Create the folder structure: logs/script_name/log_level/
    logs_folder = os.path.join(os.getcwd(), "logs", script_name, log_level)
    os.makedirs(logs_folder, exist_ok=True)

    # Define the log file name based on the current date
    log_file_name = f"{current_date}.log"
    LOG_FILE_PATH = os.path.join(logs_folder, log_file_name)

    # Configure the logger object
    logger = logging.getLogger(script_name)

    # Check if handlers are already added to avoid duplicates
    if not logger.hasHandlers():
        # Set the log level dynamically based on the input
        logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

        # Create a file handler
        file_handler = logging.FileHandler(LOG_FILE_PATH)
        file_handler.setLevel(getattr(logging, log_level.upper(), logging.INFO))

        # Define the log format
        formatter = logging.Formatter("[ %(asctime)s ] %(lineno)d %(name)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)

        # Add the handler to the logger
        logger.addHandler(file_handler)

    return logger

