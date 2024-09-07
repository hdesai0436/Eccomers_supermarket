import os
import logging

import os
import logging

class MultiFileLogger:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)  # Set the default logging level

        # Create log directories if they don't exist
        self.create_log_directories()

        # Define different handlers for different log levels
        self.debug_handler = logging.FileHandler('logs/debug/debug.log')
        self.info_handler = logging.FileHandler('logs/info/info.log')
        self.warning_handler = logging.FileHandler('logs/warning/warning.log')
        self.error_handler = logging.FileHandler('logs/error/error.log')

        # Define the log format
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        self.debug_handler.setFormatter(formatter)
        self.info_handler.setFormatter(formatter)
        self.warning_handler.setFormatter(formatter)
        self.error_handler.setFormatter(formatter)

        # Set level for each handler
        self.debug_handler.setLevel(logging.DEBUG)
        self.info_handler.setLevel(logging.INFO)
        self.warning_handler.setLevel(logging.WARNING)
        self.error_handler.setLevel(logging.ERROR)

        # Add handlers to the logger
        self.logger.addHandler(self.debug_handler)
        self.logger.addHandler(self.info_handler)
        self.logger.addHandler(self.warning_handler)
        self.logger.addHandler(self.error_handler)

    def create_log_directories(self):
        # Create directories for different log levels if they don't exist
        for level in ['debug', 'info', 'warning', 'error']:
            log_dir = os.path.join('logs', level)
            os.makedirs(log_dir, exist_ok=True)

    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)

    def critical(self, message):
        self.logger.critical(message)

    def close(self):
        for handler in [self.debug_handler, self.info_handler, self.warning_handler, self.error_handler]:
            handler.close()

# Example usage:

