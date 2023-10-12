import logging
import pytest
from logging_config import configure_logging

def test_configure_logging():
    # Reset logging configuration to default
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.getLogger().setLevel(logging.NOTSET)

    # Debug statement to check logging level before configuration
    print(f"Logging level before: {logging.getLogger().level}")
    
    # Run the function to configure logging
    configure_logging()

    # Debug statement to check logging level after configuration
    print(f"Logging level after: {logging.getLogger().level}")
    
    # Access the root logger to check its configuration
    root_logger = logging.getLogger()
    
    # Check if the logging level is set to INFO
    assert root_logger.level == logging.INFO