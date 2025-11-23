# run_file.py
"""
Entry point for the Weather App.

Hands off all logic to WeatherProcessor.  
This file only launches the application and catches fatal errors.
Author: Caden Jackson

Created: 11/16/2025
last updated: 11/23/2025
"""

import logging
from weather_processor import WeatherProcessor

# ---------------------------
# Configure logging to file
# ---------------------------
logging.basicConfig(
    filename="weather_app.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    """Launch the WeatherProcessor with error handling."""
    try:
        processor = WeatherProcessor()
        processor.run()
    except Exception as exc:
        logger.exception("Fatal error occurred in run_file: %s", exc)
        print("A fatal error occurred. Please check weather_app.log.")


if __name__ == "__main__":
    main()
