import logging
from scrape_weather import WeatherScraper
from db_operations import DBOperations

"""
This file is the execution file for the program. 

As of 11/16/2025 this file should: Scrape for data, Save said data to the database, then output the 5 most recent and oldest entries


"""

# Logs events to the log file
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    
    # ================= Scraping =================
    # This will prompt the program to start scraping for the weather data
    logger.info("Starting weather scraping...")
    scraper = WeatherScraper(start_year=2018)
    weather_data = scraper.scrape_all()
    logger.info(f"Scraped {len(weather_data)} daily records.\n")

    # ================= Initialize DB =================
    db = DBOperations()

    # ================= Save Data =================
    # This will save the date, and temperature information to the database for later use
    logger.info("Saving scraped data to database...")
    for date, temps in weather_data.items():
        db.save_data(
            date,
            temps['Max'],
            temps['Min'],
            temps['Mean']
        )

    # ================= Verification =================
    print("\n================ Verification ================\n")
    
    # This section will print out the 5 most recent and 5 oldest entries in the database

    # Total records
    total_records = db.count_records()
    print(f"Total records in database: {total_records}\n")
    
    # First 5 records
    first_5 = db.fetch_data(limit=5, order_desc=False)
    print("First 5 records:")
    for row in first_5:
        print(f"{row[0]}: Max={row[1]}, Min={row[2]}, Mean={row[3]}")
    print()

    # Last 5 records
    last_5 = db.fetch_data(limit=5, order_desc=True)
    print("Last 5 records:")
    for row in last_5:
        print(f"{row[0]}: Max={row[1]}, Min={row[2]}, Mean={row[3]}")
    print("\n=============================================\n")

if __name__ == "__main__":
    main()
