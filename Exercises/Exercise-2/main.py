import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
import logging
from datetime import datetime
import unittest

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
TARGET_TIMESTAMP = "2024-01-19 10:27"
DOWNLOAD_DIR = "downloads"

def create_download_directory():
    """Create downloads directory if it doesn't exist."""
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def scrape_file_url():
    """Scrape the webpage to find the file with the target timestamp."""
    try:
        response = requests.get(BASE_URL)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all table rows
        rows = soup.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 2:
                timestamp_str = cols[1].text.strip()
                # Clean and parse timestamp
                try:
                    timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M')
                    if timestamp_str == TARGET_TIMESTAMP:
                        filename = cols[0].find('a').text.strip()
                        return filename
                except ValueError:
                    continue
        logger.error(f"No file found with timestamp {TARGET_TIMESTAMP}")
        return None
    except requests.RequestException as e:
        logger.error(f"Error scraping webpage: {str(e)}")
        return None

def download_file(filename):
    """Download the specified file."""
    if not filename:
        return None
    
    file_url = f"{BASE_URL}{filename}"
    file_path = os.path.join(DOWNLOAD_DIR, filename)
    
    try:
        response = requests.get(file_url, stream=True)
        response.raise_for_status()
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"Downloaded {filename} to {file_path}")
        return file_path
    except requests.RequestException as e:
        logger.error(f"Error downloading {file_url}: {str(e)}")
        return None

def analyze_temperature(file_path):
    """Read CSV with pandas and find records with highest HourlyDryBulbTemperature."""
    if not file_path or not os.path.exists(file_path):
        logger.error("File path invalid or file does not exist")
        return
    
    try:
        # Read CSV, handling potential encoding issues
        df = pd.read_csv(file_path, low_memory=False)
        
        # Convert HourlyDryBulbTemperature to numeric, coercing errors to NaN
        df['HourlyDryBulbTemperature'] = pd.to_numeric(df['HourlyDryBulbTemperature'], errors='coerce')
        
        # Find max temperature
        max_temp = df['HourlyDryBulbTemperature'].max()
        
        # Get all records with max temperature
        max_temp_records = df[df['HourlyDryBulbTemperature'] == max_temp]
        
        # Print results
        logger.info(f"Highest HourlyDryBulbTemperature: {max_temp}")
        print("\nRecords with highest HourlyDryBulbTemperature:")
        print(max_temp_records.to_string(index=False))
        
        return max_temp_records
    except pd.errors.ParserError as e:
        logger.error(f"Error parsing CSV file: {str(e)}")
    except KeyError as e:
        logger.error(f"Column 'HourlyDryBulbTemperature' not found: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error analyzing file: {str(e)}")

def main():
    """Main function to execute the web scraping and analysis process."""
    create_download_directory()
    
    # Scrape webpage to find file
    filename = scrape_file_url()
    
    # Download file
    file_path = download_file(filename)
    
    # Analyze file
    if file_path:
        analyze_temperature(file_path)

class TestWebScraper(unittest.TestCase):
    def setUp(self):
        create_download_directory()
    
    def test_create_download_directory(self):
        self.assertTrue(os.path.exists(DOWNLOAD_DIR))
    
    def test_get_filename_from_scrape(self):
        filename = scrape_file_url()
        self.assertIsNotNone(filename)
        self.assertTrue(filename.endswith('.csv'))
    
    def test_invalid_file_path(self):
        result = analyze_temperature("nonexistent.csv")
        self.assertIsNone(result)

if __name__ == "__main__":
    main()
    # Run unit tests
    unittest.main(argv=[''], exit=False)
