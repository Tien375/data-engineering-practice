import requests
import os
import zipfile
import aiohttp
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
from pathlib import Path
import unittest

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

DOWNLOAD_DIR = "downloads"

def create_download_directory():
    """Create downloads directory if it doesn't exist."""
    Path(DOWNLOAD_DIR).mkdir(exist_ok=True)

def get_filename_from_uri(uri):
    """Extract filename from URI."""
    return uri.split('/')[-1]

def extract_zip(zip_path):
    """Extract CSV from zip file and remove the zip."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(DOWNLOAD_DIR)
        os.remove(zip_path)
        logger.info(f"Extracted and removed {zip_path}")
    except zipfile.BadZipFile:
        logger.error(f"Invalid zip file: {zip_path}")
        os.remove(zip_path)

def download_file(uri):
    """Download a single file synchronously."""
    filename = get_filename_from_uri(uri)
    file_path = os.path.join(DOWNLOAD_DIR, filename)
    
    try:
        response = requests.get(uri, stream=True)
        if response.status_code == 200:
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logger.info(f"Downloaded {filename}")
            extract_zip(file_path)
        else:
            logger.error(f"Failed to download {uri}: Status {response.status_code}")
    except requests.RequestException as e:
        logger.error(f"Error downloading {uri}: {str(e)}")

async def download_file_async(session, uri):
    """Download a single file asynchronously."""
    filename = get_filename_from_uri(uri)
    file_path = os.path.join(DOWNLOAD_DIR, filename)
    
    try:
        async with session.get(uri) as response:
            if response.status == 200:
                with open(file_path, 'wb') as f:
                    f.write(await response.read())
                logger.info(f"Downloaded {filename} (async)")
                extract_zip(file_path)
            else:
                logger.error(f"Failed to download {uri}: Status {response.status}")
    except aiohttp.ClientError as e:
        logger.error(f"Error downloading {uri}: {str(e)}")

async def download_files_async(uris):
    """Download files asynchronously using aiohttp."""
    async with aiohttp.ClientSession() as session:
        tasks = [download_file_async(session, uri) for uri in uris]
        await asyncio.gather(*tasks)

def download_files_threaded(uris):
    """Download files using ThreadPoolExecutor."""
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(download_file, uris)

def main():
    """Main function to execute the download process."""
    create_download_directory()
    
    # Synchronous download
    logger.info("Starting synchronous downloads")
    for uri in download_uris:
        download_file(uri)
    
    # Threaded download
    logger.info("Starting threaded downloads")
    download_files_threaded(download_uris)
    
    # Async download
    logger.info("Starting async downloads")
    asyncio.run(download_files_async(download_uris))

class TestDownloadFunctions(unittest.TestCase):
    def setUp(self):
        create_download_directory()
    
    def test_get_filename_from_uri(self):
        uri = "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip"
        self.assertEqual(get_filename_from_uri(uri), "Divvy_Trips_2018_Q4.zip")
    
    def test_create_download_directory(self):
        self.assertTrue(os.path.exists(DOWNLOAD_DIR))
    
    def test_invalid_uri(self):
        uri = "https://invalid-url/does_not_exist.zip"
        download_file(uri)
        filename = get_filename_from_uri(uri)
        self.assertFalse(os.path.exists(os.path.join(DOWNLOAD_DIR, filename)))

if __name__ == "__main__":
    main()
    # Run unit tests
    unittest.main(argv=[''], exit=False)
