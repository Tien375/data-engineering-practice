# REPPORT LAB8 - LAB9
## MÔN: NHẬP MÔN KỸ THUẬT DỮ LIỆU - LỚP: DHKHDL19A
### Danh sách thành viên:
>> 1. Trần Nhật Tiến
>> 2. Phạm Thị Anh Thư _ MSSV 23643081

# LAB 8
Case-Study 1: Xây dựng pipeline tự động cào và trực quan dữ liệu

	https://www.youtube.com/watch?v=obNqcFM866M
![image](https://github.com/user-attachments/assets/552be05e-3fd4-49f9-aea9-a81470abc994)
![image](https://github.com/user-attachments/assets/5cfa2f87-f471-490a-b667-52278e1787fe)
![image](https://github.com/user-attachments/assets/908cf416-e4fd-456b-8184-587e69e0d5c6)
![image](https://github.com/user-attachments/assets/db4c440a-37c6-419d-9234-321f1241d1d8)
![image](https://github.com/user-attachments/assets/5e98250f-e3bc-4c40-9590-d5ebddb771bd)

# LAB 9
## BÀI LÀM
>> 1. Đăng nhập vào Github
>> 2. Truy cập đường link: https://github.com/danielbeach/data-engineering-practice/tree/main
>> 3. Chọn Fork
![image](https://github.com/user-attachments/assets/9c613dcf-a3cd-4632-bc26-10417ccb5cb5)
>> 4. Click Create Fork
![image](https://github.com/user-attachments/assets/ecc7468a-b410-4d23-8f77-798249a11cd5)


One of the main obstacles of Data Engineering is the large
and varied technical skills that can be required on a 
day-to-day basis.

*** Note - If you email a link to your GitHub repo with all the completed
exercises, I will send you back a free copy of my ebook Introduction to Data Engineering. ***

This aim of this repository is to help you develop and 
learn those skills. Generally, here are the high level
topics that these practice problems will cover.

- Python data processing.
- csv, flat-file, parquet, json, etc.
- SQL database table design.
- Python + Postgres, data ingestion and retrieval.
- PySpark
- Data cleansing / dirty data.

### How to work on the problems.
You will need two things to work effectively on most all
of these problems. 
- `Docker`
- `docker-compose`

All the tools and technologies you need will be packaged
  into the `dockerfile` for each exercise.

For each exercise you will need to `cd` into that folder and
run the `docker build` command, that command will be listed in
the `README` for each exercise, follow those instructions.

### Beginner Exercises

#### Exercise 1 - Downloading files.
The [first exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-1) tests your ability to download a number of files
from an `HTTP` source and unzip them, storing them locally with `Python`.
`cd Exercises/Exercise-1` and see `README` in that location for instructions.

> 1. Thực thi lệnh sau trong CMD: git clone để clone GitHub repo về máy của mình
![image](https://github.com/user-attachments/assets/d61fb447-06b2-4656-8555-bb9707404536)

> 2. Sau đó tiến hành chạy lệnh `cd data-engineering-practice/Exercises/Exercise-1` để thay đổi đường dẫn thư mục Exercise-1

> 3. Tiếp tục thực hiện lệnh: `docker build --tag=exercise-1 .` để build Docker image Quá trình sẽ mất vài phút
![image](https://github.com/user-attachments/assets/18340206-8dc3-46b2-974d-756c246a3e50)

> 4. Sử dụng Visual để chạy main.py
![image](https://github.com/user-attachments/assets/8ede908c-dba9-4998-abf2-3aa83a48e65d)

> ##### Code sử dụng cho main.py
```
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
```
> Đoạn code trên thực hiện các tác vụ: 
- Tạo thư mục downloads nếu chưa có.

-T rích xuất tên file từ URL.

- Tải file .zip từ URL bằng 3 cách: đồng bộ, đa luồng, bất đồng bộ.

- Lưu file vào thư mục downloads.

- Giải nén file .zip sau khi tải xong thành .csv

- Xóa file .zip sau khi giải nén.

- Bỏ qua URL không hợp lệ (ví dụ: cái Divvy\_Trips\_2220\_Q1.zip không tồn tại)

- Ghi log quá trình tải và giải nén.

> 5. Kết quả sau khi thực thi code trên Visual Studio Code
![image](https://github.com/user-attachments/assets/d4837a00-d412-4e3d-a004-e3b557d02ae0)
![image](https://github.com/user-attachments/assets/be248502-369b-4215-bfba-894f1cc622af)
![image](https://github.com/user-attachments/assets/d5b900a5-0493-42ca-9ed4-6e16ba38da9e)
![image](https://github.com/user-attachments/assets/02be479f-3a09-42de-a40d-d022b5ea4a4b)
![image](https://github.com/user-attachments/assets/fb1a5a5f-214c-4590-ace3-5d39be8f3d3f)

> 6. Kết quả nhận được

![image](https://github.com/user-attachments/assets/792be522-45b3-411a-899f-369f43181966)
![image](https://github.com/user-attachments/assets/a2dfe656-27e6-4460-b9ee-ce0c6faa38f8)
![image](https://github.com/user-attachments/assets/af012498-0f98-4110-bb6e-17dc7f2a0b72)

> 7. Sau khi save `main.py`, chạy lệnh `docker-compose up run` (mất khoảng 5 phút)
![image](https://github.com/user-attachments/assets/95848f93-4780-4512-a198-a31543a0313b)

#### Exercise 2 - Web Scraping + Downloading + Pandas
The [second exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-2) 
tests your ability perform web scraping, build uris, download files, and use Pandas to
do some simple cumulative actions.
`cd Exercises/Exercise-2` and see `README` in that location for instructions.

> 1. Thay đổi đường dẫn thư mục tại CMD thành `Exercise-2`

> 2. Chạy lệnh docker `build --tag=exercise-2 .` để build image Docker (Quá trình diễn ra trong 2 – 3 phút)
![image](https://github.com/user-attachments/assets/499bb349-2512-4d0a-a3f3-2ad71c07359f)
![image](https://github.com/user-attachments/assets/d32f03db-0b90-476d-bd2d-72ea48b262ba)

> 3. Sau khi build xong, truy cập file main.py bằng VS code
![image](https://github.com/user-attachments/assets/0e4ef0c1-f24c-4d75-8fb0-18a7b0d35376)

##### Nội dung file main.py

```
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
```

> 4. Kết quả sau khi thực thi code trên Visual Studio Code
![image](https://github.com/user-attachments/assets/40b70aa4-c743-4ca2-8c2e-2d23a26b7ddd)
![image](https://github.com/user-attachments/assets/b7492cb5-c24e-4294-bfd0-33d5b7790250)


#### Exercise 3 - Boto3 AWS + s3 + Python.
The [third exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-3) tests a few skills.
This time we  will be using a popular `aws` package called `boto3` to try to perform a multi-step
actions to download some open source `s3` data files.
`cd Exercises/Exercise-3` and see `README` in that location for instructions.

#### Exercise 4 - Convert JSON to CSV + Ragged Directories.
The [fourth exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-4) 
focuses more file types `json` and `csv`, and working with them in `Python`.
You will have to traverse a ragged directory structure, finding any `json` files
and converting them to `csv`.

#### Exercise 5 - Data Modeling for Postgres + Python.
The [fifth exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-5) 
is going to be a little different than the rest. In this problem you will be given a number of
`csv` files. You must create a data model / schema to hold these data sets, including indexes,
then create all the tables inside `Postgres` by connecting to the database with `Python`.


### Intermediate Exercises

#### Exercise 6 - Ingestion and Aggregation with PySpark.
The [sixth exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-6) 
Is going to step it up a little and move onto more popular tools. In this exercise we are going
to load some files using `PySpark` and then be asked to do some basic aggregation.
Best of luck!

#### Exercise 7 - Using Various PySpark Functions
The [seventh exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-7) 
Taking a page out of the previous exercise, this one is focus on using a few of the
more common build in PySpark functions `pyspark.sql.functions` and applying their
usage to real-life problems.

Many times to solve simple problems we have to find and use multiple functions available
from libraries. This will test your ability to do that.

#### Exercise 8 - Using DuckDB for Analytics and Transforms.
The [eighth exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-8) 
Using new tools is imperative to growing as a Data Engineer. DuckDB is one of those new tools. In this
exercise you will have to complete a number of analytical and transformation tasks using DuckDB. This
will require an understanding of the functions and documenation of DuckDB.

#### Exercise 9 - Using Polars lazy computation.
The [ninth exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-9) 
Polars is a new Rust based tool with a wonderful Python package that has taken Data Engineering by
storm. It's better than Pandas because it has both SQL Context and supports Lazy evalutation 
for larger than memory data sets! Show your Lazy skills!


### Advanced Exercises

#### Exercise 10 - Data Quality with Great Expectations
The [tenth exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-10) 
This exercise is to help you learn Data Quality, specifically a tool called Great Expectations. You will
be given an existing datasets in CSV format, as well as an existing pipeline. There is a data quality issue 
and you will be asked to implement some Data Quality checks to catch some of these issues.
