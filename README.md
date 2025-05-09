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

### Exercise 1 - Downloading files.

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

### Exercise 2 - Web Scraping + Downloading + Pandas

> 1. Thay đổi đường dẫn thư mục tại CMD thành `Exercise-2`

> 2. Chạy lệnh docker `build --tag=exercise-2 .` để build image Docker (Quá trình diễn ra trong 2 – 3 phút)
![image](https://github.com/user-attachments/assets/e6c08d26-b39e-4b5e-bb5e-08947bd281b6)

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

> Hoặc sau khi file main.py , chạy dòng lệnh thực thi lệnh `docker-compose up run`. Kết quả sau khi thực hiện:

![image](https://github.com/user-attachments/assets/9ac18ee9-7d4f-4ed9-9112-aedf7d4fffa6)

### Exercise 3 - Boto3 AWS + s3 + Python.

> 1. Thay đổi đường dẫn thư mục tại CMD thành `Exercise-3`

> 2. Chạy lệnh docker `build --tag=exercise-3 .` để build image Docker (Quá trình diễn ra trong 2 – 3 phút)
![image](https://github.com/user-attachments/assets/e10d5d35-ba00-4464-b84f-da79fa9d3d6f)
![image](https://github.com/user-attachments/assets/0b28b4f2-4aad-4e28-8a62-9dc32d3a6928)

> 3. Sau khi build xong, truy cập file `main.py` bằng VS code
![image](https://github.com/user-attachments/assets/63c47578-c075-4f40-b096-d1cc3c3ed998)

##### Code sử dụng cho main.py:
```
import boto3
import gzip
import io
import logging
from botocore.exceptions import ClientError
import unittest

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BUCKET_NAME = "commoncrawl"
WET_PATHS_KEY = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"

def get_s3_client():
    """Create and return an S3 client."""
    return boto3.client('s3')

def download_and_extract_gz_in_memory(s3_client):
    """Download and extract wet.paths.gz file in memory, return first URI."""
    try:
        # Download file into memory
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=WET_PATHS_KEY)
        gz_data = response['Body'].read()
        
        # Extract gzip content in memory
        with gzip.GzipFile(fileobj=io.BytesIO(gz_data), mode='rb') as gz:
            content = gz.read().decode('utf-8')
        
        # Get first line (URI)
        first_uri = content.splitlines()[0].strip()
        logger.info(f"Extracted first URI: {first_uri}")
        return first_uri
    except ClientError as e:
        logger.error(f"Error downloading/extracting {WET_PATHS_KEY}: {str(e)}")
        return None
    except gzip.BadGzipFile as e:
        logger.error(f"Invalid gzip file: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return None

def stream_file_lines(s3_client, file_key):
    """Download and stream file contents line by line."""
    try:
        # Get object with streaming response
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
        stream = response['Body']
        
        # Stream lines one at a time
        with io.TextIOWrapper(stream, encoding='utf-8') as text_stream:
            for line in text_stream:
                print(line.strip())
        
        logger.info(f"Successfully streamed contents of {file_key}")
    except ClientError as e:
        logger.error(f"Error streaming {file_key}: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error streaming {file_key}: {str(e)}")

def main():
    """Main function to execute the S3 download and streaming process."""
    s3_client = get_s3_client()
    
    # Download and extract wet.paths.gz in memory, get first URI
    first_uri = download_and_extract_gz_in_memory(s3_client)
    
    if first_uri:
        # Stream the file contents line by line
        stream_file_lines(s3_client, first_uri)

class TestS3Functions(unittest.TestCase):
    def test_get_s3_client(self):
        client = get_s3_client()
        self.assertIsNotNone(client)
        self.assertEqual(client.meta.service_model.service_name, 's3')
    
    def test_download_invalid_key(self):
        s3_client = get_s3_client()
        try:
            s3_client.get_object(Bucket=BUCKET_NAME, Key="nonexistent/key")
            self.fail("Expected ClientError for invalid key")
        except ClientError:
            pass

if __name__ == "__main__":
    main()
    # Run unit tests
    unittest.main(argv=[''], exit=False)
```
> 4. Kết quả nhận được trên Visual Studio Code
![image](https://github.com/user-attachments/assets/8fcf455e-caa1-469d-9df6-0ba6342ab375)

> 5. Hoặc sau khi lưu file `main.py`, thực hiện lệnh `docker-compose up run`
> 6. Kết quả sau khi thực hiện
![image](https://github.com/user-attachments/assets/0b52a684-2a11-45cb-8ac7-cf33d0f80149)

### Exercise 4 - Convert JSON to CSV + Ragged Directories.
1. Thay đổi đường dẫn thư mục tại cmd thành `Exercise-4`
2. Chạy lệnh docker `build --tag=exercise-4 .` để build image Docker (Quá trình diễn ra trong 2 – 3 phút)
![image](https://github.com/user-attachments/assets/cf9c4de5-2bcb-416a-8a7c-fcdaa871759e)
3. Nội dung file `main.py`
```
import os
import glob
import json
import csv

def flatten_json(y):
    # Hàm đệ quy để làm phẳng một đối tượng JSON lồng nhau
    out = {}

    def flatten(x, name=''):
        if isinstance(x, dict):
            # Nếu là dict, duyệt từng khóa và tiếp tục đệ quy
            for a in x:
                flatten(x[a], f"{name}{a}_")
        elif isinstance(x, list):
            # Nếu là list, duyệt từng phần tử theo chỉ số
            for i, a in enumerate(x):
                flatten(a, f"{name}{i}_")
        else:
            # Nếu là giá trị đơn giản, thêm vào dict kết quả
            out[name[:-1]] = x  # Loại bỏ dấu gạch dưới cuối cùng

    flatten(y)
    return out

def convert_json_to_csv(json_path):
    # Hàm chuyển đổi một file JSON thành file CSV
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)  # Đọc dữ liệu từ file JSON

    if isinstance(data, list):
        # Nếu là danh sách các đối tượng JSON
        flat_data = [flatten_json(item) for item in data]
    else:
        # Nếu là một đối tượng JSON duy nhất
        flat_data = [flatten_json(data)]

    # Tạo đường dẫn file CSV tương ứng
    csv_path = os.path.splitext(json_path)[0] + '.csv'

    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        if not flat_data:
            return  # Nếu dữ liệu rỗng thì không ghi file
        fieldnames = sorted(flat_data[0].keys())  # Lấy danh sách các cột từ khóa của dict đầu tiên
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()  # Ghi dòng tiêu đề CSV
        for row in flat_data:
            writer.writerow(row)  # Ghi từng dòng dữ liệu

def main():
    # Hàm chính để tìm và xử lý tất cả các file JSON trong thư mục 'data'
    json_files = glob.glob('data/**/*.json', recursive=True)  # Tìm tất cả file .json trong thư mục con

    print(f"Đã tìm thấy {len(json_files)} file JSON.")
    
    for json_file in json_files:
        print(f"Đang xử lý: {json_file}")
        convert_json_to_csv(json_file)  # Chuyển từng file JSON sang CSV

if __name__ == "__main__":
    main()  # Gọi hàm main khi chạy script
```
4. save file `main.py` và thực thi lệnh `docker-compose up run`
![image](https://github.com/user-attachments/assets/c000b830-a67c-4b5d-a975-84c79e46a307)
![image](https://github.com/user-attachments/assets/ed1748d0-c796-4402-9714-3a944feba22c)
5. Kết quả: 
![image](https://github.com/user-attachments/assets/f6cbb4b4-5f1c-4d2f-8458-de593034531c)
![image](https://github.com/user-attachments/assets/b4fd5b51-8cde-4933-a65a-56102ec1413e)
![image](https://github.com/user-attachments/assets/91f3b46b-bba7-4dca-afe7-ada01065219f)
![image](https://github.com/user-attachments/assets/979318ef-431e-4e2e-9ce3-45d0d82c5318)
![image](https://github.com/user-attachments/assets/85bd802f-95b5-409e-ab32-defaf558fe08)
![image](https://github.com/user-attachments/assets/d5542882-98a9-4f5f-9c7f-c2219c7362bb)
![image](https://github.com/user-attachments/assets/b38002ec-9003-4699-8bec-9fbb804151c9)
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
