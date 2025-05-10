# REPPORT LAB8 - LAB9
## MÔN: NHẬP MÔN KỸ THUẬT DỮ LIỆU - LỚP: DHKHDL19A
### Danh sách thành viên:
>> 1. Trần Nhật Tiến _ MSSV 23673681
>> 2. Phạm Thị Anh Thư _ MSSV 23643081

# LAB 8
## Case-Study 1: Xây dựng pipeline tự động cào và trực quan dữ liệu

	https://www.youtube.com/watch?v=obNqcFM866M
![image](https://github.com/user-attachments/assets/552be05e-3fd4-49f9-aea9-a81470abc994)
![image](https://github.com/user-attachments/assets/5cfa2f87-f471-490a-b667-52278e1787fe)
![image](https://github.com/user-attachments/assets/908cf416-e4fd-456b-8184-587e69e0d5c6)
![image](https://github.com/user-attachments/assets/db4c440a-37c6-419d-9234-321f1241d1d8)
![image](https://github.com/user-attachments/assets/5e98250f-e3bc-4c40-9590-d5ebddb771bd)

## Case-Study 2: Xây dựng pipeline để tự động cào dữ liệu, huấn luyện mô hình
	https://www.youtube.com/watch?v=LtInPTXfdb8
### Case#1: Run simple DAG at Local
>> 1. Chạy lệnh `docker compose up airflow-init` để khởi tạo cơ sở dữ liệu và tài khoản admin 
![image](https://github.com/user-attachments/assets/9c4d4453-72e7-492a-b876-d13dca201c10)
![image](https://github.com/user-attachments/assets/d1a4ed00-5af7-428f-9600-bd1427112ece)
>> 2. Chạy lệnh `docker compose up -d` để chạy toàn bộ hệ thống Airflow 
![image](https://github.com/user-attachments/assets/278503d8-eb4a-4880-a527-2439000469bd)
![image](https://github.com/user-attachments/assets/aaa611b1-ace5-41a5-9a9b-b9d999fac61d)
>> 3. Nội dung DAG file `simple_dag_local.py`
```
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    'tien_dag01',
    default_args={
        'email': ['nhattien200537@gmail.com'],
        'email_on_failure': True,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple DAG sample by Tien',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 5, 10),
    tags=['tien'],
)

t1 = BashOperator(
    task_id='print_date',
    bash_command='date > D:\date.txt',
    dag = dag
)

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    retries=3,
    dag = dag
)

t3 = BashOperator(
    task_id='echo',
    bash_command='echo t3 running',
    dag = dag
)

[t1 , t2] >> t3
```
>> 4. Sau khi định nghĩa DAG file `simple_dag_local.py` copy vào thư mục dags ở trên service
![image](https://github.com/user-attachments/assets/892419a5-3429-46fa-b463-7333c9ad8192)
>> 5. Mở trình duyệt truy cập địa chỉ: `http://localhost:8080` trên thanh URL
![image](https://github.com/user-attachments/assets/d9b63c12-8330-4189-93f8-6a067eaec700)
![image](https://github.com/user-attachments/assets/3fd43a82-1174-462c-80b3-70facea9160d)
>> 7. Ấn run để chạy:
![image](https://github.com/user-attachments/assets/98e81e22-1dcf-410e-9360-8f961cc00fec)


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
2. Chạy lệnh `docker build --tag=exercise-4 .` để build image Docker (Quá trình diễn ra trong khoảng 5 phút)
![image](https://github.com/user-attachments/assets/cf9c4de5-2bcb-416a-8a7c-fcdaa871759e)
![image](https://github.com/user-attachments/assets/c000b830-a67c-4b5d-a975-84c79e46a307)
3. Nội dung file `main.py`
```
import os
import glob
import json
import csv

def flatten_json(y):
    """
    Hàm đệ quy để làm phẳng (flatten) một đối tượng JSON lồng nhau.
    Mục đích là chuyển các key lồng nhau thành một key duy nhất bằng cách nối các key cha-con bằng dấu gạch dưới (_).
    Trả về một dict phẳng, phù hợp để ghi vào CSV.
    """
    out = {}

    def flatten(x, name=''):
        if isinstance(x, dict):  # Nếu là dict, duyệt các key
            for a in x:
                flatten(x[a], f"{name}{a}_")
        elif isinstance(x, list):  # Nếu là list, duyệt từng phần tử theo chỉ số
            for i, a in enumerate(x):
                flatten(a, f"{name}{i}_")
        else:  # Nếu là giá trị đơn giản, lưu vào dict kết quả
            out[name[:-1]] = x  # name[:-1] để loại bỏ dấu "_" cuối cùng

    flatten(y)
    return out


def convert_json_to_csv(json_path):
    """
    Hàm chuyển đổi một file JSON (có thể là object hoặc list các object) thành file CSV.
    - Đọc dữ liệu từ file JSON.
    - Làm phẳng dữ liệu nếu có lồng nhau.
    - Ghi dữ liệu ra file CSV cùng tên với file JSON (chỉ thay phần mở rộng).
    """
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Nếu là danh sách các object thì làm phẳng từng object
    if isinstance(data, list):
        flat_data = [flatten_json(item) for item in data]
    else:
        flat_data = [flatten_json(data)]

    # Tạo tên file CSV
    csv_path = os.path.splitext(json_path)[0] + '.csv'

    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        if not flat_data:
            return  # Nếu dữ liệu rỗng thì không ghi
        fieldnames = sorted(flat_data[0].keys())  # Lấy danh sách cột từ khóa của dict đầu tiên
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()  # Ghi tiêu đề cột
        for row in flat_data:
            writer.writerow(row)  # Ghi từng dòng dữ liệu


def main():
    """
    Hàm chính:
    - Tìm tất cả các file JSON trong thư mục 'data' và các thư mục con.
    - Gọi hàm chuyển đổi để xử lý từng file JSON sang CSV.
    """
    json_files = glob.glob('data/**/*.json', recursive=True)  # Tìm đệ quy tất cả các file .json

    print(f"Đã tìm thấy {len(json_files)} file JSON.")
    
    for json_file in json_files:
        print(f"Đang xử lý: {json_file}")
        convert_json_to_csv(json_file)


if __name__ == "__main__":
    main()
```
4. Sau khi nhập code, save file `main.py` thì thực thi lệnh `docker-compose up run`
![image](https://github.com/user-attachments/assets/ed1748d0-c796-4402-9714-3a944feba22c)
5. Kết quả:
![image](https://github.com/user-attachments/assets/2cfc4189-33b4-4194-af6f-4ee6b80dca0b)
![image](https://github.com/user-attachments/assets/b2148c58-4a4d-44cc-90da-2aaec53a8e69)
![image](https://github.com/user-attachments/assets/efa1d525-891d-4525-9dc0-8700d1964dfa)
![image](https://github.com/user-attachments/assets/b81319d1-484d-4120-a5e7-ce4fe752d307)
![image](https://github.com/user-attachments/assets/fed35c44-fb64-41a7-a031-5f519cb86566)
![image](https://github.com/user-attachments/assets/5ceb7bb0-b485-43ff-aeab-de0a250bb01c)
![image](https://github.com/user-attachments/assets/63db612d-3c95-4734-a8f3-693bc39f2410)
![image](https://github.com/user-attachments/assets/ab3914cd-c2a5-443e-8daf-d03b1f322e9c)
![image](https://github.com/user-attachments/assets/888ebe61-9bdc-4f68-829d-3e04b210d179)

### Exercise 5 - Data Modeling for Postgres + Python.
1. Thay đổi đường dẫn cmd thành `Exercise-5`
2. Chạy lệnh `docker build --tag=exercise-5 .` để build image Docker (Quá trình diễn ra trong khoảng 1-2 phút)
![image](https://github.com/user-attachments/assets/0940a429-7df2-44d5-9000-ccd418f3ba46)
3. Tạo câu lệnh SQL CREATE cho mỗi tập tin CSV
![image](https://github.com/user-attachments/assets/a8763542-c80c-4535-a693-12430fdac39e)
4. Nội dung file `main.py`:
```
import psycopg2
import csv
import os

def execute_sql_script(cur, filename):
    """
    Hàm thực thi nội dung từ một file SQL.
    - Đọc toàn bộ nội dung file SQL.
    - Thực thi bằng con trỏ cơ sở dữ liệu (cursor).
    """
    with open(filename, 'r') as f:
        cur.execute(f.read())

def insert_csv_data(cur, conn, table_name, file_path):
    """
    Hàm chèn dữ liệu từ file CSV vào bảng trong cơ sở dữ liệu PostgreSQL.
    - Đọc file CSV.
    - Tạo câu lệnh INSERT tương ứng với số cột.
    - Thực hiện chèn từng dòng dữ liệu vào bảng.
    - Gọi commit() để lưu thay đổi.
    """
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        headers = next(reader)  # Đọc dòng đầu tiên làm tên cột
        placeholders = ','.join(['%s'] * len(headers))  # Tạo placeholders: %s,%s,...
        insert_query = f"INSERT INTO {table_name} ({','.join(headers)}) VALUES ({placeholders})"
        for row in reader:
            cur.execute(insert_query, row)
    conn.commit()  # Ghi dữ liệu vào cơ sở dữ liệu

def main():
    """
    Hàm chính:
    - Kết nối đến cơ sở dữ liệu PostgreSQL.
    - Tạo bảng từ file SQL.
    - Đọc và chèn dữ liệu từ các file CSV vào các bảng tương ứng.
    - Đóng kết nối khi hoàn tất.
    """
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"

    # Kết nối đến PostgreSQL
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    cur = conn.cursor()

    # Thực thi script tạo bảng
    execute_sql_script(cur, "create_tables.sql")

    # Chèn dữ liệu từ các file CSV vào bảng
    insert_csv_data(cur, conn, "accounts", "data/accounts.csv")
    insert_csv_data(cur, conn, "products", "data/products.csv")
    insert_csv_data(cur, conn, "transactions", "data/transactions.csv")

    # Đóng cursor và connection
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
```
4. Sau khi nhập code, save file `main.py` thì thực thi lệnh `docker-compose up run`
![image](https://github.com/user-attachments/assets/0bb57863-2a3e-49b1-8506-608809b6eb3a)
5. Kết quả:
![image](https://github.com/user-attachments/assets/5b83de03-f9dc-4ea6-beb8-ca95865af59a)
![image](https://github.com/user-attachments/assets/9ddd7713-955e-4147-81d1-9a080d1d29f8)
![image](https://github.com/user-attachments/assets/7df25829-036c-499b-bde0-394f5a190fb2)
![image](https://github.com/user-attachments/assets/02ae5eea-5af0-49e3-8e8e-dedf1178e439)

### Exercise 6 - Ingestion and Aggregation with PySpark.
1. Thay đổi đường dẫn cmd thành `Exercise-6`
2. Chạy lệnh `docker build --tag=exercise-6 .` để build image Docker (Quá trình diễn ra trong khoảng 10-15 phút)
![image](https://github.com/user-attachments/assets/8d7cd1d6-e0fd-4b51-b1f6-62c6c5bab99f)
3. Nội dung file `main.py`:
```
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

import tempfile
import os
import zipfile

def read_zip_csv(spark, path):
    """
    Đọc file ZIP chứa các file CSV và trả về một DataFrame duy nhất:
    - Giải nén tạm các file .csv trong ZIP vào ổ đĩa.
    - Đọc các file .csv bằng Spark.
    - Hợp nhất tất cả các DataFrame nếu có nhiều file.
    - Trả về một DataFrame tổng hợp.
    """
    with zipfile.ZipFile(path, 'r') as zip_ref:
        # Lọc danh sách file CSV hợp lệ
        csv_files = [
            f for f in zip_ref.namelist()
            if f.endswith('.csv') and not f.startswith('__MACOSX/') and not os.path.basename(f).startswith('._')
        ]

        dfs = []
        temp_paths = []

        for csv_file in csv_files:
            # Ghi dữ liệu CSV tạm thời ra ổ đĩa để Spark có thể đọc
            with zip_ref.open(csv_file) as file:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
                    temp_file.write(file.read())
                    temp_paths.append(temp_file.name)

                # Đọc file CSV thành DataFrame
                df = spark.read.option("header", "true").csv(temp_file.name, inferSchema=True)
                dfs.append(df)

        # Hợp nhất tất cả DataFrame nếu có nhiều
        final_df = dfs[0] if len(dfs) == 1 else dfs[0].unionByName(*dfs[1:])
        final_df.cache().count()  # Cache để tăng hiệu năng truy vấn sau

        # Xoá các file tạm
        for path in temp_paths:
            os.remove(path)

        return final_df


def standardize_schema(df):
    """
    Chuẩn hoá schema của DataFrame:
    - Đổi tên các cột nếu là dữ liệu năm 2020 (ride_id, started_at, ...).
    - Tính tripduration nếu chưa có.
    - Thêm cột gender và birthyear nếu thiếu.
    - Trả về các cột được chuẩn hoá.
    """
    columns = df.columns
    if "ride_id" in columns:
        df = df.withColumnRenamed("ride_id", "trip_id") \
               .withColumnRenamed("started_at", "start_time") \
               .withColumnRenamed("start_station_name", "from_station_name") \
               .withColumn("tripduration", unix_timestamp("ended_at") - unix_timestamp("start_time")) \
               .withColumn("gender", lit(None).cast("string")) \
               .withColumn("birthyear", lit(None).cast("int"))
        df = df.select("trip_id", "tripduration", "start_time", "from_station_name", "gender", "birthyear")
    else:
        # Dữ liệu từ 2019 đã đúng định dạng
        df = df.select("trip_id", "tripduration", "start_time", "from_station_name", "gender", "birthyear")
    return df


def preprocess_data(df):
    """
    Tiền xử lý dữ liệu:
    - Ép kiểu dữ liệu.
    - Tạo thêm các cột ngày (date), tháng (month), tuổi (age).
    """
    return df.withColumn("tripduration", col("tripduration").cast("float")) \
             .withColumn("start_time", to_timestamp("start_time")) \
             .withColumn("date", to_date("start_time")) \
             .withColumn("month", date_format(col("start_time"), "yyyy-MM")) \
             .withColumn("birthyear", col("birthyear").cast("int")) \
             .withColumn("age", 2023 - col("birthyear"))


def avg_trip_duration_per_day(df):
    """
    Báo cáo: thời lượng chuyến đi trung bình theo ngày.
    - Ghi ra file CSV duy nhất trong thư mục reports/avg_trip_duration_per_day
    """
    df.groupBy("date").agg(avg("tripduration").alias("avg_duration")) \
        .coalesce(1).write.mode("overwrite").csv("reports/avg_trip_duration_per_day", header=True)


def trips_per_day(df):
    """
    Báo cáo: tổng số chuyến đi mỗi ngày.
    - Ghi ra CSV trong thư mục reports/trips_per_day
    """
    df.groupBy("date").count() \
        .coalesce(1).write.mode("overwrite").csv("reports/trips_per_day", header=True)


def most_popular_start_station_per_month(df):
    """
    Báo cáo: bến xuất phát phổ biến nhất mỗi tháng.
    - Tính số lượt xuất phát theo tháng và bến.
    - Chọn bến có số lượt cao nhất mỗi tháng.
    """
    window = Window.partitionBy("month").orderBy(desc("count"))
    df.groupBy("month", "from_station_name").count() \
        .withColumn("rank", row_number().over(window)) \
        .filter(col("rank") == 1) \
        .coalesce(1).write.mode("overwrite").csv("reports/most_popular_start_station", header=True)


def top3_stations_last_two_weeks(df):
    """
    Báo cáo: top 3 bến xuất phát mỗi ngày trong 14 ngày gần nhất.
    - Dựa vào ngày mới nhất có trong dữ liệu.
    """
    latest_date = df.select(max("date")).first()[0]
    cutoff = latest_date - expr("INTERVAL 14 DAYS")
    window = Window.partitionBy("date").orderBy(desc("count"))

    df.filter(col("date") >= cutoff) \
        .groupBy("date", "from_station_name").count() \
        .withColumn("rank", row_number().over(window)) \
        .filter(col("rank") <= 3) \
        .coalesce(1).write.mode("overwrite").csv("reports/top3_stations_last_2_weeks", header=True)


def gender_avg_duration(df):
    """
    Báo cáo: thời lượng chuyến đi trung bình theo giới tính.
    """
    df.groupBy("gender").agg(avg("tripduration").alias("avg_duration")) \
        .coalesce(1).write.mode("overwrite").csv("reports/gender_avg_duration", header=True)


def top10_ages_trip_duration(df):
    """
    Báo cáo:
    - 10 độ tuổi có chuyến đi dài nhất.
    - 10 độ tuổi có chuyến đi ngắn nhất.
    """
    df.orderBy(desc("tripduration")).select("age").dropna().limit(10) \
        .coalesce(1).write.mode("overwrite").csv("reports/top10_longest_trip_ages", header=True)

    df.orderBy("tripduration").select("age").dropna().limit(10) \
        .coalesce(1).write.mode("overwrite").csv("reports/top10_shortest_trip_ages", header=True)


def main():
    """
    Hàm chính:
    - Khởi tạo SparkSession.
    - Đọc dữ liệu từ 2 file ZIP: 2019 Q4 và 2020 Q1.
    - Chuẩn hoá và tiền xử lý dữ liệu.
    - Sinh các báo cáo và lưu vào thư mục 'reports'.
    """
    spark = SparkSession.builder.appName("Exercise6").getOrCreate()

    # Đọc dữ liệu từ các file zip
    df1 = read_zip_csv(spark, "data/Divvy_Trips_2019_Q4.zip")
    df2 = read_zip_csv(spark, "data/Divvy_Trips_2020_Q1.zip")

    # Chuẩn hoá schema
    df1 = standardize_schema(df1)
    df2 = standardize_schema(df2)

    # Gộp và tiền xử lý
    df = df1.unionByName(df2)
    df = preprocess_data(df)

    # Sinh các báo cáo
    avg_trip_duration_per_day(df)
    trips_per_day(df)
    most_popular_start_station_per_month(df)
    top3_stations_last_two_weeks(df)
    gender_avg_duration(df)
    top10_ages_trip_duration(df)

    spark.stop()

if __name__ == "__main__":
    main()
```
4. Sau khi nhập code, save file `main.py` thì thực thi lệnh `docker-compose up run`
![image](https://github.com/user-attachments/assets/6fc312c5-7e84-4c53-a906-b4f3a19afede)
![image](https://github.com/user-attachments/assets/f0667bfc-5393-4737-9e9d-597e2de41288)
5. Kết quả:
![image](https://github.com/user-attachments/assets/70e70cc5-7bdc-4ad2-b3d5-e80db2eae5e7)
![image](https://github.com/user-attachments/assets/e5ce0fd3-9ecd-4147-b58e-2cc66124e123)
> Thời gian chuyến đi trung bình mỗi ngày
![image](https://github.com/user-attachments/assets/1a28bfd4-a890-42b8-b607-a528ae385c7a)
> Số chuyến đi mỗi ngày
![image](https://github.com/user-attachments/assets/3918bf7b-626e-433b-bc4c-62a37be564c1)
> Trạm khởi hành phổ biến nhất cho mỗi tháng
![image](https://github.com/user-attachments/assets/e71a5c21-d7c0-43b9-a2e7-6e466792c475)
> Ba trạm khởi hành hàng đầu mỗi ngày trong hai tuần
![image](https://github.com/user-attachments/assets/ec8d3675-a307-4418-857e-24269b6651d9)
> Nam hay nữ thực hiện những chuyến đi dài hơn trung bình
![image](https://github.com/user-attachments/assets/c5fd58a1-589c-4749-8d03-22e07baad87a)
> 10 độ tuổi hàng đầu của những người thực hiện những chuyến đi dài nhất
![image](https://github.com/user-attachments/assets/9d23c2b1-ab92-4b45-a91f-cda5d803565d)
> 10 độ tuổi hàng đầu của những người thực hiện những chuyến đi ngắn nhất
![image](https://github.com/user-attachments/assets/e94178b7-6daa-4471-9d91-325bac258275)

### Exercise 7 - Using Various PySpark Functions
1. Thay đổi đường dẫn cmd thành `Exercise-7`
2. Chạy lệnh `docker build --tag=exercise-7 .` để build image Docker (Quá trình diễn ra trong khoảng 10-15 phút)
![image](https://github.com/user-attachments/assets/3fa9f9ba-b596-409d-9c2c-8d8acc765457)
3. Nội dung file `main.py`:
```
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

import zipfile
import tempfile
import os


def read_zip_csv(spark, zip_path):
    """
    Đọc file .zip chứa 1 file .csv bên trong, giải nén tạm thời để Spark đọc được.
    Trả về DataFrame và đường dẫn file .csv tạm.
    """
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        # Lọc ra file .csv (bỏ qua các thư mục ẩn của macOS)
        csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv') and not f.startswith('__MACOSX')]

        if not csv_files:
            raise Exception("No CSV file found in the ZIP.")

        # Chọn file CSV đầu tiên
        csv_name = csv_files[0]

        # Giải nén ra file tạm để Spark có thể đọc được
        with zip_ref.open(csv_name) as file:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
                temp_file.write(file.read())
                temp_file_path = temp_file.name

    # Đọc file CSV bằng Spark
    df = spark.read.option("header", "true").csv(temp_file_path, inferSchema=True)

    # Gắn thêm cột tên file nguồn
    df = df.withColumn("source_file", F.lit(csv_name))

    return df, temp_file_path


def extract_file_date(df):
    """
    Trích xuất ngày (định dạng yyyy-MM-dd) từ tên file và gán vào cột mới file_date.
    """
    return df.withColumn(
        "file_date",
        F.to_date(F.regexp_extract("source_file", r"(\d{4}-\d{2}-\d{2})", 1), "yyyy-MM-dd")
    )


def extract_brand(df):
    """
    Tách brand (thương hiệu) từ model. Nếu model chứa dấu cách thì lấy phần trước dấu cách.
    Nếu không có dấu cách thì gán là 'unknown'.
    """
    return df.withColumn(
        "brand",
        F.when(F.instr(F.col("model"), " ") > 0, F.split("model", " ").getItem(0)).otherwise("unknown")
    )


def add_storage_ranking(df):
    """
    Xếp hạng các model theo dung lượng (capacity_bytes), từ cao xuống thấp.
    Dùng dense_rank để đánh số hạng không bị bỏ số.
    """
    cap_df = (
        df.select("model", "capacity_bytes")
        .dropna()
        .dropDuplicates()
        .withColumn("capacity_bytes", F.col("capacity_bytes").cast("long"))
    )

    # Định nghĩa cửa sổ xếp hạng theo capacity giảm dần
    window_spec = Window.orderBy(F.col("capacity_bytes").desc())
    cap_ranked = cap_df.withColumn("storage_ranking", F.dense_rank().over(window_spec))

    # Nối kết quả vào dataframe gốc
    return df.join(cap_ranked.select("model", "storage_ranking"), on="model", how="left")


def add_primary_key(df):
    """
    Tạo khóa chính (primary_key) duy nhất bằng cách hash tổ hợp các trường
    (date, serial_number, model) sử dụng SHA-256.
    """
    return df.withColumn(
        "primary_key",
        F.sha2(F.concat_ws("||", "date", "serial_number", "model"), 256)
    )


def main():
    # Tạo Spark session
    spark = SparkSession.builder.appName("Exercise7").getOrCreate()

    # Đường dẫn file zip
    zip_path = "data/hard-drive-2022-01-01-failures.csv.zip"

    # Đọc dữ liệu từ zip và lấy đường dẫn file tạm
    df, temp_file_path = read_zip_csv(spark, zip_path)

    # Thêm các cột mới bằng các hàm đã viết
    df = extract_file_date(df)
    df = extract_brand(df)
    df = add_storage_ranking(df)
    df = add_primary_key(df)

    # Gộp toàn bộ dữ liệu thành 1 file duy nhất
    output_path = "data/output_results"
    df = df.coalesce(1)

    # Ghi dữ liệu ra file CSV
    df.select("date", "serial_number", "model", "capacity_bytes", "source_file",
              "file_date", "brand", "storage_ranking", "primary_key") \
        .write.option("header", "true").mode("overwrite").csv(output_path)

    # Hiển thị một vài dòng kết quả
    df.select("date", "serial_number", "model", "capacity_bytes", "source_file",
              "file_date", "brand", "storage_ranking", "primary_key").show(truncate=False)

    # Xóa file tạm
    os.remove(temp_file_path)
    spark.stop()


if __name__ == "__main__":
    main()
```
4. Sau khi nhập code, save file `main.py` thì thực thi lệnh `docker-compose up run`
![image](https://github.com/user-attachments/assets/e2e5177b-44e8-45e6-b12e-0890846e9d8e)
![image](https://github.com/user-attachments/assets/1d6f4e05-07b7-4261-8dea-d714506498a6)
5. Kết quả:
![image](https://github.com/user-attachments/assets/071576b7-595e-460f-abb8-822aa036113d)
![image](https://github.com/user-attachments/assets/cee41e2a-0a3e-427d-99a4-f336b262749b)
![image](https://github.com/user-attachments/assets/c7153f15-21b9-41a1-a47e-100eb55eecc0)

