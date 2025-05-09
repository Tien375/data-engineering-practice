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
