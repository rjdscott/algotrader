from google.cloud import storage
import pandas as pd
import requests
from bs4 import BeautifulSoup
import logging
import os
import datetime

# https://cloud.google.com/storage/docs/downloading-objects#storage-download-object-python

from google.oauth2 import service_account as sa

credentials = sa.Credentials.from_service_account_file("/Users/rjdscott/keys/rs-algotrader-4d17e78e097c.json")
scoped_credentials = credentials.with_scopes(["https://www.googleapis.com/auth/cloud-platform"])

BUCKET_NAME = 'algotrader-etl'
STORAGE_CLIENT = storage.Client(credentials=credentials)
logging.basicConfig(format='%(asctime)s | %(levelname)s | %(message)s', level=logging.INFO)


def upload_blob_from_memory(bucket_name, contents, destination_blob_name, storage_client=STORAGE_CLIENT):
    """Uploads a file to the bucket."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(contents, 'text/csv')
    logging.info(f"{destination_blob_name} with uploaded to {bucket_name}.")


def download_refdata():
    url = 'https://stockmarketmba.com/stocksinthesp500.php'
    date_str = datetime.datetime.now().strftime('%Y-%m-%d')
    df = pd.read_html(url)[0]
    df['date'] = date_str
    file_base = f"sp500/constituents/{date_str.replace('-', '/')}"
    file_name = f'sp500.csv'
    file_path = os.path.join(file_base, file_name)
    upload_blob_from_memory(BUCKET_NAME, df.to_csv(), file_path)
    return None


if __name__ == '__main__':
    download_refdata()
