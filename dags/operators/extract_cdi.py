from argparse import ArgumentParser
from dotenv import load_dotenv
from pathlib import Path

import os
import requests
import boto3

from utilities.loaders import download_locally, upload_files_to_s3, upload_file_to_s3, create_bucket, create_bucket_folder


if __name__ == "__main__":
    # # Build paths inside the project like this: BASE_DIR / 'subdir'.
    # # use this only in development
    # env_dir = Path('./').resolve()
    # load_dotenv(os.path.join(env_dir, '.env'))

    parser = ArgumentParser()
    parser.add_argument("-L", type=str, help="represents the url/hyperlink that points to the file to be downloaded")
    args = parser.parse_args()

    # extract cdi which is at url
    # https://www.kaggle.com/api/v1/datasets/download/payamamanat/us-chronic-disease-indicators-cdi-2023
    link = args.L
    response = requests.get(link, stream=True)

    # download zip and extract locally
    file_path = download_locally(response, DATA_DIR="../include/data/cdi-data-raw")
    print(file_path)

    # create s3 client and pass credentials to create bucket
    credentials = {
        "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
        "region_name": os.environ["AWS_REGION_NAME"],
    }

    # define s3 client
    s3 = boto3.client("s3", **credentials)
    create_bucket(s3, region_name=credentials.get("region_name"))
    create_bucket_folder(s3)
    upload_files_to_s3(s3, local_file_paths=[file_path], callback_fn=upload_file_to_s3)

    
            
    
