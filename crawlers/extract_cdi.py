from argparse import ArgumentParser
from dotenv import load_dotenv
from pathlib import Path

import os
import requests
import zipfile
import boto3

from concurrent.futures import ThreadPoolExecutor

def create_bucket(client, region_name: str, bucket_name: str="chronic-disease-analyses-bucket"):
    try:
        client.create_bucket(
            Bucket=bucket_name, 
            CreateBucketConfiguration={
                "LocationConstraint": region_name
            })
        print(f"Bucket '{bucket_name}' created successfully.")
        
    except Exception as e:
        if "BucketAlreadyExists" in str(e) or "BucketAlreadyOwnedByYou" in str(e):
            print(f"Bucket '{bucket_name}' already exists. Skipping creation.")
        else:
            print(f"Error creating bucket: {e}")


def create_bucket_folder(client, bucket_name: str="chronic-disease-analyses-bucket", folder_name: str="cdi-data-raw/"):
    try:
        client.put_object(
            Bucket=bucket_name,
            Key=folder_name
        )
        print(f"Bucket '{bucket_name}' created successfully.")
    
    except Exception as e:
        print(f"Error {e} has occured. Skipping creation.")


def upload_file_in_chunks(client, bucket_name, file_path, chunk_size=1024 * 1024):
    """
    chunks size is set by default to 1024 bytes * 1024 or 1048576 bytes
    or 1.048576 MB. Minimal multipart upload size is 5Mb (1), even if 
    your total size is 100MB, each individual multipart upload (other 
    than the last one) can't be smaller than 5MB
    """
    
    try:
        # create multipart upload object that will be populatted
        # by upload_part objects later
        mpu = client.create_multipart_upload(
            Bucket=bucket_name, 
            Key=file_path)
        upload_id = mpu.get('UploadId')
        
        def helper(chunk, part_num):
            print(f"processing chunk {part_num}")

            # create upload parts of the chunks of the request
            part = client.upload_part(
                Bucket=bucket_name, 
                Key=file_path, 
                UploadId=upload_id, 
                PartNumber=part_num, 
                Body=chunk
            )

            # return part as dictionary
            return {"PartNumber": part_num, "ETag": part.get("ETag")}

    
        chunks = list(response.iter_content(chunk_size=chunk_size))
        with ThreadPoolExecutor(max_workers=5) as exe:
            # Part number must be an integer between 1 and 10000
            part_nums = list(range(1, len(chunks) + 1))
            parts = list(exe.map(helper, chunks, part_nums))

        client.complete_multipart_upload(
            Bucket=bucket_name, 
            Key=file_path, 
            UploadId=upload_id, 
            MultipartUpload={'Parts': parts}
        )
        print(f"File uploaded to s3://{bucket_name}/{file_path} successfully.")

    except Exception as e:
        # if error occurs it is the file uploaded is too small which we
        # can resovle by making chunk size larger, a chunk ideally greater
        # than 5 MB
        print(f"Error {e} has occured. File not uploaded")
        if 'upload_id' in locals():
            client.abort_multipart_upload(Bucket=bucket_name, Key=folder_name, UploadId=upload_id)
            print(f"Multipart upload aborted for {folder_name}")



if __name__ == "__main__":
    # Build paths inside the project like this: BASE_DIR / 'subdir'.
    # use this only in development
    env_dir = Path('../').resolve()
    load_dotenv(os.path.join(env_dir, '.env'))


    parser = ArgumentParser()
    parser.add_argument("-L", type=str, help="represents the url/hyperlink that points to the file to be downloaded")
    args = parser.parse_args()

    # extract cdi which is at url
    # https://www.kaggle.com/api/v1/datasets/download/payamamanat/us-chronic-disease-indicators-cdi-2023
    link = args.L
    response = requests.get(link, stream=True)

    # DATA_DIR = "../data/cdi-data-raw"
    # os.makedirs(DATA_DIR, exist_ok=True)

    # # save the raw extracted files to data directory 
    # FILE_NAME = f"U.S._Chronic_Disease_Indicators__CDI.zip"
    # FILE_PATH = os.path.join(DATA_DIR, FILE_NAME)

    # # download the file as a binary zip file given the urls
    # print("downloading chronic disease indicators data...")
    # with open(FILE_PATH, mode="wb") as file:
    #     for chunk in response.iter_content(chunk_size=10 * 1024):
    #         file.write(chunk)
    #     file.close()

    # identifiers of s3 bucket
    bucket_name = "chronic-disease-analyses-bucket"
    folder_name = "cdi-data-raw/"

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

    # define file_name and file_path where the file would
    # live in s3
    FILE_NAME = f"U.S._Chronic_Disease_Indicators__CDI.zip"
    FILE_PATH = os.path.join(folder_name, FILE_NAME)
    
    # save the request body content to s3 bucket
    multiplier = 5
    ONE_MB = 1024 * 1024
    CHUNK_SIZE = multiplier * ONE_MB
    upload_file_in_chunks(s3, bucket_name, FILE_PATH, chunk_size=CHUNK_SIZE)
            
    # # read the binary zip file and extract its contents
    # with zipfile.ZipFile(FILE_PATH, mode="r") as zip_ref:
    #     zip_ref.extractall(DATA_DIR)

    # # once contents are extracted remove the file
    # os.remove(FILE_PATH)
