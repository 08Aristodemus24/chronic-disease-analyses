import os
import requests
import zipfile
import boto3

from typing import Callable
from boto3.s3.transfer import TransferConfig
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



def upload_file_in_chunks_to_s3(
    client,
    multiplier,
    MB,
    local_file_path,
    s3_bucket_name,
    s3_folder_name,
    s3_file_name):
    """
    chunks size is set by default to 1024 bytes * 1024 or 1048576 bytes
    or 1.048576 MB. Minimal multipart upload size is 5Mb (1), even if 
    your total size is 100MB, each individual multipart upload (other 
    than the last one) can't be smaller than 5MB
    """
    
    try:
        S3_FILE_PATH = os.path.join(s3_folder_name, s3_file_name)
        # create multipart upload object that will be populatted
        # by upload_part objects later
        mpu = client.create_multipart_upload(
            Bucket=s3_bucket_name, 
            Key=S3_FILE_PATH)
        upload_id = mpu.get('UploadId')
        
        def helper(chunk, part_num):
            print(f"processing chunk {part_num}")

            # create upload parts of the chunks of the request
            part = client.upload_part(
                Bucket=s3_bucket_name, 
                Key=S3_FILE_PATH, 
                UploadId=upload_id, 
                PartNumber=part_num, 
                Body=chunk
            )

            # return part as dictionary
            return {"PartNumber": part_num, "ETag": part.get("ETag")}


        # CHUNK_SIZE = multiplier * MB
        # chunks = list(response.iter_content(chunk_size=CHUNK_SIZE))
        # with ThreadPoolExecutor(max_workers=5) as exe:
        #     # Part number must be an integer between 1 and 10000
        #     part_nums = list(range(1, len(chunks) + 1))
        #     parts = list(exe.map(helper, chunks, part_nums))

        # client.complete_multipart_upload(
        #     Bucket=s3_bucket_name, 
        #     Key=S3_FILE_PATH, 
        #     UploadId=upload_id, 
        #     MultipartUpload={'Parts': parts}
        # )
        print(f"File uploaded to s3://{s3_bucket_name}/{S3_FILE_PATH} successfully.")

    except Exception as e:
        # if error occurs it is the file uploaded is too small which we
        # can resovle by making chunk size larger, a chunk ideally greater
        # than 5 MB
        print(f"Error {e} has occured. File not uploaded")
        if 'upload_id' in locals():
            client.abort_multipart_upload(Bucket=s3_bucket_name, Key=S3_FILE_PATH, UploadId=upload_id)
            print(f"Multipart upload aborted for {S3_FILE_PATH}")



def upload_file_to_s3(
    client,
    multiplier,
    MB,
    local_file_path,
    s3_bucket_name,
    s3_folder_name,
    s3_file_name):

    config = TransferConfig(multipart_threshold=MB * multiplier, max_concurrency=10,
                        multipart_chunksize=MB * multiplier, use_threads=True)
    
    # upload to s3
    print(f"uploading {s3_file_name} file to s3")
    S3_FILE_PATH = os.path.join(s3_folder_name, s3_file_name)
    client.upload_file(local_file_path, s3_bucket_name, S3_FILE_PATH,
        Config=config,
    )



def download_locally(response, DATA_DIR: str="./data/cdi-data-raw", FILE_NAME: str="U.S._Chronic_Disease_Indicators__CDI.zip"):
    """
    download zip file at given directory and then 
    """

    # maek directory if one does not exist
    os.makedirs(DATA_DIR, exist_ok=True)

    # save the raw extracted files to data directory 
    ZIP_FILE_PATH = os.path.join(DATA_DIR, FILE_NAME)

    # download the file as a binary zip file given the urls
    print("downloading chronic disease indicators data...")
    with open(ZIP_FILE_PATH, mode="wb") as file:
        for chunk in response.iter_content(chunk_size=10 * 1024):
            file.write(chunk)
        file.close()

    # read the binary zip file and extract its contents
    print("unzipping compressed file")
    with zipfile.ZipFile(ZIP_FILE_PATH, mode="r") as zip_ref:
        zip_ref.extractall(DATA_DIR)
        ext_file_name = zip_ref.namelist()[-1]
        zip_ref.close()

    # once contents are extracted remove the file
    os.remove(ZIP_FILE_PATH)

    # assign extracted file name to variable
    EXT_FILE_PATH = os.path.join(DATA_DIR, ext_file_name)

    return EXT_FILE_PATH, ext_file_name



def upload_files_to_s3(
    client,
    local_file_paths: list,
    multiplier: int=5,
    MB: int=1024 * 1024,
    s3_bucket_name: str="chronic-disease-analyses-bucket", 
    s3_folder_name: str="cdi-data-raw/", 
    callback_fn: Callable=upload_file_to_s3):
    
    # concurrently uploads the list of local files to s3 
    def helper(local_file_path):
        LOCAL_FILE_PATH, FILE_NAME = local_file_path
        print(f"processing {FILE_NAME}")
        callback_fn(
            client, 
            multiplier=multiplier,
            MB=MB,
            local_file_path=LOCAL_FILE_PATH, 
            s3_bucket_name=s3_bucket_name,
            s3_folder_name=s3_folder_name,
            s3_file_name=FILE_NAME
        )

    with ThreadPoolExecutor() as exe:
        exe.map(helper, local_file_paths)