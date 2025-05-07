from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from argparse import ArgumentParser
from pathlib import Path
from dotenv import load_dotenv

import shutil
import re
import time
import os
import boto3

from utilities.loaders import upload_files_to_s3, create_bucket, create_bucket_folder, upload_file_to_s3

def extract_populations(
    links_xpaths: str, 
    service: ChromeService, 
    options: ChromeOptions, 
    downloads_dir: str="C:/Users/LARRY/Downloads",
    DATA_DIR: str="./data/population-data-raw"):

    """
    concurrently opens multiple intances of selenium driver and
    downloads the necessary files containing population data
    per state by sex, age, race, and origin
    """
    driver = webdriver.Chrome(service=service, options=options)
    
    year_ranges_lookup = {
        2010: "2000-2010",
        2019: "2010-2019",
        2023: "2020-2023"
    }

    # to be populated from href of tags
    file_paths = []
    

    # 
    for link, xpath in links_xpaths:
        print(link, xpath)
        # sleep for couple seconds to let page load
        driver.get(link)
        time.sleep(5)

        # scroll to very bottom
        driver.execute_script("window.scrollBy(0, document.body.scrollHeight)")
        time.sleep(2.5)

        # get element and download 
        resrc_element = driver.find_element(by=By.XPATH, value=xpath)
        href = resrc_element.get_attribute("href")
        OLD_FILE_NAME = href.split('/')[-1]
        OLD_FILE_PATH = os.path.join(downloads_dir, OLD_FILE_NAME)

        # calculate year range based on year of filename
        year = re.search(r"\d+", OLD_FILE_NAME)
        year = int(year[0]) if year else 1970
        year_range = year_ranges_lookup.get(year)

        # search file extension
        FILE_EXT = re.search(r'(.json|.txt|.rtf|.docx|.csv|.xlsx|.xls|.md|.pdf|.webp|.png|.jpg)$', OLD_FILE_NAME)
        FILE_EXT = FILE_EXT[0] if FILE_EXT else ".txt" 

        NEW_FILE_NAME = f"us_populations_per_state_by_sex_age_race_ho_{year_range}{FILE_EXT}"
        file_paths.append((OLD_FILE_PATH, NEW_FILE_NAME))

        # click element as last step
        resrc_element.click()
        time.sleep(5)

    # give ample time for downloads to finish
    time.sleep(30)

    # get old file paths and new file names as this
    # will be used to rename the downloaded files
    old_file_paths, new_file_names = list(zip(*file_paths))
    
    relocated_file_paths = []
    for i, OLD_FILE_PATH in enumerate(old_file_paths):
        # rename downloaded files
        NEW_FILE_NAME = new_file_names[i]
        NEW_FILE_PATH = os.path.join(downloads_dir, NEW_FILE_NAME)
        os.rename(OLD_FILE_PATH, NEW_FILE_PATH)

        # move downloaded files to local data dir
        RELOCATED_FILE_PATH = os.path.join(DATA_DIR, NEW_FILE_NAME)
        shutil.move(NEW_FILE_PATH, RELOCATED_FILE_PATH)

        # collect relocated paths and relocated file names
        relocated_file_paths.append((RELOCATED_FILE_PATH, NEW_FILE_NAME))

    return relocated_file_paths


# python ./crawlers/extract_us_population_per_state_by_sex_age_race_ho.py
if __name__ == "__main__":
    # Build paths inside the project like this: BASE_DIR / 'subdir'.
    # use this only in development
    env_dir = Path('./').resolve()
    load_dotenv(os.path.join(env_dir, '.env'))

    # setting these options will not open a browser explicitly
    # and runs the scraping job in the background, 
    # disables development shared memory usage
    chrome_options = ChromeOptions()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    # initialize driver
    chrome_options.add_experimental_option('detach', True)
    service = ChromeService(executable_path=ChromeDriverManager().install())
    

    # Export extracted data to dataframe    
    # data will be arranged in this manner:
    # ```
    # | year | state | population |
    # | 2001 | Alabama | 4,480.089 |
    # | 2001 | Alaska | 642.337 |
    # ```
    DATA_DIR = "../data/population-data-raw"
    os.makedirs(DATA_DIR, exist_ok=True)

    links = ["https://www.census.gov/programs-surveys/popest/technical-documentation/research/evaluation-estimates/2010-evaluation-estimates.html",
    "https://www.census.gov/data/tables/time-series/demo/popest/2010s-state-detail.html", 
    "https://www.census.gov/data/tables/time-series/demo/popest/2020s-state-detail.html"]
    xpaths = [
        "/html/body/div[3]/div/div/div[8]/div/div[18]/div/ul/li[4]/a",
        "/html/body/div[3]/div/div/div[8]/div/div[18]/div/ul/li/a",
        "/html/body/div[3]/div/div/div[8]/div/div[19]/div/div/div/div/ul/li/a"
    ]
    links_xpaths = zip(links, xpaths)

    # extract populations given the year ranges
    relocated_file_paths = extract_populations(links_xpaths, service, chrome_options)

    # create s3 client and pass credentials to create bucket
    credentials = {
        "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
        "region_name": os.environ["AWS_REGION_NAME"],
    }

    # define s3 client
    s3 = boto3.client("s3", **credentials)

    relocated_file_paths = [
        ("./data/population-data-raw/us_populations_per_state_by_sex_age_race_ho_2000-2010.csv", "us_populations_per_state_by_sex_age_race_ho_2000-2010.csv"),
        ("./data/population-data-raw/us_populations_per_state_by_sex_age_race_ho_2010-2019.csv", "us_populations_per_state_by_sex_age_race_ho_2010-2019.csv"),
        ("./data/population-data-raw/us_populations_per_state_by_sex_age_race_ho_2020-2023.csv", "us_populations_per_state_by_sex_age_race_ho_2020-2023.csv")
    ]

    # create bucket and then bucket folder
    BUCKET_NAME = "chronic-disease-analyses-bucket"
    FOLDER_NAME = "population-data-raw/"
    create_bucket(s3, region_name=credentials.get("region_name"), bucket_name=BUCKET_NAME)
    create_bucket_folder(s3, bucket_name=BUCKET_NAME, folder_name=FOLDER_NAME)
    upload_files_to_s3(
        s3,  
        local_file_paths=relocated_file_paths,
        s3_bucket_name=BUCKET_NAME,
        s3_folder_name=FOLDER_NAME, 
        callback_fn=upload_file_to_s3)