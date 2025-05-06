from selenium import webdriver

from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from webdriver_manager.chrome import ChromeDriverManager

from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.support import expected_conditions as EC

from concurrent.futures import ThreadPoolExecutor
from argparse import ArgumentParser

import pandas as pd
import csv
import re
import time
import os

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--start_year", type=int, help="starting year where the crawler should extract the populations")
    parser.add_argument("--end_year", type=int, help="ending year where the crawler should extract the populations")
    args = parser.parse_args()

    # extract start and end years
    start_year, end_year = args.start_year, args.end_year
    
    # setting these options will not open a browser explicitly
    # and runs the scraping job in the background, 
    # disables development shared memory usage
    chrome_options = ChromeOptions()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    # initialize driver
    # service = ChromeService(executable_path="C:/Executables/chromedriver-win64/chromedriver.exe")
    # chrome_options.add_experimental_option('detach', True)
    service = ChromeService(executable_path=ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    # Export extracted data to dataframe
    # data will be arranged in this manner:
    # ```
    # | year | state | population |
    # | 2001 | Alabama | 4,480.089 |
    # | 2001 | Alaska | 642.337 |
    # ```
    DATA_DIR = "../data/population-data-raw"
    os.makedirs(DATA_DIR, exist_ok=True)

    # save the raw extracted files to data directory 
    FILE_NAME = f"us_populations_per_state_{start_year}_to_{end_year}.csv"
    FILE_PATH = os.path.join(DATA_DIR, FILE_NAME)

    # extract populations given the year ranges
    extract_populations(driver, FILE_PATH, start_year=start_year, end_year=end_year)