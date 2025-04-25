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


def extract_populations(driver: webdriver.Chrome, file_path: str, start_year: int, end_year: int):
    years = []
    for year in list(range(start_year, end_year + 1)):
        # visit page per year
        print(f'processing year {year}')
        driver.get(f'https://fred.stlouisfed.org/release/tables?rid=118&eid=259194&od={year}-01-01#')
        
        # sleep for a moment
        time.sleep(5)

        # extract all table rows in page
        table_rows = driver.find_elements(by=By.CSS_SELECTOR, value="tr[data-tt-id]")

        # concurrently extract all states populations
        def helper(element):
            state = element.find_element(by=By.CSS_SELECTOR, value='th.fred-rls-elm-nm-cntnr').text
            state = state.strip()
            population = element.find_element(by=By.CSS_SELECTOR, value='td.fred-rls-elm-vl-td').text
            population = population.strip()

            return state, population, year
        
        with ThreadPoolExecutor() as exe:
            # state populations will be a dictionary
            state_populations = list(exe.map(helper, table_rows))

        # if csv file does not already exist yet create it 
        headers = ['State', 'Population', 'Year']
        if not os.path.exists(file_path):
            with open(file_path, "w", newline="") as csv_file:
                # identifying header
                writer = csv.writer(csv_file)
                writer.writerow(headers)
                csv_file.close()

        else:
            # open an existing csv file
            with open(file_path, "a", newline="") as csv_file:
                csv_writer = csv.writer(csv_file)
                csv_writer.writerows(state_populations)
                csv_file.close()



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
    # DATA_DIR = "../data/population-data"
    DATA_DIR = "../data/test"
    os.makedirs(DATA_DIR, exist_ok=True)

    # save the raw extracted files to data directory 
    FILE_NAME = f"us_populations_per_state_{start_year}_to_{end_year}.csv"
    FILE_PATH = os.path.join(DATA_DIR, FILE_NAME)

    # extract populations given the year ranges
    extract_populations(driver, FILE_PATH, start_year=start_year, end_year=end_year)