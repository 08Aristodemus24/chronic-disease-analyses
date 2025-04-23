
from selenium import webdriver

from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from webdriver_manager.chrome import ChromeDriverManager

from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.support import expected_conditions as EC

import pandas as pd
import time
import os
import shutil

from concurrent.futures import ThreadPoolExecutor
from argparse import ArgumentParser

import re



def download_files(elements: list, stratification: str="sex and age", downloads_dir="C:/Users/LARRY/Downloads", output_dir="./data/population-data"):
        
        # def helper(element):
        #     # extract href from anchor tag
        #     link = element.get_attribute("href")
        #     state = element.text

        #     # derivve necessary information from url path to create file names
        #     file_name = link.split('/')[-1]
        #     file_dir = "/".join(link.split('/')[:-1])
        #     years = re.search(r"\d+-\d+", file_dir)[0]
        #     extension = re.search(r".[A-Za-z]+$", file_name)[0]
        #     new_file_name = f"{state}_{years}{extension}"
        #     # record = {
        #     #     "file_name": file_name,
        #     #     "file_dir": file_dir,
        #     #     "new_file_name": new_file_name,
        #     #     "state": state
        #     # }

        #     # download file by clicking element
        #     element.click()
        #     time.sleep(3)

        #     # print(records)
        #     return file_name, file_dir, new_file_name, state
        
        # with ThreadPoolExecutor(max_workers=5) as exe:
        #     file_names, file_dirs, new_file_names, states = zip(*list(exe.map(helper, elements)))

        for element in elements:
            try:
                link = element.get_attribute("href")
                state = element.text

                # derivve necessary information from url path to create file names
                file_name = link.split('/')[-1]
                file_dir = "/".join(link.split('/')[:-1])
                years = re.search(r"\d+-\d+", file_dir)[0]
                extension = re.search(r".[A-Za-z]+$", file_name)[0]

                # download file by clicking element
                element.click()
                time.sleep(3)

                # get the recently downloaded file and rename it
                file_path = max([os.path.join(downloads_dir, f) for f in os.listdir(downloads_dir)], key=os.path.getctime)
                new_file_name = f"{state}_pop_by_{stratification.replace(' ', '_')}_{years}{extension}"
                new_file_path = os.path.join(downloads_dir, new_file_name)
                
                os.rename(file_path, new_file_path)
                print(new_file_path)

            except Exception as e:
                print(f"error {e} has occured")

            finally:
                # move downloaded file to current working directory of the script
                relocated_path = os.path.join(output_dir, new_file_name)
                shutil.move(new_file_path, relocated_path)

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--start_year", type=int, help="starting year where the crawler should extract the populations")
    parser.add_argument("--end_year", type=int, help="ending year where the crawler should extract the populations")
    parser.add_argument("--downloads_dir", type=str, help="directory where to locate downloads")
    args = parser.parse_args()

    start_year, end_year = args.start_year, args.end_year

    # setting these options will not open a browser explicitly
    # and runs the scraping job in the background, 
    # disables development shared memory usage
    chrome_options = ChromeOptions()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")

    # service = ChromeService(executable_path="C:/Executables/chromedriver-win64/chromedriver.exe")
    # chrome_options.add_experimental_option('detach', True)
    service = ChromeService(executable_path=ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    populations_00_10_url = "https://www.census.gov/data/tables/time-series/demo/popest/intercensal-2000-2010-state.html"
    populations_10_19_url = "https://www.census.gov/data/tables/time-series/demo/popest/2010s-state-detail.html"
    populations_20_23_url = "https://www.census.gov/data/tables/time-series/demo/popest/2020s-state-detail.html"

    keyword_00_10_url = "Intercensal Estimates of the Resident Population by Sex and Age for States: April 1, 2000 to July 1, 2010"
    keyword_10_19_url = "Annual Estimates of the Resident Population by Single Year of Age and Sex: April 1, 2010 to July 1, 2019"
    keyword_20_23_url = "Annual Estimates of the Resident Population by Single Year of Age and Sex: April 1, 2020 to July 1, 2023 (SC-EST2023-SYASEX)"

    # specify data directory and create it
    DATA_DIR = "../data/population-data"
    os.makedirs(DATA_DIR, exist_ok=True)

    # specify downloads directory based on given argument by user
    DOWNLOADS_DIR = "C:/Users/LARRY/Downloads"



    # extracting 2000 - 2010 populations by sex and age
    # go to page
    driver.get(populations_00_10_url)

    # sleep
    time.sleep(5)


    # select only the sections ith certain keywords

    section_index = -1

    sections = driver.find_elements(By.CSS_SELECTOR, value="#data-uscb-state-list-selector")

    for i, section in enumerate(sections):
        for header in section.find_elements(by=By.CSS_SELECTOR, value="p"):
            if keyword_00_10_url in header.text:
                section_index = i

    chosen_section = sections[section_index]
    chosen_section.text

    states = chosen_section.find_elements(by=By.CSS_SELECTOR, value="ul.uscb-margin-5 li.uscb-list-attachment a")
    downloaded_files = download_files(states, stratification="sex and age", downloads_dir=DOWNLOADS_DIR, output_dir=DATA_DIR)

    # extracting 2000 - 2010 populations by sex, race, and hispanic origin
    section = driver.find_element(By.XPATH, value="/html/body/div[3]/div/div/div[8]/div/div[6]")
    states = section.find_elements(By.CSS_SELECTOR, value="#data-uscb-state-list-selector > span > ul > li > a")
    downloaded_files = download_files(states, stratification="sex race and ho", downloads_dir=DOWNLOADS_DIR, output_dir=DATA_DIR)



    # extracting 2010 - 2019 populations by sex and age
    # go to page
    driver.get(populations_10_19_url)
    time.sleep(5)

    section = driver.find_element(By.XPATH, value='/html/body/div[3]/div/div/div[8]/div/div[9]')
    states = section.find_elements(By.CSS_SELECTOR, value="#data-uscb-state-list-selector > span > ul > li > a")
    downloaded_files = download_files(states, stratification="sex and age", downloads_dir=DOWNLOADS_DIR, output_dir=DATA_DIR)

    # extracting 2010 - 2019 populations by sex, race, and hispanic origin
    section = driver.find_element(By.XPATH, value="/html/body/div[3]/div/div/div[8]/div/div[11]")
    states = section.find_elements(By.CSS_SELECTOR, value="#data-uscb-state-list-selector > span > ul > li > a")
    downloaded_files = download_files(states, stratification="sex race and ho", downloads_dir=DOWNLOADS_DIR, output_dir=DATA_DIR)

    
    
    # extracting 2020 - 2023 populations by sex and age
    driver.get(populations_20_23_url)
    time.sleep(5)

    section = driver.find_element(By.XPATH, value="/html/body/div[3]/div/div/div[8]/div/div[12]/div/div/div[5]")
    states = section.find_elements(By.CSS_SELECTOR, value="#data-uscb-state-list-selector > span > ul > li > a")
    downloaded_files = download_files(states, stratification="sex and age", downloads_dir=DOWNLOADS_DIR, output_dir=DATA_DIR)

    # extracting 2020 - 2023 populations by sex, race, and hispanic origin
    section = driver.find_element(By.XPATH, value="/html/body/div[3]/div/div/div[8]/div/div[12]/div/div/div[8]")
    states = section.find_elements(By.CSS_SELECTOR, value="#data-uscb-state-list-selector > span > ul > li > a")
    downloaded_files = download_files(states, stratification="sex race and ho", downloads_dir=DOWNLOADS_DIR, output_dir=DATA_DIR)


