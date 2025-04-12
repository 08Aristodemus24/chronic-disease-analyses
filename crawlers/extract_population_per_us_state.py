from selenium import webdriver

from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from webdriver_manager.chrome import ChromeDriverManager

from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.support import expected_conditions as EC

import time

from concurrent.futures import ThreadPoolExecutor
from argparse import ArgumentParser

import pandas as pd



def extract_populations(start_year: int, end_year: int):
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

            return state, population
        
        with ThreadPoolExecutor() as exe:
            # state populations will be a dictionary
            state_populations = list(exe.map(helper, table_rows))

        states, populations = zip(*state_populations)
        yearly_df = pd.DataFrame({"state": states, "population": populations})
        yearly_df["year"] = year
        years.append(yearly_df)

    # return all years containing all states populations
    return years


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--start_year", type=int, help="starting year where the crawler should extract the populations")
    parser.add_argument("--end_year", type=int, help="ending year where the crawler should extract the populations")
    args = parser.parse_args()

    # extract start and end years
    start_year = args.start_year
    end_year = args.end_year

    # initialize driver
    chrome_options = ChromeOptions()
    
    # service = ChromeService(executable_path="C:/Executables/chromedriver-win64/chromedriver.exe")
    # chrome_options.add_experimental_option('detach', True)
    service = ChromeService(executable_path=ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    # extract populations given the year ranges
    years = extract_populations(start_year=start_year, end_year=end_year)
    years

    # Export extracted data to dataframe
    # data will be arranged in this manner:
    # ```
    # | year | state | population |
    # | 2001 | Alabama | 4,480.089 |
    # | 2001 | Alaska | 642.337 |
    # ```
    final = pd.concat(years, axis=0, ignore_index=True)
    final.to_csv(f'../data/us_populations_per_state_{start_year}_to_{end_year}.csv')


