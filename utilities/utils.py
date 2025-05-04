import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from functools import reduce
from concurrent.futures import ThreadPoolExecutor
from collections.abc import Callable

def get_state_populations(
    DATA_DIR: str,
    session: SparkSession,
    cols_to_remove: list,
    populations: list,
    year_range: str,
    callback_fn: Callable):
    
    """
    concurrently runs the state population processor functions for
    each dataframe loaded
    """

    # define function to read each excel file as spark
    # dataframe concurrently
    def concur_model_pop_tables(file):
        FILE_PATH = os.path.join(DATA_DIR, file)
        state = re.search(r"(^[A-Za-z\s]+)", file)
        state = "Unknown" if not state else state[0]
        FILE_EXT = re.search(r'(.json|.txt|.rtf|.docx|.csv|.xlsx|.xls|.md|.pdf|.webp|.png|.jpg)$', FILE_PATH)
        FILE_EXT = FILE_EXT[0] if FILE_EXT else ".xls" 

        # print(to_remove)
        # print(year_range)
        # read excel file
        file_format = "com.crealytics.spark.excel" if (FILE_EXT == ".xls") or (FILE_EXT == ".xlsx") else "csv"
        header = ("header", "false") if (FILE_EXT == ".xls") or (FILE_EXT == ".xlsx") else ("header", "true")
        print(header, file_format)
        df = session.read.format(file_format)\
        .option(*header)\
        .option("inferSchema", "true")\
        .load(FILE_PATH)
        
        state_population = callback_fn(df, state, cols_to_remove, year_range)
        return state_population
    
    # concurrently run function to build spark dataframes
    with ThreadPoolExecutor(max_workers=2) as exe:
        state_populations = list(exe.map(
            concur_model_pop_tables, 
            populations,
        ))

    # concatenate and unionize all spark dataframes
    state_populations_df = reduce(DataFrame.unionByName, state_populations)

    return state_populations_df
    # return state_populations