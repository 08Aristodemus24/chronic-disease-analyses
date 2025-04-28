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
    def concur_model_pop_tables(file, to_remove, year_range, callback_fn):
        FILE_PATH = os.path.join(DATA_DIR, file)
        state = re.search(r"(^[A-Za-z\s]+)", file)
        state = "Unknown" if not state else state[0]

        # print(to_remove)
        # print(year_range)
        # read excel file
        df = session.read.format("com.crealytics.spark.excel")\
        .option("header", "false")\
        .option("inferSchema", "true")\
        .load(FILE_PATH)
        
        state_population = callback_fn(df, state, to_remove, year_range=year_range)
        return state_population
    
    # concurrently run function to build spark dataframes
    with ThreadPoolExecutor(max_workers=2) as exe:
        state_populations = list(exe.map(
            concur_model_pop_tables, 
            populations, 
            [cols_to_remove] * len(populations),
            [year_range] * len(populations),
            [callback_fn] * len(populations)
        ))

    # concatenate and unionize all spark dataframes
    state_populations_df = reduce(DataFrame.unionByName, state_populations)

    return state_populations_df