import os
import re
import ast
import sys

from functools import reduce

from pyspark import SparkConf
from pyspark.sql.functions import (monotonically_increasing_id, 
    row_number, 
    col,
    lower as sparkLower,
    regexp_replace,
    regexp,
    regexp_extract_all,
    regexp_extract,
    lit,
    when,
    concat,
    array,)
from pyspark.sql.types import DoubleType, LongType, ArrayType, FloatType, IntegerType
from pyspark.sql import SparkSession, Window
from pyspark.sql.dataframe import DataFrame

from argparse import ArgumentParser
from utilities.utils import get_state_populations


def process_population_by_sex_age_race_ho_table(df: DataFrame, 
    state: str, 
    cols_to_remove: list, 
    year_range: str="2000-2009"):
    """
    process a spark dataframe loaded from csv with
    information on sex, single age, ethnicity and 
    origin (hispanic or non hispanic)
    """
    # extract numbers from year range
    years = re.findall(r"\d+", year_range)
    lo_year = ast.literal_eval(years[0])
    hi_year = ast.literal_eval(years[-1])

    # remove the columns or the columns that will not
    # be renamed
    df = df.drop(*cols_to_remove)

    def year_finder(col):
        """
        callback fn for sort function to sort based on matched
        year of year_finder. A -1 is returned as alt so that when
        it is sorted later by key the column without any year is 
        interpreted as a -1 lower than all other years
        """
        year = re.search(r"(?<=[7A-Za-z])\d+", col)
        if year:
            return int(year[0])
        
        # if a year is not matched that means col is a string
        # so return negative infinity
        return float("-inf")

    # we need to check if length of yeras_list == (length of new cols) - 1
    # in order to proceed with creating name mapper through
    # dictionary comprehension
    year_fmtr = lambda year: f"_{year}"
    year_cols = [year_fmtr(year) for year in range(lo_year, hi_year + 1)]
    print(f"year cols: {year_cols}")

    # pick out only the columns with years and sort these 
    # columns in ascending order according to a callback
    # create also dictionary mapping old year cols to new year cols
    cols_to_ren = sorted(filter(lambda col: bool(re.search(r"(?<=[7A-Za-z])\d+", col)), df.columns), key=year_finder)
    year_col_name_map = dict(zip(cols_to_ren, year_cols))
    
    # rename year columns
    df = df.withColumnsRenamed(year_col_name_map)

    # rename non year columns
    def col_generalizer(col):
        col = col.lower()
        col_name = re.search(r"(name|sex|origin|age|race)$", col)
        if col_name:
            return col_name[0]
        
        return float("-inf")
    old_non_year_cols = sorted(list(set(df.columns) - set(year_cols)), key=col_generalizer)
    new_non_year_cols = ['Age', 'State', 'Origin', 'Ethnicity', 'Sex']

    # create dictionary mapping the old col names ot new col names
    non_year_col_name_map = dict(zip(old_non_year_cols, new_non_year_cols))
    df = df.withColumnsRenamed(non_year_col_name_map)

    # drop every row under sex and origin column with 0 values as
    # these entail both female and male, and hispanic and non hispanic
    # origin which can be just aggregated. This is to reduce redundancy
    df = df.where(~((col("Sex") == 0) | (col("Origin") == 0)))

    # re encode ethnicity codes to their races
    eth_cases = when(col("Ethnicity") == 1, "White")\
    .when(col("Ethnicity") == 2, "Black")\
    .when(col("Ethnicity") == 3, "AIAN")\
    .when(col("Ethnicity") == 4, "Asian")\
    .when(col("Ethnicity") == 5, "NHPI")\
    .when(col("Ethnicity") == 6, "Multiracial")
    df = df.withColumn("Ethnicity", eth_cases)

    # re encode origin codes to their origins
    origin_cases = when(col("Origin") == 1, "Not Hispanic")\
    .when(col("Origin") == 2, "Hispanic")
    df = df.withColumn("Origin", origin_cases)

    # re encode sex codes to their sex
    sex_cases = when(col("Sex") == 1, "Male")\
    .when(col("Sex") == 2, "Female")
    df = df.withColumn("Sex", sex_cases)

    # stack year columns with all ids being state, sex, age, 
    # ethnicity, and origin 
    df = df.melt(
        ids=["State", "Age", "Ethnicity", "Origin", "Sex"],
        values=year_cols,
        variableColumnName="Year",
        valueColumnName="Population"
    )

    # clean year column, cast age to float, and cast 
    # population to long int
    df = df.withColumn("Year", regexp_replace(col("Year"), r"[_]", "").cast(IntegerType()))
    df = df.withColumn("Population", col("Population").cast(LongType()))
    df = df.withColumn("Age", col("Age").cast(FloatType()))

    return df


if __name__ == "__main__":
    # get year range and state from user input
    parser = ArgumentParser()
    parser.add_argument("--year-range-list", type=str, default=["2000-2009"], nargs="+", help="represents the lists of year ranges that spark script would base on to transform excel files of these year ranges")
    args = parser.parse_args()

    # get arguments
    year_range_list = args.year_range_list

    DATA_DIR = './data/population-data-raw'
    EXCLUSIONS = ["us_populations_per_state_2001_to_2021.csv", "population-data.zip"]
    files = list(filter(lambda file: not file in EXCLUSIONS, os.listdir(DATA_DIR)))
    cases = {
            "2000-2009": {
                "cols_to_remove": [
                    "SUMLEV",
                    "REGION",
                    "DIVISION",
                    "STATE",
                    "CENSUS2000POP",
                    "ESTIMATESBASE2000",
                    "POPESTIMATE42010",
                    "POPESTIMATE72010"
                 ],
                "populations": list(filter(lambda file: "2000-2010" in file and "by_sex_age_race_ho" in file, files))  
            },
            "2010-2019": {
                "cols_to_remove": [
                    "SUMLEV",
                    "REGION",
                    "DIVISION",
                    "STATE",
                    "CENSUS2010POP",
                    "ESTIMATESBASE2010"
                 ],
                "populations": list(filter(lambda file: "2010-2019" in file and "by_sex_age_race_ho" in file, files))  
            },
            "2020-2023": {
                "cols_to_remove": [
                    "SUMLEV",
                    "REGION",
                    "DIVISION",
                    "STATE",
                    "ESTIMATESBASE2020"
                 ],
                "populations": list(filter(lambda file: "2020-2023" in file and "by_sex_age_race_ho" in file, files))  
            }
        }
    
    # create spark session
    # default is 1g for spark.executor.memory and 1 for spark.executor.cores
    # set spark.driver.memory only when you want to use broadcast joins or want
    # to collect and concat all the partitioned data into one single dataframe 
    # at your own risk/peril
    spark = SparkSession.builder\
        .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:3.5.1_0.20.4")\
        .config("spark.executor.memory", "2g")\
        .config("spark.executor.cores", "6")\
        .getOrCreate()
    
    conf_view = spark.sparkContext.getConf()
    print(f"spark jars packages: {conf_view.get('spark.jars.packages')}")
    print(f"spark.executor.memory: {conf_view.get('spark.executor.memory')}")
    print(f"spark.executor.cores: {conf_view.get('spark.executor.cores')}")

    # create output directory 
    OUTPUT_DATA_DIR = "./data/population-data-transformed"
    os.makedirs(OUTPUT_DATA_DIR, exist_ok=True)
    
    # get year range from system arguments sys.argv
    state_populations_all_years = []

    # loop through year_ranges
    for year_range in year_range_list:
        # concurrently process state populations by year range
        state_populations = get_state_populations(
            DATA_DIR, 
            spark, 
            cases[year_range]["cols_to_remove"], 
            cases[year_range]["populations"], 
            year_range,
            callback_fn=process_population_by_sex_age_race_ho_table)
        
        state_populations.show()
        
        # collect state populations from all years using list
        # there should be 240 rows per us state regardless of year range
        # except for 2020-2023 which is 96 rows since this is only a span 
        # of 4 years. So 240 * 51 states * 2 year ranges spanning 10 years
        # + 96 * 51 states is 29376 rows all in all
        # create output file path
        indicator = year_range.replace("-", "_")
        FILE_NAME = f"us_populations_per_state_by_sex_age_race_ho_{indicator}.parquet"
        OUTPUT_FILE_PATH = os.path.join(OUTPUT_DATA_DIR, FILE_NAME)
        state_populations.write.parquet(OUTPUT_FILE_PATH, mode="overwrite")