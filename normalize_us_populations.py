import os
import itertools

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import Window
from pyspark.sql.functions import (col,
    lower as sparkLower,
    split,
    rlike,
    initcap,
    regexp,
    regexp_extract_all,
    isnull,
    regexp_extract,
    lit,
    when,
    dense_rank,
    concat,
    array,)
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType, StructField, StructType, FloatType, DoubleType, IntegerType

from argparse import ArgumentParser


def normalize_populations_per_table(df: DataFrame, session: SparkSession):
    """
    extract the unique id's of each column to be retained
    and placed also in a dimension table
    """
    
    # print(f"population count: {df.count()}")


if __name__ == "__main__":
     # get year range and state from user input
    parser = ArgumentParser()
    parser.add_argument("--year-range-list", type=str, default=["2000-2009"], nargs="+", help="represents the lists of year ranges that spark script would base on to transform excel files of these year ranges")
    args = parser.parse_args()

    # get arguments
    year_range_list = args.year_range_list

    # list paths of the transformed dataframes
    DATA_DIR = './data/population-data-transformed'
    files = os.listdir(DATA_DIR)
    cases = {
            "2000-2009": {
                "populations": list(filter(lambda file: "2000_2009" in file and "by_sex_age_race_ho" in file, files))
            },
            "2010-2019": {
                "populations": list(filter(lambda file: "2010_2019" in file and "by_sex_age_race_ho" in file, files))  
            },
            "2020-2023": {
                "populations": list(filter(lambda file: "2020_2023" in file and "by_sex_age_race_ho" in file, files))  
            }
        }
    DATA_DIR = "./data/population-data-transformed"
    # [
    #     x
    #     for xs in xss
    #     for x in xs
    # ]

    # will contain the paths to the parquet files which are folders
    # containing the partitioned segments of the data
    paths = [
        os.path.join(DATA_DIR, file)
        for year_range in cases.keys()
        for file in cases[year_range]["populations"]
    ]
    print(paths)

    spark = SparkSession.builder\
        .config("spark.driver.memory", "6g")\
        .config("spark.sql.execution.arrow.maxRecordsPerBatch","100")\
        .appName('test')\
        .getOrCreate()

    # load dataframes that will result in combined dataframe 
    # from list of paths. Note that spark.read.format.load will not work
    # we need to use spark.read.parquet as this can read lists of paths
    # to the parquet files
    upps_by_sex_age_race_ho_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .parquet(*paths)
    
    tables = normalize_populations_per_table(upps_by_sex_age_race_ho_df, spark)