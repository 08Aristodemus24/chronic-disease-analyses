import os

import pyspark
import pyspark.sql
import pyspark.sql.dataframe
import pyspark.sql.functions as f

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType

def transform(df: pyspark.sql.dataframe.DataFrame):
    # remove index column
    df = df.drop("_c0")

    # remove . and , chars in population column and cast to long
    df = df.withColumn("Population", f.regexp_replace(df.Population, f.lit(r"[.,]"), f.lit("")).cast(LongType()))
    df.show()

if __name__ == "__main__":
    DATA_DIR = "./data/population-data"

    path = os.path.join(DATA_DIR, "us_populations_per_state_2001_to_2021.csv")

    spark = SparkSession.builder.appName('test')\
        .config("spark.executor.memory", "6g")\
        .getOrCreate()

    upps_df = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load(path)
    
    # commence transformation
    transform(upps_df)


