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

def normalize_cdi_table(df: DataFrame, session: SparkSession):
    """
    extract the unique id's of each column to be retained
    and placed also in a dimension table
    """

    # once Stratification1ID column is added select only the sex,
    # ethnicity, origin, and stratification1id columns and then drop
    # the duplicates so that all unique values are kept
    strat_df = df.select("Sex", "Ethnicity", "Origin", "Stratification1ID").dropDuplicates()
    
    # drop the columns in the fact table that is already 
    # in the dimension table
    df = df.drop("Sex", "Ethnicity", "Origin")
    # strat_df.show()

    # remove location desc
    # remove location abbr
    # retain in location dimension table with location abbr as id
    df = df.drop("LocationID")
    df = df.withColumnRenamed("LocationAbbr", "LocationID")
    location_df = df.select("LocationID", "LocationDesc").dropDuplicates()

    # drop location descriptions as we have already retained its 
    # corresponding id in the dimension table in the location df
    df = df.drop("LocationDesc")
    # location_df.show(location_df.count())

    # remove topic
    # remove question
    # remove topicid
    # retain in questions table with question id
    question_df = df.select("QuestionID", "TopicID", "Question", "Topic", "AgeStart", "AgeEnd").dropDuplicates()
    df = df.drop("TopicID", "Question", "Topic", "AgeStart", "AgeEnd")
    # question_df.show()

    # remove data value type
    # retain in data value type table with data value type id as id
    dvt_df = df.select("DataValueTypeID", "DataValueType").drop_duplicates()
    df = df.drop("DataValueType")
    # dvt_df.show()

    # this will cover slowly changing dimension cases if a unique row in dimension
    # table is updated, if a unique row is added to dimension table or if another column
    # is added to dimension table
    df.show()

    # return all normalized tables
    return [df, strat_df, question_df, location_df, dvt_df]
    
    


if __name__ == "__main__":
    DATA_DIR = "./data/cdi-data-transformed"
    path = os.path.join(DATA_DIR, "cdi.parquet")

    spark = SparkSession.builder\
        .config("spark.driver.memory", "6g")\
        .config("spark.sql.execution.arrow.maxRecordsPerBatch","100")\
        .appName('test')\
        .getOrCreate()

    cdi_df = spark.read.format("parquet")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load(path)

    # print(f"CDI LENGTH: {cdi_df.count()}")
    tables = normalize_cdi_table(cdi_df, spark)
