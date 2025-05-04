import os
import itertools

from pyspark.sql.dataframe import DataFrame
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
    concat,
    array,)
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType, StructField, StructType, FloatType, DoubleType, IntegerType

def normalize_cdi_table(df: DataFrame, session: SparkSession):
    # remove location desc
    # remove location abbr
    # retain in locations dimension table with location abbr as id

    # remove topic
    # remove question
    # remove topicid
    # retain in questions table with question id

    # remove data value type
    # retain in data value type table with data value type id as id

    # remove stratification 1

    # remove sex, ethnicity, and origin get all unique
    # permutations of sex, ethnicity, and origin and
    # assign them a value

    # extract the unique id's of each column to be retained
    # and placed also in a dimension table
    loc_id = df.select("LocationAbbr").distinct().collect()
    question_id = df.select("QuestionID").distinct().collect()
    dvt_id = df.select("DataValueTypeID").distinct().collect()
    u_sex = [row["Sex"] for row in df.select("Sex").distinct().collect()]
    u_ethnicity = [row["Ethnicity"] for row in df.select("Ethnicity").distinct().collect()]
    u_origin = [row["Origin"] for row in df.select("Origin").distinct().collect()]

    # create unique sex, ethnicity, and origin permutations
    # these are the list of unique values we need to use to 
    # extract the products which are the permutations   
    values = [u_sex, u_ethnicity, u_origin]

    # since values is an iterable and product receives variable arguments,
    # variable arg in product are essentially split thus above values
    # will be split into [both, male, female], [white, black, AIAN, asian, NHPI, 
    # multiracial], and [both, hispanic, not hispanic] this can be then easily
    # used to produce all possible permutations of these values e.g. (both, white, hispanic), 
    # (male, black, not hispanic), (female, AIAN, both) and so on... which represent the
    # sex, ethnicity, and hispanic origin and all unique permuattions produced by these will
    # be assigned an id that our cdi fact table can use to determine what a cdi stratification
    # permutation is 
    strat_perms = list(itertools.product(*values))
    strat_perms_df = session.createDataFrame(strat_perms, ["Sex", "Ethnicity", "Origin"])
    strat_perms_df.show()
    


if __name__ == "__main__":
    DATA_DIR = "./data/cdi-data-transformed"
    path = os.path.join(DATA_DIR, "cdi.parquet")

    spark = SparkSession.builder\
        .master("local[1]")\
        .config("spark.driver.memory", "6g")\
        .config("spark.sql.execution.arrow.maxRecordsPerBatch","100")\
        .appName('test')\
        .getOrCreate()

    cdi_df = spark.read.format("parquet")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load(path)

    # print(f"CDI LENGTH: {cdi_df.count()}")
    normalize_cdi_table(cdi_df, spark)