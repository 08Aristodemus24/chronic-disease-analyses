import os
import ast
import re

import pyspark.sql.functions as f

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType, StructField, StructType, FloatType

def get_lat_long(geo_loc):
        
        if f.isnull(geo_loc):
            return [0.0, 0.0]
        
        # if geoloc is not null or nan extract its
        # longitude and latitude 
        print(geo_loc)
        test = re.sub(r"(POINT|[/(/)])", "", geo_loc)
        test = test.strip()
        test = test.split(" ")
        latitude, longitude = ast.literal_eval(test[0]), ast.literal_eval(test[1]) 
        
        return [latitude, longitude]


if __name__ == "__main__":
    DATA_DIR = "./data/chronic-disease-data"

    path = os.path.join(DATA_DIR, "U.S._Chronic_Disease_Indicators__CDI___2023_Release.csv")

    spark = SparkSession.builder.appName('test')\
        .config("spark.executor.memory", "16g")\
        .getOrCreate()

    test_spark_df_00_10 = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load(path)
    cols_to_drop = ["Response",
        "ResponseID",
        "DataValueFootnoteSymbol",
        "DatavalueFootnote",

        "StratificationCategory2",
        "Stratification2",
        "StratificationCategory3",
        "Stratification3",

        "StratificationCategoryID1",
        "StratificationID1",
        "StratificationCategoryID2",
        "StratificationID2",
        "StratificationCategoryID3",
        "StratificationID3"]
    test_spark_df_00_10 = test_spark_df_00_10.drop(*cols_to_drop)

    # extracting geolocation
    test_udf = f.udf(get_lat_long, ArrayType(FloatType()))

    # set(test_spark_df_00_10.withColumn("Test", f.isnull(f.col("GeoLocation"))).select("Test").collect())

    # test_spark_df_00_10.withColumn("Test", test_udf(f.col("GeoLocation"))).select("Test").collect()

    test_spark_df_00_10 = test_spark_df_00_10.withColumn("Test", f.regexp(f.col("GeoLocation"), r""))
    test_spark_df_00_10.show()
    
    # # Errors
    # * Out of memory error: 
    # - https://stackoverflow.com/questions/73111729/pyspark-java-heap-out-of-memory-when-saving-5m-rows-dataframe
    # - https://medium.com/@rakeshchanda/spark-out-of-memory-issue-memory-tuning-and-management-in-pyspark-802b757b562f
    # - https://stackoverflow.com/questions/21138751/spark-java-lang-outofmemoryerror-java-heap-space
    # * EOF errror: 


