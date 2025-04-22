import os

from pyspark.sql import SparkSession

DATA_DIR = "./data/population-data"
# DATA_DIR = "./data/chronic-disease-data"

# .\data\population-data\Alabama_pop_by_sex_and_age_2000-2010.xls
path = os.path.join(DATA_DIR, "Alabama_pop_by_sex_and_age_2000-2010.xls")
# path = os.path.join(DATA_DIR, "U.S._Chronic_Disease_Indicators__CDI___2023_Release.csv")

spark = SparkSession.builder.appName('test')\
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:3.5.1_0.20.4")\
    .getOrCreate()

test_spark_df_00_10 = spark.read.format("com.crealytics.spark.excel")\
    .option("header", "false")\
    .option("inferSchema", "true")\
    .load(path)

# test_spark_df_00_10 = spark.read.format("csv")\
#     .option("header", "true")\
#     .option("inferSchema", "true")\
#     .load(path)

test_spark_df_00_10.show()

