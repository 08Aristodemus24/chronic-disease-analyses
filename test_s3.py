import os
import itertools

from dotenv import load_dotenv
from pathlib import Path

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
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.types import StringType, ArrayType, StructField, StructType, FloatType, DoubleType, IntegerType



if __name__ == "__main__":
    # Build paths inside the project like this: BASE_DIR / 'subdir'.
    # use this only in development
    env_dir = Path('./').resolve()
    load_dotenv(os.path.join(env_dir, '.env'))

    # load env vars
    AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
    AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
    
    spark_conf = SparkConf()
    spark_conf.setAppName("test")
    spark_conf.set("spark.driver.memory", "14g") 
    spark_conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "100")

    spark_ctxt = SparkContext(conf=spark_conf)

    hadoop_conf = spark_ctxt._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    hadoop_conf.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    hadoop_conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    # it is imperative that our hadoop aws version matches the hadoop version
    # our winutils.exe and hadoop.dll files are under in
    # spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.0,\
    # com.amazonaws:aws-java-sdk-bundle:1.11.563,\
    # org.apache.httpcomponents:httpcore:4.4.16,\
    # com.google.guava:guava:33.4.0-jre test_s3.py
    # the maven package org.apache.hadoop:hadoop-aws:3.3.0
    # when we read the packages compile dependencies requires
    # com.amazonaws:aws-java-sdk-bundle:1.11.563 so it makes sense 
    # to also include this in our spark submit

    
    spark = SparkSession(spark_ctxt).builder\
        .getOrCreate()

    test = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load("s3a://usd-php-ml-pipeline-bucket/raw/usd_php_forex_4hour.csv")
    
    test.show()