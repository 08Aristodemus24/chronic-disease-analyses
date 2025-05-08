import os
import re
import ast
import boto3

from dotenv import load_dotenv
from pathlib import Path
from functools import reduce

from pyspark import SparkConf
from pyspark.sql.functions import (
    col,
    lower as sparkLower,
    upper as sparkUpper,
    regexp_replace,
    regexp_extract_all,
    substring,
    lit,
    when,
    array_join,
    concat,)
from pyspark.sql.types import DoubleType, LongType, ArrayType, FloatType, IntegerType
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession, Window
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

from argparse import ArgumentParser
from utilities.utils import get_state_populations


def process_population_per_state_by_sex_age_race_ho_table(df: DataFrame, 
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
    print(old_non_year_cols)
    print(new_non_year_cols)

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
    
    # for origin and sex pick out the initial character of each word
    # Not Hispanic -> NH
    # Both -> B
    # Hispanic -> H
    # Male -> M
    # Female -> F
    # Both -> B
    origin_codes = array_join(
        regexp_extract_all(col("Origin"), lit(r"(\b[A-Za-z])"), 1),
        ""
    )
    sex_codes = array_join(
        regexp_extract_all(col("Sex"), lit(r"(\b[A-Za-z])"), 1),
        ""
    )

    # for races pick out all characters that are at most 5 
    # chars in length 
    # Black -> BLACK
    # AIAN -> AIAN
    # Asian -> ASIAN
    # WHITE -> WHITE
    # NHPI -> NHPI
    # Other -> OTHER
    # Multiracial -> MULTI
    # All -> ALL
    eth_codes = sparkUpper(substring(col("Ethnicity"), 1, 5))
    strat_id = concat(origin_codes, lit("_"), sex_codes, lit("_"), eth_codes)
    df = df.withColumn("StratificationID", strat_id)

    # create id for States and permutations of origin, sex,
    # and ethnicity before unpivoting/melting df
    state_id_cases = when(col("State") == "Michigan", "MI")\
    .when(col("State") == "Washington", "WA")\
    .when(col("State") == "Delaware", "DE")\
    .when(col("State") == "Arkansas", "AR")\
    .when(col("State") == "Georgia", "GA")\
    .when(col("State") == "Vermont", "VT")\
    .when(col("State") == "Montana", "MT")\
    .when(col("State") == "Idaho", "ID")\
    .when(col("State") == "Texas", "TX")\
    .when(col("State") == "New York", "NY")\
    .when(col("State") == "Connecticut", "CT")\
    .when(col("State") == "Louisiana", "LA")\
    .when(col("State") == "Missouri", "MO")\
    .when(col("State") == "Kentucky", "KY")\
    .when(col("State") == "California", "CA")\
    .when(col("State") == "Alabama", "AL")\
    .when(col("State") == "Florida", "FL")\
    .when(col("State") == "North Dakota", "ND")\
    .when(col("State") == "South Carolina", "SC")\
    .when(col("State") == "Iowa", "IA")\
    .when(col("State") == "South Dakota", "SD")\
    .when(col("State") == "Oklahoma", "OK")\
    .when(col("State") == "Pennsylvania", "PA")\
    .when(col("State") == "Virginia", "VA")\
    .when(col("State") == "Rhode Island", "RI")\
    .when(col("State") == "Utah", "UT")\
    .when(col("State") == "Wisconsin", "WI")\
    .when(col("State") == "Arizona", "AZ")\
    .when(col("State") == "New Mexico", "NM")\
    .when(col("State") == "New Hampshire", "NH")\
    .when(col("State") == "Illinois", "IL")\
    .when(col("State") == "Maryland", "MD")\
    .when(col("State") == "Mississippi", "MS")\
    .when(col("State") == "Wyoming", "WY")\
    .when(col("State") == "Nevada", "NV")\
    .when(col("State") == "Ohio", "OH")\
    .when(col("State") == "Minnesota", "MN")\
    .when(col("State") == "Hawaii", "HI")\
    .when(col("State") == "Tennessee", "TN")\
    .when(col("State") == "Indiana", "IN")\
    .when(col("State") == "West Virginia", "WV")\
    .when(col("State") == "Maine", "ME")\
    .when(col("State") == "Colorado", "CO")\
    .when(col("State") == "District of Columbia", "DC")\
    .when(col("State") == "Nebraska", "NE")\
    .when(col("State") == "Kansas", "KS")\
    .when(col("State") == "Alaska", "AK")\
    .when(col("State") == "North Carolina", "NC")\
    .when(col("State") == "New Jersey", "NJ")\
    .when(col("State") == "Oregon", "OR")\
    .when(col("State") == "Massachusett", "MA")

    df = df.withColumn("StateID", state_id_cases)

    # stack year columns with all ids being state, sex, age, 
    # ethnicity, and origin 
    df = df.melt(
        ids=["StateID", "State", "Age", "Ethnicity", "Origin", "Sex", "StratificationID"],
        values=year_cols,
        variableColumnName="Year",
        valueColumnName="Population"
    )

    # clean year column, cast age to float, and cast 
    # population to long int
    df = df.withColumn("Year", regexp_replace(col("Year"), r"[_]", "").cast(IntegerType()))
    df = df.withColumn("Population", col("Population").cast(LongType()))
    df = df.withColumn("Age", col("Age").cast(FloatType()))
    # df.write.csv("./data/population-data-transformed/Population_first_stage.csv", mode="overwrite")

    return df


def normalize_population_per_state_by_sex_age_race_ho_table(df: DataFrame, session: SparkSession, year_range: str):
    """
    extract the unique id's of each column to be retained
    and placed also in a dimension table
    """

    # create state dimension table and then drop state column
    # and retain stateID as foreign key to state dim table
    state_df = df.select("State", "StateID").dropDuplicates()
    df = df.drop("State")

    # drop sex, ethnicity, and origin strat columns and retain
    # in separate dimension table like state table
    strat_df = df.select("Sex", "Ethnicity", "Origin", "StratificationID").dropDuplicates()
    df = df.drop("Sex", "Ethnicity", "Origin")
    print(year_range)
    tables = list(zip(
        ["Population", "State", "Stratification"], 
        [df, state_df, strat_df], 
        [year_range] * 3)
    )

    # return all normalized tables
    return tables


def save_tables(tables_all_years: list[tuple[str, DataFrame, str]], OUTPUT_DATA_DIR: str="./data/population-data-transformed"):
    """
    decouples the population fact tables since these are large tables from
    rows with dimension tables as these dimension tables will be unionized.
    The tables_all_years is detailed below  

    [
        ('Population', DataFrame[StateID: string, Age: float, StratificationID: string, Year: int, Population: bigint], '2000-2009'), 
        ('State', DataFrame[State: string, StateID: string], '2000-2009'), 
        ('Stratification', DataFrame[Sex: string, Ethnicity: string, Origin: string, StratificationID: string], '2000-2009'), 
        ('Population', DataFrame[StateID: string, Age: float, StratificationID: string, Year: int, Population: bigint], '2010-2019'), 
        ('State', DataFrame[State: string, StateID: string], '2010-2019'), 
        ('Stratification', DataFrame[Sex: string, Ethnicity: string, Origin: string, StratificationID: string], '2010-2019'), 
        ('Population', DataFrame[StateID: string, Age: float, StratificationID: string, Year: int, Population: bigint], '2020-2023'), 
        ('State', DataFrame[State: string, StateID: string], '2020-2023'), 
        ('Stratification', DataFrame[Sex: string, Ethnicity: string, Origin: string, StratificationID: string], '2020-2023')
    ]
    """

    if not OUTPUT_DATA_DIR.startswith("s3a"):
        # create output directory
        os.makedirs(OUTPUT_DATA_DIR, exist_ok=True)
    
    # tables_all_years contains list of tuples which can be unzipped
    # into names, dataframes, and year range
    # other dimension tables
    # [
    #     ('Stratification', DataFrame[Sex: string, Ethnicity: string, Origin: string, StratificationID: string], '2020-2023'), 
    #     ('Stratification', DataFrame[Sex: string, Ethnicity: string, Origin: string, StratificationID: string], '2000-2009'), 
    #     ('Stratification', DataFrame[Sex: string, Ethnicity: string, Origin: string, StratificationID: string], '2010-2019'), 
    # ]

    # [
    #     ('State', DataFrame[State: string, StateID: string], '2010-2019'), 
    #     ('State', DataFrame[State: string, StateID: string], '2020-2023'), 
    #     ('State', DataFrame[State: string, StateID: string], '2000-2009')
    # ]

    # population fact tables
    # [
    #     ('Population', DataFrame[StateID: string, Age: float, StratificationID: string, Year: int, Population: bigint], '2000-2009'), 
    #     ('Population', DataFrame[StateID: string, Age: float, StratificationID: string, Year: int, Population: bigint], '2010-2019'), 
    #     ('Population', DataFrame[StateID: string, Age: float, StratificationID: string, Year: int, Population: bigint], '2020-2023')
    # ]
    population_tables_all_years = list(filter(lambda table: table[0] == "Population", tables_all_years))
    state_tables_all_years = list(filter(lambda table: table[0] == "State", tables_all_years))
    stratification_tables_all_years = list(filter(lambda table: table[0] == "Stratification", tables_all_years))
    # print(dimension_tables_all_years)
    # print(population_tables_all_years)

    # state dimension table
    _, state_tables, _ = zip(*state_tables_all_years)
    state_df = reduce(DataFrame.unionByName, state_tables).dropDuplicates()
    
    # save to disk or s3 bucket
    FILE_NAME = "State.parquet"
    OUTPUT_FILE_PATH = os.path.join(OUTPUT_DATA_DIR, FILE_NAME)
    state_df.write.parquet(OUTPUT_FILE_PATH, mode="overwrite")
    # state_df.write.csv(os.path.join(OUTPUT_DATA_DIR, f"State.csv"), mode="overwrite")

    # stratification dimension table
    _, stratification_tables, _ = zip(*stratification_tables_all_years)
    stratification_df = reduce(DataFrame.unionByName, stratification_tables).dropDuplicates()
    
    # save to disk or s3 bucket
    FILE_NAME = "Stratification.parquet"
    OUTPUT_FILE_PATH = os.path.join(OUTPUT_DATA_DIR, FILE_NAME)
    stratification_df.write.parquet(OUTPUT_FILE_PATH, mode="overwrite")
    # stratification_df.write.csv(os.path.join(OUTPUT_DATA_DIR, f"Stratification.csv"), mode="overwrite")

    for population_table_name, population_table, population_year_range in population_tables_all_years:
        indicator = population_year_range.replace("-", "_")
        FILE_NAME = f"Population_{indicator}.parquet"
        OUTPUT_FILE_PATH = os.path.join(OUTPUT_DATA_DIR, FILE_NAME)
        population_table.write.parquet(OUTPUT_FILE_PATH, mode="overwrite")
        # population_table.write.csv(os.path.join(OUTPUT_DATA_DIR, f"Population_{indicator}.csv"), mode="overwrite")


# spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.11.563,org.apache.httpcomponents:httpcore:4.4.16 transform_us_population_per_state_by_sex_age_race_ho.py --year-range-list 2000-2009 2010-2019 2020-2023
if __name__ == "__main__":
    # run this only locally 
    # get year range and state from user input
    parser = ArgumentParser()
    parser.add_argument("--year-range-list", type=str, default=["2000-2009"], nargs="+", help="represents the lists of year ranges that spark script would base on to transform excel files of these year ranges")
    args = parser.parse_args()

    # get arguments
    year_range_list = args.year_range_list

    DATA_DIR = './data/population-data-raw'
    EXCLUSIONS = ["us_population_per_state_2001_to_2021.csv", "population-data.zip"]
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
        .config("spark.executor.memory", "2g")\
        .config("spark.executor.cores", "6")\
        .getOrCreate()
    
    conf_view = spark.sparkContext.getConf()
    print(f"spark.executor.memory: {conf_view.get('spark.executor.memory')}")
    print(f"spark.executor.cores: {conf_view.get('spark.executor.cores')}")
    
    # get year range from system arguments sys.argv
    tables_all_years = []

    # loop through year_ranges
    for year_range in year_range_list:
        # concurrently process state populations by year range
        first_stage_state_population_df = get_state_populations(
            DATA_DIR, 
            spark, 
            cases[year_range]["cols_to_remove"], 
            cases[year_range]["populations"], 
            year_range,
            callback_fn=process_population_per_state_by_sex_age_race_ho_table)
    
        # # pass first stage state population df to function
        # tables = normalize_population_per_state_by_sex_age_race_ho_table(first_stage_state_population_df, spark, year_range)
        # tables_all_years.extend(tables)

    # save_tables(tables_all_years)

    # # Build paths inside the project like this: BASE_DIR / 'subdir'.
    # # use this only in development
    # env_dir = Path('./').resolve()
    # load_dotenv(os.path.join(env_dir, '.env'))

    # # get year range and state from user input
    # parser = ArgumentParser()
    # parser.add_argument("--year-range-list", type=str, default=["2000-2009"], nargs="+", help="represents the lists of year ranges that spark script would base on to transform excel files of these year ranges")
    # args = parser.parse_args()

    # # get arguments
    # year_range_list = args.year_range_list

    # # 
    # BUCKET_NAME = "chronic-disease-analyses-bucket"
    # INPUT_FOLDER_NAME = "population-data-raw/"
    # INPUT_DATA_DIR = f"s3a://{BUCKET_NAME}/{INPUT_FOLDER_NAME}"
    # EXCLUSIONS = ["us_population_per_state_2001_to_2021.csv", "population-data.zip"]

    # # load env vars
    # credentials = {
    #     "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
    #     "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
    #     "region_name": os.environ["AWS_REGION_NAME"],
    # }

    # # define s3 client
    # s3 = boto3.client("s3", **credentials)

    # # list specified s3 bucket files here 
    # response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=INPUT_FOLDER_NAME)
    # files = [content["Key"].replace(INPUT_FOLDER_NAME, "") for content in response.get("Contents")]

    # # ['us_populations_per_state_by_sex_age_race_ho_2000-2010.csv', 
    # # 'us_populations_per_state_by_sex_age_race_ho_2010-2019.csv', 
    # # 'us_populations_per_state_by_sex_age_race_ho_2020-2023.csv']
    # files = list(filter(lambda file: file and (not file in EXCLUSIONS), files))
    # cases = {
    #         "2000-2009": {
    #             "cols_to_remove": [
    #                 "SUMLEV",
    #                 "REGION",
    #                 "DIVISION",
    #                 "STATE",
    #                 "CENSUS2000POP",
    #                 "ESTIMATESBASE2000",
    #                 "POPESTIMATE42010",
    #                 "POPESTIMATE72010"
    #              ],
    #             "populations": list(filter(lambda file: "2000-2010" in file and "by_sex_age_race_ho" in file, files))
    #         },
    #         "2010-2019": {
    #             "cols_to_remove": [
    #                 "SUMLEV",
    #                 "REGION",
    #                 "DIVISION",
    #                 "STATE",
    #                 "CENSUS2010POP",
    #                 "ESTIMATESBASE2010"
    #              ],
    #             "populations": list(filter(lambda file: "2010-2019" in file and "by_sex_age_race_ho" in file, files))  
    #         },
    #         "2020-2023": {
    #             "cols_to_remove": [
    #                 "SUMLEV",
    #                 "REGION",
    #                 "DIVISION",
    #                 "STATE",
    #                 "ESTIMATESBASE2020"
    #              ],
    #             "populations": list(filter(lambda file: "2020-2023" in file and "by_sex_age_race_ho" in file, files))  
    #         }
    #     }    
    
    # spark_conf = SparkConf()
    # spark_conf.setAppName("test")
    # spark_conf.set("spark.driver.memory", "14g") 
    # spark_conf.set("spark.executor.memory", "2g")
    # spark_conf.set("spark.executor.cores", "6")
    # spark_conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "100")

    # spark_ctxt = SparkContext(conf=spark_conf)

    # hadoop_conf = spark_ctxt._jsc.hadoopConfiguration()
    # hadoop_conf.set("fs.s3a.access.key", credentials["aws_access_key_id"])
    # hadoop_conf.set("fs.s3a.secret.key", credentials["aws_secret_access_key"])
    # hadoop_conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # hadoop_conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    # spark = SparkSession(spark_ctxt).builder\
    #     .getOrCreate()

    # # get year range from system arguments sys.argv
    # tables_all_years = []

    # # loop through year_ranges
    # for year_range in year_range_list:
    #     # concurrently process state populations by year range
    #     first_stage_state_population_df = get_state_populations(
    #         INPUT_DATA_DIR, 
    #         spark, 
    #         cases[year_range]["cols_to_remove"], 
    #         cases[year_range]["populations"], 
    #         year_range,
    #         callback_fn=process_population_per_state_by_sex_age_race_ho_table)
    
    #     # pass first stage state population df to function
    #     tables = normalize_population_per_state_by_sex_age_race_ho_table(first_stage_state_population_df, spark, year_range)
    #     tables_all_years.extend(tables)

    # # create bucket and create bucket folder
    # OUTPUT_FOLDER_NAME = "population-data-transformed/"
    # OUTPUT_DATA_DIR = f"s3a://{BUCKET_NAME}/{OUTPUT_FOLDER_NAME}"
    # save_tables(tables_all_years, OUTPUT_DATA_DIR=OUTPUT_DATA_DIR)