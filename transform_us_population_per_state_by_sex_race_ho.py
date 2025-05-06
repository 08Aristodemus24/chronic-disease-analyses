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
    upper as sparkUpper,
    initcap,
    regexp_replace,
    lit,
    when,)
from pyspark.sql.types import DoubleType, LongType, ArrayType, FloatType, IntegerType
from pyspark.sql import SparkSession, Window
from pyspark.sql.dataframe import DataFrame

from argparse import ArgumentParser
from utilities.utils import get_state_populations



def process_population_by_sex_race_ho_table(df: DataFrame, 
    state: str, 
    cols_to_remove: list, 
    year_range: str="2000-2009"):
    """
    process a loaded spark dataframe
    """

    # extract numbers from year range
    years = re.findall(r"\d+", year_range)
    lo_year = ast.literal_eval(years[0])
    hi_year = ast.literal_eval(years[-1])
    year_fmtr = lambda year: f"_{year}"
    year_cols = [year_fmtr(year) for year in range(lo_year, hi_year + 1)]
    print(f"year cols: {year_cols}")

    # since cols in spark are _c0, _c1, ..., _cN we need
    # to format the numbers into these string values so that
    # we can specify the columns to be removed later 
    col_fmtr = lambda col: f"_c{col}"
    cols_to_remove = [col_fmtr(col) for col in cols_to_remove]
    
    # remove unnecessary columns lets say [1, 12, 13]
    # and rename the columns that are left to the years_list
    # excluding ethnicity column which is always the first column 0
    # {_c0, _c1, _c2, _c3, _c4, _c5, _c6, _c7, _c8, _c9, _c10, 
    # _c11, _c12} - {_c1, _c12, _c13, _c0} = 
    # {_c2, _c3, _c4, _c5, _c6, _c7, _c8, _c9, _c10, _c11}
    cols_left = sorted(
        list(
            set(df.columns) - set(cols_to_remove + [col_fmtr(0)])
        ), 
        key=lambda col: int(re.sub(r"_c", "", col))
    )
    print(f"cols left: {cols_left}")
    # new cols is calculated through set(df.columns) - set(cols_to_remove) 
    # we need to check if length of yeras_list == (length of new cols) - 1
    # in order to proceed with creating name mapper through
    # dictionary comprehension

    # {_c2: _2000, _c3: _c2001, _c4: _2002, _c5: _2004, 6: 2005, 7: 2006, 8: 2007, 9: 2008
    # 10: 2009} will be the name mapper to rename the left out columns in the dataframe
    name_map = {col: year_cols[i] for i, col in enumerate(cols_left)}
    print(f"new col names: {name_map}")

    # remove the columns or the columns that will not
    # be renamed and remove rows that have more than 5
    # columns of null values also
    df = df.drop(*cols_to_remove)
    df = df.dropna(thresh=5)

    # # if there are string columns then check if there are rows
    # # in this column with alphabetical chars and if so remove those
    # # rows as e only want number strings in this column to be casted
    # # to ints
    # str_cols_left = [col for col in cols_left if df.schema(col).dataType.typeName == "string"]
    # cond = " | ".join([f'(regexp({col}, r"([A-Za-z]+)"))' for col in str_cols_left])
    # df = df.where(~cond)
    
    
    # rename columns
    for old_col, new_col in name_map.items():
        df = df.withColumnRenamed(old_col, new_col)
    df = df.withColumnRenamed("_c0", "Ethnicity")

    # _2000 column is unfortunately read as string by spark so remove , chars 
    # in number and cast to long int type. Then cast other year columns to longs 
    type_map = {
        year_col: regexp_replace(col(year_col), r"[,]+", "").cast(LongType()) if year_col == "_2000" or year_col == "_2010" or year_col == "_2020" \
        else col(year_col).cast(LongType()) \
        for year_col in year_cols
    }
    df = df.withColumns(type_map)

    # remove periods
    df = df.withColumn("Ethnicity", regexp_replace(sparkLower(col("Ethnicity")), r"[.]+", ""))

    # convert ethnicities to shorter keywords
    cases = when(
        col("Ethnicity") == "black or african american",
        "Black"
    )\
    .when(
        (col("Ethnicity") == "american indian and alaska native") | (col("Ethnicity") == "AIAN"),
        "AIAN"
    )\
    .when(
        (col("Ethnicity") == "native hawaiian and other pacific islander") | (col("Ethnicity") == "NHPI"),
        "NHPI"
    )\
    .when(
        col("Ethnicity") == "two or more races",
        "Multiracial"
    )\
    .otherwise(
        initcap(col("Ethnicity"))
    )
    df = df.withColumn("Ethnicity", cases)

    # create index for spark dataframe
    increasing_col = monotonically_increasing_id()
    window = Window.orderBy(increasing_col)

    # returns a column object going from 0 to n
    index_col = row_number().over(window) - 1
    df = df.withColumn("Index", index_col)

    # extract the index location of where the row first indicates male
    # calculate the list slices here
    male_start = df.where(col("Ethnicity") == "Male").select("Index").collect()[0]["Index"]
    female_start = df.where(col("Ethnicity") == "Female").select("Index").collect()[0]["Index"]
    
    # since there are multiple indeces with the two 
    # or more races value we need to pick out the last value
    female_end = df.where(col("Ethnicity") == "Multiracial").select("Index").collect()[-1]["Index"]

    # collects population brackets of females and males 
    # and origins of both sexes
    pop_brackets_final = []
    for gender in ["Male", "Female"]:
        # determine the list slices during loop
        range_cond_1 = col("Index").between(male_start, female_start - 1) if gender == "Male"\
            else col("Index").between(female_start, female_end)
        pop_bracket_1 = df.where(range_cond_1)
        pop_bracket_1 = pop_bracket_1.withColumn("Index", index_col)
        # pop_bracket_1.show(pop_bracket_1.count())

        # calculate the list slices here for origin
        non_hisp_start = pop_bracket_1.where(col("Ethnicity") == "Not Hispanic").select("Index").collect()[0]["Index"]
        hisp_start = pop_bracket_1.where(col("Ethnicity") == "Hispanic").select("Index").collect()[-1]["Index"]
        end_indeces = pop_bracket_1.where(col("Ethnicity") == "Multiracial").select("Index").collect()
        non_hisp_end, hisp_end = end_indeces[-2]["Index"], end_indeces[-1]["Index"]

        for origin in ["Not Hispanic", "Hispanic"]:
            # determine the list slices for origin during loop
            range_cond_2 = col("Index").between(non_hisp_start + (2 if lo_year == 2000 and hi_year == 2009 else 1), non_hisp_end) \
                if origin == "Not Hispanic" \
                else col("Index").between(hisp_start + (2 if lo_year == 2000 and hi_year == 2009 else 1), hisp_end)
            pop_bracket_2 = pop_bracket_1.where(range_cond_2)
            # pop_bracket_2.show(pop_bracket_2.count())

            # add new columns and rename columns before and after stacking
            pop_bracket_2 = pop_bracket_2.withColumn("Origin", lit(origin))
            pop_bracket_2 = pop_bracket_2.withColumn("Sex", lit(gender))
            pop_bracket_2 = pop_bracket_2.melt(
                ids=["Ethnicity", "Origin", "Sex"],
                values=year_cols,
                variableColumnName="Year",
                valueColumnName="Population"
            )
            pop_bracket_2 = pop_bracket_2.withColumn("State", lit(state))

            # remove underscore in year and cast to int
            pop_bracket_2 = pop_bracket_2.withColumn("Year", regexp_replace(col("Year"), r"[_]", "").cast(IntegerType()))

            # append to population brackets list
            pop_brackets_final.append(pop_bracket_2)

    # clear from memory
    df.unpersist()
    pop_bracket_1.unpersist()
    pop_bracket_2.unpersist()

    # concatenate collected list of spark dataframes
    final = reduce(DataFrame.unionByName, pop_brackets_final)
    return final


if __name__ == "__main__":
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
                "cols_to_remove": [1, 12, 13],
                "populations": list(filter(lambda file: "2000-2010" in file and "by_sex_race_and_ho" in file, files))  
            },
            "2010-2019": {
                "cols_to_remove": [1, 2],
                "populations": list(filter(lambda file: "2010-2019" in file and "by_sex_race_and_ho" in file, files))  
            },
            "2020-2023": {
                "cols_to_remove": [1],
                "populations": list(filter(lambda file: "2020-2023" in file and "by_sex_race_and_ho" in file, files))  
            }
        }
    
    # create spark session
    # default is 1g for spark.executor.memory and 1 for spark.executor.cores
    spark = SparkSession.builder\
        .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:3.5.1_0.20.4")\
        .config("spark.executor.memory", "4g")\
        .config("spark.executor.cores", "2")\
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
            callback_fn=process_population_by_sex_race_ho_table)
        
        # # collect state populations from all years using list
        # # there should be 240 rows per us state regardless of year range
        # # except for 2020-2023 which is 96 rows since this is only a span 
        # # of 4 years. So 240 * 51 states * 2 year ranges spanning 10 years
        # # + 96 * 51 states is 29376 rows all in all  
        # state_populations_all_years.extend(state_populations)
        # create output file path
        indicator = year_range.replace("-", "_")
        FILE_NAME = f"us_population_per_state_by_sex_race_ho_{indicator}.parquet"
        OUTPUT_FILE_PATH = os.path.join(OUTPUT_DATA_DIR, FILE_NAME)
        state_populations.write.parquet(OUTPUT_FILE_PATH, mode="overwrite")

    