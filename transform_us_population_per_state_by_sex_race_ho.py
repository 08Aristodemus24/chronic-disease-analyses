import os
import re
import ast
import sys

from functools import reduce

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

from concurrent.futures import ThreadPoolExecutor


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

    # remove the columns or the columns that will not
    # be renamed
    df = df.drop(*cols_to_remove)
    
    # {_c2: _2000, _c3: _c2001, _c4: _2002, _c5: _2004, 6: 2005, 7: 2006, 8: 2007, 9: 2008
    # 10: 2009} will be the name mapper to rename the left out columns in the dataframe
    name_map = {col: year_cols[i] for i, col in enumerate(cols_left)}
    print(f"new col names: {name_map}")

    # rename columns
    for old_col, new_col in name_map.items():
        df = df.withColumnRenamed(old_col, new_col)
    df = df.withColumnRenamed("_c0", "Ethnicity")

    # _2000 column is unfortunately read as string by spark so remove , chars 
    # in number and cast to long int type. Then cast other year columns to longs 
    type_map = {
        year_col: regexp_replace(col(year_col), r"[,]", "").cast(LongType()) if year_col == "_2000" \
        else col(year_col).cast(LongType()) \
        for year_col in year_cols
    }
    df = df.withColumns(type_map)

    # 
    df = df.withColumn("Ethnicity", regexp_replace(sparkLower(col("Ethnicity")), r"[.]", ""))
    
    # create index for spark dataframe
    increasing_col = monotonically_increasing_id()
    window = Window.orderBy(increasing_col)

    # returns a column object going from 0 to n
    index_col = row_number().over(window) - 1
    df = df.withColumn("Index", index_col)

    # extract the index location of where the row first indicates male
    # calculate the list slices here
    male_start = df.where(col("Ethnicity") == "male").select("Index").collect()[0]["Index"]
    female_start = df.where(col("Ethnicity") == "female").select("Index").collect()[0]["Index"]
    
    # since there are multiple indeces with the two 
    # or more races value we need to pick out the last value
    female_end = df.where(col("Ethnicity") == "two or more races").select("Index").collect()[-1]["Index"]
    
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
        non_hisp_start = pop_bracket_1.where(col("Ethnicity") == "not hispanic").select("Index").collect()[0]["Index"]
        hisp_start = pop_bracket_1.where(col("Ethnicity") == "hispanic").select("Index").collect()[-1]["Index"]
        end_indeces = pop_bracket_1.where(col("Ethnicity") == "two or more races").select("Index").collect()
        non_hisp_end, hisp_end = end_indeces[-2]["Index"], end_indeces[-1]["Index"]

        # print(f"non hispanic indeces: {non_hisp_start}, {non_hisp_end}")
        # print(f"hispanic indeces: {hisp_start}, {hisp_end}")

        for origin in ["Not Hispanic", "Hispanic"]:
            # determine the list slices for origin during loop
            range_cond_2 = col("Index").between(non_hisp_start + 2, non_hisp_end) if origin == "Not Hispanic" else col("Index").between(hisp_start + 2, hisp_end)
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
    year_range = sys.argv[1]
    state = sys.argv[2].capitalize()

    # spark-submit --packages com.crealytics:spark-excel_2.12:3.5.1_0.20.4 transform_us_population_per_state_by_sex_race_ho.py
    # define file paths, and data directories
    DATA_DIR = './data/population-data'
    FILE = f"{state}_pop_by_sex_race_and_ho_{"2000-2010" if year_range == "2000-2009" else year_range}.xls{"" if year_range == "2000-2009" else "x"}"
    FILE_PATH = os.path.join(DATA_DIR, FILE)

    # create spark session
    spark = SparkSession.builder.appName('test')\
        .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:3.5.1_0.20.4")\
        .getOrCreate()

    # read excel file
    df = spark.read.format("com.crealytics.spark.excel")\
        .option("header", "false")\
        .option("inferSchema", "true")\
        .load(FILE_PATH)
    
    if year_range == "2000-2009":
        cols_to_remove = [1, 12, 13]
    elif year_range == "2010-2019":
        cols_to_remove = [1, 2]
    elif year_range == "2020-2023":
        cols_to_remove = [1]

    state_population = process_population_by_sex_race_ho_table(df, state, cols_to_remove, year_range)
    state_population.show(state_population.count())