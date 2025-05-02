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

from argparse import ArgumentParser
from utilities.utils import get_state_populations



def process_population_by_sex_age_table(df: DataFrame, 
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

    if lo_year == 2000 and hi_year == 2009:
        # {_c2: _2000, _c3: _c2001, _c4: _2002, _c5: _2004, 6: 2005, 7: 2006, 8: 2007, 9: 2008
        # 10: 2009} will be the name mapper to rename the left out columns in the dataframe
        name_map = {col: year_cols[i] for i, col in enumerate(cols_left)}
        print(f"new col names: {name_map}")

        # rename columns
        for old_col, new_col in name_map.items():
            df = df.withColumnRenamed(old_col, new_col)
        df = df.withColumnRenamed("_c0", "Bracket")

        # _2000 column is unfortunately read as string by spark so remove , chars 
        # in number and cast to long int type. Then cast other year columns to longs 
        type_map = {
            year_col: regexp_replace(col(year_col), r"[,]", "").cast(LongType()) if year_col == "_2000" \
            else col(year_col).cast(LongType()) \
            for year_col in year_cols
        }
        df = df.withColumns(type_map)
        
        # create index for spark dataframe
        increasing_col = monotonically_increasing_id()
        window = Window.orderBy(increasing_col)

        # returns a column object going from 0 to n
        index_col = row_number().over(window) - 1
        df = df.withColumn("Index", index_col)

        # extract the index location of where the row first indicates male
        male_start = df.where(col("Bracket") == "MALE").select("Index").collect()[0]["Index"]
        pop_bracket_raw = df.where(col("Index").between(male_start, df.count() - 1))
        
        # get indeces
        female_start = pop_bracket_raw.where(col("Bracket") == "FEMALE").select("Index").collect()[0]["Index"]
        end_indeces = pop_bracket_raw.where(col("Bracket") == ".Median age (years)").select("Index").collect()
        male_end, female_end = end_indeces[0]["Index"], end_indeces[-1]["Index"]

        # print(f"male start: {male_start}")
        # print(f"female start: {female_start}")
        # print(f"male end: {male_end}")
        # print(f"female end: {female_end}")

        # split the excel spreadsheet into the male and female population brackets
        # and reset index using earlier made row_num window function 
        pop_bracket_raw = {
            "Male": {
                # Remove the following`
                # * column `1`, column `12`, and column `13` (the reasoning is these contain only the population estimates of april 1 and not the most recent one which is supposed to be at july 1, and that column `13` is the year 2010 which already exists in the next population years)
                # * rows with mostly Nan and the a dot symbol in column `1` i.e. `[. Nan Nan Nan Nan Nan ... Nan]`
                # * and the male column 
                "DataFrame": df.where(col("Index").between(male_start, male_end - 1))
                .where((col("Bracket") != ".") & (col("Bracket") != "MALE"))
                .withColumn("Index", index_col)
            },
            "Female": {
                "DataFrame": df.where(col("Index").between(female_start, female_end - 1))
                .where((col("Bracket") != ".") & (col("Bracket") != "FEMALE"))
                .withColumn("Index", index_col)
            },
        }

        # clear previous spark dataframe from memory
        df.unpersist()

    elif (lo_year == 2010 and hi_year == 2019) or (lo_year == 2020 and hi_year == 2023):
        # create index for spark dataframe
        increasing_col = monotonically_increasing_id()
        window = Window.orderBy(increasing_col)

        # returns a column object going from 0 to n
        index_col = row_number().over(window) - 1
        df = df.withColumn("Index", index_col)

        # get only needed rows
        start_index = df.where(col("_c0") == ".0").select("Index").collect()[0]["Index"]
        end_index = df.where(col("_c0") == ".85+").select("Index").collect()[0]["Index"]
        df = df.where(col("Index").between(start_index, end_index))
        
        # ['_c8', '_c9', '_c11', '_c12', '_c14', '_c15', '_c17', 
        #  '_c18', '_c20', '_c21', '_c23', '_c24', '_c26', '_c27', 
        #  '_c29', '_c30', '_c32', '_c33', '_c35', '_c36']
        # the left out columns we will have to paritition into male and female
        male_cols = [col for i, col in enumerate(cols_left) if i % 2 == 0]
        female_cols = sorted(list(set(cols_left) - set(male_cols)), key=lambda col: int(re.sub(r"_c", "", col)))
        print(f"male cols: {male_cols}")
        print(f"female cols: {female_cols}")
        
        
        # what we can do instead is extract out the columns that 
        # are male and plce in separate dfs and extract out the 
        # columns that are female adn place in separate df and 
        # then melt each df on each year cols assigned to male 
        # and female each partitioned dataframe 
        pop_bracket_raw = {
            "Male": {
                "DataFrame": df.select(*male_cols + ["_c0"]),
                # {_c2: _2000, _c3: _c2001, _c4: _2002, _c5: _2004, 6: 2005, 
                # 7: 2006, 8: 2007, 9: 2008, 10: 2009} will be the name mapper
                # to rename the left out columns in the dataframe
                "NameMaps": {col: year_cols[i] for i, col in enumerate(male_cols)}
            }, 
            "Female": {
                "DataFrame": df.select(*female_cols + ["_c0"]),
                "NameMaps": {col: year_cols[i] for i, col in enumerate(female_cols)}
            }
        }

        # clear previosu dataframe from memory 
        df.unpersist()

        # rename columns and cast columns to long ints
        for gender in ["Male", "Female"]:
            for old_col, new_col in pop_bracket_raw[gender]["NameMaps"].items():
                pop_bracket_raw[gender]["DataFrame"] = pop_bracket_raw[gender]["DataFrame"].withColumnRenamed(old_col, new_col)
            pop_bracket_raw[gender]["DataFrame"] = pop_bracket_raw[gender]["DataFrame"].withColumnRenamed("_c0", "Bracket")
        
        # _2000 column is unfortunately read as string by spark so remove , chars 
        # in number and cast to long int type. Then cast other year columns to longs 
        type_map = {year_col: regexp_replace(col(year_col), r"[,]+", "").cast(LongType()) for year_col in year_cols}
        for gender in ["Male", "Female"]:
            pop_bracket_raw[gender]["DataFrame"] = pop_bracket_raw[gender]["DataFrame"].withColumns(type_map)
        
    # # print(f"male count: {pop_bracket_raw["Male"]["DataFrame"].count()}")
    # # print(f"99999999999999 {pop_bracket_raw["Male"]["DataFrame"].select("Bracket").distinct().collect()}")
    # # print(f"female count: {pop_bracket_raw["Female"]["DataFrame"].count()}")

    # collects population brackets of females and males
    pop_brackets_final = []
    for gender in ["Male", "Female"]:
        
        # we remove any duplicates in the dataframe especially those with same 
        # age brackets
        temp = pop_bracket_raw[gender]["DataFrame"].drop_duplicates(subset=year_cols + ["Bracket"])

        # remove rows with at least 5 nan values
        temp = temp.dropna(thresh=5)

        # clean bracket column then stack the year columns
        # onto each other and have each value in their rows
        # to be the population values
        temp = temp.withColumn(
            "Bracket", 
            # we place square brackets to match . because if we remvoe
            # brackets this will mean to match anything except for a 
            # linebreak which is what only . does 
            regexp_replace(sparkLower(col("Bracket")), r"[.]+", "")
        )
        # print(f"data types: {temp.dtypes}")
        temp = temp.melt(
            ids=["Bracket"],
            values=year_cols,
            variableColumnName="Year",
            valueColumnName="Population"
        )

        # set sex and state columns
        temp = temp.withColumn("Sex", lit(gender))
        temp = temp.withColumn("State", lit(state))

        # extract keyword from column
        keyword_col = regexp_extract(col("Bracket"), r"(under|to|and\s*over|\+)", 1)
        age_cases = when(
            keyword_col == "under",
            concat(
                array(lit(0.0)),
                regexp_extract_all(
                    col("Bracket"), 
                    lit(r"(\d+)"), 
                    1
                ).cast(ArrayType(FloatType()))
            )
        )\
        .when(
            keyword_col == "to",
            regexp_extract_all(
                col("Bracket"), 
                lit(r"(\d+)"), 
                1
            ).cast(ArrayType(FloatType()))
        )\
        .when(
            keyword_col == "and over",
            concat(
                regexp_extract_all(
                    col("Bracket"), 
                    lit(r"(\d+)"), 
                    1
                ).cast(ArrayType(FloatType())),
                array(lit(float("inf"))),
            )
        )\
        .otherwise(
            concat(
                # [1] or [85] or [2] these imply that the
                # bracket column was only a single number and had
                # no substrings like "to", "under", "+", and "and over" 
                regexp_extract_all(
                    col("Bracket"), 
                    lit(r"(\d+)"), 
                    1
                ).cast(ArrayType(FloatType())),
                # [NULL]
                array(lit(None))
            )
        )
        
        # create AgeStart and AgeEnd columns
        temp = temp.withColumn("AgeStart", age_cases[0])\
        .withColumn("AgeEnd", age_cases[1])

        # remove underscore in year values and cast to int
        temp = temp.withColumn("Year", regexp_replace(col("Year"), r"[_]", "").cast(IntegerType()))

        # append to pop_brackets_list for later concatenation
        pop_brackets_final.append(temp)

    # clear temp dataframes from memory
    pop_bracket_raw["Male"]["DataFrame"].unpersist()
    pop_bracket_raw["Female"]["DataFrame"].unpersist()

    # concatenate final population brackets using
    # unionByName as callback
    final = reduce(DataFrame.unionByName, pop_brackets_final)

    # return spark dataframe
    return final



if __name__ == "__main__":
    # get year range and state from user input
    parser = ArgumentParser()
    parser.add_argument("--year-range-list", type=str, default=["2000-2009"], nargs="+", help="represents the lists of year ranges that spark script would base on to transform excel files of these year ranges")
    args = parser.parse_args()

    # get arguments
    year_range_list = args.year_range_list

    DATA_DIR = './data/population-data-raw'
    EXCLUSIONS = ["us_populations_per_state_2001_to_2021.csv"]
    files = list(filter(lambda file: not file in EXCLUSIONS, os.listdir(DATA_DIR)))
    cases = {
            "2000-2009": {
                "cols_to_remove": [1, 12, 13],
                "populations": list(filter(lambda file: "2000-2010" in file and "by_sex_and_age" in file, files))  
            },
            "2010-2019": {
                "cols_to_remove": [1, 2, 3, 4, 5, 6, 7, 10, 13, 16, 19, 22, 25, 28, 31, 34],
                "populations": list(filter(lambda file: "2010-2019" in file and "by_sex_and_age" in file, files))  
            },
            "2020-2023": {
                "cols_to_remove": [1, 2, 3, 4, 7, 10, 13],
                "populations": list(filter(lambda file: "2020-2023" in file and "by_sex_and_age" in file, files))  
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
    print(f"spark jars packages: {conf_view.get("spark.jars.packages")}")
    print(f"spark.executor.memory: {conf_view.get("spark.executor.memory")}")
    print(f"spark.executor.cores: {conf_view.get("spark.executor.cores")}")

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
            callback_fn=process_population_by_sex_age_table)
        
        # collect state populations from all years using list
        # there should be 240 rows per us state regardless of year range
        # except for 2020-2023 which is 96 rows since this is only a span 
        # of 4 years. So 240 * 51 states * 2 year ranges spanning 10 years
        # + 96 * 51 states is 29376 rows all in all  
        # create output file path
        indicator = year_range.replace("-", "_")
        FILE_NAME = f"us_populations_per_state_by_sex_age_{indicator}.parquet"
        OUTPUT_FILE_PATH = os.path.join(OUTPUT_DATA_DIR, FILE_NAME)
        state_populations.write.parquet(OUTPUT_FILE_PATH)