import os
import re
import ast

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


# def get_state_populations(DATA_DIR: str, cols_to_remove: list, populations: list, year_range: str, by: str) -> pd.DataFrame:
#     def concur_model_pop_tables(file, to_remove, year_range, callback_fn=model_population_table):
#         FILE_PATH = os.path.join(DATA_DIR, file)
#         state = re.search(r"(^[A-Za-z\s]+)", file)
#         state = "Unknown" if not state else state[0]

#         # print(to_remove)
#         # print(year_range)
#         # read excel file
#         df = pd.read_excel(FILE_PATH, dtype=object, header=None)
        
#         state_population = callback_fn(df, state, to_remove, year_range=year_range)
#         return state_population
    
#     with ThreadPoolExecutor() as exe:
#         callback_fn = model_population_by_sex_race_ho_table if "sex race and ho" in by else model_population_table
#         state_populations = list(exe.map(
#             concur_model_pop_tables, 
#             populations, 
#             [cols_to_remove] * len(populations),
#             [year_range] * len(populations),
#             [callback_fn] * len(populations)
#         ))

#     state_populations_df = pd.concat(state_populations, axis=0, ignore_index=True)

#     return state_populations_df


def process_population_by_sex_age_table(df: DataFrame, state: str, cols_to_remove: list, year_range: str="2000-2009"):
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

    # remove the columns or the columns that weren't renamed
    df = df.drop(*cols_to_remove)

    df.show()

    # generate and create multi index for columns
    years = sorted(list(range(lo_year, hi_year + 1)) * 2)
    genders = ["male", "female"] * (len(years) // 2)
    multi_index_list = list(zip(years, genders))
    multi_index = MultiIndex.from_tuples(multi_index_list)
    print(multi_index)

    # what we can do instead is extract out the columns that are male and plce in separate
    # df
    # and extract out the columns that are female adn place in separate df
    # and then melt each df on each year cols

    # # create index for spark dataframe
    # increasing_col = monotonically_increasing_id()
    # window = Window.orderBy(increasing_col)

    # # returns a column object going from 0 to n
    # index_col = row_number().over(window) - 1
    # df = df.withColumn("Index", index_col)

    # # extract the index location of where the row first indicates male
    # male_start = df.where(col("Bracket") == "MALE").select("Index").collect()[0]["Index"]
    # pop_bracket_raw = df.where(col("Index").between(male_start, df.count() - 1))
    
    # # get indeces
    # female_start = pop_bracket_raw.where(col("Bracket") == "FEMALE").select("Index").collect()[0]["Index"]
    # end_indeces = pop_bracket_raw.where(col("Bracket") == ".Median age (years)").select("Index").collect()
    # male_end, female_end = end_indeces[0]["Index"], end_indeces[-1]["Index"]

    # # print(f"male start: {male_start}")
    # # print(f"female start: {female_start}")
    # # print(f"male end: {male_end}")
    # # print(f"female end: {female_end}")

    # # split the excel spreadsheet into the male and female population brackets
    # # and reset index using earlier made row_num window function 
    # pop_bracket_raw = {
    #     "Male": df.where(col("Index").between(male_start, male_end - 1)).withColumn("Index", index_col), 
    #     "Female": df.where(col("Index").between(female_start, female_end - 1)).withColumn("Index", index_col)
    # }

    # # clear previous spark dataframe from memory
    # df.unpersist()

    # # print(f"male count: {pop_bracket_raw["Male"].count()}")
    # # print(f"99999999999999 {pop_bracket_raw["Male"].select("Bracket").distinct().collect()}")
    # # print(f"female count: {pop_bracket_raw["Female"].count()}")

    # # collects population brackets of females and males
    # pop_brackets_final = []
    # for gender in ["Male", "Female"]:
    #     # Remove the following`
    #     # * column `1`, column `12`, and column `13` (the reasoning is these contain only the population estimates of april 1 and not the most recent one which is supposed to be at july 1, and that column `13` is the year 2010 which already exists in the next population years)
    #     # * rows with mostly Nan and the a dot symbol in column `1` i.e. `[. Nan Nan Nan Nan Nan ... Nan]`
    #     # * and the male column 
    #     cond = (col("Bracket") != ".") & (col("Bracket") != gender.upper())
    #     temp = pop_bracket_raw[gender].where(cond)

    #     # we remove any duplicates in the dataframe especially those with same 
    #     # age brackets
    #     temp = temp.drop_duplicates(subset=year_cols + ["Bracket"])
        
    #     # remove rows with at least 5 nan values
    #     temp = temp.dropna(thresh=5)

    #     # clean bracket column then stack the year columns
    #     # onto each other and have each value in their rows
    #     # to be the population values
    #     temp = temp.withColumn(
    #         "Bracket", 
    #         # we place square brackets to match . because if we remvoe
    #         # brackets this will mean to match anything except for a 
    #         # linebreak which is what only . does 
    #         regexp_replace(sparkLower(col("Bracket")), r"[.]", "")
    #     )
    #     # print(f"data types: {temp.dtypes}")
    #     temp = temp.melt(
    #         ids=["Bracket"],
    #         values=year_cols,
    #         variableColumnName="Year",
    #         valueColumnName="Population"
    #     )

    #     # set sex and state columns
    #     temp = temp.withColumn("Sex", lit(gender))
    #     temp = temp.withColumn("State", lit(state))

    #     # extract keyword from column
    #     keyword_col = regexp_extract(col("Bracket"), r"(under|to|and\s*over|\+)", 1)
    #     age_cases = when(
    #         keyword_col == "under",
    #         concat(
    #             array(lit(0.0)),
    #             regexp_extract_all(
    #                 col("Bracket"), 
    #                 lit(r"(\d+)"), 
    #                 1
    #             ).cast(ArrayType(FloatType()))
    #         )
    #     )\
    #     .when(
    #         keyword_col == "to",
    #         regexp_extract_all(
    #             col("Bracket"), 
    #             lit(r"(\d+)"), 
    #             1
    #         ).cast(ArrayType(FloatType()))
    #     )\
    #     .when(
    #         keyword_col == "and over",
    #         concat(
    #             regexp_extract_all(
    #                 col("Bracket"), 
    #                 lit(r"(\d+)"), 
    #                 1
    #             ).cast(ArrayType(FloatType())),
    #             array(lit(float("inf"))),
    #         )
    #     )\
    #     .otherwise(
    #         concat(
    #             # [1] or [85] or [2] these imply that the
    #             # bracket column was only a single number and had
    #             # no substrings like "to", "under", "+", and "and over" 
    #             regexp_extract_all(
    #                 col("Bracket"), 
    #                 lit(r"(\d+)"), 
    #                 1
    #             ).cast(ArrayType(FloatType())),
    #             # [NULL]
    #             array(lit(None))
    #         )
    #     )
        
    #     # create AgeStart and AgeEnd columns
    #     temp = temp.withColumn("AgeStart", age_cases[0]) \
    #     .withColumn("AgeEnd", age_cases[1])

    #     # remove underscore in year values and cast to int
    #     temp = temp.withColumn("Year", regexp_replace(col("Bracket"), r"[_]", "").cast(IntegerType()))
    #     temp.show()

    #     # append to pop_brackets_list for later concatenation
    #     pop_brackets_final.append(temp)

    # # clear temp dataframes from memory
    # pop_bracket_raw["Male"].unpersist()
    # pop_bracket_raw["Female"].unpersist()

    # # concatenate final population brackets using
    # # unionByName as callback
    # final = reduce(DataFrame.unionByName, pop_brackets_final)
    # final.show(580)

    # return final

if __name__ == "__main__":
    DATA_DIR = './data/population-data'
    EXCLUSIONS = ["us_populations_per_state_2001_to_2021.csv"]
    files = list(filter(lambda file: not file in EXCLUSIONS, os.listdir(DATA_DIR)))
    populations_by_sex_age_00_10 = list(filter(lambda file: "2000-2010" in file and "by_sex_and_age" in file, files))
    populations_by_sex_race_ho_00_10 = list(filter(lambda file: "2000-2010" in file and "by_sex_race_and_ho" in file, files))
    populations_by_sex_age_10_19 = list(filter(lambda file: "2010-2019" in file and "by_sex_and_age" in file, files))
    populations_by_sex_race_ho_10_19 = list(filter(lambda file: "2010-2019" in file and "by_sex_race_and_ho" in file, files))
    populations_by_sex_age_20_23 = list(filter(lambda file: "2020-2023" in file and "by_sex_and_age" in file, files))
    populations_by_sex_race_ho_20_23 = list(filter(lambda file: "2020-2023" in file and "by_sex_race_and_ho" in file, files))

    year_range = "2010-2019"
    state = "Alabama"

    # .\data\population-data\Alabama_pop_by_sex_and_age_2000-2010.xls
    path = os.path.join(DATA_DIR, "Alabama_pop_by_sex_and_age_2010-2019.xlsx")
    # path = os.path.join(DATA_DIR, "U.S._Chronic_Disease_Indicators__CDI___2023_Release.csv")

    spark = SparkSession.builder.appName('test')\
        .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:3.5.1_0.20.4")\
        .getOrCreate()

    df = spark.read.format("com.crealytics.spark.excel")\
        .option("header", "false")\
        .option("inferSchema", "true")\
        .load(path)

    # 2010 - 2019
    cols_to_remove = [1, 2, 3, 4, 5, 6] + list(range(7, len(df.columns), 3))
    process_population_by_sex_age_table(df, state, cols_to_remove=cols_to_remove, year_range=year_range)


    # start_index = df[df[0] == ".0"].index.to_list()[0]
    # end_index = df[df[0] == ".Median Age (years)"].index.to_list()[0]

    # # extract necessary rows
    # pop_brackets_raw = df.iloc[start_index: end_index]
    
    # # remove duplicatess
    # temp = pop_brackets_raw.drop_duplicates()

    