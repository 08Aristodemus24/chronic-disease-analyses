import os
import re

import  pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window


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

    test_spark_df_00_10.select("_c0", "_c1").show()

    # create index for spark dataframe
    increasing_col = f.monotonically_increasing_id()
    window = Window.orderBy(increasing_col)
    test_spark_df_00_10 = test_spark_df_00_10.withColumn("index", f.row_number().over(window) - 1)

    # extract the index location of where the row first indicates male
    male_start = test_spark_df_00_10.filter(f.col("_c0") == "MALE").select("index") 
    print(male_start.collect())
    male_start.show()


