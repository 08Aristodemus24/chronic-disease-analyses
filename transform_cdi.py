import os

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


def transform(df: DataFrame):
    # Drop uneccessary columns
    cols_to_drop = ["Response",
        "ResponseID",
        "DataValueFootnoteSymbol",
        "DatavalueFootnote",
        
        "DataSource", 
        "DataValue",

        "StratificationCategory2",
        "Stratification2",
        "StratificationCategory3",
        "Stratification3",

        "StratificationCategoryID2",
        "StratificationID2",
        "StratificationCategoryID3",
        "StratificationID3"]
    df = df.drop(*cols_to_drop)

    # drop duplicates
    df = df.dropDuplicates()

    # Remove rows with null values either in datavalue, datavalueunit, 
    # and datavaluetype means thhat if datavalueunit or datavalue or 
    # datavaluetype is null then return true and negate it
    data_val_cond = ~(isnull("DataValueUnit") | isnull("DataValue") | isnull("DataValueType"))
    df = df.where(data_val_cond)

    # remvoe rows with location desc set to Guam, Puerto Rico, United States, and Virgin Islands
    loc_cond = ~(col("LocationDesc").isin("Guam", "Puerto Rico", "United States", "Virgin Islands"))
    df = df.where(loc_cond)

    # Extract latitude and longitude from geolocation
    df = df.withColumn("GeoLocation", regexp_extract_all(col("GeoLocation"), lit(r"(-*\d+.\d+)"), 1))
    
    # Cast latitude and longitude str columns to doubles
    df = df.withColumn("Latitude", df.GeoLocation[0].cast(DoubleType()))
    df = df.withColumn("Longitude", df.GeoLocation[1].cast(DoubleType()))

    # Delete GeoLocation after extracting latitude and longitude as this is redundant
    df = df.drop("GeoLocation")

    # rename datavaluealt column (which is already a double) to just datavalue 
    df = df.withColumnRenamed("DataValueAlt", "DataValue")
    df = df.withColumnRenamed("StratificationID1", "Stratification1ID")
    df = df.withColumnRenamed("StratificationCategoryID1", "StratificationCategory1ID")

    # Replace `per 100,000` and `per 100,000 residents` with
    # `cases per 100,000` instead to reduce redundancy
    dvu_cases = when(col("DataValueUnit") == "per 100,000", "cases per 100,000")\
    .when(col("DataValueUnit") == "per 100,000 residents", "cases per 100,000")\
    .otherwise(col("DataValueUnit"))
    df = df.withColumn("DataValueUnit", dvu_cases)
    
    # Extract out the age brackets in each question if there are any
    # however other strings that might imply an age bracket would be the
    # youth which is defined by United Nations—without prejudice to any
    # other definitions made by Member States, as the persons between the
    # ages of 18 and 24 years. Others like high school students may 
    # according to data from US range from ages 14 to 18, middle school 
    # is 11 to 13, and elementary school is 5 to 10
    pattern = r"(aged\s*[><=]*\s*\d*-*\d*\s*years|youth|high school student|middle school student|elementary student)"

    # when our column extracted by f.regexp_extract() has already been built 
    # where we have 
    # Row(AgeBracket='aged >= 45 years'),
    #  Row(AgeBracket='aged 50-75 years'),
    #  Row(AgeBracket='aged 18-24 years'),
    #  Row(AgeBracket='aged >= 18 years'),
    #  Row(AgeBracket='aged >= 65 years'),
    #  Row(AgeBracket='aged 21-44 years'),
    #  Row(AgeBracket='aged 50-64 years'),
    #  Row(AgeBracket='youth'),
    #  Row(AgeBracket='aged 18-44 years'),
    #  Row(AgeBracket='high school student'),
    #  Row(AgeBracket='aged 21-65 years'),
    #  Row(AgeBracket='aged >= 14 years'),
    #  Row(AgeBracket='aged 45-64 years'),
    #  Row(AgeBracket='aged 18-64 years'),
    #  Row(AgeBracket='aged 50-74 years'),
    #  Row(AgeBracket='aged 1-17 years')

    age_info_col = regexp_extract(col("Question"), pattern, 1)
    cases = when(
            regexp(
                age_info_col,
                # this is where we check if a column has >=, <=, >, <, 
                # then we return whatever number is in this as a list
                # and then cast this list of matched string numbers to
                # a list of int numbers  
                lit(r"([><=]+(?=\s*\d+))")
            ),
            # returns a list of all number strings 
            concat(
                regexp_extract_all(
                    age_info_col,
                    lit(r"(\d+)"), 
                    1
                ).cast(ArrayType(FloatType())),
                array(lit(float("inf")))
            )
        ).when(
            regexp(
                age_info_col,
                # this is to match a hyphen that occurs once or twice
                # if a digit/s (?>=\d+) precedes it and succeeds it (?=\d+)
                lit(r"((?<=\d+)-+(?=\d+))")
            ),
            regexp_extract_all(
                age_info_col, 
                lit(r"(\d+)"), 
                1
            ).cast(ArrayType(FloatType()))
        ).when(
            regexp(
                age_info_col,
                lit(r"(youth)")
            ),
            # when youth is detected in the age info column we
            # regexp will return true and when a row is true we return
            # in this case an array of literal/constant float values of 
            # 18 and 24 as these are the age ranges of this group 
            array(lit(float(18)), lit(float(24)))
        ).when(
            regexp(
                age_info_col,
                lit(r"(high\s*school student)")
            ),
            # and lastly in a case where no age bracket numbers or youth
            # keywords are detected it is assumed that this group is highschool
            # which has an age range of 14 to 18 
            array(lit(float(14)), lit(float(18)))
        ).otherwise(
            # if there is no age ranges, arithmetic operators, or groups
            # implying age range then question has no age range to extract
            # so return null values instead
            array(lit(None), lit(None))
        )

    # create a dataframe with column AgeBracket and 
    # select only the AgeBracket column
    df = df.withColumn(
        "AgeStart",
        # expr
        cases[0]
        # arith
    ).withColumn("AgeEnd", cases[1])
    
    # # lastly for the stratification column we will have the 
    # following unique values like `Male`, `Overall`, `Female`, 
    # `Asian or Pacific Islander`, `White, non-Hispanic`, 
    # `Hispanic`, `American Indian or Alaska Native`, `Black, non-Hispanic`, 
    # `Asian, non-Hispanic`, `Other, non-Hispanic`, `Multiracial, non-Hispanic` 
    # which we will need to separate further into sex, ethnicity, and origin e.g. 
    # 
    # A Stratification of `Male` may imply that overall the origin may 
    # encompass both hispanic and non hispanic. A Stratitication of `Female` 
    # may imply that overall the origin may encompass both hispanic and non hispanic 
    # ```
    # | Sex | Race | Origin |
    # | Male | All Races | Both Hispanic and non hispanic |
    # | Female | All Races | Both Hispanic and non hispanic |
    # ```
    # 
    # A Stratification of `Overall` may imply that it encompasses both male 
    # and female gender of a state of any race of both hispanic and non 
    # hispanic origin
    # ```
    # | Sex | Race | Origin |
    # | Both genders | All Races | Both Hispanic and non hispanic |
    # ```
    # 
    # A Stratification of `hispanic` may imply
    # ```
    # | Sex | Race | Origin |
    # | Both Genders | All races | Hispanic
    # ```
    # 
    # A Stratification of `Asian or Pacific Islander`, `American Indian or
    # Alaska Native` may imply non hispanic since if it was hispanic it 
    # would've been indicated explicitly
    # ```
    # | Sex | Race | Origin |
    # | Both genders | Asian or Pacific Islander | non hispanic 
    # | Both genders | American Indian or Alaska Native | non Hispanic |
    # ```
    # 
    # A Stratification of `white, non hispanic`, `black, non hispanic`, 
    # `asian, non hispanic`, `other, non-hispanic`, `multiracial, non-hispanic` 
    # may imply both genders and must be split into
    # ```
    # | Sex | Race | Origin |
    # | Both genders | White | Non hispanic |
    # | Both genders | Black | non Hispanic |
    # | Both genders | White | non Hispanic |
    # | Both genders | Asian | non Hispanic |
    # | Both genders | Other | non Hispanic |
    # | Both genders | Multiracial | non hispanic |
    # ```
    # 
    # Other stratifications not in the dataset but might be in the future might
    # include `white, hispanic`, `black, hispanic`, `asian, hispanic`, 
    # `other, hispanic`, `multiracial, hispanic`, `asian or pacific islander, hispanic`, 
    # `ameriacn indian or alaska native, hispanic` which again since gender is 
    # not mentioned may imply both genders and must be split into
    # ```
    # | Sex | Race | Origin |
    # | Both genders | White | hispanic |
    # | Both genders | Black | Hispanic |
    # | Both genders | Asian | Hispanic |
    # | Both genders | Other | Hispanic |
    # | Both genders | Multiracial | hispanic |
    # | Both genders | AIAN | Hispanic |
    # | Both genders | NHPI | Hispanic
    # ```
    # 
    # note we can just convert asian or pacific islander into native 
    # hawaiian or pacific islander (NHPI) in order to match the 
    # population table with native hawaiian or pacific islander since 
    # it doesn't have exactly asian or pacific islander on it and so 
    # we use a closer one instead like NHPI. Again this is breaking 
    # rules but again we don't need to be perfect. 
    # 
    # All in all the unique values for `Sex` would be `male`, `female`, 
    # or `both`, for `Ethnicity` `white`, `black`, `asian`, `other`, 
    # `multiracial`, `AIAN`, `NHPI`, or `all`, and for `Origin` either 
    # `Hispanic`, `Non Hispanic`, or `both` (not to imply though that
    # a person can be both but that a population may have both non 
    # hispanic or hispanic individuals)

    # so all in all the condition would be the ff.
    strat_col = split(sparkLower(col("Stratification1")), ",")
    strat_cases = when(
            strat_col[0] == "overall",
            # | Sex | Ethnicity | Origin |
            # | Both | All | Both |
            array(lit("Both"), lit("All"), lit("Both"))
        ).when(
            strat_col[0] == "hispanic",
            # | Sex | Ethnicity | Origin |
            # | Both | All | Hispanic |
            array(lit("Both"), lit("All"), lit("Hispanic"))
        ).when(
            strat_col[0] == "american indian or alaska native",
            # | Sex | Ethnicity | Origin |
            # | Both | AIAN | Non Hispanic |
            array(lit("Both"), lit("AIAN"), lit("Not Hispanic"))
        ).when(
            strat_col[0] == "asian or pacific islander",
            # | Sex | Ethnicity | Origin |
            # | Both | NHPI | Non Hispanic |
            array(lit("Both"), lit("NHPI"), lit("Not Hispanic"))
        ).when(
            rlike(strat_col[0], lit(r"(asian|black|white|other|multiracial)")),
            # | Sex | Ethnicity | Origin |
            # | Both | white | Non Hispanic |
            # | Both | black | Non Hispanic |
            # | Both | asian | Non Hispanic |
            # | Both | other | Non Hispanic |
            # | Both | multiracial | Non Hispanic |
            array(lit("Both"), initcap(strat_col[0]), lit("Not Hispanic"))
        ).when(
            rlike(strat_col[0], lit(r"(male|female)")),
            # | Sex | Ethnicity | Origin |
            # | Male | All | Both |
            # | Female | All | Both |
            array(initcap(strat_col[0]), lit("All"), lit("Both"))
        )

    df = df.withColumn("Sex", strat_cases[0])\
    .withColumn("Ethnicity", strat_cases[1])\
    .withColumn("Origin", strat_cases[2])   

    df = df.drop("Stratification1")
    df.show()

    # clear dataframe from memory
    df.unpersist()

    return df


if __name__ == "__main__":
    DATA_DIR = "./data/cdi-data-raw"
    path = os.path.join(DATA_DIR, "U.S._Chronic_Disease_Indicators__CDI___2023_Release.csv")

    spark = SparkSession.builder.appName('test')\
        .config("spark.driver.memory", "6g")\
        .config("spark.sql.execution.arrow.maxRecordsPerBatch","100")\
        .getOrCreate()

    cdi_df = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load(path)
    
    # commence transformation
    final = transform(cdi_df)

    # create output directory 
    OUTPUT_DATA_DIR = "./data/cdi-data-transformed"
    os.makedirs(OUTPUT_DATA_DIR, exist_ok=True)

    FILE_NAME = f"cdi.parquet"
    OUTPUT_FILE_PATH = os.path.join(OUTPUT_DATA_DIR, FILE_NAME)
    final.write.parquet(OUTPUT_FILE_PATH, mode="overwrite")