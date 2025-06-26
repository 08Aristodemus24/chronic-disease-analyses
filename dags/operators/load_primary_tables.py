# Now that all necessary data have now been extracted, 
# transformed, dumped in a lake (AWS S3) as parquet files,
# we can then load this in an in-process OLAP DB like duckdb
# /motherduck 
import duckdb
import os
from dotenv import load_dotenv
from pathlib import Path

if __name__ == "__main__":
    # Load AWS credentials in order for duck db to read parquet files in s3 bucket 

    # # Build paths inside the project like this: BASE_DIR / 'subdir'.
    # # use this only in development
    # print("loading env variables...")
    # env_dir = Path('./').resolve()
    # load_dotenv(os.path.join(env_dir, '.env'))
    # print("env variables loaded.\n")

    # jdbc:duckdb:md:chronic_disease_analyses_db
    # duckdb:///md:chronic_disease_analyses_db
    print("connecting to duckdb...")
    conn = duckdb.connect(f"md:chronic_disease_analyses_db?motherduck_token={os.environ['MOTHERDUCK_TOKEN']}")
    # conn = duckdb.connect("chronic_disease_analyses_db.db")
    print("connected to duckdb.\n")

    # load env vars
    credentials = {
        "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
        "region_name": os.environ["AWS_REGION_NAME"],
    }

    # installing dependencies and creating secrets object
    conn.sql(f"""INSTALL httpfs""")
    conn.sql(f"""LOAD httpfs""")
    conn.sql(f"""
        CREATE OR REPLACE SECRET (
            TYPE s3,
            KEY_ID '{credentials["aws_access_key_id"]}',
            SECRET '{credentials["aws_secret_access_key"]}',
            REGION '{credentials["region_name"]}',
            ENDPOINT 's3.{credentials["region_name"]}.amazonaws.com'
        );
    """)


    # loading CDI fact table
    cdi_url = "s3://chronic-disease-analyses-bucket/cdi-data-transformed/CDI.parquet/*.parquet"
    query = f"""
        CREATE OR REPLACE TABLE CDI AS
        SELECT *
        FROM read_parquet('{cdi_url}', union_by_name=True, filename=False)
    """
    conn.sql(query)

    conn.sql("""
        SELECT * FROM CDI
    """)

    # loading Population fact table
    us_population_file_names = [
        "s3://chronic-disease-analyses-bucket/population-data-transformed/Population_2000_2009.parquet/*.parquet",
        "s3://chronic-disease-analyses-bucket/population-data-transformed/Population_2010_2019.parquet/*.parquet",
        "s3://chronic-disease-analyses-bucket/population-data-transformed/Population_2020_2023.parquet/*.parquet",
    ]
    query = f"""
        CREATE OR REPLACE TABLE Population AS
        SELECT *
        FROM read_parquet({us_population_file_names}, union_by_name=True, filename=False)
    """
    conn.sql(query)

    conn.sql("""
        SELECT * FROM Population
    """)


    # Loading CDI dimension tables
    #### location table
    cdi_location_url = "s3://chronic-disease-analyses-bucket/cdi-data-transformed/Location.parquet/*.parquet"
    cdi_location_url

    # note that if we only specify a string instead of a list in read_parquet it must be enclosed in a quote or double quotes
    query = f"""
        CREATE OR REPLACE TABLE CDILocation AS
        SELECT *
        FROM read_parquet('{cdi_location_url}', union_by_name=True, filename=False)
    """

    conn.sql(query)

    conn.sql("""
        SELECT *
        FROM CDILocation
    """)


    #### Stratification table
    cdi_stratification_url = "s3://chronic-disease-analyses-bucket/cdi-data-transformed/Stratification.parquet/*.parquet"
    query = f"""
        CREATE OR REPLACE TABLE CDIStratification AS
        SELECT *
        FROM read_parquet('{cdi_stratification_url}', union_by_name=True, filename=False)
    """
    conn.sql(query)

    conn.sql("""
        SELECT *
        FROM CDIStratification
    """)

    #### Question table
    question_url = "s3://chronic-disease-analyses-bucket/cdi-data-transformed/Question.parquet/*.parquet"
    query = f"""
        CREATE OR REPLACE TABLE Question AS
        SELECT *
        FROM read_parquet('{question_url}', union_by_name=True, filename=False)
    """
    conn.sql(query)

    #### Topic table
    topic_url = "s3://chronic-disease-analyses-bucket/cdi-data-transformed/Topic.parquet/*.parquet"
    query = f"""
        CREATE OR REPLACE TABLE Topic AS
        SELECT *
        FROM read_parquet('{topic_url}', union_by_name=True, filename=False)
    """
    conn.sql(query)

    #### DataValueType table
    data_value_type_url = "s3://chronic-disease-analyses-bucket/cdi-data-transformed/DataValueType.parquet/*.parquet"
    query = f"""
        CREATE OR REPLACE TABLE DataValueType AS
        SELECT *
        FROM read_parquet('{data_value_type_url}', union_by_name=True, filename=False)
    """
    conn.sql(query)

    conn.sql("""
        SELECT *
        FROM DataValueType
    """)


    # Loading Population dimension tables
    #### State table 
    population_state_url = "s3://chronic-disease-analyses-bucket/population-data-transformed/State.parquet/*.parquet"
    query = f"""
        CREATE OR REPLACE TABLE PopulationState AS
        SELECT *
        FROM read_parquet('{population_state_url}', union_by_name=True, filename=False)
    """
    conn.sql(query)

    conn.sql("""
        SELECT *
        FROM PopulationState
    """)


    #### Stratification table
    population_stratification_url = "s3://chronic-disease-analyses-bucket/population-data-transformed/Stratification.parquet/*.parquet"
    query = f"""
        CREATE OR REPLACE TABLE PopulationStratification AS
        SELECT *
        FROM read_parquet('{population_stratification_url}', union_by_name=True, filename=False)
    """
    conn.sql(query)

    conn.sql(f"""
        SELECT *
        FROM PopulationStratification
    """)
    
    # close connection
    conn.close()
