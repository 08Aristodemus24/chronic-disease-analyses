import duckdb
import os
import time

from dotenv import load_dotenv
from pathlib import Path

def update_calculated_population(conn):
    """
    
    """

    query = """
        -- no condition needed to update rows as we are updating all 
        -- rows from blank to the population values
        UPDATE CDI
        SET Population = CalculatedPopulation.Population
        FROM (
            -- Creates a CTE that will join the necessary
            -- values from the dimension tables to the fact
            -- table
            WITH MergedCDI AS (
                SELECT
                    c.LogID,
                    c.DataValueUnit,
                    c.DataValue,
                    c.YearStart, 
                    c.YearEnd,
                    cl.LocationID,
                    cl.LocationDesc, 
                    q.QuestionID,
                    q.AgeStart,
                    q.AgeEnd,
                    dvt.DataValueTypeID,
                    dvt.DataValueType,
                    s.StratificationID,
                    s.Sex,
                    s.Ethnicity,
                    s.Origin
                FROM CDI c
                LEFT JOIN CDILocation cl
                ON c.LocationID = cl.LocationID
                LEFT JOIN Question q
                ON c.QuestionID = q.QuestionID
                LEFT JOIN DataValueType dvt
                ON c.DataValueTypeID = dvt.DataValueTypeID
                LEFT JOIN Stratification s
                ON c.StratificationID = s.StratificationID
            ),
            
            -- joins necessary values to Population table 
            -- via primary keys of its dimension tables
            MergedPopulation AS (
                SELECT
                    ps.StateID,
                    ps.State,
                    p.Age,
                    p.Year,
                    s.Sex,
                    s.Ethnicity,
                    s.Origin,
                    p.Population
                FROM Population p
                LEFT JOIN PopulationState ps
                ON p.StateID = ps.StateID
                LEFT JOIN Stratification s
                ON p.StratificationID = s.StratificationID
            ),
            
            -- performs an inner join on both CDI and Population
            -- tables based
            CDIWithPop AS (
                SELECT 
                    mcdi.LogID AS LogID,
                    -- mcdi.DataValueUnit AS DataValueUnit,
                    -- mcdi.DataValue AS DataValue,
                    -- mcdi.YearStart AS YearStart, 
                    -- mcdi.YearEnd AS YearEnd,
                    -- mcdi.LocationID AS LocationID,
                    -- mcdi.LocationDesc AS LocationDesc, 
                    -- mcdi.QuestionID as QuestionID,
                    -- mcdi.AgeStart AS AgeStart,
                    -- mcdi.AgeEnd AS AgeEnd,
                    -- mcdi.DataValueTypeID AS DataValueTypeID,
                    -- mcdi.DataValueType AS DataValueType,
                    -- mcdi.StratificationID AS StratificationID,
                    -- mcdi.Sex AS Sex,
                    -- mcdi.Ethnicity AS Ethnicity,
                    -- mcdi.Origin AS Origin,
                
                    SUM(mp.Population) AS Population
                    -- mp.State PState,
                    -- mp.Age AS PAge,
                    -- mp.Year AS PYear,
                    -- mp.Sex AS PSex,
                    -- mp.Ethnicity AS PEthnicity,
                    -- mp.Origin AS POrigin
                FROM MergedPopulation mp
                INNER JOIN MergedCDI mcdi
                ON (mp.Year BETWEEN mcdi.YearStart AND mcdi.YearEnd) AND
                (mp.StateID = mcdi.LocationID) AND
                ((mp.Age BETWEEN mcdi.AgeStart AND (CASE WHEN mcdi.AgeEnd = 'infinity' THEN 85 ELSE mcdi.AgeEnd END)) OR (mcdi.AgeStart IS NULL AND mcdi.AgeEnd IS NULL)) AND
                (mp.Sex = mcdi.Sex OR mcdi.Sex = 'Both') AND
                (mp.Ethnicity = mcdi.Ethnicity OR mcdi.Ethnicity = 'All') AND
                (mp.Origin = mcdi.Origin OR mcdi.Origin = 'Both')
                GROUP BY LogID
                ORDER BY LogID ASC
            )

            -- return all columns from resulting CTE in
            -- subquery
            SELECT *
            FROM CDIWithPop
        ) AS CalculatedPopulation
        WHERE CDI.LogID = CalculatedPopulation.LogID
    """

    # %, Prevalence
    # %, Percent
    # %, Age-adjusted Prevalence

    conn.sql(query)

def add_calculated_population(conn):
    """
    adds a blank column for CDI table to be later populated
    during update query
    """

    conn.sql("""
        -- adds a new column to the cdi table to be populated
        -- later on with values from update query
        ALTER TABLE CDI
        ADD COLUMN IF NOT EXISTS Population BIGINT
    """)
    

def create_stratification(conn):
    """
    unionize stratification tables from both created tables
    and create new table from it
    """
         
    # SELECT * FROM Stratification
    # GROUP BY ALL
    query = """
        CREATE OR REPLACE TABLE Stratification AS (
            SELECT * FROM CDIStratification
            UNION BY NAME
            SELECT * FROM PopulationStratification
        )
    """
    conn.sql(query)


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
    
    # create stratification table
    print("Creating Stratification table...")
    create_stratification(conn)
    print("Created Stratification table.\n")

    # give 5 second delay
    print("running next query in 5 seconds\n")
    time.sleep(5)

    # add population column to cdi table to be imputed
    # later with population values during update
    print("Adding column to CDI table...")
    add_calculated_population(conn)
    print("Added column to CDI table.\n")

    # give 5 second delay
    print("running next query in 5 seconds\n")
    time.sleep(5)

    # calculated population column depends on stratification
    # table being created and a column being added to CDI
    # this is why the create_stratification and 
    # add_calculated_population is ran first
    print("Updating column in CDI table...")
    update_calculated_population(conn)
    print("Updated column in CDI table.")