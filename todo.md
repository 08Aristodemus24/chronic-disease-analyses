* <s>goal is to read data first each excel spreadsheet and csv</s>
* <s>read and transform in pandas first then do it in pyspark</s>
* identify which dataframes have common columns and join them using sql statements
* once data is joined we do exploratory data analysis and feature engineering
* use box plot to see interquartile ranges (get it from data-mining-hw repo)

# To implement as much as possible
## data extraction
* <s>with population data download csv's directly to local file system to bypass census.gov security </s>
* <s>use selenium to automate download of us populations per state by sex age race ho csv's from `www.census.gov` </s>

## data transformation
* <s>cdi fact table still contains id's that need to be dropped and retained at a separate dimension table as part of normalization process for later loading to warehouse </s>
* <s>break down us_populations_by_sex_age_race_ho table into fact table and dimension table by dropping the id's that are contained in this fact table and then retaining it in the dimension table as part of normalization process for later loading to warehouse </s>
* <s>union the stratification dimension table from cdi and us populations per state by sex age race ho tables</s>
* find some way to unionize the dimension tables from each year produced by `normalize_population_per_state_by_sex_age_race_ho()` function except the `population_per_state_by_sex_age_race_ho` fact tables
* <s>clean and transform cdi data using pyspark</s>
* <s>we use pyspark for preprocessing the data to make sql queries</s>
* <s>with CDI data download zip file to local file system then delete</s>
* <s>once normalization stage of cdi table is finished setup another bucket and bucket folder again to save these normalized tables, this goes the same for population fact tables</s>
* <s>draw diagram of raw cdi to first stage cdi to its normalized tables, this goes also for population data</s>
* <s>because there aren't any populations for the 'Other' ethnicity we will have to figure out some way how to create dummy data for this ethnicity, maybe imputation through averaging by, sex, origin, not ethnicity, state, and age so that male, hispanic, alabama, with ages 0 can have its population be averaged adn then divided by 10 to get a fraction of this average ppoulation which can be used for our population value for the other ethnicity</s>
* <s>create the final calculated population based on data value type of CDI</s>
* filter cdi table by each unique topic and under each unique topic see the unique questions, since these questions will resemble eaech other, figure out to add another transformation to generalize these questions 
* and figiure out the questions that only really use the staet population by sex age ethnicity origin and only then will we calculate their tangible number of cases since other questions like adults with medicare aged 30+ isn't really tailored for hte populations we have or number of alcohol consumption is 3.6 since it only really focuses on the sex, age, race, origin of a demographic and not whether they have medicare etc.
* what i'm thinking of in the future is if this is the case we must visualize it in powerbi like this
```
|- topic1
    |- question1
    |- question2
    |- ...
    |- questionn
|- topic2
    |- question1
    |- question2
    |- ...
    |- questionn
|- ...
|- topic n
    |- question1
    |- question2
    |- ...
    |- questionn
```
and user would be able to view each topic and see what kinds of information or value each question holds in each us state

# Data loading
* <s>load the parquet files to snowflake or motherduck (for free trials and free tier). For motherduck load the s3 parquets into duckdb and then download a duckdb connector for powerbi in order to connect to this OLAP datawarehouse.</s>
* <s>load s3 parquets into duckdb local or remote database (motherduck)</s>
* <s>load the data already in OLAP DB like motherduck/duckdb or snowflake into powerbi</s>

# Data analysis
* <s>create relationships to loaded tables</s>\
* <s>learn how to unionize the `PopulationStratification` and `CDIStratification` dimension tables in powerbi as we want the tables already loaded to be as customizable as possible for lack of a better word as doing this unionization during transformation would not allow potential users of the tables flexibility not unlike the pure unadulterated version of the tables where its transformation only involved the necessary ones and it allowed still users to make further transformations if they wanted only now in PowerBI itself.</s>
* <s>introduce the big guns and learn to aggregate the Population table based on DataValueType, Sex, Ethnicity, Origin, AgeStart, and AgeEnd (if any) columns found in the DataValueType, CDIStratification, and Question dimension tables</s>

e.g. cancer among youth where AgeStart is 18, and AgeEnd is 24, where stratification is male, all, and hispanic. Now I need to do a calculation based on these values maybe a case when in SQL and and aggregate the Population table based on these values. Now I use the calculated population and do another calculation based on the DataValueType to maybe combine or do an operation with the DataValue column in the CDI table itself.

* Okay, with this sample of your CDI table, we can start brainstorming some interesting questions you can ask using SQL to analyze this healthcare data and potentially draw insights about these chronic disease indicators (in this case, "Alcohol use - Binge drinking prevalence among adults aged 18-24 years").

<s>1. What is the prevalence of alcohol use among youth (male and female) in listed year ranges?</s>

2. What is the average binge drinking prevalence among adults aged >= 18?

3. What was the binge drinking prevalence in each state in 2015?

4. What is the average binge drinking prevalence for each reported ethnicity?



* Thanks to power bi, we can group what important chronic disease indicators are:
- alcohol use among youth, alcohol use during pregnancy 
- because there are different questions like these datavaluetype may change, in this case the datavaluetype here implies an amount of measurement since amount of alcohol consumed is measured, and not for instance those who have alcoholic diseases e.g. chronic liver disease mortality
- try to analyze alcohol topic first
- how tangible nubmer of per capita alcohol consumption and binge drinkin frequency etc. can be calculated and what are thier corresponding datavaluetypes and datavalueunits respectively

* alternatively instead of creating the joined Population data and then calculating further the sum through aggregation of LogID in the CDI tables, in sql, we can do this in DAX, we just have to rewrite this query:
```
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
        mcdi.DataValueUnit AS DataValueUnit,
        mcdi.DataValue AS DataValue,
        mcdi.YearStart AS YearStart, 
        mcdi.YearEnd AS YearEnd,
        mcdi.LocationID AS LocationID,
        mcdi.LocationDesc AS LocationDesc, 
        mcdi.QuestionID as QuestionID,
        mcdi.AgeStart AS AgeStart,
        mcdi.AgeEnd AS AgeEnd,
        mcdi.DataValueTypeID AS DataValueTypeID,
        mcdi.DataValueType AS DataValueType,
        mcdi.StratificationID AS StratificationID,
        mcdi.Sex AS Sex,
        mcdi.Ethnicity AS Ethnicity,
        mcdi.Origin AS Origin,
    
        mp.Population,
        mp.State PState,
        mp.Age AS PAge,
        mp.Year AS PYear,
        mp.Sex AS PSex,
        mp.Ethnicity AS PEthnicity,
        mp.Origin AS POrigin
    FROM MergedPopulation mp
    INNER JOIN MergedCDI mcdi
    ON (mp.Year BETWEEN mcdi.YearStart AND mcdi.YearEnd) AND
    (mp.StateID = mcdi.LocationID) AND
    ((mp.Age BETWEEN mcdi.AgeStart AND (CASE WHEN mcdi.AgeEnd = 'infinity' THEN 85 ELSE mcdi.AgeEnd END)) OR (mcdi.AgeStart IS NULL AND mcdi.AgeEnd IS NULL)) AND
    (mp.Sex = mcdi.Sex OR mcdi.Sex = 'Both') AND
    (mp.Ethnicity = mcdi.Ethnicity OR mcdi.Ethnicity = 'All') AND
    (mp.Origin = mcdi.Origin OR mcdi.Origin = 'Both')
)

-- aggregate final time based on LogID as this will be duplicated
-- during prior join process so might as well join here rather than
-- using state_id, yearstart, yearend, agestart, ageend sex, ethnicity
-- and origin columns
SELECT 
    LogID,
    SUM(Population) AS Population
FROM CDIWithPop
GROUP BY LogID
ORDER BY LogID ASC
```

# to address in the future:
* there may be potential for error in creating buckets from extraction scripts like `extract_cdi.py`, `extract_us_population_per_state_by_sex_age_race_ho.py` and `extract_us_population_per_state.py`, because if we try to run these simultaneously or concurrently like in airflow it might result in conflicts, so separate creation of `cdi-data-raw`, `population-data-raw`, `population-data-transformed`, and `cdi-data-transformed` folders
* use selenium, docker, and airflow to automate extraction process and then use pyspark and databricks to transform extracted data and load the final data into a warehouse like databricks. All of this is orchestrated using airflow. 
* document everything
- from automating the extraction of data
- your thought process of loading the raw data locally
- your thought process of getting extra population data 
- how you noticed sex and age or sex race origin wasn't enough and that it needed to be sex age race origin and why you needed this extra data
- how you structured the tables first using spark for easier analytical queries by joins and group bys
- thought process behind each transformation step
- learning to finally implement read and write operations using spark to aws s3
- the process of normalization for faster querying
- weaving every process of extraction and transformation and loading using orchestration tools like airflow 