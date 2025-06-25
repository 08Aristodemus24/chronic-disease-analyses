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

* there are other binge drinking prevalence from other demographics that I need to somehow analyze
- binge drinking among women aged 18-44 years   
- binge drinking prevalencec among youth
- heavy drinking among adults aged >= 18 years
- heavy drinking among women aged 18-44 years

what we can do is generalize the slicer to not only the binge drinking among adults aged >= 18 years but also to those women aged 18-44 years, and the youth, making sure that when either these 3 are selected the title also changes and whatever ethnicity or stratification we pick the line graphs also reflect that e.g. when B_M_ALL is chosen show this line graph, when NH_B_WHITE, and NH_B_BLACK is chosen show the different lines and also reflect in the title



1. what are the most and least common chronic disease indicator categories based on average population?
what I want to do is instaed of using the same CDI table I want to make a calculated table for each graph namely the top 5 most common categories of chronic disease indicators in the US in all states and stratifications, years, questions, and data value types

the most common were those relating to reproductive health, oral health, arthritis, diabetes, and immunization
the least common were those relating to chronic kidney disease, cancer, older adults, disability, mental heatlh

2. in the 5 most and least common chronic disease indicator categories which question has the most avergae and total occurences? E.g. 
3. in these questions what are the top and bottom 5 states 
4. in these questions what is the trend across all years in the top and bottom 5 states

5. What is the prevalence of alcohol use among youth (male and female) in listed year ranges?
6. What is the average binge drinking prevalence among adults aged >= 18?
7. What is the average binge drinking prevalence for each reported ethnicity?

* because there are 10 years worth of data and 51 states for each of those 10 years we can use a slicer again to filter by state

# Data orchestration
1. download `docker-compose.yaml` by `curl -LfO "https://airflow.apache.org/docs/apache-airflow/3.0.2/docker-compose.yaml"`

preqrequisites:
- I will probably need selenium for data extraction part
- java 8
- java development kit 17
- spark 3.5.5
- hadoop 3.3.0 (but this isn't needed if we will be using Docker)
    |- hadoop.dll to avoid `java.lang.UnsatisfiedLinkError: 'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(java.lang.String, int)`
    |- winutils.exe to avoid `java.lang.UnsatisfiedLinkError`
- python 3.11.x (locally not docker I used 3.11.8) to avoid `EOF exception` in creating spark dataframes
- once spark is downloaded I will have to replace the guava-14.0 maven package that comes with it with guava-27.0-jre   
- include hadoop-aws 3.3.4, aws-java-sdk-bundle 1.11.563, and httpcore 4.4.16 to avoid `java.lang.NoSuchMethodError: org.apache.hadoop.util.SemaphoredDelegatingExecutor.<init>` and `Class org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider not found`
- need the python version to be 3.11.x as part of the specs of this orchestration
- change `AIRFLOW__CORE__EXECUTOR` from `CeleryExecutor` to `LocalExecutor`
- `AIRFLOW__CELERY__RESULT_BACKEND` and `AIRFLOW__CELERY__BROKER_URL` from `x-airflow-common` environment
- add `${AIRFLOW_PROJ_DIR:-.}/include:/opt/airflow/include` to `x-airflow-common` volumes
- define `network` with value `michael` or `anything you want` as this will be used by other services
- define `x-spark-common` and add the ff:
``` 
  &spark-common
  image: bitnami/spark:latest
  volumes:
    - ${AIRFLOW_PROJ_DIR:-.}/include:/opt/airflow/include
  networks:
    - michael
```

- add networks to both x-spark-common and x-airflow-common with any values that's same for both
- delete airflow-worker, redis, and flower services
- add spark-master and spark-worker services and add the ff. respectively:
```
<<: *spark-common
command: bin/spark-class org.apache.spark.deploy.master.Master
environment:
    - SPARK_MODE=master
    - SPARK_RPC_AUTHENTICATION_ENABLED=no
    - SPARK_RPC_ENCRYPTION_ENABLED=no
    - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
    - SPARK_SSL_ENABLED=no
    - SPARK_MASTER_WEBUI_PORT=8081
ports:
    - "8081:8081"
    - "7077:7077"
```

and 

```
<<: *spark-common
command: bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
environment:
    - SPARK_MODE=worker
    - SPARK_MASTER_URL=spark://spark-master:7077
    - SPARK_WORKER_MEMORY=2G
    - SPARK_WORKER_CORES=2
    - SPARK_RPC_AUTHENTICATION_ENABLED=no
    - SPARK_RPC_ENCRYPTION_ENABLED=no
    - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
    - SPARK_SSL_ENABLED=no
depends_on:
    - spark-master
```

- create Dockerfile with the ff.
```
FROM apache/airflow:2.10.5

USER root

# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
# if in macos use 
# ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64/
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

# switch to airflow user right after setting env variables
USER airflow

# copy and install dependencies in airflow container
COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
```

and

```
FROM apache/airflow:2.9.2
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY quarto.sh /
RUN cd / && bash /quarto.sh

COPY setup_conn.py $AIRFLOW_HOME

User root

# RUN python $AIRFLOW_HOME/setup_conn.py
# RUN apt-get update && \
#     apt-get install -y --no-install-recommends \
#     default-jdk

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jdk

# export JAVA_HOME='/usr/lib/jvm/java-17-openjdk-amd64'
# export PATH=$PATH:$JAVA_HOME/bin
# export SPARK_HOME='/opt/spark'
# export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
RUN curl https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz -o spark-3.5.1-bin-hadoop3.tgz

# Change permissions of the downloaded tarball
RUN chmod 755 spark-3.5.1-bin-hadoop3.tgz

# Create the target directory and extract the tarball to it
RUN mkdir -p /opt/spark && tar xvzf spark-3.5.1-bin-hadoop3.tgz --directory /opt/spark --strip-components=1

#### These set the environment variables in the container
ENV JAVA_HOME='/usr/lib/jvm/java-17-openjdk-amd64'
ENV PATH=$PATH:$JAVA_HOME/bin
ENV SPARK_HOME='/opt/spark'
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
```

customizing python version of airflow image: https://airflow.apache.org/docs/docker-stack/build.html#base-images
https://stackoverflow.com/questions/76699047/can-i-set-up-an-airflow-docker-container-with-python-3-11-using-docker-compose
https://github.com/apache/airflow/discussions/36074

* maybe we don't need to use spark
once we have installed spark via downloading the tar.gz file and the extracting its components and setting the SPARK_HOME env variable

* figure out a way to RUN curl commands after pulling spark image, to install necessary jar packages, then after these are removed we need to remove the guava-14.0 package built in spark 3.5.5 

# to address in the future:
* there may be potential for error in creating buckets from extraction scripts like `extract_cdi.py`, `extract_us_population_per_state_by_sex_age_race_ho.py` and `extract_us_population_per_state.py`, because if we try to run these simultaneously or concurrently like in airflow it might result in conflicts, so separate creation of `cdi-data-raw`, `population-data-raw`, `population-data-transformed`, and `cdi-data-transformed` folders
* use selenium, docker, and airflow to automate extraction process and then use pyspark and databricks to transform extracted data and load the final data into a warehouse like databricks. All of this is orchestrated using airflow. 
* document everything
- write write up on updates of data analytics projects
- compile write ups and pictures involved in linked in posts in a single read me file

- from automating the extraction of data
- your thought process of loading the raw data locally
- your thought process of getting extra population data 
- how you noticed sex and age or sex race origin wasn't enough and that it needed to be sex age race origin and why you needed this extra data
- how you structured the tables first using spark for easier analytical queries by joins and group bys
- thought process behind each transformation step
- learning to finally implement read and write operations using spark to aws s3
- the process of normalization for faster querying
- weaving every process of extraction and transformation and loading using orchestration tools like airflow 

- report colors
canvas bg:
#010A13


#0d1a28


B_B_ALL: #04D98B
B_F_ALL: #46d6dd
B_M_ALL: #153F61
H_B_ALL: #39ab97
NH_B_AIAN: #0d655f
NH_B_ASIAN: #5D8099
NH_B_BLACK: #124040
NH_B_MULTI: #848c93
NH_B_NHPI: #6d8a9d
NH_B_OTHER: #4c535e
NH_B_WHITE: #022829

in order: #46D6DD, #848C93, #04D98B, #6D8A9D, #39AB97, #5D8099, #4C535E, #0D655F, #153F61, #124040, #022829


highest value of map: #124554

#237676

letter color:
#A7AEB6

button fill:
#022829

letter color if on bright bg:
#1A1A1A

# peer feedback:
* in terms sa composition ng dashboard, hindi aligned yung mga tiles
* then text is small. Enlarge font size
* <s>then too dark, maybe naka-dark mode?</s>
* <s>then lacks contex, particularly the stratification ids. Need some way to translate B_B_ALL, B_M_ALL, B_F_ALL, etc. to understandable values</s>
* ano yung yung dark green, and ano yung light green sa US map
* and for me, as much as possible, I would avoid numbers kasi kaya ka nag-data-data viz to represent a story and give more context sa numbers
* the slicer buttons are too thick so I need to make them responsive such that if user filters and isa nalang natira na button then this single button must be of the right size or responsive
* wala din kasing title yung dashboard
* I can say na hindi cohesive yung mga graphs to tell a story
* gawin drop down ang CDI questions since the slicer takes up too much space
* sayang yung space to explain or add annotations
* then make it lighter kasi hindi mabasa yung text
* nasa taas yung pinakamahalaga
* isipin mo sa dyaryo may pinaka headline then may picture na tells about the hot news for today
* sa Data Story Telling, may mga concepts na tinuturo about psychology ng mata on getting the attention. Sa Dashboards, same din
* Yung mga important should be in bigger chunkcs
* then yung less important or supporting details, maliliit
* So dashboard mo, pinakamalaki yung line graph. Bakit sya yung pinaka malaki? kasi syan yung pinakaimportante? kasi sya yung nagssabi na dapat ko itong tignan kasi it concerns me?
* and rule of thumb ko dyan is, dapat ko muna mapakita yung mga important insight, then secondary yung aesthetics

