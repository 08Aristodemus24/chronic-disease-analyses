* <s>goal is to read data first each excel spreadsheet and csv</s>
* <s>read and transform in pandas first then do it in pyspark</s>
* identify which dataframes have common columns and join them using sql statements
* once data is joined we do exploratory data analysis and feature engineering
* visualize the data using powerbi or something
* use box plot to see interquartile ranges (get it from data-mining-hw repo)

* add primary key to each table during transformation phase as this will be needed when we finally upload these tables to a data warehosue for cdi the pks will be topicid, questionid, and locationid
* cdi fact table still contains id's that need to be dropped and retained at a separate dimension table as part of normalization process for later loading to warehouse 
* break down us_populations_by_sex_age_race_ho table into fact table and dimension table by dropping the id's that are contained in this fact table and then retaining it in the dimension table as part of normalization process for later loading to warehouse 
* union the stratification dimension table from cdi and us populations per state by sex age race ho tables 
* <s>clean and transform cdi data using pyspark</s>
* <s>we use pyspark for preprocessing the data to make sql queries</s>

* use selenium to automate download of us populations per state by sex age race ho csv's from `www.census.gov` 

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

* use PowerBI to make analyses on the data from databricks