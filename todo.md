* <s>goal is to read data first each excel spreadsheet and csv</s>
* <s>read and transform in pandas first then do it in pyspark</s>
* identify which dataframes have common columns and join them using sql statements
* once data is joined we do exploratory data analysis and feature engineering
* visualize the data using powerbi or something
* use box plot to see interquartile ranges (get it from data-mining-hw repo)

* add primary key to each table during transformation phase as this will be needed when we finally upload these tables to a data warehosue for cdi the pks will be topicid, questionid, and locationid
* the cdi will also be normalized and split into two fact tables one with age brackets and another table without age brackets e.g. 'Infants breastfed at 6 months', 'Receiving formula supplementation within the first 2 days of life among breastfed infants', 'Life expectancy at age 65 years', etc.

* use selenium, docker, and airflow to automate extraction process and then use pyspark and databricks to transform extracted data and load the final data into a warehouse like databricks. All of this is orchestrated using airflow. 
* we use pyspark for preprocessing the data to make sql queries
* use PowerBI to make analyses on the data from databricks