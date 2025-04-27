
# Insights:

## credit card data
* meta data for `fraudTest.csv` and `fraudTrain.csv`
```
index - Unique Identifier for each row
trans_date_trans_time - Transaction DateTime
cc_num - Credit Card Number of Customer
merchant - Merchant Name
category - Category of Merchant
amt - Amount of Transaction
first - First Name of Credit Card Holder
last - Last Name of Credit Card Holder
gender - Gender of Credit Card Holder
street - Street Address of Credit Card Holder
city - City of Credit Card Holder
state - State of Credit Card Holder
zip - Zip of Credit Card Holder
lat - Latitude Location of Credit Card Holder
long - Longitude Location of Credit Card Holder
city_pop - Credit Card Holder's City Population
job - Job of Credit Card Holder
dob - Date of Birth of Credit Card Holder
trans_num - Transaction Number
unix_time - UNIX Time of transaction
merch_lat - Latitude Location of Merchant
merch_long - Longitude Location of Merchant
is_fraud - Fraud Flag <--- Target Class
```

## functions in excel 
* functions in excel are akin to statistical functions in scipy, numpy, pandas etc. operating on arrays, matrices or rows of data usually
* `=<name of function e.g. SUM>(<cell column letter and row number e.g. A1>: <cell column letter and row number e.g. A10>)` or `=<name of function e.g. RANDARRAY>(<num rows>, <num cols>, <min random num to be generated>, <max random num to be generated>, <boolean value indicating whether to generate only integers e.g. TRUE or FALSE>)`
* selecting a cell and then inputting `=RANDARRAY(10, 10, -100, 100, FALSE)` will generate a matrix of 10 x 10 dimensions of random float numbers (since integer arg is set to false) between -100 and 100  
* copying a range of cells (vector or matrices) and then pasting the cell range where you've pasted the values to will be still highlighted from here you can still press ctrl and see what paste options you want to have happen, 
* if you want a vector or matrix of cells to remain unmoved while you scroll horizontally and/or vertically click `view -> freeze panes`
autofill iss useful for repeating sequences of  data with patterns, e.g. 

Mon then next Tues, next Wed
5, 10, then next 15, next 20, next 25, 
Mon, Wed, then next Fri, next Sun, next, Tues and so on..
* `=UNIQUE(B2:B5601)` creates a list of all the unique values in a column
* `=COUNTIF(<range>, "?")` counts the number of occurences of a unique value in a column 
* now I know that a pivot table is essentially the excel equivalent of a group by in SQL
* Type `=` the formula name and once the hint for the formula you want to use pops up press tab so you don't have to type every single letter of the formula
* `VLOOKUP` is a common formula/function used by DAs to look up values in the vertical orientation or columns. All formulas and rheir categories are grouped in the formulas tab in the app

## important shortcuts in excel
* ctrl + n
* to make widths or heights of each column or row the same select the range of columns or rows and right click one of the divider lines of these selected range of columns and row indeces, a dialog will popup indicating what value you want for the width or height then press enter
* to edit cell you can type directly into it or press f2
* ctrl + shift + arrow keys (left right down up) will select all the cells in a range of rows or columns that have values
* set goal tab
* pivot table
* You can drag across or down the bottom right corner of cell using the formula to apply the same formula to other columns or rows in the spreadsheet
* You can also double click the corner to apply the same formula for every row down

## EDA
* exploratory data analysis can be basically used to find out which features give the most insight to us when we have certain questions i.e. "what is the average price of the top 3 most common cars?", "what is the maximum or minimum population of an age group?"
* can be done sb.regplot to see the correlation between two variables/features
* df.describe to see descriptive statistics of each feature in the dataframe
* grouping to see the mean, max, count, median, sum, value of one or groups of feature/s
* using box plots to find out outliers in a feature
* what is pivoting?
* how to use pearson correlation in EDA?
* analysis of variance
* what are p-test and t-test
* a patient from Arizona, Diagnosed with Leukemia, in year 2014 can be multiple attribute that can be grouped and then aggregated
```
| state | year_diagnosed | disease | age |
| Arizona | 2014 | Leukemia | 23 |
| Arizona | 2014 | Leukemia | 25 |
| Detroit | 2015 | Depression | 16 |
| Detroit | 2015 | Depression | 27 |
| Detroit | 2015 | Depression | 30 |
| Alaska | 2008 | PTSD | 30 |
| Alaska | 2008 | PTSD | 29 |
```

this may emerge by asking a question prior: what is the mean age of patients diagnosed 2014 in the state of arizona with leukemia

and from here we write a query like `SELECT state, year_diagnosed, disease, AVG(age) AS mean_age FROM chronic_disease GROUP BY state, year_diagnosed, disease
WHERE state = 'Arizona' AND year_diagnosed = 2014 AND disease = 'Leukemia';`

to group the `year_diagnosed`, `state`, and `disease` columns representing a patient profile and aggregating all patients with these profiles to know their mean age

we can ask another broad spectrum question: what is the population of those diagnosed with the same disease, state, and year when they were diagnosed. which would mean aggregating again on the year_diagnosed, state, and disease columns and then using a count aggregator to determine the ciunt of each patient profile with these grouped columns e.g. `SELECT state, year_diagnosed, disease, AVG(age) AS mean_age FROM chronic_disease GROUP BY state, year_diagnosed, disease;`


## chronic disease data
* my questions are what can be teh possible variables/features of this data that will be molded into a large data set
* will probably use react or django to render the dashboard I will be building in PowerBI
* Based on the sample rows and column names you provided, it appears that the dataset contains time series health care data regarding

* chronic disease indicators (CDI) across the nation from 2001-2014. Below is a description of each column:
```
YearStart: The starting year of the data collection period (e.g., 2013 in the first row).
YearEnd: The ending year of the data collection period (e.g., 2013 in the first row).
LocationAbbr: Abbreviation code for the geographic location (e.g., "CA" for California in the first row).
LocationDesc: Full name of the geographic location (e.g., "California" in the first row).
DataSource: The source of the data (e.g., "YRBSS" - Youth Risk Behavior Surveillance System in the first row).
Topic: The topic/category of the chronic disease indicator (e.g., "Alcohol" in the first row).
Question: The specific question related to the chronic disease indicator (e.g., "Alcohol use among youth" in the first row).
Response: The response to the question (e.g., blank in the provided sample).
DataValueUnit: The unit of measurement for the data values (e.g., "%" for percentage in the first row).
DataValueTypeID: ID representing the type of data value (e.g., "CrdPrev" for Crude Prevalence in the first row).
DataValueType: The type of data value (e.g., "Crude Prevalence" in the first row).
DataValue: The actual data value for the indicator (e.g., "-" or "No data available" in the provided sample).
DataValueAlt: Alternative data value (e.g., blank in the provided sample).
DataValueFootnoteSymbol: Symbol representing the footnote for the data value (e.g., "-" in the provided sample).
DatavalueFootnote: Footnote for the data value (e.g., "No data available" in the provided sample).
LowConfidenceLimit: The lower limit of confidence interval for the data value (e.g., blank in the provided sample).
HighConfidenceLimit: The higher limit of confidence interval for the data value (e.g., blank in the provided sample).
StratificationCategory1: The first category for stratification (e.g., "Overall" in the first row).
Stratification1: The stratification value for category 1 (e.g., "Overall" in the first row).
StratificationCategory2: The second category for stratification (e.g., blank in the provided sample).
Stratification2: The stratification value for category 2 (e.g., blank in the provided sample).
StratificationCategory3: The third category for stratification (e.g., blank in the provided sample).
Stratification3: The stratification value for category 3 (e.g., blank in the provided sample).
GeoLocation: Geographic location coordinates (latitude and longitude) for the data point (e.g., "(37.63864012300047, -120.99999953799971)" for California in the first row).
TopicID: ID representing the topic/category of the chronic disease indicator (e.g., "ALC" for Alcohol in the first row).
QuestionID: ID representing the specific question related to the chronic disease indicator (e.g., "ALC1_1" for Alcohol use among youth in the first row).
ResponseID: ID representing the response to the question (e.g., blank in the provided sample).
LocationID: ID representing the geographic location (e.g., "6" for California in the first row).
StratificationCategoryID1: ID representing the first category for stratification (e.g., "OVERALL" in the first row).
StratificationID1: ID representing the stratification value for category 1 (e.g., "OVR" for Overall in the first row).
StratificationCategoryID2: ID representing the second category for stratification (e.g., blank in the provided sample).
StratificationID2: ID representing the stratification value for category 2 (e.g., blank in the provided sample).
StratificationCategoryID3: ID representing the third category for stratification (e.g., blank in the provided sample).
StratificationID3: ID representing the stratification value for category 3 (e.g., blank in the provided sample).
Please note that the data in the sample rows provided contains missing values or placeholders (e.g., "-") in some columns, and there might be more columns in the actual dataset.
```


* You are also a healthcare expert. So in the context of health care what could be descriptions of these columns still from a data science perspective but with domain knowledge like that of a health care expert
```
YearStart: The starting year of the data collection period. In healthcare data, this column represents the year when the specific health indicators' measurements or observations begin.
YearEnd: The ending year of the data collection period. In healthcare, this column indicates the year when the data collection for the particular health indicators concludes.
LocationAbbr: Abbreviation code for the geographic location. In healthcare data, this column provides the abbreviated code representing the specific state or region where the data was collected.
LocationDesc: Full name of the geographic location. This column gives the full name of the state or region corresponding to the LocationAbbr.
DataSource: The source of the data. In healthcare, this column identifies the organization or system responsible for collecting the data, such as the Youth Risk Behavior Surveillance System (YRBSS) in the provided sample.
Topic: The topic/category of the chronic disease indicator. This column categorizes the health indicators into broader topics or areas of focus, such as "Alcohol" in the provided sample.
Question: The specific question related to the chronic disease indicator. This column contains the actual question asked during data collection, e.g., "Alcohol use among youth" in the provided sample.
Response: The response to the question. In some cases, data might have multiple response options, but in the provided sample, this column appears to be blank, possibly indicating a non-response or open-ended question.
DataValueUnit: The unit of measurement for the data values. In healthcare data, this column specifies the unit in which the health indicators are measured, such as percentages ("%") in the provided sample.
DataValueTypeID: ID representing the type of data value. This column provides an identifier for the type of data value recorded, e.g., "CrdPrev" indicating Crude Prevalence in the provided sample.
DataValueType: The type of data value. This column describes the nature or representation of the data, such as "Crude Prevalence" in the provided sample, which may indicate the raw or unadjusted prevalence value.
DataValue: The actual data value for the indicator. This column contains the measured value of the health indicator, e.g., "-" or "No data available" in the provided sample, indicating missing or unavailable data.
DataValueAlt: Alternative data value. In some cases, data might have alternative values or transformations, but this column appears to be blank in the provided sample.
DataValueFootnoteSymbol: Symbol representing the footnote for the data value. This column contains a symbol that corresponds to a footnote explaining specific aspects or caveats related to the data value.
DatavalueFootnote: Footnote for the data value. Footnotes provide additional contextual information or explanations for the data value, e.g., "No data available" in the provided sample.
LowConfidenceLimit: The lower limit of confidence interval for the data value. In healthcare data, this column might contain the lower bound of the confidence interval, which helps estimate the range within which the true value lies.
HighConfidenceLimit: The higher limit of confidence interval for the data value. Similarly, this column might contain the upper bound of the confidence interval for the health indicator's value.
StratificationCategory1: The first category for stratification. Healthcare data often requires stratification to analyze variations in health indicators across different groups. This column represents the first category used for stratification, such as "Overall" in the provided sample.
Stratification1: The stratification value for category 1. This column contains the specific value or label corresponding to the first stratification category, e.g., "Overall" in the provided sample.
StratificationCategory2: The second category for stratification. If further stratification is required, this column would represent the second category.
Stratification2: The stratification value for category 2. Similarly, this column would contain the value or label corresponding to the second stratification category.
StratificationCategory3: The third category for stratification. If additional stratification is used, this column would represent the third category.
Stratification3: The stratification value for category 3. Likewise, this column would contain the value or label corresponding to the third stratification category.
GeoLocation: Geographic location coordinates (latitude and longitude) for the data point. In healthcare data, this column provides the geographic location of where the data was collected, which can be useful for geospatial analysis.
TopicID: ID representing the topic/category of the chronic disease indicator. This column might serve as a unique identifier for the broader topics or areas of interest in the dataset.
QuestionID: ID representing the specific question related to the chronic disease indicator. Similarly, this column could act as a unique identifier for individual questions in the dataset.
ResponseID: ID representing the response to the question. If the responses to the questions have specific identifiers, they would be captured in this column.
LocationID: ID representing the geographic location. This column may serve as a unique identifier for different geographic locations, such as states or regions.
StratificationCategoryID1: ID representing the first category for stratification. This column could be used to uniquely identify the different stratification categories, such as "OVERALL" in the provided sample.
StratificationID1: ID representing the stratification value for category 1. Similarly, this column would contain a unique identifier for the first stratification value, e.g., "OVR" for Overall in the provided sample.
StratificationCategoryID2: ID representing the second category for stratification. If there are more stratification categories, their unique identifiers would be captured in this column.
StratificationID2: ID representing the stratification value for category 2. Likewise, this column would contain the unique identifier for the second stratification value.
StratificationCategoryID3: ID representing the third category for stratification. If the data requires additional stratification, this column would include the unique identifiers for the third stratification category.
StratificationID3: ID representing the stratification value for category 3. Similarly, this column would contain the unique identifier for the third stratification value.
```

* As far as I know, in order to get myself acquainted and understand the data I need to first visualize and manipulate it as best I know how using pandas and python and matplotlib
* the vague next step would now be to import the same now understood data to pgAdmin 4 so I can use pgsql to manipulate it and do data cleaning, preprocessing, cleaning, feature engineering there separately without python
* I think I'll be using a series of selects and then once the final table is out I can then save it as a separate table as a .csv file
* the next is to now showcase these result in some kind of visualization platform like PowerBI
* to run scripts enter postgresql cli by running `psql -U <name of user e.g. postgres>` then enter password then enter path of `.sql` script relative to the current directory `\ir "./<name of file>.sql"`


* queries used:
1. `select "DataValueFootnote" from "ChronicDisease" where 'No data available' in ("DataValueFootnote");`
2. `select "DataValueFootnote" from "ChronicDisease" where ' ' in ("DataValueFootnote");` reveals the empty strings in the dataaluefootnote column
3. `select distinct "<column>" from "ChronicDisease";`

* I learned that `netstat -a` lists all ports being used
```
C:\Users\LARRY>netstat -a

Active Connections

  Proto  Local Address          Foreign Address        State
  TCP    0.0.0.0:135            LAPTOP-3GL266K9:0      LISTENING
  TCP    0.0.0.0:445            LAPTOP-3GL266K9:0      LISTENING
  TCP    0.0.0.0:1801           LAPTOP-3GL266K9:0      LISTENING
  TCP    0.0.0.0:3307           LAPTOP-3GL266K9:0      LISTENING
  ...
  TCP    192.168.1.6:139        LAPTOP-3GL266K9:0      LISTENING
  TCP    192.168.1.6:49422      mnl07s01-in-f2:https   TIME_WAIT
  TCP    192.168.1.6:49710      52.109.124.4:https     FIN_WAIT_1
  TCP    192.168.1.6:49796      105.103.242.100:57805  ESTABLISHED
```

which is useful especially if we are troubleshooting an error in our mysql workbench that a connection has not been established because it can either be two things that causes this error: one is if the service is not started which can be solved by typing services in our search bar and in the services application find the MySql80 service and start or restart this service, if this does not work it could bev that when installing your MySql applications you changed the default port number of 3306 to another port number i.e. 3307 which we can see in our active ports clearly listed and not 3306, and now when you make another connection or server with the default port number of 3306 it fails to connect since the default port number has indeed been changed to 3307. So when we make a connection or server we must specify its port number to be the one that we picked during installation which was port 3307. And from here if we click test connection it will now ask for our root password which we also set during installation (which we also need to remember at all times) and once entered it will show that it has successfully connected to our server and from here we can create databases and tables.

* below error can be resolved by again specifying the default port number we changed earlier and the user using the -P and -u arguments respectively e.g. `mysql -P 3307 -p test_db -u root`

```
C:\Users\LARRY>mysql
ERROR 2003 (HY000): Can't connect to MySQL server on 'localhost:3306' (10061)

C:\Users\LARRY>mysql -P 3307 -p test_db
Enter password: ****
ERROR 1045 (28000): Access denied for user 'ODBC'@'localhost' (using password: YES)
```

once corrected we will be shown...

```
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 23
Server version: 8.0.41 MySQL Community Server - GPL

Copyright (c) 2000, 2025, Oracle and/or its affiliates.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql>
```

* in mysql workbench or mysql server we can now just grab a column without using double quotes not unlike fuckin postgresql where we have to always fucking use double quotes  na nakakalito
```
mysql> INSERT INTO employees (EmployeeID, FirstName, LastName, Office, HireDate)
    -> VALUES (2, "Michael", "Scott", "Dunder Mifflin", "2024-12-30");
```

* we can show the list of processes in our mysql server through `SHOW PROCESSLIST;`
```
mysql> show processlist
    -> ;
+----+-----------------+-----------------+------+---------+------+------------------------+------------------+
| Id | User            | Host            | db   | Command | Time | State                  | Info             |
+----+-----------------+-----------------+------+---------+------+------------------------+------------------+
|  5 | event_scheduler | localhost       | NULL | Daemon  | 2677 | Waiting on empty queue | NULL             |
| 18 | root            | localhost:55813 | NULL | Sleep   |  400 |                        | NULL             |
| 19 | root            | localhost:55814 | NULL | Sleep   |  320 |                        | NULL             |
| 26 | root            | localhost:58241 | sys  | Query   |    0 | init                   | show processlist |
+----+-----------------+-----------------+------+---------+------+------------------------+------------------+
4 rows in set, 1 warning (0.00 sec)
```

* we can display what database we are currently connected to by `SELECT DATABASE();`
```
mysql> select database();
+------------+
| database() |
+------------+
| sys        |
+------------+
1 row in set (0.00 sec)
```

* to switch to another database we can use `USE <database we want to use>;`
```
mysql> use test_db;
Database changed
mysql> select database();
+------------+
| database() |
+------------+
| test_db    |
+------------+
1 row in set (0.00 sec)

mysql>
```

*
```
mysql> insert into employees values
    -> (4, "Katie", "Martin", "CA", "2020-06-30"),
    -> (5, "Ryan", "Phillips", "NY", "2020-07-15"),
    -> (6, "Lauren", "Paulson", "PA", "2010-08-13");
Query OK, 3 rows affected (0.01 sec)
Records: 3  Duplicates: 0  Warnings: 0

mysql> select * from employees;
+------------+--------------+----------+----------------+------------+
| EmployeeID | FirstName    | LastName | Office         | HireDate   |
+------------+--------------+----------+----------------+------------+
|          1 | Larry Miguel | Cueva    | Dunder Mifflin | 2025-01-01 |
|          2 | Michael      | Scott    | Dunder Mifflin | 2024-12-30 |
|          3 | Bob          | Andrews  | NY             | 2025-06-30 |
|          4 | Katie        | Martin   | CA             | 2020-06-30 |
|          5 | Ryan         | Phillips | NY             | 2020-07-15 |
|          6 | Lauren       | Paulson  | PA             | 2010-08-13 |
+------------+--------------+----------+----------------+------------+
6 rows in set (0.00 sec)

mysql> update employees set office = "NY"
    -> where firstname = "Larry Miguel";
Query OK, 1 row affected (0.01 sec)
Rows matched: 1  Changed: 1  Warnings: 0

mysql> update employees set office = "NY"
    -> where firstname = "Michael";

        -> where firstname = "Larry Miguel";
mysql> select * from employees;
+------------+--------------+----------+--------+------------+
| EmployeeID | FirstName    | LastName | Office | HireDate   |
+------------+--------------+----------+--------+------------+
|          1 | Larry Miguel | Cueva    | NY     | 2025-01-01 |
|          2 | Michael      | Scott    | PA     | 2024-12-30 |
|          3 | Bob          | Andrews  | NY     | 2025-06-30 |
|          4 | Katie        | Martin   | CA     | 2020-06-30 |
|          5 | Ryan         | Phillips | NY     | 2020-07-15 |
|          6 | Lauren       | Paulson  | PA     | 2010-08-13 |
+------------+--------------+----------+--------+------------+
6 rows in set (0.00 sec)
```

* to use mysql commands in cmd we need to copy the path to the bin folder located where we installed our mysql server package which will usually have a path `C:\Program Files\MySQL\MySQL Server 8.0\bin` and then paste it inside our environment variables `PATH` variable

* we can clear screen using `\! cls`

* we can run a sql script that in a certain directory by first going into our connection/server then our database via command line and once there we can use `source <absolute path or relative path to .sql script>`. For relative files we use `./path/to/file`
```
mysql> source ./data-engineering-path/variable-data-analyses/sql scripts/update_data_value_unit.sql
Query OK, 0 rows affected (0.43 sec)
```


* running `SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '<table name>'` returns the data types of a tables
```
mysql> SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
    ->   WHERE table_name = 'chronicdisease';
+-----------+
| DATA_TYPE |
+-----------+
| text      |
| double    |
| double    |
| text      |
| text      |
| text      |
| double    |
| bigint    |
| double    |
| text      |
| text      |
| double    |
| double    |
| text      |
| text      |
| text      |
| text      |
| bigint    |
| bigint    |
+-----------+
19 rows in set (0.01 sec)
```

* if you get a `SELECT list is not in GROUP BY clause and contains nonaggregated column .... incompatible with sql_mode=only_full_group_by` error in the sql console run `SET GLOBAL sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));` then exit then re enter console

* ms sql server seems to be the industry grade database management system compared to mysql or postgresql. https://www.microsoft.com/en-us/sql-server/sql-server-downloads we can download the express version here. This tool we will need to learn apart from ms powerbi in order to familiarize ourselves with these tools functionality from the inside out to eventually be ready to pass the certification exams like PL-300 for the power bi data analyst certification adn the DP-300 for the ms azure dtabase administrator certification

* once ms sql server is installed we copy the connection string e.g. `Server=localhost\SQLEXPRESS;Database=master;Trusted_Connection=True;`. The installation location is in `C:/Program Files/Microsoft SQL Server/`

* if a `A connection was successfully established with the server, but then an error occurred during the login process. (provider: SSL Provider, error: 0 - The certificate chain was issued by an authority that is not trusted.) (Framework Microsoft SqlClient Data Provider)` error occurs when connecting to a server even in windows authentication chekc the `trust server certificate` option and then hit ok in dialog box

* to use ms sql server in command line we type the command `sqlcmd -S <name of laptop e.g. LAPTOP-3GL266K9\name of instance e.g SQLEXPRESS>`. Note it is imperativee that the device name and the name of the instane/server must have a backward slash in betweeen them and not a forward slash. 

```
1> select * from sys.databases;
2> go
1>
2> select name from sys.databases;
3> go
name
--------------------------------------------------------------------------------------------------------------------------------
master
tempdb
model
msdb

(4 rows affected)
1>
```

* other useful flags we can use when entering the CLI of ms sql server is `-C` trust the server certificate, `-d <db_name>`, `-U <login_id>`, `-P <password>` e.g. `sqlcmd -S <laptop name or "localhost">\<server name> -U <login id/username> -P <password>`

* we can get the database we are currently in by...
```
1> select db_name();
2> go

--------------------------------------------------------------------------------------------------------------------------------
master

(1 rows affected)
```

* we can also switch betweeen databases using...
```
1> use tempdb;
2> go
Changed database context to 'tempdb'.
```

* we can start a windows service via command line if we run first and foremost the command prompt as administrator (to have administrative permissions of course like starting/stopping a service) and then running `net start "<name of windows service (even with spaces and with casing) e.g. SQL Server (SQLEXPRESS)>"`. And if we want to stop it we run 

here once we go into the command line itnerface of ms sql server the way we execute sql queries or statements is done always after typing the `go` keyword. So every statement or query we type goes into the next line when we press the enter key and once in this line we type `go` nd hit enter again to execute the query

* So cardinality pala is yung relationship ng two or more tables like yung 1 to 1, 1 to many (many to 1), and many to many. If a column of table A has unique rows (strictly one value) and column of table B has unique rows we can say we can establish a 1 to 1 relationship between these values of these columns. If a column of table A has 1 unique value and a column of table B ohowever has more than 1 unique values then we can establish a relationship that's 1 to many or many to 1 if the direction of filter is from B to A. And if many to many then it means that a column from table A has more than 1 unique values and that column from table B has more than 1 unique values also which we can both use to establish relationships. 

* And cross filter is how you want these cardinalities to flow whether single i.e. one way from table A to table B or bidirectional meaning both the flow of cardinality can be from take A to B or vice versa

* I realized you can use sql sub queries in either two places: with the FROM keyword and with the WHERE clause using an IN condition
 
As these seem to be the only ones where we can use tables

I.e. `... FROM (<sub query>)` or `... WHERE <column> IN (<sub query)`

* online transaction processing (OLTP) is used for everyday queries made by applications to databases to maybe retrieve, insert, update, or delete data. And online analytics processing (OLAP) is where data consolidated over a long period of time and doesn't come in an everyday manner like users interacting with servers, and where complex queries are made that erhaps make aggregations on large amounts data for data analysis and business intelligence and insights.

basically OLTP is your typical application database like MySQL, Postgres, Microsoft SQL Server, MongoDB, Firebase, etc. And OLAP is basically a database but for extremely large amounst of data (a database on steroids) which is used for business intelligence and analytics and extracting insights from data, these are now what's considered your typical data warehouses like GCP Big Query, Azure Synapse, AWS Redshift, Snowflake, Databricks

* However because snowflake, aws redshift, azure synapse, databricks (since it after 2 hours you have to recreate cluster for databases to run on), and bigquery aren't free some other free OLAP alternatives could be DuckDB which is an open-source column-oriented Relational Database Management System (RDBMS) designed to provide high performance on complex queries against large databases. Another is Apache Druid. However since connectors to powerbi for these OLAp alternatives aren't directly available we will have to download an Open Database Connectivity (ODBC) driver for powerbi. 

for DuckDB: shttps://motherduck.com/docs/integrations/bi-tools/powerbi/

* Headless mode in Selenium refers to the ability to run browser automation scripts without launching a visible browser window or GUI. Instead, the browser operates in the background, performing all the actions you've programmed (navigating to pages, interacting with elements, executing JavaScript) but without displaying anything on your screen.

Think of it like this:

Headful (Normal): Selenium opens a full browser window that you can see and interact with (or watch the script interact with).
Headless: Selenium runs a browser instance in memory. You don't see a browser window, but the browser engine is still fully functional and processing the web pages.
Why use headless mode?

Speed and Efficiency: Headless browsing is generally faster and less resource-intensive because it doesn't need to render the GUI. This can significantly speed up your scraping or automation tasks.
Server Environments: Headless mode is essential for running Selenium scripts on servers or in environments where there's no graphical interface available (e.g., cloud servers, CI/CD pipelines).
Scalability: Running multiple headless browser instances in parallel is often easier on server resources than running multiple full browser instances.
Background Tasks: For tasks like web scraping or automated testing that don't require visual confirmation, running in the background is ideal.
Common Headless Browsers used with Selenium:

Chrome Headless: Chrome offers a built-in headless mode.
Firefox Headless: Firefox also has a headless mode.
In essence, when you run Selenium in headless mode, it's like having a virtual browser working silently in the background, fetching and processing web content without any visual output. This makes it a powerful tool for automated tasks that don't require a user interface.

Ah so this is similar to dockers detached mode allpwing docker to just run in the background instead of taking up control of the command line interface


That's an excellent analogy! You've बिल्कुल grasped the core concept of running something in the background without direct visual interaction.

Yes, headless mode in Selenium is very similar in principle to Docker's detached mode (-d).

Here's why the comparison works so well:

Running in the Background: Both headless Selenium and detached Docker allow a process (a browser instance or a container) to execute without actively occupying your terminal or displaying a graphical user interface.
Resource Efficiency: Both approaches are often used for efficiency. Headless Selenium consumes fewer resources than a full browser GUI, and detached Docker containers free up your command line and can run more efficiently in the background.
Automation and Server Use: Both are crucial for automation and running tasks on servers where direct interaction isn't needed or possible. You wouldn't typically run a full browser GUI on a production server for web scraping, just like you'd usually run your web application in a detached Docker container.
Control via Other Means: Just as you interact with a detached Docker container using Docker commands (docker logs, docker exec, etc.), you control a headless Selenium browser through your script, telling it which websites to visit, what actions to perform, and how to extract data.
The key difference lies in what is being run in the background:

Headless Selenium: Runs a web browser engine (like Chrome or Firefox) in memory without a visible UI. Its purpose is to interact with web pages programmatically.
Docker Detached Mode: Runs an entire isolated environment (a container) in the background. This container can house various applications, including web servers, databases, or even Selenium itself.
So, while the underlying principle of running a process in the background without direct UI interaction is the same, the nature of the process differs. You could even run a headless Selenium instance inside a detached Docker container!

Your analogy is spot-on for understanding the fundamental idea of background execution and resource management in both contexts.

* if pyspark is not yet added to our path upon installation in our environment or globally we will need to locate the bin directory inside pyspark directory and add the bin directory path to our `PATH` environment variable. Why we do this is so we can run spark-submit and other spark related commands in our command line.

* if an error 
```
25/04/22 12:52:59 WARN Shell: Did not find winutils.exe: java.io.FileNotFoundException: java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset. -see https://wiki.apache.org/hadoop/WindowsProblems
Python was not found; run without arguments to install from the Microsoft Store, or disable this shortcut from Settings > Apps > Advanced app settings > App execution aliases.
25/04/22 12:52:59 INFO ShutdownHookManager: Shutdown hook called
25/04/22 12:52:59 INFO ShutdownHookManager: Deleting directory C:\Users\LARRY\AppData\Local\Temp\spark-b0654aae-f91c-442d-b27a-66b287ffd557
```
occurs this means that we have to install winutils via pip in our conda environment or globally in our  local machine.

* another solution is gooing to manage app execution aliases and turning off python and python3: https://stackoverflow.com/questions/65348890/python-was-not-found-run-without-arguments-to-install-from-the-microsoft-store

* another error connected to the above is...
```
Missing Python executable 'python3', defaulting to 'C:\Users\LARRY\anaconda3\envs\tech-interview\Scripts\..' for SPARK_HOME environment variable. Please install Python or specify the correct Python executable in PYSPARK_DRIVER_PYTHON or PYSPARK_PYTHON environment variable to detect SPARK_HOME safely.
The system cannot find the path specified.
The system cannot find the path specified.
```
this maybe due to dependency errors and certain values not being added to the path system environment variable or an environment variable not being added such as `SPARK_HOME`, `HADOOP_HOME`, and `JAVA_HOME` as system environment variables containing the installation location of these softwares

take note that spark 3.5.4 requires java 8 or 17 and later. When on the downloads page it will also indicate that it is prevuilt for hadoop 3.3 and later meaning we have to install hadoop 3.3.0 and later releases (but specifically the winutils executable file as it requires winutils) and must be under these release versions.

steps for setting up apache spark from scratch
- java development kit 17: https://www.oracle.com/java/technologies/javase/jdk17-archive-downloads.html
- apache spark: https://spark.apache.org/downloads.html
- hadoop winutils: https://github.com/kontext-tech/winutils/blob/master/hadoop-3.3.0/bin/winutils.exe
- once downloaded extract the `spark-3.x.x-bin.hadoop3.tgz`
- rename the extracted folder `spark-3.x.x-bin.hadoop3` to just `spark-3.x.x`
- once jdk17 is downloaded run executable file and install JDK and keep track fo installation location which is commonly at `C:\Program Files\Java\jdk-17` 
- create folder named hadoop and inside it create sub directory/ named bin and move the downloaded hadoop `winutils.exe` file inside
- move the spark and hadoop folders in any directory or perhaps the `C:\Program Files` directory
- copy the `C:\Program Files\spark-3.5.5`, `C:\Program Files\hadoop`, `C:\Program Files\Java\jdk-17` paths which contain the bin files of spark, hadoop, and jdk 17
- add new system environment variables named `SPARK_HOME`, `HADOOP_HOME`, and `JAVA_HOME`, with these values respectively. AH so now I know that you can download these software in a docker container and replicate the same process of copying their installation paths and creating system environment variables through `export JAVA_HOME="installation/dir/of/jdk"`, `export SPARK_HOME="installation/dir/of/jdk"`, `export HADOOP_HOME="installation/dir/of/hadoop"` (however note this inly does it for the current shell and all processes in current shell if you want to do it globally or add it as a system environment variable you need to use `sudo -H gedit /etc/environment`)
- in windows we can reference these system environment variables as `%<name of env var>%` e.g. `%SPARK_HOME%` and we'd get the value we assigned to this environment/system environment variable and add backslashes to it to reference sub directories in this directory e.g. `%SPARK_HOME%/bin` will be `C:\Program Files\spark-3.5.5\bin`. In linux we use `$<name of env var>`. But we add new values to the system path environment variable where we will now reference these newly created system environment variables. We add `%SPARK_HOME%\bin`, `%HADOOP_HOME%\bin`, and `%JAVA_HOME%\bin`
- restart the command line and run `javac --version`, `spark-shell`, to check if the installed software has been installed and commands are able to run in command line. spark-shell is a CLI for spark. Now we can use `spark-submit` for our python scripts containing spark sessions
- we need to also add PYSPARK_HOME containing the path to our global python interpreter which would be in path `C:\Users\LARRY\AppData\Local\Programs\Python\Python312\` and appended to it the `python.exe` e.g. `C:\Users\LARRY\AppData\Local\Programs\Python\Python312\python.exe` as this string will be needed in order for `spark-submit` to run our python scripts containing spark commands

```
C:\Users\LARRY>spark-shell
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
25/04/22 13:50:20 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
25/04/22 13:50:22 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
Spark context Web UI available at http://LAPTOP-3GL266K9.bbrouter:4041
Spark context available as 'sc' (master = local[*], app id = local-1745301022738).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.5.5
      /_/

Using Scala version 2.12.18 (Java HotSpot(TM) 64-Bit Server VM, Java 17.0.12)
Type in expressions to have them evaluated.
Type :help for more information.

scala>
```

* in order to read excel files spark needs to download and then include external packages which can be run by command `spark-submit --packages com.crealytics:spark-excel_2.12:3.5.1_0.20.4 transform_us_population_per_state_by_sex_age.py <arg1> <arg2>`. In order to execute this spark script it is imperative to add this packages argument as this indicates the dependency that we need installed when running this script transforming excel files

* `com.crealytics:spark-excel_2.12:3.5.1_0.20.4` is actuall ythe package we need to read these excel files using spark where `com.crealytics` is the group id, `spark-excel_2.12` is the artifact id, and `3.5.1_0.20.4` is the release version

* but how come this works when using `spark-submit` but when using jupyter notebooks the extra packages are not downloaded 

* this is why if your going to use spark with airflow or in a docker container it is better to install it globally in the container rather than as a package and then set the paths manually

# Questions:
* how to fill in missing values?
* how to drop undesired values based on a filter?
* what is formatting data? Select a column of numbers then we select the formatting of this column and set it to currency to turn numbers into currency e.g. with decimals etc.

# Relevant articles and links:
* https://stackoverflow.com/questions/696506/sql-datatype-how-to-store-a-year
* https://stackoverflow.com/questions/47357855/sql-add-only-a-year-value-in-a-date-column
* https://medium.com/analytics-vidhya/analysis-of-time-series-data-dad4afa56358
* performance tuning of apache spark: https://medium.com/@manoj.kdas37/how-to-optimize-your-apache-spark-jobs-top-10-approaches-and-best-practices-for-performance-tuning-4630ae864f52

# Problems to solve:
1. I can't save year as 4 byte int for 200000+ rows since that would be a waste of space
2. implement headless browsing of selenium to scrape population data transform the excel sheets and automatically in a data warehouse for data analysis 
