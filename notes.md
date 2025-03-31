
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

## important functions in excel 
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
* 

## important shortcuts in excel
* ctrl + n
* to make widths or heights of each column or row the same select the range of columns or rows and right click one of the divider lines of these selected range of columns and row indeces, a dialog will popup indicating what value you want for the width or height then press enter
* to edit cell you can type directly into it or press f2
* ctrl + shift + arrow keys (left right down up) will select all the cells in a range of rows or columns that have values
* set goal tab
* pivot table
* You can drag across or down the bottom right corner of cell using the formula to apply the same formula to other columns or rows in the spreadsheet
* You can also double click the corner to apply the same formula for every row down
* Type `=` the formula name and once the hint for the formula you want to use pops up press tab so you don't have to type every single letter of the formula
* `VLOOKUP` is a common formula/function used by DAs to look up values in the vertical orientation or columns. All formulas and rheir categories are grouped in the formulas tab in the app

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

*  

# Questions:
* how to fill in missing values?
* how to drop undesired values based on a filter?
* what is formatting data? Select a column of numbers then we select the formatting of this column and set it to currency to turn numbers into currency e.g. with decimals etc.

# Relevant articles and links:
1. https://stackoverflow.com/questions/696506/sql-datatype-how-to-store-a-year
2. https://stackoverflow.com/questions/47357855/sql-add-only-a-year-value-in-a-date-column
3. https://medium.com/analytics-vidhya/analysis-of-time-series-data-dad4afa56358

# Problems to solve:
1. I can't save year as 4 byte int for 200000+ rows since that would be a waste of space
