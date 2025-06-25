# Insights:

## Technologies
### Data Lake:
* S3
* apache iceberg
* Google cloud storage
* Azure data lake storage

### DWH:
* MotherDuck/DuckDB

* Clickhouse
Apache druid
* Snowflake
* AWS redshift 
* GCP big query
* Azure synapse
* Databricks

### Data lake house:
* Apache iceberg
* Delta lake

### Data processing (not pandas that's for kids):
#### Batch processing:
* Apache spark
#### Stream processing: 
* Apache kafka

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

#### we would have the following calculations for each unique pair of `datavalueunit` and `datavaluetype`
the most important ones that maybe indicate chronic disease would be the ff. :
* %, Prevalence e.g. `prevalence of gastrointestinal diabetes is 3.6%`
question: `so if I have a question "prevalence of gastrointestinal diabetes", datavaluetype of prevalence, datavalueunit of %, and datavalue of 3.6 which when translated would be "prevalence of gastrointestinal diabetes is 3.6%". How can I then convert this to a tangible number given the population of let's say the state of arizona in 2015 which is 18003249`
answer: $(data value / 100) * population = estimated cases$ 



* %, Percent e.g. `Live births occurring at Baby Friendly Facilities is 3.6%`
question: `so if I have a question "live births occuring at baby facilities", datavaluetype of percent, datavalueunit of %, and datavalue of 3.6 which when translated would be "live births occuring at baby facilities is 3.6%". How can I then convert this to a tangible number given the population of let's say the state of arizona in 2015 which is 18003249`
answer: To convert this percentage into a tangible number, you need: Total live births in Arizona (2015), not the total population.

Example: If Arizona had 85,000 live births in 2015, then: Births at facilities = 0.036 × 85,000 = 3,060 births. Births at facilities=0.036×85,000=3,060 births. Why not use total population? The metric is a % of births, not the general population. Most of the 18M people in Arizona (e.g., elderly, children) didn’t give birth



* %, Age-adjusted Prevalence e.g. `Current smokeless tobacco use among adults aged >= 18 years is 3.6%` (ah okay so gets ko na pag age adjusted may age number sa question)
question: `so if I have a question "Current smokeless tobacco use among adults aged >= 18 years", datavaluetype of age-adjusted prevalence, datavalueunit of %, and datavalue of 3.6 which when translated would be "Current smokeless tobacco use among adults aged >= 18 years is 3.6%". How can I then convert this to a tangible number given the population of let's say the state of arizona in 2015 which is 18003249`
answer: Step 2: Isolate the Adult Population (18+)
Since the metric applies only to adults (≥18 years), you need:

Total adult population (not total state population). Example: If adults make up ~75% of Arizona's population (2015):

Adult population = 0.75 × 18,003,249=13,502,437 adults.



* %, Crude Prevalence e.g. `Individuals meeting aerobic physical activity guidelines for substantial health benefits among adults aged >= 18 years is 3.6%`
question: `so if I have a question "Individuals meeting aerobic physical activity guidelines for substantial health benefits among adults aged >= 18", datavaluetype of crude prevalence, datavalueunit of %, and datavalue of 3.6 which when translated would be "prevalence of gastrointestinal diabetes is 3.6%". How can I then convert this to a tangible number given the population of let's say the state of arizona in 2015 adults aged 18 and above which is 18003249`

The formula is straightforward:

Number of Individuals (Cases) = (Prevalence Rate / 100) * Total Population

Let's apply this to your example:

Prevalence Rate: 3.6%
Total Population (Arizona, 2015, adults aged 18 and above): 18,003,249
Calculation:

Number of Individuals = (3.6 / 100) * 18,003,249
Number of Individuals = 0.036 * 18,003,249
Number of Individuals = 648,116.964



* Number, Median e.g. `Median daily frequency of fruit consumption among adults aged >= 18 years is 3.6`
question: `so if I have a question "Median daily frequency of fruit consumption among adults aged >= 18 years", datavaluetype of number, datavalueunit of median, and datavalue of 3.6 which when translated would be "Median daily frequency of fruit consumption among adults aged >= 18 years is 3.6". How can I then convert this to a tangible number given the population of let's say the state of arizona in 2015 adulst aged 18 and above which is 150000 or does population have no effect in this?`

You are absolutely correct! In the case of "Median daily frequency of fruit consumption," the population size does not directly convert the median value into a tangible number of individuals consuming fruit with that frequency.

Here's why:

Median Represents the Middle Value: The median is the middle value in a sorted dataset. In this context, a median of 3.6 means that if you lined up all the adults in Arizona (aged >= 18) according to their daily fruit consumption from lowest to highest, the person in the very middle would consume fruit approximately 3.6 times per day.
It Doesn't Tell You How Many People Consume Exactly 3.6 Times: The median doesn't tell you how many individuals consume fruit exactly 3.6 times a day. It's a measure of central tendency. Some people will consume more, and some will consume less.
No Direct Multiplication: Unlike prevalence (a proportion of the population affected by a condition), the median is a characteristic of the consumption behavior itself, not a count or a proportion of the population. Multiplying a median frequency by the total population wouldn't yield a meaningful number of individuals.
What the Median of 3.6 Does Tell You:

Approximately 50% of the adult population in Arizona (aged >= 18) consumes fruit 3.6 times per day or less.
Approximately 50% of the adult population in Arizona (aged >= 18) consumes fruit 3.6 times per day or more.
To get a tangible number related to fruit consumption frequency, you would need different types of data, such as:

The distribution of fruit consumption: You would need to know how many people consume fruit 0 times a day, 1 time a day, 2 times a day, 3 times a day, 4 times a day, and so on. With this distribution, you could then count the number of people falling into specific frequency ranges.
The percentage of the population consuming fruit at or above a certain frequency: For example, you might know that "20% of adults in Arizona consume fruit 5 or more times per day." Then you could multiply that percentage by the total population to get a tangible number.



* Number, Age-adjusted Mean e.g. `Average binge drinking frequency among adults aged >= 18 yeras who binge drink is 3.6`
question: `so if I have a question "binge drinking frequency among adults aged >= 18 yeras who binge drink", datavaluetype of number, datavalueunit of age-adjusted mean, and datavalue of 3.6 which when translated would be "Average binge drinking frequency among adults aged >= 18 yeras who binge drink is 3.6". How can I then convert this to a tangible number given the population of let's say the state of arizona in 2015 adulst aged 18 and above which is 150000 or does population have no effect in this?`

To get a tangible number, you would need additional information:

Prevalence of Binge Drinking: You would need to know what percentage or the actual number of adults aged 18 and above in Arizona in 2015 were classified as binge drinkers.

Example: If a study showed that 20% of the 150,000 adults in Arizona were binge drinkers, then the number of binge drinkers would be 0.20 * 150,000 = 30,000.
Calculating Tangible Numbers with Prevalence:

Number of Binge Drinkers: Once you have the prevalence, you can calculate the estimated number of individuals in the target demographic who engage in binge drinking (as shown in the example above).
Total Binge Drinking Episodes: You could then estimate the total number of binge drinking episodes in that population by multiplying the number of binge drinkers by their average frequency: 30,000 binge drinkers * 3.6 episodes/period = 108,000 binge drinking episodes per period.



* Number, Mean e.g. `Average recent physically unhealthy days among adults aged >= 18 years is 3.6`
question: `so if I have a question "Average recent physically unhealthy days among adults aged >= 18 years", datavaluetype of number, datavalueunit of mean, and datavalue of 3.6 which when translated would be "Average recent physically unhealthy days among adults aged >= 18 years is 3.6". How can I then convert this to a tangible number given the population of let's say the state of arizona in 2015 adulst aged 18 and above which is 150000 or does population have no effect in this?`

Here's the breakdown:

Mean as the Total Divided by the Count: An average of 3.6 physically unhealthy days means that if you summed up the total number of physically unhealthy days reported by all 150,000 adults and then divided by 150,000, you would get 3.6.

You Can Find the Total Number of Unhealthy Days: In this case, you can use the population to find the total number of physically unhealthy days experienced by the entire adult population.

Total Unhealthy Days = Average Unhealthy Days * Total Population
Total Unhealthy Days = 3.6 * 150,000
Total Unhealthy Days = 540,000

So, while you can't say that 540,000 individuals experienced 3.6 unhealthy days (because individuals experience whole numbers of days), you can say that the total burden of recent physically unhealthy days across the entire adult population of Arizona in 2015 was 540,000 days.

Why You Still Can't Get the Number of Individuals at Exactly 3.6 Days:

Average Represents a Distribution: The average is a summary statistic of a distribution. Some individuals will have had 0 unhealthy days, some 1, some 10, and so on. The average smooths out these individual differences.
Fractional Averages: An average can be a fraction (like 3.6), but an individual can only experience a whole number of days.
What the Average of 3.6 (Mean) Tells You:

The central tendency of the number of recent physically unhealthy days for this population is 3.6 days.
The total number of physically unhealthy days experienced by the entire population is estimated to be 540,000. This can be useful for understanding the overall health burden.
In summary:

Population does have an effect when you want to calculate the total number of events (in this case, unhealthy days) across the entire population, given the average rate.
Population does not directly tell you the number of individuals who experienced that exact average number of days. To know that, you would need information about the distribution of physically unhealthy days within the population.



* Number, Number e.g. `Mortality from cerebrovascular disease (stroke) is 2735`
question: `how about if a question is "Mortality from disease (stroke)", and its datavalue 2735, and datavaluetype is number, and datavalueunit is number?`

When you have a question like "Mortality from disease (stroke)" with a datavalue of 2735, a datavalueunit of "number", and a datavalueunit also as "number", the interpretation is relatively direct:

Interpretation:

The value 2735 represents the absolute number of deaths attributed to stroke within a specific population and time period.

Population Effect:

In this case, the datavalue itself is already a tangible number of individuals (the deceased). You do not need to multiply or divide this number by a broader population to understand its direct meaning.

However, the context of the population is crucial for understanding the significance of this number:

What is the population size? 2735 deaths from stroke in a population of 10,000 would be a very high mortality rate compared to 2735 deaths in a population of 1 million.
What is the time period? 2735 deaths in one year is different from 2735 deaths over a decade.
What is the geographic area? 2735 deaths in a small city has a different impact than 2735 deaths across an entire country.
What are the age demographics? Stroke mortality is generally higher in older populations.
Are there other relevant subgroups? Mortality rates might differ by sex, ethnicity, etc.
To calculate a mortality rate, you would need the population size:

Mortality Rate = (Number of Deaths / Total Population) * Multiplier (e.g., 100,000)

For example, if the 2735 deaths occurred in the adult population of Arizona (1,500,000 in your earlier example), the crude mortality rate from stroke would be:

Mortality Rate = (2735 / 1,500,000) * 100,000
Mortality Rate ≈ 182.3 deaths per 100,000 adults

This rate provides a better sense of the proportion of the population dying from stroke and allows for comparisons across different populations or time periods.

In summary:

The datavalue of 2735 directly tells you the number of deaths from stroke.
The population size is needed to calculate a mortality rate, which helps in understanding the impact of stroke mortality within that population and for comparisons. Without knowing the population size, the absolute number alone gives you the count of deaths but not the relative frequency.



* cases per 1,000, Age-adjusted Rate e.g. `age-adjusted rate of chronic liver disease mortality is 14.4 cases per 1000`
question: `how about if a question is "age-adjusted rate of chronic liver disease mortality", and its datavalue 14.4, and datavaluetype is "age-adjusted rate", and datavalueunit is "cases per 1000"?`

Age-Adjusted Rate: This means the mortality rate has been statistically adjusted to account for differences in the age distribution of the population being studied compared to a standard population. This allows for fairer comparisons of mortality rates between different groups or over time, as age is a significant factor in mortality.
14.4: This is the age-adjusted mortality rate.
Cases per 1000: This is the unit of the rate. It means that for every 1000 people in the standard population, 14.4 deaths are attributed to chronic liver disease, after accounting for age differences.
Converting to a Tangible Number for a Specific Population (Arizona, 2015, Adults >= 18 - Let's assume a population of 1,500,000 for this example):

To estimate the number of chronic liver disease deaths in the Arizona adult population in 2015, you would apply this age-adjusted rate to that population:

Convert the rate to a proportion: 14.4 cases per 1000 is equal to 14.4 / 1000 = 0.0144.

Multiply the proportion by the population size: 0.0144 * 1,500,000 = 21,600

Therefore, based on an age-adjusted mortality rate of 14.4 cases per 1000 and an estimated adult population of 1,500,000 in Arizona in 2015, we would estimate approximately 21,600 deaths due to chronic liver disease in that population, after adjusting for age.

Important Considerations:

The "Standard Population": Remember that age-adjusted rates are specific to the standard population used for adjustment. If you were comparing this rate to another region or time period, it would be important to know if they used the same standard population for their calculations.
Crude vs. Age-Adjusted: This is an age-adjusted rate. The actual (crude) mortality rate in Arizona's adult population might be higher or lower depending on the age distribution of that specific population compared to the standard population.
Specificity of the Population: The accuracy of the estimated number of deaths depends on how well the age distribution of the Arizona adult population in 2015 aligns with the age distribution of the standard population used to calculate the 14.4 per 1000 rate.

* cases per 10,000, Age-adjusted Rate e.g. `age-adjusted rate of hospitalization for chronic obstructive pulmonary disease as any diagnosis is 178.99 cases per 10000`
question: `how about if a question is "age-adjusted rate of hospitalization for chronic obstructive pulmonary disease as any diagnosis", and its datavalue 178, and datavaluetype is "age-adjusted rate", and datavalueunit is "cases per 10000"?`

Age-Adjusted Rate: As before, this mortality rate has been statistically adjusted to remove the effect of different age distributions between populations, allowing for more meaningful comparisons.
178: This is the age-adjusted mortality rate.
Cases per 10000: This is the unit of the rate. It means that for every 10,000 people in the standard population, 178 deaths are attributed to chronic liver disease, after accounting for age differences.
Converting to a Tangible Number for a Specific Population (Arizona, 2015, Adults >= 18 - assuming a population of 1,500,000):

To estimate the number of chronic liver disease deaths in the Arizona adult population in 2015 based on this rate:

Convert the rate to a proportion: 178 cases per 10,000 is equal to 178 / 10,000 = 0.0178.

Multiply the proportion by the population size: 0.0178 * 1,500,000 = 26,700

Therefore, based on an age-adjusted mortality rate of 178 cases per 10,000 and an estimated adult population of 1,500,000 in Arizona in 2015, we would estimate approximately 26,700 deaths due to chronic liver disease in that population, after adjusting for age.

Key Points to Remember:

The Standard Population: The estimated number of deaths is still influenced by how well the age structure of the Arizona population aligns with the standard population used for the age adjustment.
Rate per Different Unit: The key difference here is the unit ("per 10000" instead of "per 1000"). You need to adjust the conversion to a proportion accordingly.
Direct Estimation: Age-adjusted rates are valuable for comparing mortality risks across populations, and by applying them to a specific population, you can get an estimate of the number of deaths expected in that population, considering its size.



* cases per 100,000, Age-adjusted Rate e.g. `age-adjusted rate of mortality from total cardiovascular diseases is 206.5 cases per 100000`
question: `how about if a question is "age-adjusted rate of chronic liver disease mortality", and its datavalue 206.5, and datavaluetype is "age-adjusted rate", and datavalueunit is "cases per 100000"?`

Age-Adjusted Rate: As before, this mortality rate has been statistically adjusted to account for differences in the age distribution of the population being studied compared to a standard population.
206.5: This is the age-adjusted mortality rate.
Cases per 100000: This is the unit of the rate. It means that for every 100,000 people in the standard population, 206.5 deaths are attributed to chronic liver disease, after accounting for age differences.
Converting to a Tangible Number for a Specific Population (Philippines, 2025, Adults >= 18 - Let's assume an estimated adult population of 70,000,000 for this example, considering the current time and location):

To estimate the number of chronic liver disease deaths in the adult population of the Philippines in 2025 based on this rate:

Convert the rate to a proportion: 206.5 cases per 100,000 is equal to 206.5 / 100,000 = 0.002065.

Multiply the proportion by the population size: 0.002065 * 70,000,000 = 144,550

Therefore, based on an age-adjusted mortality rate of 206.5 cases per 100,000 and an estimated adult population of 70,000,000 in the Philippines in 2025, we would estimate approximately 144,550 deaths due to chronic liver disease in that population, after adjusting for age.

Key Points to Remember:

The Standard Population: The estimated number of deaths is still influenced by how well the age structure of the Philippines' adult population in 2025 aligns with the standard population used for the age adjustment.
Rate per Different Unit: The unit here is "per 100000". Make sure to adjust the conversion to a proportion accordingly.
Direct Estimation: Age-adjusted rates provide a standardized measure of mortality risk, and by applying them to a specific population, you can estimate the expected number of deaths for that cause, considering the population size.



* cases per 1,000,000, Age-adjusted Rate e.g. `age-adjusted rate of asthma mortality rate is 8.3 cases per 1000000`
question: `how about if a question is "age-adjusted rate of chronic liver disease mortality", and its datavalue 8.3, and datavaluetype is "age-adjusted rate", and datavalueunit is "cases per 1000000"?`

Age-Adjusted Rate: As before, this mortality rate has been statistically adjusted to remove the effect of different age distributions between populations, allowing for more meaningful comparisons.
8.3: This is the age-adjusted mortality rate.
Cases per 1000000: This is the unit of the rate. It means that for every 1,000,000 people in the standard population, 8.3 deaths are attributed to chronic liver disease, after accounting for age differences.
Converting to a Tangible Number for a Specific Population (Philippines, 2025, Adults >= 18 - assuming an estimated adult population of 70,000,000):

To estimate the number of chronic liver disease deaths in the adult population of the Philippines in 2025 based on this rate:

Convert the rate to a proportion: 8.3 cases per 1,000,000 is equal to 8.3 / 1,000,000 = 0.0000083.

Multiply the proportion by the population size: 0.0000083 * 70,000,000 = 581

Therefore, based on an age-adjusted mortality rate of 8.3 cases per 1,000,000 and an estimated adult population of 70,000,000 in the Philippines in 2025, we would estimate approximately 581 deaths due to chronic liver disease in that population, after adjusting for age.

Key Points to Remember:

The Standard Population: The estimated number of deaths is still influenced by how well the age structure of the Philippines' adult population in 2025 aligns with the standard population used for the age adjustment.
Rate per Different Unit: The unit here is "per 1,000,000". Make sure to adjust the conversion to a proportion accordingly.
Direct Estimation: Age-adjusted rates provide a standardized measure of mortality risk, and by applying them to a specific population, you can estimate the expected number of deaths for that cause, considering the population size.



* cases per 1,000, Crude Rate e.g. `crude rate of hospitalization for heart failure among medicare-eligible persons aged >= 65 years is 16.5 cases per 1000`
question: `how about if a question is "crude rate of hospitalization for heart failure among medicare-eligible persons aged >= 65 years", and its datavalue 16.5, and datavaluetype is "crude rate", and datavalueunit is "cases per 1000"?`

Okay, let's break down the interpretation of a crude rate with a unit of "cases per 1000".

Interpretation:

Crude Rate: This means the rate is calculated based on the total number of events (hospitalizations for heart failure) occurring within the entire population of interest (Medicare-eligible persons aged >= 65 years) without any adjustment for factors like age distribution within that specific population.
16.5: This is the crude rate of hospitalization for heart failure.
Cases per 1000: This is the unit of the rate. It means that for every 1000 Medicare-eligible persons aged 65 years or older in the population, there are 16.5 hospitalizations for heart failure.
Converting to a Tangible Number for a Specific Population (Mandaluyong City, Metro Manila, Philippines, 2025 - We'll need to estimate the Medicare-eligible population aged >= 65 years):

To estimate the number of hospitalizations for heart failure in the Medicare-eligible population aged 65 and above in Mandaluyong City in 2025, we need an estimate of that specific population.

Let's make a hypothetical assumption: Suppose there are approximately 20,000 Medicare-eligible persons aged 65 years or older in Mandaluyong City in 2025.

Now we can estimate the number of hospitalizations:

Convert the rate to a proportion per individual: 16.5 cases per 1000 people is equal to 16.5 / 1000 = 0.0165 hospitalizations per person.

Multiply the proportion by the specific population size: 0.0165 * 20,000 = 330

Therefore, based on a crude hospitalization rate of 16.5 cases per 1000 Medicare-eligible persons aged >= 65 years and a hypothetical population of 20,000 individuals in that demographic in Mandaluyong City in 2025, we would estimate approximately 330 hospitalizations for heart failure in that group.

Key Points to Remember:

Crude Rate Specificity: This rate applies to the entire Medicare-eligible population aged 65 and above. It doesn't account for differences in risk factors or age within that broad group.
Population Specificity: You must apply this rate to the specific population of interest (Medicare-eligible persons aged >= 65 in Mandaluyong) to get a tangible number for that location. Using the general adult population of Mandaluyong would be inaccurate as the rate is specific to the Medicare-eligible older adult group.
Data Availability: The accuracy of your estimate heavily relies on having a reasonably accurate estimate of the Medicare-eligible population aged 65 and above in Mandaluyong City for the year 2025. This kind of specific demographic data might require local government or health authority statistics.

* cases per 10,000, Crude Rate e.g. `crude rate of Emergency department visit rate for asthma is 132.33 cases per 10000`
same idea as above i.e. (132.33 / 10000) * population of those had emergency department visits
* cases per 100,000, Crude Rate e.g. `crude rate of mortality from total cardiovascular diseases is 53 cases per 100000`
* cases per 1,000,000, Crude Rate e.g. `crude rate of asthma mortality rate is 9.3 cases per 1000000`

* cases per 1,000,000, Adjusted by age, sex, race and ethnicity e.g. `cases per 1000000 of Incidence of treated end-stage renal disease attributed to diabetes adjusted by age, sex, race and ethnicity is 156.8`
* cases per 1,000,000, Number e.g. `cases per 1000000 of Incidence of treated end-stage renal disease is 1125`

*NOTE: annual is yearly*
* cases per 100,000, Average Annual Age-adjusted Rate e.g. `Average yearly age-adjusted rate of Invasive cancer (all sites combined), mortality is 11.9 cases per 100000`
same idea (11.9 / 100000) * population count = total cases in the population  

* cases per 100,000, Average Annual Crude Rate e.g. `Average yearly Crude Rate of Invasive cancer of the female breast, incidence is 13.3 cases er 100000`

* cases per 100,000, Number e.g. `mortality from heart failure is 3799 cases per 100000`
* per 100,000 residents, Number e.g. `Number of farmers markets per 100,000 residents is 7.3`

and other miscellaneous pairs would be:
* gallons, Per capita alcohol consumption e.g. per capita alchol consumption aged >= 14 years is 2.1 gallons
* pack sales per capita, Number e.g. sale of cigarette packs is 44.3 pack sales per capita
* Years, Number e.g. Life expectancy at birth is 78.2 years
* `$`, US Dollars e.g. Amount of alcohol excise tax by beverage type (wine) is 0.72$

maybe the idea now is dahil maraming questions but of same topics we need to group them according to their topic ID
and then group them  

* following are the unique combinations fo datavalunit and datavaluetype and we will 
```
DataValueUnit, DataValue
%, Prevalence
%, Percent
%, Age-adjusted Prevalence
%, Crude Prevalence, 

Number, Median
Number, Age-adjusted Mean
Number, Mean
Number, Number

These require the Population counts based on their stratification
cases per 1,000, Age-adjusted Rate
cases per 10,000, Age-adjusted Rate
cases per 100,000, Age-adjusted Rate
cases per 1,000,000, Age-adjusted Rate

cases per 1,000, Crude Rate
cases per 10,000, Crude Rate
cases per 100,000, Crude Rate
cases per 1,000,000, Crude Rate

cases per 1,000,000, Adjusted by age, sex, race and ethnicity

cases per 100,000, Average Annual Age-adjusted Rate

cases per 100,000, Average Annual Crude Rate

cases per 100,000, Number

per 100,000 residents, Number

gallons, Per capita alcohol consumption
pack sales per capita, Number
Years, Number
`$`, US Dollars
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

take note that spark 3.5.5 requires java 8 or 17 and later. When on the downloads page it will also indicate that it is prevuilt for hadoop 3.3 and later meaning we have to install hadoop 3.3.0 and later releases (but specifically the winutils.exe file and hadoop.dll files as it requires these in order to run spark in windows) and must be under these release versions.

steps for setting up apache spark from scratch locally in windows
- java development kit 17: https://www.oracle.com/java/technologies/javase/jdk17-archive-downloads.html
- apache spark: https://spark.apache.org/downloads.html
- hadoop winutils: https://github.com/kontext-tech/winutils/blob/master/hadoop-3.3.0/bin/winutils.exe
- hadoop winutils: https://github.com/kontext-tech/winutils/blob/master/hadoop-3.3.0/bin/hadoop.dll. Note that whatever spark version you download must be compatible with hadoop version e.g. if we go to with a 3.5.5 version of apache spark we check https://github.com/apache/spark/blob/v3.5.5/pom.xml and see the the ff.
```
...
<slf4j.version>2.0.7</slf4j.version>
<log4j.version>2.20.0</log4j.version>
<!-- make sure to update IsolatedClientLoader whenever this version is changed -->
```
**`<hadoop.version>3.3.4</hadoop.version>`**
```
<!-- SPARK-41247: When updating `protobuf.version`, also need to update `protoVersion` in `SparkBuild.scala` -->
<protobuf.version>3.23.4</protobuf.version>
<protoc-jar-maven-plugin.version>3.11.4</protoc-jar-maven-plugin.version>
...
```
here it indicates that under the 3.5.5 version or branch of spark it needs the hadoop version to be 3.3.4 and so we search for hadoop with version 3.3.4 and download it specifically the `.tgz` it usually is encased in at https://hadoop.apache.org/release/3.3.4.html

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
- because we will be running spark on a windows machine locally it is imperative that even if we already added the `HADOOP_HOME` system environment variable and added added `%HADOOP_HOME%\bin` containing our `hadoop.dll` and `winutils.exe` to our PATH system environment variable, we must also add the `hadoop.dll` to our `system32` folder as this will be needed in order to prevent future errors like `java.lang.UnsatisfiedLinkError` this is because spark requires certain Hadoop binaries (particularly `winutils.exe` & `hadoop.dll`) to simulate the Hadoop environment that Spark relies on. This step is essential for enabling Spark to run properly on Windows, as these binaries help in file system operations like setting permissions configurations that are otherwise required in unix and posix systems like those that use Linux OS. This is why in a docker container/linux machien this wouldn't be the case; all these unnecessary hardwork to install spark as spark and hadoop distributed file system (HDFS) already works in synergy with linux os. Lesson learned don't try to make Spark applications for Windows unless you need to. But you can avoid the pain if you follow the above steps so maybe it's not so bad. Good luck and thanks for reading! **https://medium.com/data-engineer-things/deploying-a-spark-based-application-as-a-windows-application-4ddc88231784**


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

* in order to read excel files spark needs to download and then include external packages which can be run by command `spark-submit --packages com.crealytics:spark-excel_2.12:3.5.1_0.20.4 transform_us_populations_per_state_by_sex_age.py <arg name> <value>`. In order to execute this spark script it is imperative to add this packages argument as this indicates the dependency that we need installed when running this script transforming excel files

* `com.crealytics:spark-excel_2.12:3.5.1_0.20.4` is actuall ythe package we need to read these excel files using spark where `com.crealytics` is the group id, `spark-excel_2.12` is the artifact id, and `3.5.1_0.20.4` is the release version

* but how come this works when using `spark-submit` but when using jupyter notebooks the extra packages are not downloaded 

* this is why if your going to use spark with airflow or in a docker container it is better to install it globally in the container rather than as a package and then set the paths manually

* AFAIK the way to dockerize everything every dependency of this data pipeline would be to do the ff.
- download spark, hadoop, and java development kit
1st reference
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

2nd reference
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
- extract the components of the downloaded tar.gz or .tgz file using `tar xvzf <name of compressed tar file> --directory <directory in linux/docker container you want the tar file contents to be dumped once extracted>`
- know where each package namely spark, hadoop, and jdk was installed and copy its path and use this path to set environment variables in the docker file e.g. SPARK_HOME, JAVA_HOME, PYSPARK_HOME
- somehow you're going to need to change permissions when copying, moving, and reading the files downloaded in the container so you have to learn `chmod` and other commands like it 

* `-L`, `--location` - Allow curl to follow any redirections. E.g. `curl -L https://example.com` 
* `f`, `--fail` - If the server returns an error, curl fails silently and returns error 22. e.g. `curl --fail https://example.com`
* `-o`, `--output` <file> - Store output in a file. The output is not shown in stdout. E.g. `curl -o file https://example.com -o file2 https://example.net`
* `-O`, `--remote-name`	- Specify that the local file should have the name of the remote file that was downloaded. E.g. `curl -O https://example.com/filename` otherwise we can provide our own file name using the `-o` argument
* we can combine multiple arguments in one, using only a hyphen and the letters upper or lowercase that represent a specific argument e.g. `curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.0.0/docker-compose.yaml'` means we will download a file in this location and that the local file should have this same remote files name and if it returns an error it will do so silently and return only `22``

* with the `chmod` command you can use either numeric or symbols to change permissions of a file or directory in linux
* `ls` command with argument `-l` indicates that we want to list the files in a directory with meta data such as the permission level of each user for a file or files
* there are 3 types of users, root, group, and other users
* when we see the meta data of a file using `ls -l`, it might list the ff. examples
- `---------- 1 <e.g. > <e.g. > <timestamp e.g. 24 Oct 1 22:31><name of file e.g. spark-3.5.1-bin-hadoop3.tgz>`
- `-rwxrwxrwx 1 <e.g. > <e.g. > <timestamp e.g. 24 Oct 1 22:31><name of file e.g. spark-3.5.1-bin-hadoop3.tgz>`
- `drwxr-xr-x 4 <e.g. > <e.g. > <timestamp e.g. 24 Oct 1 22:31><name of file e.g. spark-3.5.1-bin-hadoop3.tgz>`
as you can see the first char can either be `d` or `-` where `d` just means the listed content is a directory and a `-` is a file. The rest of the 9 chars moreover are what's more important because when we divide this into 3 groups we get exactly three divisions of `---` where each hyphen represents a permission a user can apply on a file such as read represented as `r`, write repd. as `w`, and execute repd. as `x` and each group of `---` is what represents the type of user such as root user or the admin of the system which can usually virtually have all permissions like reading, writing, and executing files where a file woudl have `-rwx------`, the grouped users, and other users in this order.
- in the numeric way of adding because there are always 3 hyphens per division or user type each hyphen can be represented as an octal number from 0 to 7. E.g. the `755` in `chmod 755` when converted to binary is 111 101 101 because 111 when converted to back to octal is 7 since $1(2*2) + 1(2*1) + 1(2*0) = 1(4) + 1(2) + 1(1) = 7$, 101 when converted back to octal is 5 since $1(2*2) + 0(2*1) + 1(2*0) = 1(4) + 0(2) + 1(1) = 5$. this means that since there are 3 binary digits each digit represents in order the read, write, and execute permissions, so if we were to have a user only have read and execute permissions we would have the binary value 101 and when converted to octal would be 5 and then this 5 we use for that specific user be it in our root,  grouped user, or other user divisions e.g.
```
[0-7]  [0-7]   [0-7]
  |      |       |
root  grouped  other  
```
when we want a user specifically a root user and the other users to have all permissions but except this root to have write only we would have a binary value 010 and when converted to octal would be 2 and when we run the command to change the permission since we want all other users to retain their permission we write 277 overall the command would be `chmod 277 <name of file or directory>`
- now for an easier way using symbolic mode we can change the permission by indicating either `u`, `g`, `o`, two of these or all of them in our chmod command. `u` represents a root user, `g` the grouped users, and `o` the other users then succeeded by a `+` char then the letters `r`, `w`, `x`, two of these, or all of them which we know represent read, write, and execute permissions. So when we write `chmod ug+rw <name of file>` we are adding read & write permissions to hte root and grouped users. When we write `chmod o+rwx` we add read, write, execute permissions to the other users

* In Docker containers, Quarto refers to using the Quarto publishing system to create static and interactive content within a Docker environment. You can utilize Docker to render Quarto documents and projects, deploy them as static websites, or host interactive web applications with backends like R Shiny. 

* if an `java.lang.UnsatisfiedLinkError: 'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(java.lang.String, int)` error occurs it maybe because you don't have `hadoop.dll` inside your system32 folder

* Master URLs passed to Spark can be in one of the following formats:
- `local` - Run Spark locally with one worker thread (i.e. no parallelism at all).
- `spark://HOST:PORT` - Connect to the given Spark standalone cluster master. The port must be whichever one your master is configured to use, which is `7077` by default (which we know in airflow images is what is used).

* important configurations in spark
- `spark.driver.cores` - 1 - Number of cores to use for the driver process, only in cluster mode.	1.3.0
- `spark.driver.maxResultSize` - 1g - Limit of total size of serialized results of all partitions for each Spark action (e.g. collect) in bytes. Should be at least 1M, or 0 for unlimited. Jobs will be aborted if the total size is above this limit. Having a high limit may cause out-of-memory errors in driver (depends on spark.driver.memory and memory overhead of objects in JVM). Setting a proper limit can protect the driver from out-of-memory errors.	1.2.0
- `spark.driver.memory` - 1g - Amount of memory to use for the driver process, i.e. where SparkContext is initialized, in the same format as JVM memory strings with a size unit suffix ("k", "m", "g" or "t") (e.g. 512m, 2g).
Note: In client mode, this config must not be set through the SparkConf directly in your application, because the driver JVM has already started at that point. Instead, please set this through the --driver-memory command line option or in your default properties file.
- `spark.executor.memory` - 1g - Amount of memory to use per executor process, in the same format as JVM memory strings with a size unit suffix ("k" for kilobytes, "m" for megabytes, "g" for gigabytse or "t" for terabytes) (e.g. 512m, 2g).
- `spark.executor.cores` - 1 - in YARN mode, all the available cores on the worker in standalone and Mesos coarse-grained modes. The number of cores to use on each executor. In standalone and Mesos coarse-grained modes, for more detail, see this description.

* the erason why you get out of memory errors in your script is because even though you are yes not using broadcast joins or collecting the results from each executor/worker you are implicitly collecting the results in a list and then concnatenating these resultant tables into one big table thus overwhelming the driver in the spark cluster. 

- Accumulation of Data: As you process more data and generate more of these lists of lists of DataFrames, the memory consumption on the driver grows proportionally to the total size of the data contained within all those DataFrames.
- Large Concatenation Operation: When you finally perform the concatenation of all these DataFrames at once (using pd.concat() if you're bringing them to the driver as Pandas DataFrames, or even Spark's union() if you're keeping them as Spark DataFrames but performing the union on the driver), the driver needs to hold the entire combined dataset in its memory before writing it to Parquet. This is exactly the "one big table" scenario you mentioned.
- Driver Memory Limits: The Spark driver has a finite amount of memory allocated to it (controlled by the spark.driver.memory configuration). If the total size of the data you are accumulating and then trying to concatenate exceeds this limit, the driver will run out of memory and throw an OOM error.

What you could do is either do iterative saving processing the data in chunks as a Spark DataFrame and saving it to a separate Parquet file. You can then read all these Parquet files later for further analysis. This avoids accumulating everything in the driver's memory. Another is by increasing the Driver Memory (as a last resort and with caution). While increasing `spark.driver.memory` might temporarily alleviate the issue for moderately larger datasets, it's not a sustainable long-term solution for truly large-scale data. It also makes your driver a potential bottleneck and point of failure. It's generally better to address the underlying data processing pattern.

a solution could be is to save the dataframe of each state of specific year ranges. Since there are 51 states 24 years this would be
(10 * 51) + (10 * 51) + (4 * 51) = 1224 dataframes all in all

* `=COUNTIF(<range of cells>, "*")` actually counts cells given range of cells that are not empty strings 

* finally found the complete population data with all age years, sex, ethnicity, and origin at https://www.census.gov/programs-surveys/popest/technical-documentation/research/evaluation-estimates/2010-evaluation-estimates.html, https://www.census.gov/data/tables/time-series/demo/popest/2010s-state-detail.html, and https://www.census.gov/data/tables/time-series/demo/popest/2020s-state-detail.html

only thing left to do is to preprocess these csv's again with spark

* if we have 24 gb of installed ram and 23 gb usable and have 8 cores in our CPU we can utilize this memory to partition it across all 8 cores of our cpu for concurrent processing in spark. We need to also take note of background processes in our task manager taking up our memory so if need be we have to end these background processes that take up too much memory in order to free up space for our spark driver memory and spark executor memory 

* so doing the following in spark isn't good for the driver memory since it is what collects all executors processed dataframes. And if we try and concatenate all our dataframes without sufficient memory for the driver it will result in an out of memory error whereas if we can partition our memory with our executors and save it instead by chunks then we can avoid this error
```
    # loop through year_ranges
    for year_range in year_range_list:
        # 2000 - 2010
        if year_range == "2000-2009":
            cols_to_remove = [1, 12, 13]
            populations = populations_by_sex_age_00_10 

        # 2010 - 2019
        elif year_range == "2010-2019":
            cols_to_remove = [1, 2, 3, 4, 5, 6, 7, 10, 13, 16, 19, 22, 25, 28, 31, 34]
            populations = populations_by_sex_age_10_19

        # 2020 - 2023
        elif year_range == "2020-2023":
            cols_to_remove = [1, 2, 3, 4, 7, 10, 13]
            populations = populations_by_sex_age_20_23

        # concurrently process state populations by year range
        state_populations_df = get_state_populations(
            DATA_DIR, 
            spark, 
            cols_to_remove, 
            populations, 
            year_range,
            callback_fn=process_population_by_sex_age_table)
    
        # collect state populations from all years using list
        state_populations_all_years.append(state_populations_df)

    # concatenate all state populations from all year ranges
    final = reduce(DataFrame.unionByName, state_populations_all_years)
    final.show(final.count())
```

* if we ever get the ff. error when we jsut merely create a dataframe and show it e.g. `strat_perms_df = session.createDataFrame(strat_perms, ["Sex", "Ethnicity", "Origin"]) strat_perms_df.show()` then it means according to this post **https://stackoverflow.com/questions/77369508/python-worker-keeps-on-crashing-in-pyspark** that our installed python that spark uses may not be compatible with it, this was the case when I had python `3.12.6` installed but when I downgraded to `3.11.5` or `3.11.8` or any other `3.11.x` version accordin the the post then the `EOF exception` disappears 

```
23/10/26 14:10:52 ERROR Executor: Exception in task 0.0 in stage 0.0 (TID 0)/ 1]
org.apache.spark.SparkException: Python worker exited unexpectedly (crashed)
        at org.apache.spark.api.python.BasePythonRunner$ReaderIterator$$anonfun$1.applyOrElse(PythonRunner.scala:612)
        ...

23/10/26 14:10:52 ERROR TaskSetManager: Task 0 in stage 0.0 failed 1 times; aborting job
Traceback (most recent call last):
  File "c:\Users\<USER>\PySparkTesting\spark_test.py", line 14, in <module>
    df.show()
  File "C:\Users\<USER>\AppData\Local\Programs\Python\Python312\Lib\site-packages\pyspark\sql\dataframe.py", line 959, in show
    print(self._jdf.showString(n, 20, vertical))
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\<USER>\AppData\Local\Programs\Python\Python312\Lib\site-packages\py4j\java_gateway.py", line 1322, in __call__
    return_value = get_return_value(
                   ^^^^^^^^^^^^^^^^^
  File "C:\Users\<USER>\AppData\Local\Programs\Python\Python312\Lib\site-packages\pyspark\errors\exceptions\captured.py", line 179, in deco
    return f(*a, **kw)
           ^^^^^^^^^^^
  File "C:\Users\<USER>\AppData\Local\Programs\Python\Python312\Lib\site-packages\py4j\protocol.py", line 326, in get_return_value
    raise Py4JJavaError(
py4j.protocol.Py4JJavaError: An error occurred while calling o42.showString.
: org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 0.0 failed 1 times, most recent failure: Lost task 0.0 in stage 0.0 (TID 0) 
(LAPTOP-3GL266K9 executor driver): org.apache.spark.SparkException: Python worker exited unexpectedly (crashed)
        at org.apache.spark.api.python.BasePythonRunner$ReaderIterator$$anonfun$1.applyOrElse(PythonRunner.scala:612)
        ...
        at java.lang.Thread.run(Thread.java:750)
Caused by: org.apache.spark.SparkException: Python worker exited unexpectedly (crashed)
        at org.apache.spark.api.python.BasePythonRunner$ReaderIterator$$anonfun$1.applyOrElse(PythonRunner.scala:612)
        ...
        ... 1 more
Caused by: java.io.EOFException
        at java.io.DataInputStream.readInt(DataInputStream.java:392)
        at org.apache.spark.api.python.PythonRunner$$anon$3.read(PythonRunner.scala:774)
        ... 26 more
```

* `<groupid>:<artifactid>:<release version>` e.g. `com.google.guava:guava:33.4.8-jre` which whne translated to `.xml` is...
```
<dependency>
    <groupId>com.google.guava</groupId>
    <artifactId>guava</artifactId>
    <version>23.6-jre</version>
</dependency> 
```

example 2: 
`org.apache.hadoop:hadoop-aws:3.3.0` that we include during spark submit in `.xml` is 
```
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-aws</artifactId>
    <version>3.3.0</version>
</dependency>
```

* to specify multiple packages in spark-submit the list of packages should be separated using commas without whitespaces (breaking lines should work just fine) for instance: `spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.0,com.google.guava:guava:23.6-jre test_s3.py`

* reading csv files from s3 requires many packages 
- `org.apache.hadoop:hadoop-aws:3.3.0` this is the right version
- `com.amazonaws:aws-java-sdk-bundle:1.11.563` tried version 1.12.367
- `org.apache.httpcomponents:httpcore:4.4.16`
- `com.google.guava:guava:33.4.0-jre` tried version 33.4.0-jre, 27.0-jre, 33.3.1-jre, 33.3.0-jre
 
- the maven package `org.apache.hadoop:hadoop-aws:3.3.0` when we read the packages compile dependencies requires `com.amazonaws:aws-java-sdk-bundle:1.11.563` so it makes sense to also include this in our spark submit

* solved the java no such method error com google common base preconditions by installing guava 27.0-jre version since by default spark 3.5.5 uses guava 14 which does not have the common base preconditions check argument methodas the error says but guava 27 does and so installing this is what is required to sovle the above problem. All in all thhe command would be `spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.0,com.amazonaws:aws-java-sdk-bundle:1.11.563,com.google.guava:guava:27.0-jre test_s3.py`. Or we can go to the location where we installed spark directly and in it go to its jars folder and locate the outdated guava jar, remove it, download the updated guava jar file at `https://repo1.maven.org/maven2/com/google/guava/guava/27.0-jre/` and place it inside the jars folder in the root spark directory. We can omit the com.google.guava:guava:27.0-jre package as it will be included in the jars folder in our root spark directory, as a matter of fact we can download and add all these packages manually and move it all to the root spark directory so we don't haev to explicitly specify these packages anymore during our `spark-submit` and be downloaded over and over.

* to install aws cli run `msiexec.exe /i https://awscli.amazonaws.com/AWSCLIV2.msi` or download the AWS CLI MSI installer for Windows (64-bit) at `https://awscli.amazonaws.com/AWSCLIV2.msi`

* always take note if you are assigning the right value to the `fs.s3a.access.key` and `fs.s3a.secret.key` keys as even interchanging your IAM user credentials will result in `The AWS Access Key Id you provided does not exist in our records` error. 

* another error `java.lang.NoSuchMethodError: org.apache.hadoop.util.SemaphoredDelegatingExecutor.<init>` when writing parquet file to aws s3 via spark. using `hadoop-aws 3.2.2` might work. Solved since spark is 3.5.5, hadoop.dll and winutils.exe under hadoop 3.3.0, and jdk 17, version of compatible external jars for these versions are hadoop-aws 3.3.4, aws-java-sdk-bundle 1.11.563, and guava 27.0,

* another error `Class org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider not found` may occur during spark-submit, which occurs mainly because of the external jar hadoop-aws package not being of a compatible version. This must be the same version or above the hadoop version our spark environment uses, in this case our hadoop had winutils.exe and hadoop.dll be under hadoop version 3.3.0 this means that when we spark-submit our hadoop-aws external jar package must also be 3.3.0 and above (note that 3.3.4 already works in order to avoid the above SemaphoredDelegatingExecutor when writing parquet to s3 using spark)

* Permissions: The error could be related to permissions. If your IAM policies or bucket policies have changed, or if the object's ACL has been modified, you might receive a NoSuchKey error instead of an Access Denied error. This is because S3 doesn't always distinguish between non-existent objects and objects you don't have permission to access.
```
cdi_url = "s3://chronic-disease-analyses-bucket/cdi-data-transformed/CDI.parquet/"
cdi_url

# note that if we only specify a string instead of a list in read_parquet it must be enclosed in a quote or double quotes
query = f"""
    CREATE OR REPLACE TABLE CDI AS
    SELECT *
    FROM read_parquet('{cdi_url}', union_by_name=True, filename=False)
"""

duckdb.sql(query)
```

but the reason the above code could not work before is because we are reading a folder with the partitioned files inside since it was after all done by spark. The way we could read this in DuckDB is to use a glob expression `*` that reads anything and in particular `*.parquet` which reads anything ending in a `.parquet` file and then unionizing these parquet files by column name. So we have to actually read an object or a file in s3 which we can achieve by using `*.parquet`, in order to actually load the parquet files we have just dumped to s3 in OLAP DBs like DuckDB. So our url would be instead `"s3://chronic-disease-analyses-bucket/cdi-data-transformed/CDI.parquet/*.parquet"`

* `IOException: IO Error: Could not establish connection error for HTTP HEAD to 'https://chronic-disease-analyses-bucket/cdi-data-transformed/CDI.parquet/'` with status 1096710176. Is solved by adding `ENDPOINT` key to creating a `SECRETS` object in duckdb e.g. 
```
duckdb.sql(f"""
    CREATE SECRET (
        TYPE s3,
        KEY_ID '{credentials["aws_access_key_id"]}',
        SECRET '{credentials["aws_secret_access_key"]}',
        REGION '{credentials["region_name"]}',
        ENDPOINT 's3.{credentials["region_name"]}.amazonaws.com'
    );
""")
```

* in order to use duckdb in powerbi we have to use its cloud based version motherduck which requires us to now create a motherduck account and then create an access token at https://app.motherduck.com/settings/tokens in order to connect to the data
```
con <- dbConnect(duckdb(), dbdir = "quack.duckdb", read_only = FALSE)

My expectation would be an error with the second attempt, because duckdb only allows one write connection to avoid conflicts.

It used to give you the The process cannot access the file because it is being used by another process. error posted above, but now in v0.10.1 it doesn't error (or give a warning) at all.
```

* solving powerbi `Error: File is already open in C:\Program Files\WindowsApps\Microsoft.MicrosoftPowerBIDesktop_2.142.1277.0_x64__8wekyb3d8bbwe\bin\Microsoft.Mashup.Container.NetFX45.exe` error: https://github.com/duckdb/duckdb-r/issues/56

* There are three ways in which DAX formulas and expressions can be written in Power BI:
- Calculated Tables - These calculations will add an additional table to the report based on a formula. 
- Calculated Columns - These calculations will add an additional column to a table based on a formula. These columns are treated like any other field in the table. 
- Measures - These calculations will add a summary or aggregated measure to a table based on a formula.

To add any one of these types of calculations to a model, navigate to the Modeling tab of the ribbon. Here, you will find three choices for adding a new measure, calculated column, or table.

So dito siya by clicking a table loaded in powerbi > going to ribbon and selecting table tools > selecting either new measure, new column, or new table. A measure basically calculates a scalar or single value from a column/s or table/s i.e. aggregating column A, aggregating column B and then combining these aggregated values to a single value

* if we wanted to create a new table based on a DAX expression we would select a table temporarily > go to ribbon > create new table > in the bar we can expand it to write a longer query i.e.

```
TestTable = ADDCOLUMNS(
	SUMMARIZE(
		'chronic_disease_analyses_db   main   Population',
		'chronic_disease_analyses_db   main   Population'[StateID],
		'chronic_disease_analyses_db   main   Population'[Year],
		"TotalStatePopulation", SUM('chronic_disease_analyses_db   main   Population'[Population])
	),
	"State", LOOKUPVALUE(
		'chronic_disease_analyses_db   main   PopulationState'[State],
		'chronic_disease_analyses_db   main   PopulationState'[StateID],
		'chronic_disease_analyses_db   main   Population'[StateID]
	)
)
```

* In DAX, measures the output as always is a single, scalar value (e.g., a total sum, an average, a count, a percentage). It also automatically adapts its calculation based on the filters applied by slicers, rows in a table visual, columns in a bar chart, etc. Recall that a slicer from the list of visualizers powerbi has to offer is like a fully functional GUI for a WHERE clause in SQL, where we can filter our data in our tables to pick what unique data we want

* you can fitler the table based on its columns to view only rows you want, but does not reflect the operation on the table itself. Just go to table view > select column > click down arrow next to it > select the unique values you only want to view 

* to use a map in powerbi that uses a gradient map we need to go to settings and

* to publish report to powerbi services we go to home in ribbon and click publish, sign in with our microsoft email, then choose our workspace, 

* go to file tab in workspace under your published report > embed report > publish to web. This will provide you with the `iframe` html element which embeds your published report which you can use in any html web page, or web app like react or svelte. But since this is only a dashboard a simple html file will do. We create one named index in our current projects directory in our local machine, then go to vercel and add new project. There will be no used preset to use for this deployment like svelte, react, etc. but only other, or no preset. Vercel will automatically detect the html we want to render through its file name, named `index` we just need to specify in our deployment settings where this file lives. Once deployed we will see our dashboard publicly.

* we can actually get the median, mode, kurtosis, entropy with DuckDB's built in functions
```
CREATE TEMPORARY SEQUENCE IF NOT EXISTS id_sequence START 1;
  
-- Create the transactions table
CREATE TEMPORARY TABLE IF NOT EXISTS transactions (
    transaction_id INT DEFAULT nextval('id_sequence') PRIMARY KEY,
    user_id INT NOT NULL,
    transaction_date DATE NOT NULL,
    amount DECIMAL(10, 2) NOT NULL
);

-- Insert sample data
INSERT INTO transactions (user_id, transaction_date, amount) VALUES
(101, '2024-01-01', 10.50),
(101, '2024-01-02', 15.00),
(101, '2024-01-03', 20.00),
(101, '2024-01-05', 12.75), -- Gap on Jan 4
(101, '2024-01-06', 18.20),
(101, '2024-01-07', 25.00),
(101, '2024-01-08', 30.00),
(101, '2024-01-09', 11.00),
(101, '2024-01-12', 14.50), -- Another gap
(101, '2024-01-13', 22.00),
(101, '2024-01-14', 16.80),
(102, '2024-01-01', 5.00),
(102, '2024-01-03', 7.50),
(102, '2024-01-04', 12.00),
(102, '2024-01-08', 9.00),
(102, '2024-01-09', 11.20),
(102, '2024-01-10', 8.00);
```

```
SELECT MEDIAN(amount) as median FROM transactions
```

as opposed to using when in using other SQL flavors

```
SELECT PERCENTILE(amount, 0.5) AS median
```

```
SELECT 
  *,
  AVG(amount) OVER(
    PARTITION BY user_id 
    ORDER BY transaction_date ASC 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS ma_7,
  MEDIAN(amount) OVER(
    PARTITION BY user_id 
    ORDER BY transaction_date ASC 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS mm_7
FROM transactions
```

```
transaction_id	user_id	transaction_date	amount	ma_7	mm_7
12	102	2024-01-01	5.00	5.0	5.00
13	102	2024-01-03	7.50	6.25	6.25
14	102	2024-01-04	12.00	8.166666666666666	7.50
15	102	2024-01-08	9.00	8.375	8.25
16	102	2024-01-09	11.20	8.94	9.00
17	102	2024-01-10	8.00	8.783333333333333	8.50
1	101	2024-01-01	10.50	10.5	10.50
2	101	2024-01-02	15.00	12.75	12.75
3	101	2024-01-03	20.00	15.166666666666666	15.00
4	101	2024-01-05	12.75	14.5625	13.87
5	101	2024-01-06	18.20	15.29	15.00
6	101	2024-01-07	25.00	16.908333333333335	16.60
7	101	2024-01-08	30.00	18.77857142857143	18.20
8	101	2024-01-09	11.00	18.85	18.20
9	101	2024-01-12	14.50	18.77857142857143	18.20
10	101	2024-01-13	22.00	19.064285714285713	18.20
11	101	2024-01-14	16.80	19.642857142857142	18.20
```

## Spark Optimization   
* Say I have 24 gb ram installed and 16 gb is currently usable because of other background processes and I have 8 cores in the CPU. Rule is to leave out 1 gb and 1 core for hadoop distributed file system processes and OS daemon processes during spark submissions. So we would have 15 gb and 7 cores to work with

When we define executors e.g. 1 then 15 gb and 7 would have to be divided by 1 since these spaces and cores will be used by an executor so it will be 15 gb and 7 cores to be used by one executor still

When we want 5 executors our cores will have to be partitioned across all these executors and sicne the num of execs is an odd number some executors will have unequal amount of cores compared to other executors. In this case 15 gb and 7 both divided by 5 will yield 5 executors each having 1 1 1 2 and 2 cores respectively and 3 3 3 3 and 3 gb memory respectively. 

Notice how our number of executors will always depend on the number of cores in our cpu, in that **we can always add any number of executors so long as we still have cores available but we cannot add any more number of cores than the amount our cpu has just because we want more executors**. 

Thin executor with 15 gb memory and 7 cores means each executor uses 1 core at the most and 1 gb memory at the most resulting in 7 executors using only 1 core and each having only 1 gb memory. Notice again that our number of executors will always depend on the number of cores as these courses cores are distributed evenly across these executors and if there are more executors than there is cores to distribute then it is a violation of this rule

* Spark physical plan. Physical plan is read bottom to up. Catalyst optimizer is divided into 3 parts which is what precede the physical plan of spark. 3 arts are unresolved/unoptimized logical plan and catalog (if data exists in the first place), logical plan, optimized logical plan respectively

* https://medium.com/geekculture/pandas-vs-pyspark-fe110c266e5c 

## Docker
What does ${SPARK_HOME:-opt/spark/} mean in a dockerfile?

In a Dockerfile, ${SPARK_HOME:-/opt/spark/} is a standard shell parameter expansion syntax.

Let's break down what each part means:

$: Indicates a variable substitution.

{}: Braces are used to isolate the variable name and the parameter expansion operator, preventing ambiguity.

SPARK_HOME: This is an environment variable. By convention in the Spark ecosystem, SPARK_HOME is set to the directory where Spark is installed.

:-: This is a specific shell parameter expansion operator. It means:

"If SPARK_HOME is unset or null (empty string), then use the word that follows."
/opt/spark/: This is the default value that will be used if SPARK_HOME is not set or is empty.

Putting it all together:

${SPARK_HOME:-/opt/spark/} evaluates to:

The value of the SPARK_HOME environment variable, IF it is set and not empty.
OR, /opt/spark/, IF SPARK_HOME is either not set at all or is set to an empty string.
Why is this used in a Dockerfile?

It's a common practice to make Dockerfiles more robust and flexible:

Providing a Default Path: It ensures that certain commands or configurations that rely on SPARK_HOME (like navigating to the Spark installation directory) will always have a valid path, even if the SPARK_HOME environment variable isn't explicitly set earlier in the Dockerfile or by the base image.
Flexibility: It allows users or subsequent Dockerfile layers to override the SPARK_HOME variable if Spark is installed in a different location, without breaking the script if no override is provided.
Example usage in a Dockerfile:

You'll most commonly see this within RUN commands:

Dockerfile

# Option 1: Base image might not set SPARK_HOME, or it's being set later
# This RUN command will default to /opt/spark/ if SPARK_HOME is not defined.
RUN ls -l ${SPARK_HOME:-/opt/spark/}

# Option 2: SPARK_HOME is explicitly set
ENV SPARK_HOME /usr/local/spark
# Now, the expression will use /usr/local/spark
RUN ls -l ${SPARK_HOME:-/opt/spark/}
It's a powerful way to manage variable defaults and make your container builds more adaptable.

what does RUN export SPARK_HOME do or JAVA_HOME do?

Show thinking
You're asking about a common point of confusion when writing Dockerfiles!

Let's break down what RUN export SPARK_HOME= or RUN export JAVA_HOME= does, and why it's usually not the best way to set persistent environment variables in a Dockerfile.

Understanding the Components
RUN command:

In a Dockerfile, RUN executes commands in a new layer on top of the current image and commits the results. Each RUN command is a separate shell execution.
export command (shell built-in):

In a standard Linux shell (like bash), the export command marks a variable to be passed to child processes that are spawned from the current shell. It essentially makes the variable part of the environment of subsequent commands executed within that same shell session.
SPARK_HOME and JAVA_HOME:

These are standard environment variables:
JAVA_HOME: Points to the installation directory of the Java Development Kit (JDK) or Java Runtime Environment (JRE). Many Java applications and tools (including Spark) rely on this to find their Java dependency.
SPARK_HOME: Points to the root directory of a Spark installation. Spark scripts (like spark-submit) use this to locate their components.
What RUN export SPARK_HOME=... Actually Does in a Dockerfile
When you use RUN export SPARK_HOME=/path/to/spark, it means:

Docker executes a shell (e.g., /bin/sh -c) for this specific RUN command.
Within that single shell session, the SPARK_HOME variable is set and exported.
Any commands executed later in the same RUN instruction will see this SPARK_HOME variable.
However, once that RUN command finishes, the shell session (and its exported variables) is terminated. The variable SPARK_HOME will not be available in subsequent RUN instructions or in the final running container.

Example:

Dockerfile

# Dockerfile snippet
RUN export SPARK_HOME=/opt/spark && echo "Inside RUN: $SPARK_HOME"
RUN echo "Second RUN: $SPARK_HOME"
Output:

Step 1/2 : RUN export SPARK_HOME=/opt/spark && echo "Inside RUN: $SPARK_HOME"
 ---> Running in ...
Inside RUN: /opt/spark
...
Step 2/2 : RUN echo "Second RUN: $SPARK_HOME"
 ---> Running in ...
Second RUN:
...
As you can see, SPARK_HOME is gone in the second RUN command.

The Correct Way to Set Persistent Environment Variables
To set environment variables that persist across all subsequent RUN instructions and are available when the container is finally run, you should use the ENV instruction in your Dockerfile:

Dockerfile

# Correct way to set persistent environment variables
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64 # Example path
ENV SPARK_HOME /opt/spark # Example path

# Now, any subsequent RUN commands and the final container will have these variables set
RUN echo "Inside RUN: $SPARK_HOME"
CMD ["spark-submit", "--version"] # Will use SPARK_HOME
When Might RUN export Be Used (and why it's rare/specific)
Sometimes you might see RUN export used for very specific, temporary purposes where a variable is only needed for a single complex RUN command line:

Dockerfile

RUN export MY_TEMP_VAR="some_value" && \
    ./configure --with-my-feature=$MY_TEMP_VAR && \
    make install
In this case, MY_TEMP_VAR is only needed by the configure script within that single RUN instruction, and its persistence isn't required. But for JAVA_HOME and SPARK_HOME, which are fundamental for the application's runtime, ENV is the definitive solution.


*  this is what the entrypoint file made during docker compose up fo airflow contains
```
airflow@58b02296185b:/$ cat entrypoint
#!/usr/bin/env bash
AIRFLOW_COMMAND="${1:-}"

set -euo pipefail

LD_PRELOAD="/usr/lib/$(uname -m)-linux-gnu/libstdc++.so.6"
export LD_PRELOAD

function run_check_with_retries {
    local cmd
    cmd="${1}"
    local countdown
    countdown="${CONNECTION_CHECK_MAX_COUNT}"

    while true
    do
        set +e
        local last_check_result
        local res
        last_check_result=$(eval "${cmd} 2>&1")
        res=$?
        set -e
        if [[ ${res} == 0 ]]; then
            echo
            break
        else
            echo -n "."
            countdown=$((countdown-1))
        fi
        if [[ ${countdown} == 0 ]]; then
            echo
            echo "ERROR! Maximum number of retries (${CONNECTION_CHECK_MAX_COUNT}) reached."
            echo
            echo "Last check result:"
            echo "$ ${cmd}"
            echo "${last_check_result}"
            echo
            exit 1
        else
            sleep "${CONNECTION_CHECK_SLEEP_TIME}"
        fi
    done
}

function run_nc() {
    # Checks if it is possible to connect to the host using netcat.
    #
    # We want to avoid misleading messages and perform only forward lookup of the service IP address.
    # Netcat when run without -n performs both forward and reverse lookup and fails if the reverse
    # lookup name does not match the original name even if the host is reachable via IP. This happens
    # randomly with docker-compose in GitHub Actions.
    # Since we are not using reverse lookup elsewhere, we can perform forward lookup in python
    # And use the IP in NC and add '-n' switch to disable any DNS use.
    # Even if this message might be harmless, it might hide the real reason for the problem
    # Which is the long time needed to start some services, seeing this message might be totally misleading
    # when you try to analyse the problem, that's why it's best to avoid it,
    local host="${1}"
    local port="${2}"
    local ip
    ip=$(python -c "import socket; print(socket.gethostbyname('${host}'))")
    nc -zvvn "${ip}" "${port}"
}


function wait_for_connection {
    # Waits for Connection to the backend specified via URL passed as first parameter
    # Detects backend type depending on the URL schema and assigns
    # default port numbers if not specified in the URL.
    # Then it loops until connection to the host/port specified can be established
    # It tries `CONNECTION_CHECK_MAX_COUNT` times and sleeps `CONNECTION_CHECK_SLEEP_TIME` between checks
    local connection_url
    connection_url="${1}"
    local detected_backend
    detected_backend=$(python -c "from urllib.parse import urlsplit; import sys; print(urlsplit(sys.argv[1]).scheme)" "${connection_url}")
    local detected_host
    detected_host=$(python -c "from urllib.parse import urlsplit; import sys; print(urlsplit(sys.argv[1]).hostname or '')" "${connection_url}")
    local detected_port
    detected_port=$(python -c "from urllib.parse import urlsplit; import sys; print(urlsplit(sys.argv[1]).port or '')" "${connection_url}")

    echo BACKEND="${BACKEND:=${detected_backend}}"
    readonly BACKEND

    if [[ -z "${detected_port=}" ]]; then
        if [[ ${BACKEND} == "postgres"* ]]; then
            detected_port=5432
        elif [[ ${BACKEND} == "mysql"* ]]; then
            detected_port=3306
        elif [[ ${BACKEND} == "mssql"* ]]; then
            detected_port=1433
        elif [[ ${BACKEND} == "redis"* ]]; then
            detected_port=6379
        elif [[ ${BACKEND} == "amqp"* ]]; then
            detected_port=5672
        fi
    fi

    detected_host=${detected_host:="localhost"}

    # Allow the DB parameters to be overridden by environment variable
    echo DB_HOST="${DB_HOST:=${detected_host}}"
    readonly DB_HOST

    echo DB_PORT="${DB_PORT:=${detected_port}}"
    readonly DB_PORT
    if [[ -n "${DB_HOST=}" ]] && [[ -n "${DB_PORT=}" ]]; then
        run_check_with_retries "run_nc ${DB_HOST@Q} ${DB_PORT@Q}"
    else
        >&2 echo "The connection details to the broker could not be determined. Connectivity checks were skipped."
    fi
}

function create_www_user() {
    local local_password=""
    # Warning: command environment variables (*_CMD) have priority over usual configuration variables
    # for configuration parameters that require sensitive information. This is the case for the SQL database
    # and the broker backend in this entrypoint script.
    if [[ -n "${_AIRFLOW_WWW_USER_PASSWORD_CMD=}" ]]; then
        local_password=$(eval "${_AIRFLOW_WWW_USER_PASSWORD_CMD}")
        unset _AIRFLOW_WWW_USER_PASSWORD_CMD
    elif [[ -n "${_AIRFLOW_WWW_USER_PASSWORD=}" ]]; then
        local_password="${_AIRFLOW_WWW_USER_PASSWORD}"
        unset _AIRFLOW_WWW_USER_PASSWORD
    fi
    if [[ -z ${local_password} ]]; then
        echo
        echo "ERROR! Airflow Admin password not set via _AIRFLOW_WWW_USER_PASSWORD or _AIRFLOW_WWW_USER_PASSWORD_CMD variables!"
        echo
        exit 1
    fi

    if airflow config get-value core auth_manager | grep -q "FabAuthManager"; then
        airflow users create \
           --username "${_AIRFLOW_WWW_USER_USERNAME="admin"}" \
           --firstname "${_AIRFLOW_WWW_USER_FIRSTNAME="Airflow"}" \
           --lastname "${_AIRFLOW_WWW_USER_LASTNAME="Admin"}" \
           --email "${_AIRFLOW_WWW_USER_EMAIL="airflowadmin@example.com"}" \
           --role "${_AIRFLOW_WWW_USER_ROLE="Admin"}" \
           --password "${local_password}" || true
    else
        echo "Skipping user creation as auth manager different from Fab is used"
    fi
}

function create_system_user_if_missing() {
    # This is needed in case of OpenShift-compatible container execution. In case of OpenShift random
    # User id is used when starting the image, however group 0 is kept as the user group. Our production
    # Image is OpenShift compatible, so all permissions on all folders are set so that 0 group can exercise
    # the same privileges as the default "airflow" user, this code checks if the user is already
    # present in /etc/passwd and will create the system user dynamically, including setting its
    # HOME directory to the /home/airflow so that (for example) the ${HOME}/.local folder where airflow is
    # Installed can be automatically added to PYTHONPATH
    if ! whoami &> /dev/null; then
      if [[ -w /etc/passwd ]]; then
        echo "${USER_NAME:-default}:x:$(id -u):0:${USER_NAME:-default} user:${AIRFLOW_USER_HOME_DIR}:/sbin/nologin" \
            >> /etc/passwd
      fi
      export HOME="${AIRFLOW_USER_HOME_DIR}"
    fi
}

function set_pythonpath_for_root_user() {
    # Airflow is installed as a local user application which means that if the container is running as root
    # the application is not available. because Python then only load system-wide applications.
    # Now also adds applications installed as local user "airflow".
    if [[ $UID == "0" ]]; then
        local python_major_minor
        python_major_minor=$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
        export PYTHONPATH="${AIRFLOW_USER_HOME_DIR}/.local/lib/python${python_major_minor}/site-packages:${PYTHONPATH:-}"
        >&2 echo "The container is run as root user. For security, consider using a regular user account."
    fi
}

function wait_for_airflow_db() {
    # Wait for the command to run successfully to validate the database connection.
    run_check_with_retries "airflow db check"
}

function migrate_db() {
    # Runs airflow db migrate
    airflow db migrate || true
}

function wait_for_celery_broker() {
    # Verifies connection to Celery Broker
    local executor
    executor="$(airflow config get-value core executor)"
    if [[ "${executor}" == "CeleryExecutor" ]]; then
        local connection_url
        connection_url="$(airflow config get-value celery broker_url)"
        wait_for_connection "${connection_url}"
    fi
}

function exec_to_bash_or_python_command_if_specified() {
    # If one of the commands: 'bash', 'python' is used, either run appropriate
    # command with exec
    if [[ ${AIRFLOW_COMMAND} == "bash" ]]; then
       shift
       exec "/bin/bash" "${@}"
    elif [[ ${AIRFLOW_COMMAND} == "python" ]]; then
       shift
       exec "python" "${@}"
    fi
}

function check_uid_gid() {
    if [[ $(id -g) == "0" ]]; then
        return
    fi
    if [[ $(id -u) == "50000" ]]; then
        >&2 echo
        >&2 echo "WARNING! You should run the image with GID (Group ID) set to 0"
        >&2 echo "         even if you use 'airflow' user (UID=50000)"
        >&2 echo
        >&2 echo " You started the image with UID=$(id -u) and GID=$(id -g)"
        >&2 echo
        >&2 echo " This is to make sure you can run the image with an arbitrary UID in the future."
        >&2 echo
        >&2 echo " See more about it in the Airflow's docker image documentation"
        >&2 echo "     http://airflow.apache.org/docs/docker-stack/entrypoint"
        >&2 echo
        # We still allow the image to run with `airflow` user.
        return
    else
        >&2 echo
        >&2 echo "ERROR! You should run the image with GID=0"
        >&2 echo
        >&2 echo " You started the image with UID=$(id -u) and GID=$(id -g)"
        >&2 echo
        >&2 echo "The image should always be run with GID (Group ID) set to 0 regardless of the UID used."
        >&2 echo " This is to make sure you can run the image with an arbitrary UID."
        >&2 echo
        >&2 echo " See more about it in the Airflow's docker image documentation"
        >&2 echo "     http://airflow.apache.org/docs/docker-stack/entrypoint"
        # This will not work so we fail hard
        exit 1
    fi
}

unset PIP_USER

check_uid_gid

umask 0002

CONNECTION_CHECK_MAX_COUNT=${CONNECTION_CHECK_MAX_COUNT:=20}
readonly CONNECTION_CHECK_MAX_COUNT

CONNECTION_CHECK_SLEEP_TIME=${CONNECTION_CHECK_SLEEP_TIME:=3}
readonly CONNECTION_CHECK_SLEEP_TIME

create_system_user_if_missing
set_pythonpath_for_root_user
if [[ "${CONNECTION_CHECK_MAX_COUNT}" -gt "0" ]]; then
    wait_for_airflow_db
fi

if [[ -n "${_AIRFLOW_DB_UPGRADE=}" ]] || [[ -n "${_AIRFLOW_DB_MIGRATE=}" ]] ; then
    migrate_db
fi

if [[ -n "${_AIRFLOW_DB_UPGRADE=}" ]] ; then
    >&2 echo "WARNING: Environment variable '_AIRFLOW_DB_UPGRADE' is deprecated please use '_AIRFLOW_DB_MIGRATE' instead"
fi

if [[ -n "${_AIRFLOW_WWW_USER_CREATE=}" ]] ; then
    create_www_user
fi

if [[ -n "${_PIP_ADDITIONAL_REQUIREMENTS=}" ]] ; then
    >&2 echo
    >&2 echo "!!!!!  Installing additional requirements: '${_PIP_ADDITIONAL_REQUIREMENTS}' !!!!!!!!!!!!"
    >&2 echo
    >&2 echo "WARNING: This is a development/test feature only. NEVER use it in production!"
    >&2 echo "         Instead, build a custom image as described in"
    >&2 echo
    >&2 echo "         https://airflow.apache.org/docs/docker-stack/build.html"
    >&2 echo
    >&2 echo "         Adding requirements at container startup is fragile and is done every time"
    >&2 echo "         the container starts, so it is only useful for testing and trying out"
    >&2 echo "         of adding dependencies."
    >&2 echo
    pip install --root-user-action ignore ${_PIP_ADDITIONAL_REQUIREMENTS}
fi


exec_to_bash_or_python_command_if_specified "${@}"

if [[ ${AIRFLOW_COMMAND} == "airflow" ]]; then
   AIRFLOW_COMMAND="${2:-}"
   shift
fi

if [[ ${AIRFLOW_COMMAND} =~ ^(scheduler|celery)$ ]] \
    && [[ "${CONNECTION_CHECK_MAX_COUNT}" -gt "0" ]]; then
    wait_for_celery_broker
fi

exec "airflow" "${@}"
```

* this is the binary files that the /usr/bin folder contaisn so we can run our apt-get, apt, python3, chmod, chown, commands
```
airflow@58b02296185b:/usr/bin$ ls
 X11                                  ischroot                           prtstat
'['                                   isql                               ps
 addpart                              iusql                              pslog
 ant                                  jar                                psql
 appres                               jarsigner                          pstree
 apt                                  java                               pstree.x11
 apt-cache                            javac                              ptar
 apt-cdrom                            javadoc                            ptardiff
 apt-config                           javap                              ptargrep
 apt-extracttemplates                 jcmd                               ptx
 apt-ftparchive                       jconsole                           pwd
 apt-get                              jdb                                pwdx
 apt-key                              jdeprscan                          py3clean
 apt-mark                             jdeps                              py3compile
 apt-sortpkgs                         jexec                              py3versions
 arch                                 jfr                                pydoc3
 awk                                  jhsdb                              pydoc3.11
 b2sum                                jimage                             pygettext3
 base32                               jinfo                              pygettext3.11
 base64                               jlink                              python3
 basename                             jmap                               python3.11
 basenc                               jmod                               rbash
 bash                                 join                               readlink
 bashbug                              journalctl                         realpath
 bsqldb                               jpackage                           reindexdb
 bsqlodbc                             jps                                rename.ul
 busctl                               jrunscript                         renice
 c_rehash                             jshell                             replace
 captoinfo                            json_pp                            reset
 cat                                  jstack                             resizepart
 chage                                jstat                              resolve_stack_dump
 chattr                               jstatd                             rev
 chcon                                k5srvutil                          rgrep
 chfn                                 kadmin                             rm
 chgrp                                kbxutil                            rmdir
 chmod                                kdestroy                           rmiregistry
 choom                                kernel-install                     rrsync
 chown                                keytool                            rsync
 chrt                                 kill                               rsync-ssl
 chsh                                 killall                            run-parts
 cksum                                kinit                              runcon
 clear                                klist                              sasl-sample-client
 clear_console                        kpasswd                            saslfinger
 clusterdb                            ksu                                savelog
 cmp                                  kswitch                            scp
 comm                                 ktutil                             script
 corelist                             kvno                               scriptlive
 cp                                   last                               scriptreplay
 cpan                                 lastb                              sdiff
 cpan5.36-x86_64-linux-gnu            lastlog                            sed
 createdb                             ld.so                              select-editor
 createlang                           ldapadd                            sensible-browser
 createuser                           ldapcompare                        sensible-editor
 csplit                               ldapdelete                         sensible-pager
 curl                                 ldapexop                           seq
 cut                                  ldapmodify                         serialver
 cvtsudoers                           ldapmodrdn                         setarch
 dash                                 ldappasswd                         setpriv
 datacopy                             ldapsearch                         setsid
 date                                 ldapurl                            setterm
 db5.3_archive                        ldapwhoami                         sftp
 db5.3_checkpoint                     ldd                                sg
 db5.3_deadlock                       libnetcfg                          sh
 db5.3_dump                           link                               sha1sum
 db5.3_hotbackup                      linux32                            sha224sum
 db5.3_load                           linux64                            sha256sum
 db5.3_log_verify                     listres                            sha384sum
 db5.3_printlog                       ln                                 sha512sum
 db5.3_recover                        locale                             shasum
 db5.3_replicate                      localectl                          shred
 db5.3_stat                           localedef                          shuf
 db5.3_upgrade                        logger                             skill
 db5.3_verify                         login                              slabtop
 db_archive                           loginctl                           sleep
 db_checkpoint                        logname                            slogin
 db_deadlock                          ls                                 snice
 db_dump                              lsattr                             sort
 db_hotbackup                         lsb_release                        splain
 db_load                              lsblk                              split
 db_log_verify                        lscpu                              sqlite3
 db_printlog                          lsfd                               ssh
 db_recover                           lsipc                              ssh-add
 db_replicate                         lsirq                              ssh-agent
 db_stat                              lslocks                            ssh-argv0
 db_upgrade                           lslogins                           ssh-copy-id
 db_verify                            lsmem                              ssh-keygen
 dbus-cleanup-sockets                 lsns                               ssh-keyscan
 dbus-daemon                          lspgpot                            stat
 dbus-monitor                         luit                               stdbuf
 dbus-run-session                     mariadb                            streamzip
 dbus-send                            mariadb-access                     stty
 dbus-update-activation-environment   mariadb-admin                      su
 dbus-uuidgen                         mariadb-analyze                    sudo
 dd                                   mariadb-binlog                     sudoedit
 deb-systemd-helper                   mariadb-check                      sudoreplay
 deb-systemd-invoke                   mariadb-conv                       sum
 debconf                              mariadb-convert-table-format       sync
 debconf-apt-progress                 mariadb-dump                       systemctl
 debconf-communicate                  mariadb-dumpslow                   systemd
 debconf-copydb                       mariadb-find-rows                  systemd-analyze
 debconf-escape                       mariadb-fix-extensions             systemd-ask-password
 debconf-set-selections               mariadb-hotcopy                    systemd-cat
 debconf-show                         mariadb-import                     systemd-cgls
 defncopy                             mariadb-optimize                   systemd-cgtop
 delpart                              mariadb-plugin                     systemd-creds
 df                                   mariadb-repair                     systemd-cryptenroll
 diff                                 mariadb-report                     systemd-delta
 diff3                                mariadb-secure-installation        systemd-detect-virt
 dir                                  mariadb-setpermission              systemd-escape
 dircolors                            mariadb-show                       systemd-firstboot
 dirmngr                              mariadb-slap                       systemd-id128
 dirmngr-client                       mariadb-tzinfo-to-sql              systemd-inhibit
 dirname                              mariadb-waitpid                    systemd-machine-id-setup
 dmesg                                mariadbcheck                       systemd-mount
 dnsdomainname                        mawk                               systemd-notify
 docker                               mcookie                            systemd-path
 domainname                           md5sum                             systemd-repart
 dpkg                                 md5sum.textutils                   systemd-run
 dpkg-deb                             mesg                               systemd-socket-activate
 dpkg-divert                          migrate-pubring-from-classic-gpg   systemd-stdio-bridge
 dpkg-maintscript-helper              mkdir                              systemd-sysext
 dpkg-query                           mkfifo                             systemd-sysusers
 dpkg-realpath                        mknod                              systemd-tmpfiles
 dpkg-split                           mktemp                             systemd-tty-ask-password-agent
 dpkg-statoverride                    more                               systemd-umount
 dpkg-trigger                         mount                              tabs
 dropdb                               mountpoint                         tac
 droplang                             msql2mysql                         tail
 dropuser                             mv                                 tar
 du                                   my_print_defaults                  taskset
 dumb-init                            mysql                              tdspool
 echo                                 mysql_find_rows                    tee
 editres                              mysql_fix_extensions               tempfile
 egrep                                mysql_waitpid                      test
 enc2xs                               mysqlaccess                        tic
 encguess                             mysqladmin                         timedatectl
 env                                  mysqlanalyze                       timeout
 expand                               mysqlcheck                         tload
 expiry                               mysqldump                          toe
 expr                                 mysqldumpslow                      top
 factor                               mysqlimport                        touch
 faillog                              mysqloptimize                      tput
 fallocate                            mysqlrepair                        tr
 false                                mysqlreport                        true
 fc-cache                             mysqlshow                          truncate
 fc-cat                               mysqlslap                          tset
 fc-conflist                          mytop                              tsort
 fc-list                              namei                              tsql
 fc-match                             nawk                               tty
 fc-pattern                           nc                                 tzselect
 fc-query                             nc.openbsd                         uclampset
 fc-scan                              netcat                             umount
 fc-validate                          networkctl                         uname
 fgrep                                newgrp                             uncompress
 fincore                              nice                               unexpand
 find                                 nisdomainname                      uniq
 findmnt                              nl                                 unlink
 fisql                                nohup                              unshare
 flock                                nproc                              update-alternatives
 fmt                                  nsenter                            update-mime-database
 fold                                 numfmt                             uptime
 free                                 od                                 users
 freebcp                              odbcinst                           utmpdump
 fuser                                openssl                            vacuumdb
 gdk-pixbuf-csource                   osql                               vacuumlo
 gdk-pixbuf-pixdata                   pager                              vdir
 gdk-pixbuf-thumbnailer               partx                              viewres
 gen-auth                             passwd                             vmstat
 geos-config                          paste                              w
 getconf                              pathchk                            wall
 getent                               pdb3                               watch
 getopt                               pdb3.11                            watchgnupg
 gpasswd                              peekfd                             wc
 gpg                                  perl                               wdctl
 gpg-agent                            perl5.36-x86_64-linux-gnu          whereis
 gpg-connect-agent                    perl5.36.0                         which
 gpg-wks-server                       perlbug                            which.debianutils
 gpg-zip                              perldoc                            who
 gpg2                                 perlivp                            whoami
 gpgcompose                           perlthanks                         x86_64
 gpgconf                              perror                             xargs
 gpgparsemail                         pg_basebackup                      xdg-user-dir
 gpgsm                                pg_dump                            xdg-user-dirs-update
 gpgsplit                             pg_dumpall                         xdpyinfo
 gpgtar                               pg_isready                         xdriinfo
 gpgv                                 pg_receivewal                      xev
 grep                                 pg_receivexlog                     xfd
 groups                               pg_recvlogical                     xfontsel
 gtk-update-icon-cache                pg_restore                         xkill
 gunzip                               pgbench                            xlsatoms
 gzexe                                pgrep                              xlsclients
 gzip                                 piconv                             xlsfonts
 h2ph                                 pidof                              xmessage
 h2xs                                 pidwait                            xprop
 hardlink                             pinentry                           xsubpp
 head                                 pinentry-curses                    xvinfo
 hostid                               pinky                              xwininfo
 hostname                             pkill                              yes
 hostnamectl                          pl2pm                              ypdomainname
 i386                                 pldd                               zcat
 iconv                                pmap                               zcmp
 id                                   pod2html                           zdiff
 infocmp                              pod2man                            zdump
 infotocap                            pod2text                           zegrep
 innotop                              pod2usage                          zfgrep
 install                              podchecker                         zforce
 instmodsh                            pr                                 zgrep
 ionice                               printenv                           zipdetails
 ipcmk                                printf                             zless
 ipcrm                                prlimit                            zmore
 ipcs                                 prove                              znew
```

* command printenv prints the following
```
PYTHON_SHA256=8fb5f9fbc7609fa822cb31549884575db7fd9657cbffb89510b5d7975963a83a
DUMB_INIT_SETSID=1
HOSTNAME=58b02296185b
PYTHON_VERSION=3.11.13
LANGUAGE=C.UTF-8
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/
AIRFLOW_USER_HOME_DIR=/home/airflow
ADDITIONAL_RUNTIME_APT_DEPS=
PWD=/usr/bin
AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-apiserver:8080/execution/
AIRFLOW_VERSION=3.0.2
AIRFLOW__CORE__LOAD_EXAMPLES=true
INSTALL_MSSQL_CLIENT=true
INSTALL_MYSQL_CLIENT_TYPE=mariadb
GUNICORN_CMD_ARGS=--worker-tmp-dir /dev/shm
HOME=/home/airflow
VIRTUAL_ENV=/home/airflow/.local
AIRFLOW_HOME=/opt/airflow
AIRFLOW_SETUPTOOLS_VERSION=80.8.0
GPG_KEY=A035C8C19219BA821ECEA86B64E628F8D684696D
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW_USE_UV=false
AIRFLOW__CORE__EXECUTOR=LocalExecutor
PIP_CACHE_DIR=/tmp/.cache/pip
AIRFLOW_PIP_VERSION=25.1.1
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
_PIP_ADDITIONAL_REQUIREMENTS=
AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK=true
AIRFLOW_IMAGE_TYPE=prod
AIRFLOW_INSTALLATION_METHOD=apache-airflow
AIRFLOW_CONFIG=/opt/airflow/config/airflow.cfg
PATH=/root/bin:/home/airflow/.local/bin:/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
PYTHON_BASE_IMAGE=python:3.11-slim-bookworm
AIRFLOW_UID=50000
AIRFLOW__CORE__FERNET_KEY=
```

* our PATH user environment variable has the following values 
/root/bin
/home/airflow/.local/bin
/usr/local/bin
/usr/local/sbin
/usr/local/bin
/usr/sbin
/usr/bin
/sbin
/bin

* solving the `ValueError: The value api_auth/jwt_secret must be set!`. This is because in the docker-compose file the environment variables in the `x-airflow-common`, `AIRFLOW__API_AUTH__JWT_SECRET` is not defined, and because `airflow.cfg` is basically the `.env` file used by the `docker-compose.yaml` file which contains `jwt_secret` variable to use this variable in the `docker-compose.yaml` file we have to use `AIRFLOW__API_AUTH__JWT_SECRET`: https://github.com/apache/airflow/issues/50538

we can generate one and set the env variable explicitly in the `docker-compose.yaml` file using `openssl rand -base64 16` which generates `<some key>` then we can use this to assign it to the explicitly dfined `AIRFLOW__API_AUTH__JWT_SECRET` env variable in the `docker-compose.yaml` file

* you can try to see the values of the `airflow.cfg` file by `airflow config get-value <section e.g. core, secrets, api, etc.> <variable name e.g. load_examples, jwt_secret, aws_secret_access_key_id>`

* to fix this selenium.common.exceptions.WebDriverException: Message: unknown error: cannot find Chrome binary occuring in the docker container when I run `python ./operators/extract_us_population_per_state_by_sex_age_race_ho.py` 

we will need to install chrome from https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb using wget and unzip which we will also need to install as dependencies first. E.g. after `apt-get install -y wget unzip` we run `wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb`, once downloaded in the current working directory it will be in the form of `google-chrome-stable_current_amd64.deb` but is actually a folder as the real installer is inside. So we will run `apt install -y ./google-chrome-stable_current_amd64.deb`

it will prompt you (or not if you don't use the -y flag) with The following additional packages will be installed:
```
  cpp cpp-12 fonts-liberation libauthen-sasl-perl libclone-perl libcolord2 libdata-dump-perl libegl-mesa0 libegl1
  libencode-locale-perl libepoxy0 libfile-basedir-perl libfile-desktopentry-perl libfile-listing-perl
  libfile-mimeinfo-perl libfont-afm-perl libgbm1 libgles2 libgtk-3-0 libgtk-3-bin libgtk-3-common libhtml-form-perl
  libhtml-format-perl libhtml-parser-perl libhtml-tagset-perl libhtml-tree-perl libhttp-cookies-perl
  libhttp-daemon-perl libhttp-date-perl libhttp-message-perl libhttp-negotiate-perl libio-html-perl
  libio-socket-ssl-perl libio-stringy-perl libipc-system-simple-perl libisl23 liblwp-mediatypes-perl
  liblwp-protocol-https-perl libmailtools-perl libmpc3 libmpfr6 libnet-dbus-perl libnet-http-perl libnet-smtp-ssl-perl
  libnet-ssleay-perl libregexp-ipv6-perl libtext-iconv-perl libtie-ixhash-perl libtimedate-perl libtry-tiny-perl
  liburi-perl libvulkan1 libwayland-client0 libwayland-cursor0 libwayland-egl1 libwayland-server0 libwww-perl
  libwww-robotrules-perl libx11-protocol-perl libxkbcommon0 libxml-parser-perl libxml-twig-perl
  libxml-xpathengine-perl mesa-vulkan-drivers perl-openssl-defaults x11-xserver-utils xdg-utils xkb-data zutty
```

once done we just do "cleanup" and removing the installer itself since it has already been installed in the docker container by running the remove command `rm` e.g. `rm google-chrome-stable_current_amd64.deb`

these are actually the dependencies that chrome needs in order to run in linux

Once installed when you check where it has been installed by navigating to /usr/bin/ directory where google-chrome, google-chrome-stable are the installed binary files. Because this is the bin folder which is added to the docker containers PATH user or system/root environment variable we can always run chrome related commands,in this case that will be selenium doing it in the background

note that when using this selenium script this will by default be located in the /root/Downloads/ directory but in order toeven access this or navigate to it, it requires us to be the root user. To switch to root user in interactive mode in docker command line we `docker exec -it --user root <container name e.g. chronic-disease-analyses-airflow-apiserver-1> bash`

* however switching to the root user is not enough as we need to work with the airflow user itself which does not have access to the /root/Downloads/ directory

* `rm -r /path/to/dir/*` to remove all files in a directory but not the directory itself
* `mkdir: cannot create directory ‘seluser’: Permission denied` and maybe this implies that downloads using regular usre (airflow) are restricuted
* need to figure out a way to grant permissions to airflow user as root user during the building of dockerfile


* shows us the user id (uid) of current user
```
airflow@848360f97c04:/opt/airflow$ id
uid=50000(airflow) gid=0(root) groups=0(root)
```

```
root@848360f97c04:/opt/airflow# id
uid=0(root) gid=0(root) groups=0(root)
```

the reason why we can't seem to download files with the airflow user (which is a non root user) is because unlike the root user it doesn't have the permission to go into some files, create directories, or edit files

the way we can elevate a non root users permissions is to add it to the group fo the root user. But do note that this can only be done as the root user. What we basically do is have the root user grant root permissions to the non root user through `usermod -aG root airflow`. While `sudo <any command that can be done by a root user>`. Tried this didn't work. My test case was to add the non root user to a group then once done I logged in as non root user again and navigated to hte directory where the data would be downloaded supposedly or written to, so I simulated it with touch test.txt, and sure enough it gave a `permission denied` error again. Another workaround could be to mount a volume 

* `cat /etc/passwd` lists the users
```
root@848360f97c04:/opt/airflow# cat /etc/passwd
root:x:0:0:root:/root:/bin/bash
daemon:x:1:1:daemon:/usr/sbin:/usr/sbin/nologin
bin:x:2:2:bin:/bin:/usr/sbin/nologin
sys:x:3:3:sys:/dev:/usr/sbin/nologin
sync:x:4:65534:sync:/bin:/bin/sync
games:x:5:60:games:/usr/games:/usr/sbin/nologin
man:x:6:12:man:/var/cache/man:/usr/sbin/nologin
lp:x:7:7:lp:/var/spool/lpd:/usr/sbin/nologin
mail:x:8:8:mail:/var/mail:/usr/sbin/nologin
news:x:9:9:news:/var/spool/news:/usr/sbin/nologin
uucp:x:10:10:uucp:/var/spool/uucp:/usr/sbin/nologin
proxy:x:13:13:proxy:/bin:/usr/sbin/nologin
www-data:x:33:33:www-data:/var/www:/usr/sbin/nologin
backup:x:34:34:backup:/var/backups:/usr/sbin/nologin
list:x:38:38:Mailing List Manager:/var/list:/usr/sbin/nologin
irc:x:39:39:ircd:/run/ircd:/usr/sbin/nologin
_apt:x:42:65534::/nonexistent:/usr/sbin/nologin
nobody:x:65534:65534:nobody:/nonexistent:/usr/sbin/nologin
airflow:x:50000:0:First Last,RoomNumber,WorkPhone,HomePhone:/home/airflow:/bin/bash
systemd-network:x:998:998:systemd Network Management:/:/usr/sbin/nologin
systemd-timesync:x:997:997:systemd Time Synchronization:/:/usr/sbin/nologin
messagebus:x:100:102::/nonexistent:/usr/sbin/nologin
```



# Questions:
* how to fill in missing values?
* how to drop undesired values based on a filter?
* what is formatting data? Select a column of numbers then we select the formatting of this column and set it to currency to turn numbers into currency e.g. with decimals etc.

# Relevant articles and links:
* https://stackoverflow.com/questions/696506/sql-datatype-how-to-store-a-year
* https://stackoverflow.com/questions/47357855/sql-add-only-a-year-value-in-a-date-column
* performance tuning of apache spark: https://medium.com/@manoj.kdas37/how-to-optimize-your-apache-spark-jobs-top-10-approaches-and-best-practices-for-performance-tuning-4630ae864f52
* configuring apache spark to enhance performance and avoid memory limit errors: https://spark.apache.org/docs/latest/configuration.html
* error reading s3 `com google common base preconditions`: 
- https://stackoverflow.com/questions/42206440/java-lang-nosuchmethoderror-com-google-common-base-preconditions-checkargument
- http://www.openkb.info/2022/07/spark-writing-to-s3-failed_19.html
- https://abrkljac.medium.com/solving-the-nosuchmethoderror-exception-in-spark-minio-integration-8324f3d464e3
* resolving `Class org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider not found` error: https://stackoverflow.com/questions/71546208/class-org-apache-hadoop-fs-s3a-auth-iaminstancecredentialsprovider-not-found-whe. 
* connecting parquet file to powerbi: https://community.fabric.microsoft.com/t5/Desktop/How-to-read-a-directory-of-Parquet-files-in-power-Bi-ie-with/td-p/3009353
* Describes how hadoop version meeds to be aligned with what is described in pom.xml file for a specific spark version: https://stackoverflow.com/questions/78269281/how-to-read-from-s3-on-pyspark-on-local
* Because we are in windows installing hadoop is not enough as we need winutils and hadoopdll for hadoop to write and read files in our windows os as it is made specifically for Unix and posix systems: https://medium.com/@dhirajmishra57/installing-spark-3-5-on-windows-e9bd183f84b9. If we can do this we can install spark and setup a cluster on docker containers and Linux systems
* Describes how hadoop version needs to be aligned with what is described in pom.xml file for a specific spark version in order to read a .csv file from an s3 bucket: https://stackoverflow.com/questions/78269281/how-to-read-from-s3-on-pyspark-on-local
* for color picking and picking the middle gradient of two colors: https://meyerweb.com/eric/tools/color-blend/#00FFAE:4B0069:10:hex
* for creating custom maps in powerbi that have other countries and their states, other than the pre built ones in powerbi like US, UK, italy, etc.: https://simplemaps.com/gis/country/us
* for exporting the downloaded geojson map from https://simplemaps.com/gis/country/us to topojson so that it can be used in powerbi: https://mapshaper.org/
* Joining tables in DAX powerbi with multiple conditions: https://stackoverflow.com/questions/41846571/how-to-joint-two-tables-in-dax-using-custom-condition 
* being able to use selected value in slicer to make a dynamic table: https://community.fabric.microsoft.com/t5/Desktop/Create-Dynamic-Tables-based-on-slicer-from-the-same-data-set-and/td-p/2003909

# Problems to solve:
1. I can't save year as 4 byte int for 200000+ rows since that would be a waste of space
2. <s>implement headless browsing of selenium to scrape population data transform the excel sheets and automatically in a data warehouse for data analysis</s>
3. joining tables in dax powerbi with multiple conditions
4. automatically extract the spreadsheets using selenium using airflow








