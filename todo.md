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


* what I want to do is see the correlation if there is any the binge drinking frequency and intensity of those who binge drink and chronic liver disease mortality, if there is a positive correlation or not (which spoiler there is since it is an established fact in medical science)

To assess the correlation between binge drinking frequency/intensity and chronic liver disease mortality, you'll primarily use DAX measures and Power BI visuals. You'll need to:

Identify the relevant CDI QuestionIDs for liver disease mortality and the other specified drinking behaviors.
Create DAX measures to pull these data points, ensuring proper filtering for demographics (women 18-44, youth, etc.).
Combine the data in a meaningful way for visualization.
Choose the right visuals to display correlation.
Let's break down the steps:

Step 1: Identify Relevant QuestionIDs from your CDI Data
First, you need to know the exact QuestionID values for the indicators you want to analyze. Based on the CDI (Chronic Disease Indicators) dataset structure, here are some common ones or those you'd need to find in your specific dataset:

Chronic Liver Disease Mortality:

Look for terms like "Chronic Liver Disease and Cirrhosis mortality" or similar. A common QuestionID might be CLD1_0 or something similar (you'll need to confirm this from your data's QuestionID column). It's typically a rate (e.g., deaths per 100,000 population).
Binge Drinking Prevalence (General Adults):

You already have ALC2_2 ("Binge drinking prevalence among adults").
Binge Drinking Frequency (among those who binge drink):

This is often a mean number of binge drinking episodes. Look for ALC3_0 (as you've already used for AlcFreqDataValue).
Binge Drinking Intensity (among those who binge drink):

This is often a mean number of drinks per binge episode. Look for ALC4_0 (as you've already used for AlcIntDataValue).
Specific Demographics:

Binge drinking among women aged 18-44 years: You'll need to find the QuestionID that specifies this group AND the StratificationID for "Females, 18-44 years".
Binge drinking prevalence among youth: Look for QuestionIDs related to "youth" or "high school students" (e.g., ALC1_2 might be "Current alcohol use among high school students"). The StratificationID would likely be "Overall" or specific age groups for youth.
Heavy drinking among adults aged >= 18 years: Look for QuestionID like ALC5_0 ("Heavy drinking prevalence among adults").
Heavy drinking among women aged 18-44 years: Similar to binge drinking for women, find the QuestionID for heavy drinking combined with the StratificationID for "Females, 18-44 years".
Action: Go to your data view in Power BI for the CDI table and filter the QuestionID and StratificationID columns to identify the exact values for each of these specific indicators.

Step 2: Create DAX Measures for Each Indicator
You'll create a separate measure for each indicator you want to analyze. These measures will aggregate the DataValue based on the specific QuestionID, StratificationID, and possibly DataValueTypeID.

Assuming TotalEvents is a Calculated Column in your CDI table (as previously discussed), you can use it. If you need the raw DataValue for rates, use that.

Here are examples. Replace placeholder QuestionID and StratificationID with your actual findings from Step 1.



Step 3: Combine Data for Correlation Analysis
For correlation, you ideally need the corresponding data points for each LocationID, YearStart, YearEnd (and StratificationID if you're analyzing at that granular level).

Since all your data comes from the CDI table, the shared columns (LocationID, YearStart, YearEnd, StratificationID) act as your natural join keys.

Option A: Using the BingeDrinkingIntFreq Calculated Table (Recommended if it works)

If your BingeDrinkingIntFreq calculated table (the one we've been refining in DAX) successfully captures all the necessary columns (BingeDrinkingPopInt, BingeDrinkingPopFreq, AvgAlcPrevDataValue, LocationID, YearStart, etc.), then you can directly use this table.

This table will contain the core drinking metrics, and you just need to bring in the LiverDiseaseMortalityRate.

Approach: You would then use ADDCOLUMNS to add the LiverDiseaseMortalityRate to your BingeDrinkingIntFreqTable as another column, again using a CALCULATE(AVERAGE(...), FILTER(ALL(CDI), ...)) pattern to pull the mortality rate for the corresponding LocationID, YearStart, etc.




You're moving into the realm of advanced analytics and correlation in Power BI, which is exciting! This is definitely achievable.

To assess the correlation between binge drinking frequency/intensity and chronic liver disease mortality, you'll primarily use DAX measures and Power BI visuals. You'll need to:

Identify the relevant CDI QuestionIDs for liver disease mortality and the other specified drinking behaviors.
Create DAX measures to pull these data points, ensuring proper filtering for demographics (women 18-44, youth, etc.).
Combine the data in a meaningful way for visualization.
Choose the right visuals to display correlation.
Let's break down the steps:

Step 1: Identify Relevant QuestionIDs from your CDI Data
First, you need to know the exact QuestionID values for the indicators you want to analyze. Based on the CDI (Chronic Disease Indicators) dataset structure, here are some common ones or those you'd need to find in your specific dataset:

Chronic Liver Disease Mortality:

Look for terms like "Chronic Liver Disease and Cirrhosis mortality" or similar. A common QuestionID might be CLD1_0 or something similar (you'll need to confirm this from your data's QuestionID column). It's typically a rate (e.g., deaths per 100,000 population).
Binge Drinking Prevalence (General Adults):

You already have ALC2_2 ("Binge drinking prevalence among adults").
Binge Drinking Frequency (among those who binge drink):

This is often a mean number of binge drinking episodes. Look for ALC3_0 (as you've already used for AlcFreqDataValue).
Binge Drinking Intensity (among those who binge drink):

This is often a mean number of drinks per binge episode. Look for ALC4_0 (as you've already used for AlcIntDataValue).
Specific Demographics:

Binge drinking among women aged 18-44 years: You'll need to find the QuestionID that specifies this group AND the StratificationID for "Females, 18-44 years".
Binge drinking prevalence among youth: Look for QuestionIDs related to "youth" or "high school students" (e.g., ALC1_2 might be "Current alcohol use among high school students"). The StratificationID would likely be "Overall" or specific age groups for youth.
Heavy drinking among adults aged >= 18 years: Look for QuestionID like ALC5_0 ("Heavy drinking prevalence among adults").
Heavy drinking among women aged 18-44 years: Similar to binge drinking for women, find the QuestionID for heavy drinking combined with the StratificationID for "Females, 18-44 years".
Action: Go to your data view in Power BI for the CDI table and filter the QuestionID and StratificationID columns to identify the exact values for each of these specific indicators.

Step 2: Create DAX Measures for Each Indicator
You'll create a separate measure for each indicator you want to analyze. These measures will aggregate the DataValue based on the specific QuestionID, StratificationID, and possibly DataValueTypeID.

Assuming TotalEvents is a Calculated Column in your CDI table (as previously discussed), you can use it. If you need the raw DataValue for rates, use that.

Here are examples. Replace placeholder QuestionID and StratificationID with your actual findings from Step 1.


```
-- Liver Disease Mortality Rate
LiverDiseaseMortalityRate =
CALCULATE(
    AVERAGE('chronic_disease_analyses_db main CDI'[DataValue]), -- Or SUM, depending on if DataValue is a rate or raw count
    FILTER(
        ALL('chronic_disease_analyses_db main CDI'),
        'chronic_disease_analyses_db main CDI'[QuestionID] = "CLD1_0" -- Replace with actual CLD Mortality QuestionID
        // Add any specific StratificationID if needed for overall mortality
    )
)

-- Binge Drinking Prevalence (General Adults - ALC2_2 is already this)
BingeDrinkingPrevalence_Adults =
CALCULATE(
    AVERAGE('chronic_disease_analyses_db main CDI'[DataValue]),
    FILTER(
        ALL('chronic_disease_analyses_db main CDI'),
        'chronic_disease_analyses_db main CDI'[QuestionID] = "ALC2_2" &&
        ('chronic_disease_analyses_db main CDI'[StratificationID] = "B_F_ALL" || 'chronic_disease_analyses_db main CDI'[StratificationID] = "B_M_ALL")
    )
)

-- Binge Drinking Frequency (Adults who binge drink - ALC3_0)
BingeDrinkingFrequency_Adults =
CALCULATE(
    AVERAGE('chronic_disease_analyses_db main CDI'[DataValue]),
    FILTER(
        ALL('chronic_disease_analyses_db main CDI'),
        'chronic_disease_analyses_db main CDI'[QuestionID] = "ALC3_0" &&
        ('chronic_disease_analyses_db main CDI'[StratificationID] = "B_F_ALL" || 'chronic_disease_analyses_db main CDI'[StratificationID] = "B_M_ALL")
    )
)

-- Binge Drinking Intensity (Adults who binge drink - ALC4_0)
BingeDrinkingIntensity_Adults =
CALCULATE(
    AVERAGE('chronic_disease_analyses_db main CDI'[DataValue]),
    FILTER(
        ALL('chronic_disease_analyses_db main CDI'),
        'chronic_disease_analyses_db main CDI'[QuestionID] = "ALC4_0" &&
        ('chronic_disease_analyses_db main CDI'[StratificationID] = "B_F_ALL" || 'chronic_disease_analyses_db main CDI'[StratificationID] = "B_M_ALL")
    )
)

-- Binge Drinking Prevalence (Women 18-44)
BingeDrinkingPrev_Women18_44 =
CALCULATE(
    AVERAGE('chronic_disease_analyses_db main CDI'[DataValue]),
    FILTER(
        ALL('chronic_disease_analyses_db main CDI'),
        'chronic_disease_analyses_db main CDI'[QuestionID] = "YOUR_BINGE_DRINK_QID_FOR_WOMEN" && -- Find this QID
        'chronic_disease_analyses_db main CDI'[StratificationID] = "B_F_18_44" -- Confirm this StratificationID
    )
)

-- Binge Drinking Prevalence (Youth)
BingeDrinkingPrev_Youth =
CALCULATE(
    AVERAGE('chronic_disease_analyses_db main CDI'[DataValue]),
    FILTER(
        ALL('chronic_disease_analyses_db main CDI'),
        'chronic_disease_analyses_db main CDI'[QuestionID] = "ALC1_2" && -- Example Youth QID
        'chronic_disease_analyses_db main CDI'[StratificationID] = "OVERALL" -- Or specific youth age group stratification
    )
)

-- Heavy Drinking Prevalence (Adults >= 18)
HeavyDrinkingPrev_Adults =
CALCULATE(
    AVERAGE('chronic_disease_analyses_db main CDI'[DataValue]),
    FILTER(
        ALL('chronic_disease_analyses_db main CDI'),
        'chronic_disease_analyses_db main CDI'[QuestionID] = "ALC5_0" && -- Example Heavy Drinking QID
        ('chronic_disease_analyses_db main CDI'[StratificationID] = "B_F_ALL" || 'chronic_disease_analyses_db main CDI'[StratificationID] = "B_M_ALL")
    )
)

-- Heavy Drinking Prevalence (Women 18-44)
HeavyDrinkingPrev_Women18_44 =
CALCULATE(
    AVERAGE('chronic_disease_analyses_db main CDI'[DataValue]),
    FILTER(
        ALL('chronic_disease_analyses_db main CDI'),
        'chronic_disease_analyses_db main CDI'[QuestionID] = "YOUR_HEAVY_DRINK_QID_FOR_WOMEN" && -- Find this QID
        'chronic_disease_analyses_db main CDI'[StratificationID] = "B_F_18_44" -- Confirm this StratificationID
    )
)
```

Important Note: The AVERAGE aggregation in these measures assumes you want the average value across any filters applied in your visual (e.g., if you show by LocationID and YearStart, you'll get the average for that specific location and year). If you need the TotalEvents calculation for each row before aggregation, you'd involve your TotalEvents calculated column.

Step 3: Combine Data for Correlation Analysis
For correlation, you ideally need the corresponding data points for each LocationID, YearStart, YearEnd (and StratificationID if you're analyzing at that granular level).

Since all your data comes from the CDI table, the shared columns (LocationID, YearStart, YearEnd, StratificationID) act as your natural join keys.

Option A: Using the BingeDrinkingIntFreq Calculated Table (Recommended if it works)

If your BingeDrinkingIntFreq calculated table (the one we've been refining in DAX) successfully captures all the necessary columns (BingeDrinkingPopInt, BingeDrinkingPopFreq, AvgAlcPrevDataValue, LocationID, YearStart, etc.), then you can directly use this table.

This table will contain the core drinking metrics, and you just need to bring in the LiverDiseaseMortalityRate.

Approach: You would then use ADDCOLUMNS to add the LiverDiseaseMortalityRate to your BingeDrinkingIntFreqTable as another column, again using a CALCULATE(AVERAGE(...), FILTER(ALL(CDI), ...)) pattern to pull the mortality rate for the corresponding LocationID, YearStart, etc.

```
-- Building upon your previous BingeDrinkingIntFreq calculated table
BingeDrinkingIntFreq_WithMortality =
VAR ALC2_2_Filtered = ... -- Your existing ALC2_2_Filtered
VAR JoinedWithALC4_0 = ... -- Your existing JoinedWithALC4_0
VAR JoinedWithALC3_0 = ... -- Your existing JoinedWithALC3_0
VAR PreCalculatedFinal = ... -- Your existing PreCalculatedFinal

RETURN
    ADDCOLUMNS(
        SUMMARIZECOLUMNS ( -- The result of your previous BingeDrinkingIntFreq table
            PreCalculatedFinal[LocationID],
            PreCalculatedFinal[StratificationID],
            PreCalculatedFinal[YearEnd],
            PreCalculatedFinal[YearStart],
            PreCalculatedFinal[QuestionID],
            PreCalculatedFinal[AlcIntID],
            PreCalculatedFinal[AlcFreqID],
            "LogID", MAXX(PreCalculatedFinal, PreCalculatedFinal[LogID]),
            "AvgAlcPrevDataValue", AVERAGE(PreCalculatedFinal[DataValue]),
            "AvgAlcIntDataValue", AVERAGE(PreCalculatedFinal[AlcIntDataValue]),
            "AvgAlcFreqDataValue", AVERAGE(PreCalculatedFinal[AlcFreqDataValue]),
            "AvgBingeDrinkingPopInt", AVERAGE(PreCalculatedFinal[BingeDrinkingPopInt]),
            "AvgBingeDrinkingPopFreq", AVERAGE(PreCalculatedFinal[BingeDrinkingPopFreq]),
            "AvgBingeDrinkingPopulation", AVERAGE(PreCalculatedFinal[BingeDrinkingPopulation])
        ),
        "LiverDiseaseMortality",
            VAR _currentLoc = [LocationID]
            VAR _currentStrat = [StratificationID] // You might need to adjust stratification for mortality
            VAR _currentYS = [YearStart]
            VAR _currentYE = [YearEnd]
            RETURN
                CALCULATE(
                    AVERAGE('chronic_disease_analyses_db main CDI'[DataValue]),
                    ALL('chronic_disease_analyses_db main CDI'),
                    'chronic_disease_analyses_db main CDI'[QuestionID] = "CLD1_0", -- Replace with actual CLD Mortality QID
                    'chronic_disease_analyses_db main CDI'[LocationID] = _currentLoc,
                    'chronic_disease_analyses_db main CDI'[YearStart] = _currentYS,
                    'chronic_disease_analyses_db main CDI'[YearEnd] = _currentYE
                    // You might need to explicitly set a common stratification for mortality,
                    // e.g., 'chronic_disease_analyses_db main CDI'[StratificationID] = "OVERALL"
                )
    )
```

Important: For the LiverDiseaseMortality part, pay close attention to StratificationID. If liver disease mortality data is typically "Overall" for a location/year, and your binge drinking data is specific to "B_F_ALL" / "B_M_ALL", you'll need to decide how to align them. You might need to pull the mortality data using StratificationID = "OVERALL" or calculate an average across stratifications if applicable.

Step 4: Visualize for Correlation
Once you have your calculated table (or measures that can be put in a visual context together), you can start visualizing.

Scatter Plot: This is the most common and effective way to show correlation.

Drag a Scatter Plot visual onto your report page.
Place AvgBingeDrinkingPopInt (or AvgBingeDrinkingPopFreq) on the X-axis.
Place LiverDiseaseMortality on the Y-axis.
Place LocationID and/or YearStart on the Details field. This will create individual points for each location/year, allowing you to see the scatter.
Add a Trend Line: In the Visualizations pane, under "Analytics" (the magnifying glass icon), you can add a "Trend line." This will help you visually confirm the positive correlation.
Interpretation: If the points cluster around an upward-sloping line, it indicates a positive correlation.
Correlation Coefficient (Optional, but more robust):
While Power BI visuals help, you can also calculate the Pearson correlation coefficient directly in DAX for a more precise numerical measure.

```
Correlation_BingeInt_Mortality =
VAR SummaryTable =
    SUMMARIZECOLUMNS (
        'chronic_disease_analyses_db main CDI'[LocationID],
        'chronic_disease_analyses_db main CDI'[YearStart],
        "BingeInt", [BingeDrinkingIntensity_Adults], -- Using the measure for overall adults
        "Mortality", [LiverDiseaseMortalityRate] -- Using the measure
    )
RETURN
    CORRELATIONX(
        FILTER(
            SummaryTable,
            NOT ISBLANK([BingeInt]) && NOT ISBLANK([Mortality])
        ),
        [BingeInt],
        [Mortality]
    )
```
You would create separate Correlation_... measures for each pair you want to analyze (e.g., BingeDrinkingFrequency vs. Mortality, BingeDrinkingPopInt vs. Mortality).
CORRELATIONX iterates over a table. SUMMARIZECOLUMNS creates a temporary table with the relevant granularities (LocationID, YearStart) and the measures evaluated in that context.
FILTER(..., NOT ISBLANK(...)) is important to exclude data points where one or both measures might be blank, which would skew the correlation.
Interpretation: A value closer to +1 indicates a strong positive correlation.
Step 5: Analyzing Specific Demographics
To analyze correlations for specific demographics (women 18-44, youth, heavy drinkers):

Use the specific measures you created in Step 2.
Create separate scatter plots (e.g., BingeDrinkingPrev_Women18_44 vs. LiverDiseaseMortalityRate).
Consider StratificationID in your visuals: If you want to see how different stratifications relate to mortality, you can put StratificationID on the "Legend" or "Small Multiples" well of your scatter plot. However, keep in mind that liver disease mortality itself might not always be available at every specific demographic stratification in your CDI data; it's often reported for broader groups (e.g., "Overall" or "Age-adjusted"). You'll need to align the granularity.



and if per capita consumption also has influence on chronic liver disease mortality

1. what are the most and least common chronic disease indicator categories?
what I want to do is instaed of using the same CDI table I want to make a calculated table for each graph namely the top 5 most common categories of chronic disease indicators in the US in all states and stratifications, years, questions, and data value types
2. what is the trend of chronic disease indicator categories across all years
3. in the 5 most common chronic disease indicator categories which question

4. What is the prevalence of alcohol use among youth (male and female) in listed year ranges?
5. What is the average binge drinking prevalence among adults aged >= 18?
6. What is the average binge drinking prevalence for each reported ethnicity?

* because there are 10 years worth of data and 51 states for each of those 10 years we can use a slicer again to filter by state


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