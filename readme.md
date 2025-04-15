# Usage:
* navigate to directory with `readme.md` and `requirements.txt` file
* run command; `conda create -n <name of env e.g. chronic-disease-analyses> python=3.12.3`. Note that 3.11.4 must be the python version otherwise packages to be installed would not be compatible with a different python version
* once environment is created activate it by running command `conda activate`
* then run `conda activate chronic-disease-analyses`
* check if pip is installed by running `conda list -e` and checking list
* if it is there then move to step 8, if not then install `pip` by typing `conda install pip`
* if `pip` exists or install is done run `pip install -r requirements.txt` in the directory you are currently in
* run `python extract_population_per_us_state.py` 
* run notebook `extract_population_with_age_per_us_state.ipynb` with kernel `chronic-disease-analyses` 
* 

# For Sharing:
* Day 2 of this data analytics project: https://github.com/08Aristodemus24/chronic-disease-analyses

TLDR: I've learned that the main `chronic disease indicators` dataset from https://www.kaggle.com/datasets/irakozekelly/u-s-chronic-disease-indicators-2023-release?resource=download just wasn't enough, and pulling more population data from other sources made sense, but funnily enough also needed more cleaning and some sort data modelling as it was still in a spreadsheet that was difficult to query using tools like SQL.

Going through much cleaning and preprocessing the dataset had rows with attribute/column values like these
```
yearstart: 2012
yearend: 2016
locationdesc: connecticut
question: cancer of the lung and bronchus mortality
datavalueunit: cases per 100000
datavaluetype: average annual crude rate
datavalue: 9.6
stratificationcategory: race/ethnicity
stratification: asian or pacific islander
```

where this can be interpreted as the `average annual crude rate of cancer of the lung and bronchus mortality from 2012 to 2016 in the state of connecticut for an asian or pacific islander is 9.6 cases per 100000`. Another example:

```
yearstart: 2015
yearend: 2015
locationdesc: florida
question: Hospitalization for chronic obstructive pulmonary disease as any diagnosis
datavalueunit: cases per 10000
datavaluetype: crude rate
datavalue: 9.6
stratificationcategory: race/ethnicity
stratification: asian or pacific islander
```

interpretation: `crude rate of for hospitalization for chronic obstructive pulmonary disease as any diagnosis from 2015 to 2015 in the state of florida for the male population was 420 cases per 10000`

I thought to myself that this if different rows of the data had different data value units and different data value types how then was I supposed to make some analyses if the values to begin with were incomparable. So I asked Gemini how was I going to make these rows have tangible numbers to work with particularly how was I going to calculate perhaps the total cases of a chronic disease indicator (CDI) on a population scale? And the answer was to pull the total population values of particularly each state per year of the US from 2001 to 2021. 

So that I did but somehow I saw that there was still problems like stratifications I had to deal with; using the total population for all ethnicities for both sexes wasn't enough, because what if the CDI had `current asthma prevalence among male adults aged >= 18 years` and a stratification of `asian`? Then using the total population is not enough as this CDI entails that the number was measured was with a demographic only of the male population aged 18 and above and on top of that were only asian.

Initially I thought removing the rows with a stratification of race/ethnicity but I found that discarding roughly 500000+ rows out of 600000+ total rows seemed to waste too much data points, and that keeping the remaining data points which only had a stratitification of gender seemed to be easier since I already extracted other data pertaining to the population with stratification of gender and their respective age brackets. But doing this seemed to be an injustice to the data and doing analysis with little data would be a waste. So I did my best to collate more population data for each us state that included stratifications of all races and ethnicities. 

But the hard part wasn't this suprisingly, the hard part was transforming the spreadsheets into a format that was not only readable but also easier to query and make some sort of aggregation so that when some operation like summing the population arises it can be easily done through tools like SQL. How I thought of doing the process of somewhat modellin the data from spreadsheet to a SQL table is detailed in the pictures below.

Thanks for reading