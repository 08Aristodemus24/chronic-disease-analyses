import pandas as pd
import numpy as np
import os
import json 
import re

def column_summary(df: pd.DataFrame):
    """
    returns a dataframe of all columns in the dataframe
    along with their corresponding data type, no. of nulls,
    no. of non-null values, the number of each distinct values
    of the column
    """

    summary_data = []
    
    # loops through each column in dataframe
    for col_name in df.columns:
        # get dtype of column
        col_dtype = df[col_name].dtype

        # count number of nulls in column
        num_of_nulls = df[col_name].isnull().sum()

        # count number of 
        num_of_non_nulls = df[col_name].notnull().sum()

        # get the number of all unique values in the column
        num_of_distinct_values = df[col_name].nunique()
        
        # if number of unique values is less than 10 then we can turn the value_counts()
        # return value of the column to a dictionary
        if num_of_distinct_values <= 10:
            distinct_values_counts = df[col_name].value_counts().to_dict()
        else:
            # limit only value counts to the top 10 distinct values with the most counts
            top_10_values_counts = df[col_name].value_counts().head(10).to_dict()
            distinct_values_counts = {k: v for k, v in sorted(top_10_values_counts.items(), key=lambda item: item[1], reverse=True)}

        summary_data.append({
            'col_name': col_name,
            'col_dtype': col_dtype,
            'num_of_nulls': num_of_nulls,
            'num_of_non_nulls': num_of_non_nulls,
            'num_of_distinct_values': num_of_distinct_values,
            'distinct_values_counts': distinct_values_counts
        })
    
    summary_df = pd.DataFrame(summary_data)
    return summary_df



def model_population_table(df: pd.DataFrame, state: str) -> pd.DataFrame:
    """
    models a table from an excel spreadsheet containing
    all population numbers of a state by age group and sex
    """

    def helper(bracket: str | None):
        bracket = bracket.lower()
        keyword = re.search(r"(under|to|and over)", bracket)
        keyword = np.nan if not keyword else keyword[0]
        numbers = re.findall(r"\d+", bracket)
        # print(keyword)
        # print(numbers)

        # e.g. "under 5" becomes "< 5"
        if keyword == "under":
            return f"< {numbers[-1]}"
        
        # e.g. "5 to 9" becomes "5 <= 9"
        elif keyword == "to":
            return f"{numbers[0]} <= {numbers[-1]}"
        
        # e.g. "9 and over" becomes ">= 9"
        elif keyword == "and over": 
            return f">= {numbers[-1]}"
    
    # get start of population values with male sex
    male_start = df[df[0] == "MALE"].index.to_list()[0]

    pop_brackets_raw = df.iloc[male_start:]

    female_start = pop_brackets_raw[pop_brackets_raw[0] == "FEMALE"].index.to_list()[0]
    male_end, female_end = pop_brackets_raw[pop_brackets_raw[0] == ".Median age (years)"].index.to_list()

    # split the excel spreadsheet into the male and female population brackets
    pop_brackets_raw = {"male": df.iloc[male_start:male_end], "female": df.iloc[female_start:female_end]}

    # collects population brackets of females and males
    pop_brackets_final = []
    for gender in ["male", "female"]:
        # Remove the following`
        # * column `1`, column `12`, and column `13` (the reasoning is these contain only the population estimates of april 1 and not the most recent one which is supposed to be at july 1, and that column `13` is the year 2010 which already exists in the next population years)
        # * rows with mostly Nan and the a dot symbol in column `1` i.e. `[. Nan Nan Nan Nan Nan ... Nan]`
        # * and the male column 

        # we also rename the columns to be `bracket`, `2000`, `2001`, `2002`, `2003`, `2004`, `2005`, `2006`, `2007`, `2008`, `2009`
        cond = (pop_brackets_raw[gender][0] != ".") & (pop_brackets_raw[gender][0] != gender.upper())
        name_map = {0: "bracket", 2: 2000, 3: 2001, 4: 2002, 5: 2003, 6: 2004, 7: 2005, 8: 2006, 9: 2007, 10: 2008, 11: 2009}
        temp = pop_brackets_raw[gender][cond].drop(columns=[1, 12, 13]).rename(columns=name_map).reset_index(drop=True)
        
        # we rename also the bracket column values  
        temp["bracket"] = temp["bracket"].apply(helper)

        # we remove any duplicates in the dataframe especially those with same 
        # age brackets
        temp = temp.drop_duplicates(ignore_index=True)

        # wee transpose the dataframe
        temp = temp.T

        # we would want our first row which would now be our age brackets
        # to be our headers instead and the indeces we have which contain
        # our years we would want as a column instead
        # get first row as headers but exclude the value with bracket as we
        # won't use this as a column header
        temp = temp.reset_index()
        headers = temp.iloc[0]
        temp.columns = headers
        temp = temp.iloc[1:]

        final_name_map = {"bracket": "year"}
        pop_bracket_final = temp.rename(columns=final_name_map)

        pop_bracket_final["sex"] = gender
        pop_bracket_final["state"] = state

        # append genders final population brackets
        pop_brackets_final.append(pop_bracket_final)

    final = pd.concat(pop_brackets_final, axis=0, ignore_index=True)
    return final