import pandas as pd
import numpy as np
import os
import json 
import re
import ast

from concurrent.futures import ThreadPoolExecutor

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



def model_population_table(df: pd.DataFrame, state: str, cols_to_remove: list, year_range: str="2000-2009") -> pd.DataFrame:
    """
    models a table from an excel spreadsheet containing
    all population numbers of a state by age group and sex
    """

    def helper(bracket: str | None):
        bracket = bracket.lower()
        keyword = re.search(r"(under|to|and over|\+)", bracket)
        keyword = np.nan if not keyword else keyword[0]
        numbers = re.findall(r"\d+", bracket)
        numbers = [ast.literal_eval(number) for number in numbers]
        # print(keyword)
        # print(numbers)

        # e.g. "under 5" becomes "_under_5"
        if keyword == "under":
            return (0, numbers[-1])
        
        # e.g. "5 to 9" becomes "_5_to_9"
        elif keyword == "to":
            return (numbers[0], numbers[-1])
        
        # e.g. "9 and over" becomes "_9_and_over"
        elif keyword == "and over" or keyword == "+": 
            return (numbers[-1], float('inf'))
        
        # if it is a single number just return that number
        return (numbers[-1], np.nan)
    
    try:
        # ex`tract numbers from year range
        years = re.findall(r"\d+", year_range)
        lo_year = ast.literal_eval(years[0])
        hi_year = ast.literal_eval(years[-1])
        
        # commence preprocessing
        if lo_year == 2000 and hi_year == 2009:
            # get start of population values with male sex
            male_start = df[df[0] == "MALE"].index.to_list()[0]

            pop_brackets_raw = df.iloc[male_start:] 

            female_start = pop_brackets_raw[pop_brackets_raw[0] == "FEMALE"].index.to_list()[0]
            male_end, female_end = pop_brackets_raw[pop_brackets_raw[0] == ".Median age (years)"].index.to_list()

            # split the excel spreadsheet into the male and female population brackets
            pop_brackets_raw = {
                "male": df.iloc[male_start:male_end].reset_index(drop=True), 
                "female": df.iloc[female_start:female_end].reset_index(drop=True)
            }

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
                temp = pop_brackets_raw[gender][cond].drop(columns=cols_to_remove).rename(columns=name_map).reset_index(drop=True)

                # we remove any duplicates in the dataframe especially those with same 
                # age brackets
                temp = temp.drop_duplicates(ignore_index=True)

                # clean bracket column then turn the bracket
                # into an index
                temp["bracket"] = temp["bracket"].apply(lambda x: x.lower().strip("."))
                temp.index = temp["bracket"]

                # delete the bracket that's a column
                del temp["bracket"]

                # stack the rows of the dataframe vertically
                temp = temp.stack().reset_index()

                # rename the column names after stacking
                temp = temp.rename(columns={"level_1": "year", 0: "population"})
                temp["population"] = temp["population"].astype(int)

                # extract the lower and upper bound values in the age brackets
                age_ranges = temp["bracket"].apply(helper).to_list()
                temp["age_start"], temp["age_end"] = list(zip(*age_ranges))

                temp["sex"] = gender
                temp["state"] = state

                # append genders final population brackets
                pop_brackets_final.append(temp)

            final = pd.concat(pop_brackets_final, axis=0, ignore_index=True)
            return final
        
        elif (lo_year == 2010 and hi_year == 2019) or (lo_year == 2020 and hi_year == 2023):
            start_index = df[df[0] == ".0"].index.to_list()[0]
            end_index = df[df[0] == ".Median Age (years)"].index.to_list()[0]

            # extract necessary rows
            pop_brackets_raw = df.iloc[start_index: end_index]
            
            # remove duplicatess
            temp = pop_brackets_raw.drop_duplicates()

            # remove rows with at least 5 nan values
            temp = temp.dropna(thresh=5, axis=0)

            # remove columns 1 to 7 then increment by 3
            # [1, 2, 3, 4, 5, 6, 7, 10, 13, 16, 19, 22, 25, 28, 31, 34]
            temp = temp.drop(columns=cols_to_remove)

            temp.index = temp[0]
            del temp[0]

            # generate and create multi index for columns
            years = sorted(list(range(lo_year, hi_year + 1)) * 2)
            genders = ["male", "female"] * (len(years) // 2)
            multi_index_list = list(zip(years, genders))
            multi_index = pd.MultiIndex.from_tuples(multi_index_list)

            # set multi indexed columns and delete index name of rows
            temp.columns = multi_index
            temp.index.name = None

            # now we will have to stack each row vertically 
            # on each other and because we have multi 
            # indexed columns we will need to stack it twice 
            # in order to make these column indeces now be 
            # the row indeces
            temp = temp.stack(future_stack=True).stack(future_stack=True)

            # now we can reset the index such that these multi 
            # index rows now become columns of our new dataframe
            temp = temp.reset_index()

            # rename the newly converted columns to bracket, sex, year, and population respectively
            temp = temp.rename(columns={"level_0": "bracket", "level_1": "sex", "level_2": "year", 0: "population"})

            # clean bracket and population columns
            temp["bracket"] = temp["bracket"].apply(lambda x: x.lower().strip("."))
            temp["population"] = temp["population"].astype(int)
            
            # extract the age ranges
            age_ranges = temp["bracket"].apply(helper).to_list()
            temp["age_start"], temp["age_end"] = list(zip(*age_ranges))
            
            temp["state"] = "Alabama"

            return temp
    except Exception as e:
        print(f"Error {e} occured at state {state}.")    

def model_population_by_sex_race_ho_table(df: pd.DataFrame, state: str, cols_to_remove: list, year_range: str="2000-2009") -> pd.DataFrame:
    try:
        # extract numbers from year range
        years = re.findall(r"\d+", year_range)
        lo_year = ast.literal_eval(years[0])
        hi_year = ast.literal_eval(years[-1])
        years_list = list(range(lo_year, hi_year + 1))
        
        # commence preprocessing
        
        # save sub dataframes for it will be used for 
        # concatenation later
        pop_brackets_final = []

        # remove unnecessary columns lets say [1, 2] or [1, 12]
        # and rename the columns that are left to the years_list
        # excluding ethnicity column which is always the first column 0
        # {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12} - {0, 1, 12} = 
        # {2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
        new_cols = list(set(df.columns) - set(cols_to_remove + [0]))

        # new cols is calculated through set(df.columns) - set(cols_to_remove) 
        # we need to check if length of yeras_list == (length of new cols) - 1
        # in order to proceed with creating name mapper through
        # dictionary comprehension

        # assert len(new_cols) == len(years_list)

        # {2: 2000, 3: 2001, 4: 2002, 5: 2004, 6: 2005, 7: 2006, 8: 2007, 9: 2008
        # 10: 2009} will be the name mapper to rename the left out columns in the dataframe
        name_mapper = {new_col: years_list[i] for i, new_col in enumerate(new_cols)}
        
        # drop columsn first then rename columns left
        # and rename 0 to ethnicity column 
        temp = df.drop(columns=cols_to_remove)
        temp = temp.rename(columns=name_mapper)
        temp = temp.rename(columns={0: "ethnicity"})

        # clean ethnicity indicator column
        temp["ethnicity"] = temp["ethnicity"].apply(lambda string: np.nan if pd.isna(string) else string.strip(".").lower())

        if lo_year == 2000 and hi_year == 2009:

            # calculate the list slices here
            male_start = temp.index[temp["ethnicity"] == "male"].to_list()[0]
            female_start = temp.index[temp["ethnicity"] == "female"].to_list()[0]
            
            # since there are multiple indeces with the two 
            # or more races value we need to pick out the last value
            female_end = temp.index[temp["ethnicity"] == "two or more races"].to_list()[-1]
            
            for gender in ["male", "female"]:
                # determine the list slices during loop
                range_1 = slice(male_start, female_start) if gender == "male" else slice(female_start, female_end + 1)
                pop_bracket_1 = temp.iloc[range_1].reset_index(drop=True)

                # calculate the list slices here for origin
                non_hisp_start = pop_bracket_1.index[pop_bracket_1["ethnicity"] == "not hispanic"].to_list()[-1]
                hisp_start = pop_bracket_1.index[pop_bracket_1["ethnicity"] == "hispanic"].to_list()[-1]
                hisp_end = pop_bracket_1.index[pop_bracket_1["ethnicity"] == "two or more races"].to_list()[-1]

                for origin in ["not hispanic", "hispanic"]:
                    # determine the list slices for origin during loop
                    range_2 = slice(non_hisp_start + 2, hisp_start) if origin == "not hispanic" else slice(hisp_start + 2, hisp_end + 1)
                    pop_bracket_2 = pop_bracket_1.iloc[range_2].reset_index(drop=True)

                    # add new columns and rename columns before and after stacking
                    pop_bracket_2["origin"] = origin
                    pop_bracket_2["sex"] = gender
                    pop_bracket_2 = pop_bracket_2.set_index(keys=["ethnicity", "origin", "sex"])
                    pop_bracket_2 = pop_bracket_2.stack().reset_index()
                    pop_bracket_2 = pop_bracket_2.rename(columns={"level_3": "year", 0: "population"})

                    # set population to int
                    pop_bracket_2["population"] = pop_bracket_2["population"].astype(int)
                    pop_brackets_final.append(pop_bracket_2)
            
            # concatenate all sub dataframes into one single dataframe
            final = pd.concat(pop_brackets_final, axis=0, ignore_index=True)
            final["state"] = state

            return final

        elif (lo_year == 2010 and hi_year == 2019) or (lo_year == 2020 and hi_year == 2023):
            # start partitioning the spreadsheet by its important rows 
            # like the sex, and whether or not it is of hispanic origin
            male_start = temp.index[temp["ethnicity"] == "male"].to_list()[0]
            female_start = temp.index[temp["ethnicity"] == "female"].to_list()[0]

            # since there are multiple indeces with the two 
            # or more races value we need to pick out the last value
            female_end = temp.index[temp["ethnicity"] == "two or more races"].to_list()[-1]


            for gender in ["male", "female"]:
                # determine the list slices during loop
                range_1 = slice(male_start, female_start) if gender == "male" else slice(female_start, female_end + 1)
                pop_bracket_1 = temp.iloc[range_1].reset_index(drop=True)

                # calculate the list slices here for origin
                non_hisp_start = pop_bracket_1.index[pop_bracket_1["ethnicity"] == "not hispanic"].to_list()[0]
                
                # get the first occurence of the index as we are 
                # not looking for multiple occurences until it reaches 
                # the last occurence
                non_hisp_end = pop_bracket_1.loc[non_hisp_start:, :] \
                .index[pop_bracket_1.loc[non_hisp_start:, "ethnicity"].str.contains("race alone or in combination")].to_list()[0]

                hisp_start = pop_bracket_1.index[pop_bracket_1["ethnicity"] == "hispanic"].to_list()[-1]
                hisp_end = pop_bracket_1.index[pop_bracket_1["ethnicity"] == "two or more races"].to_list()[-1]

                for origin in ["not hispanic", "hispanic"]:
                    # determine the list slices for origin during loop
                    range_2 = slice(non_hisp_start + 2, non_hisp_end) if origin == "not hispanic" else slice(hisp_start + 2, hisp_end + 1)
                    pop_bracket_2 = pop_bracket_1.iloc[range_2].reset_index(drop=True)

                    # add new columns and rename columns before and after stacking
                    pop_bracket_2["origin"] = origin
                    pop_bracket_2["sex"] = gender
                    pop_bracket_2 = pop_bracket_2.set_index(keys=["ethnicity", "origin", "sex"])
                    pop_bracket_2 = pop_bracket_2.stack().reset_index()
                    pop_bracket_2 = pop_bracket_2.rename(columns={"level_3": "year", 0: "population"})
                    # print(pop_bracket_2)
                    # set population to int
                    pop_bracket_2["population"] = pop_bracket_2["population"].astype(int)
                    pop_brackets_final.append(pop_bracket_2)

            # concatenate all sub dataframes into one single dataframe
            final = pd.concat(pop_brackets_final, axis=0, ignore_index=True)
            final["state"] = state

            return final
        
    except Exception as e:
        print(f"Error {e} occured at state {state}.") 



def get_state_populations(DATA_DIR: str, cols_to_remove: list, populations: list, year_range: str, by: str) -> pd.DataFrame:
    def concur_model_pop_tables(file, to_remove, year_range, callback_fn=model_population_table):
        FILE_PATH = os.path.join(DATA_DIR, file)
        state = re.search(r"(^[A-Za-z\s]+)", file)
        state = "Unknown" if not state else state[0]

        # print(to_remove)
        # print(year_range)
        # read excel file
        df = pd.read_excel(FILE_PATH, dtype=object, header=None)
        
        state_population = callback_fn(df, state, to_remove, year_range=year_range)
        return state_population
    
    with ThreadPoolExecutor() as exe:
        callback_fn = model_population_by_sex_race_ho_table if "sex race and ho" in by else model_population_table
        state_populations = list(exe.map(
            concur_model_pop_tables, 
            populations, 
            [cols_to_remove] * len(populations),
            [year_range] * len(populations),
            [callback_fn] * len(populations)
        ))

    state_populations_df = pd.concat(state_populations, axis=0, ignore_index=True)

    return state_populations_df