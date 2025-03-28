import pandas as pd
import numpy as np
import os
import json 

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