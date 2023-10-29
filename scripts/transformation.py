from typing import Any
import pandas as pd
import json

def rename_columns(df : pd.DataFrame, new_columns:str) -> pd.DataFrame:
    with open('scripts/static/new_columns.json', 'r') as json_file:
        map = json.load(json_file)
    
    df = df.rename(columns=map[new_columns])
    
    return df

def transpose_columns(df : pd.DataFrame,
                      columns:list, 
                      transpose_column:list, 
                      column_name: str, 
                      column_value_name:str) -> pd.DataFrame:
    df = pd.melt(df, id_vars=columns, value_vars=transpose_column, var_name=column_name, value_name=column_value_name)
    return df

def concat_columns(df : pd.DataFrame, column_name:str, col_1: Any, col_2: Any) -> pd.DataFrame:
    df[column_name] = df[col_1].astype(str) + df[col_2].astype(str)
    return df

def filter_column(df : pd.DataFrame, _filter: object) -> pd.DataFrame:
    df = df.loc[_filter]
    return df

def add_lit_column(df : pd.DataFrame, column_name:str, value:Any) -> pd.DataFrame:
    df[column_name] = value
    return df

def add_month_number_column(df: pd.DataFrame) -> pd.DataFrame:
    with open('scripts/static/month_number.json', 'r') as json_file:
        month_mapping = json.load(json_file)
    
    df['month_number'] = df['month'].map(month_mapping)
    
    return df

def replace_values(df : pd.DataFrame, column_name:str, old_value: str, new_value : str) -> pd.DataFrame:
    df[column_name] = df[column_name].str.replace(old_value, new_value)
    return df

def title_columns(df : pd.DataFrame) -> pd.DataFrame:
    df = df.map(lambda x: x.title() if (isinstance(x, str)) & (x != 'm3') else x)
    return df

def cast_type(df : pd.DataFrame) -> pd.DataFrame:
    df['year_month'] = pd.to_datetime(df['year_month'], format='%Y%m')
    df['uf'] = df['uf'].astype(str)
    df['product'] = df['product'].astype(str)
    df['unit'] = df['unit'].astype(str)
    df['volume'] = df['volume'].astype(float)
    df['created_at'] = pd.to_datetime(df['created_at'])
    return df
