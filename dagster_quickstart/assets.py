import json
import requests
import io
import polars
import datetime
import pandas as pd

from dagster import (
    MaterializeResult,
    MetadataValue,
    asset,
)

    
@asset
def people_asset():
    url = "https://drive.google.com/uc?id=1phaHg9objxK2MwaZmSUZAKQ8kVqlgng4&export=download"
    filename = "people-100"

    try:
        response = requests.get(url)
        response.raise_for_status()  
        
        dataframe = polars.read_csv(io.StringIO(response.text))
        
        # dataframe.write_csv(f"{filename}.csv")

    except Exception as e:
        print("An error occurred:", e)
        return None
    
    return dataframe


@asset(deps=[people_asset])
def unique_firstnames() -> MaterializeResult:
    dataframe = people_asset()
    column = "First Name"

    unique_firstnames = dataframe.select(column).unique()

    preview = str(unique_firstnames)

    return MaterializeResult(
        metadata={
            "num_unique_firstnames": len(unique_firstnames),
            "preview": MetadataValue.md(preview),
        }
    )
    
    
@asset(deps=[people_asset])
def anonymization_by_lastname() -> MaterializeResult:
    dataframe = people_asset()
    column = "Last Name"

    shuffled_lastnames = polars.col(column).shuffle(seed=1)
    
    dataframe_shuffled = dataframe.with_columns(shuffled_lastnames.alias(column))

    preview = str(dataframe_shuffled)

    return MaterializeResult(
        metadata={
            "num_shuffled_lastnames": len(dataframe_shuffled),
            "preview": MetadataValue.md(preview),
        }
    )
    

def helper_calculate_age(birthdates: polars.Series) -> polars.Series:
    today = datetime.date.today()
    
    ages = polars.Series([], dtype=polars.Int64)
    
    for birthdate in birthdates:
        age = today.year - birthdate.year - ((today.month, today.day) < (birthdate.month, birthdate.day))
        
        series = polars.Series([age])
        ages.append(series)
        
    return ages


@asset(deps=[people_asset])
def mean_age_per_profession() -> MaterializeResult:
    dataframe = people_asset()
    column_profession = "Job Title"
    column_birthdate = "Date of birth"
    column_age = "Age"

    # Convert birthdate column to datetime using polars to_date
    birthdate = dataframe[column_birthdate].str.to_date("%Y-%m-%d")
    dataframe = dataframe.with_columns(birthdate.alias(column_birthdate))
    
    ages = helper_calculate_age(dataframe[column_birthdate])

    dataframe = dataframe.with_columns(ages.alias(column_age))
    print(dataframe)
    
    preview = str(dataframe)
    
    return MaterializeResult(
        metadata={
            "num_mean_age": len(dataframe),
            "preview": MetadataValue.md(preview),
        }
    )

    
mean_age_per_profession()