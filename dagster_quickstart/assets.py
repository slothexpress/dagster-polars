import json
import requests
import io
import polars
import datetime

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
def anonymise_by_lastname() -> MaterializeResult:
    dataframe = people_asset()
    column = "Last Name"

    print(dataframe)
    shuffled_lastnames = polars.col(column).shuffle(seed=1)
    print(shuffled_lastnames)
    
    dataframe_shuffled = dataframe.with_columns(shuffled_lastnames.alias(column))
    print(dataframe_shuffled)

    preview = str(dataframe)

    return MaterializeResult(
        metadata={
            "num_unique_firstnames": len(dataframe),
            "preview": MetadataValue.md(preview),
        }
    )
    
    
@asset(deps=[people_asset])
def mean_age_per_profession() -> MaterializeResult:
    dataframe = people_asset()
    column_profession = "Job Title"
    column_birthdate = "Date of birth"
    column_age = "Age"
    today = datetime.date.today()

    profession_and_birthdate_dataframe = dataframe.select([column_profession, column_birthdate])
    
    profession_and_age = profession_and_birthdate_dataframe.with_columns((polars.col(column_birthdate)).alias(column_age))
    print(profession_and_age)
    
    
    
anonymise_by_lastname()