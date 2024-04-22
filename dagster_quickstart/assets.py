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
        
        dataframe.write_csv(f"{filename}.csv")

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
    