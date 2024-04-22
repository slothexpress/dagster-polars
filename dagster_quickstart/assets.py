import json
import requests
import polars
import io

from dagster import (
    MaterializeResult,
    MetadataValue,
    asset,
)


    
@asset
def people_100_asset():
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