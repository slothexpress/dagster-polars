import json
import requests

import pandas as pd

from dagster import (
    MaterializeResult,
    MetadataValue,
    asset,
)