# pull only specific data for testing

import requests
from datetime import datetime
import json
import psycopg2 
import pandas as pd 
from sqlalchemy import create_engine

current_date = datetime.today().strftime('%Y_%m_%d')

# --------------------
# - teams -
# open raw data
with open('teams_2023_09_03_18_51_03.json') as json_data:
    data = json.loads(json_data.read())

# store data in dataframe
df = pd.DataFrame([data])

# transformations
df = pd.json_normalize(df.to_dict(orient='records'), record_path=["response"])
df = df.drop(['venue.name','venue.address','venue.city','venue.capacity','venue.surface','venue.image'], axis=1)

ct = datetime.now()
df['load_timestamp'] = ct

# upload to staging area
print(df)

# --------------------
# - venues -
# open raw data
with open('venues_2023_09_03_18_45_40.json') as json_data:
    data = json.loads(json_data.read())

# store data in dataframe
df = pd.DataFrame([data])

# transformations
df = pd.json_normalize(df.to_dict(orient='records'), record_path=["response"])

ct = datetime.now()
df['load_timestamp'] = ct

# upload to staging

print(df)