# *** stage raw data ***

import requests
from datetime import datetime
import json
import psycopg2 
import pandas as pd 
from sqlalchemy import create_engine

current_date = datetime.today().strftime('%Y_%m_%d')

conn_string = 'postgres://api_football_user:tkilper42@Postgres 15 - Localhost - api_football_user/api_football_db'
  
db = create_engine(conn_string) 
conn = db.connect()

# ---------------------
# - fixture events -
# open raw data
with open('fixture_events_' + current_date + '.json') as json_data:
    data = json.loads(json_data.read())

# store raw data into a dataframe
df = pd.json_normalize(data, ["response"], [["parameters",'fixture']])

# transformations
df = df.drop(['team.name','team.logo','player.name','assist.name'], axis=1)

ct = datetime.datetime.now()
df['load_timestamp'] = ct

# upload to staging area
conn_string = 'postgresql://api_football_user:tkilper42@127.0.0.1/api_football_db'
  
db = create_engine(conn_string) 
conn = db.connect()

df.to_sql('dim_fixture_events', con=conn, schema='Staging', if_exists='replace', index=False) 
conn1 = psycopg2.connect(conn_string) 
conn1.autocommit = True
cursor = conn1.cursor() 

sql1 = '''select * from "Staging"."dim_fixture_events";'''
cursor.execute(sql1) 
for i in cursor.fetchall(): 
    print(i)
  
# conn.commit()
conn.close()
conn1.close() 

# --------------------
# - fixture lineups -
# open raw data
with open('fixture_lineups_2023_09_06_18_12_50.json') as json_data:
    data = json.loads(json_data.read())

# store data in dataframe
df = pd.DataFrame([data])

# transformations
df = pd.json_normalize(df.to_dict(orient='records'), record_path=["response"], meta=[["parameters",'fixture']])
df_startxi = pd.json_normalize(df.to_dict(orient='records'), record_path=["startXI"], meta=["formation","team.id","coach.id","parameters.fixture"])
df_startxi["status"] = 'start'
df_subs = pd.json_normalize(df.to_dict(orient='records'), record_path=["substitutes"], meta=["formation","team.id","coach.id","parameters.fixture"])
df_subs["status"] = 'sub'
dfs = [df_startxi, df_subs]
df = pd.concat(dfs)
df = df.drop(['player.name'],axis=1)

ct = datetime.now()
df['load_timestamp'] = ct

print(df)

# upload to staging area
conn_string = 'postgresql://api_football_user:tkilper42@127.0.0.1/api_football_db'
  
db = create_engine(conn_string) 
conn = db.connect()

df.to_sql('dim_fixture_lineups', con=conn, schema='Staging', if_exists='replace', index=False) 
conn1 = psycopg2.connect(conn_string) 
conn1.autocommit = True
cursor = conn1.cursor() 

sql1 = '''select * from "Staging"."dim_fixture_lineups";'''
cursor.execute(sql1) 
for i in cursor.fetchall(): 
    print(i)
  
# conn.commit()
conn.close()
conn1.close()

# ---------------------
# - fixture stats -
# open raw data
with open('fixture_stats_2023_09_06_17_59_17.json') as json_data:
    data = json.loads(json_data.read())

# store data in dataframe
df = pd.DataFrame([data])

# transformations
df = pd.json_normalize(df.to_dict(orient='records'), record_path=["response"], meta=[["parameters",'fixture']])
df = pd.json_normalize(df.to_dict(orient='records'), record_path=["statistics"], meta=["team.id","parameters.fixture"])

ct = datetime.now()
df['load_timestamp'] = ct

# upload to staging area

# --------------------
# - fixture general -
# open raw data
with open('fixtures_' + current_date + '.json') as json_data:
    data = json.loads(json_data.read())

# store data in dataframe
df = pd.json_normalize(data, "response")

# transformations
df = df.drop(['fixture.venue.name','fixture.venue.city','teams.home.name','teams.home.logo','teams.away.name','teams.away.logo'],axis=1)

ct = datetime.datetime.now()
df['load_timestamp'] = ct

# upload to staging area
conn_string = 'postgresql://api_football_user:tkilper42@127.0.0.1/api_football_db'
  
db = create_engine(conn_string) 
conn = db.connect()

df.to_sql('fact_fixture', con=conn, schema='Staging', if_exists='replace', index=False) 
conn1 = psycopg2.connect(conn_string) 
conn1.autocommit = True
cursor = conn1.cursor() 

sql1 = '''select * from "Staging"."fact_fixture";'''
cursor.execute(sql1) 
for i in cursor.fetchall(): 
    print(i)
  
# conn.commit()
conn.close()
conn1.close()

# --------------------
# - players -
# open raw data
with open('players_' + current_date + '.json') as json_data:
    data = json.loads(json_data.read())

# store data in dataframe
df = pd.DataFrame([data])

# transformations
response_df = pd.json_normalize(df.to_dict(orient='records'), record_path=["response"])
stats_df = pd.json_normalize(response_df.to_dict(orient='records'), record_path=["statistics"], meta=["player.id"])
df = pd.merge(response_df.drop('statistics', axis=1), stats_df, on='player.id')

ct = datetime.now()
response_df['load_timestamp'] = ct
stats_df['load_timestamp'] = ct

response_df = response_df.drop(['statistics'],axis=1)
stats_df = stats_df.drop(['team.name','team.logo'],axis=1)

print(stats_df)
print(list(stats_df.columns))

# upload to staging area
conn_string = 'postgresql://api_football_user:tkilper42@127.0.0.1/api_football_db'
  
db = create_engine(conn_string) 
conn = db.connect()

df.to_sql('fact_fixture', con=conn, schema='Staging', if_exists='replace', index=False) 
conn1 = psycopg2.connect(conn_string) 
conn1.autocommit = True
cursor = conn1.cursor() 

sql1 = '''select * from "Staging"."fact_fixture";'''
cursor.execute(sql1) 
for i in cursor.fetchall(): 
    print(i)
  
# conn.commit()
conn.close()
conn1.close()

# --------------------
# - teams -
# open raw data
with open('teams_' + current_date + '.json') as json_data:
    data = json.loads(json_data.read())

# store data in dataframe
df = pd.DataFrame([data])

# transformations
df = pd.json_normalize(df.to_dict(orient='records'), record_path=["response"])
df = df.drop(['venue.name','venue.address','venue.city','venue.capacity','venue.surface','venue.image'], axis=1)

ct = datetime.datetime.now()
df['load_timestamp'] = ct

# upload to staging area

# --------------------
# - venues -
# open raw data
with open('venues_' + current_date + '.json') as json_data:
    data = json.loads(json_data.read())

# store data in dataframe
df = pd.DataFrame([data])

# transformations
df = pd.json_normalize(df.to_dict(orient='records'), record_path=["response"])

ct = datetime.datetime.now()
df['load_timestamp'] = ct

# upload to staging
