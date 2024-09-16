"""
File to store constants
"""
from enum import Enum

class Players(Enum):
    """
    Players ETL constants
    """
    headers = {
	    "X-RapidAPI-Key": "09024c9128msh264ab4739c90641p185fcajsnb24343c30706",
	    "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
    }
    url = "https://api-football-v1.p.rapidapi.com/v3/players"
    conn_string = 'postgresql://api_football_user:tkilper42@192.168.65.254/api_football_db' 
    #postgresql://api_football_user:tkilper42@127.0.0.1/api_football_db
    # host.minikube.internal = 192.168.65.254
    table_name = 'fact_players'
    schema_name = 'test_user'
    querystring = {"league":"39","season":"2024"}


class Teams(Enum):
    """
    Teams ETL constants
    """
    headers = {
	    "X-RapidAPI-Key": "09024c9128msh264ab4739c90641p185fcajsnb24343c30706",
	    "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
    }
    url = "https://api-football-v1.p.rapidapi.com/v3/teams"
    conn_string = 'postgresql://api_football_user:tkilper42@192.168.65.254/api_football_db'
    table_name = 'dim_teams'
    schema_name = 'test_user'
    querystring = {"league":"39","season":"2024"}

class Venues(Enum):
    """
    Venues ETL constants
    """
    url = "https://api-football-v1.p.rapidapi.com/v3/venues"
    headers = {
	    "X-RapidAPI-Key": "09024c9128msh264ab4739c90641p185fcajsnb24343c30706",
	    "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
    }
    conn_string = 'postgresql://api_football_user:tkilper42@192.168.65.254/api_football_db'
    table_name = 'dim_venues'
    schema_name = 'test_user'
    querystring = {"country":"England"}

class Fixtures(Enum):
    """
    Fixtures ETL constants
    """
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
    url_stats = "https://api-football-v1.p.rapidapi.com/v3/fixtures/statistics"
    url_lineups = "https://api-football-v1.p.rapidapi.com/v3/fixtures/lineups"
    url_events = "https://api-football-v1.p.rapidapi.com/v3/fixtures/events"
    url_addons = ["/statistics", "/lineups", "/events"]
    querystring = {"league":"39","season":"2024"}
    headers = {
	    "X-RapidAPI-Key": "09024c9128msh264ab4739c90641p185fcajsnb24343c30706",
	    "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
    }
    conn_string = 'postgresql://api_football_user:tkilper42@192.168.65.254/api_football_db'
    table_name_1 = 'fact_fixtures'
    table_name_2 = 'dim_fixtures_events'
    table_name_3 = 'dim_fixtures_lineups'
    table_name_4 = 'dim_fixtures_stats'
    schema_name = 'test_user'