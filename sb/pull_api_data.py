# Pull raw data from API-Footbal and store it in system memory as json

import requests
from datetime import datetime
import json

# current date
current_date = datetime.today().strftime('%Y_%m_%d')

# fixtures
url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"

querystring = {"league":"39","season":"2023"}

headers = {
	"X-RapidAPI-Key": "09024c9128msh264ab4739c90641p185fcajsnb24343c30706",
	"X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
}

fixtures = requests.get(url, headers=headers, params=querystring)

print(fixtures.json())

file_name = 'C:\\Users\\Tristan\\OneDrive\\Desktop\\API_football_etl_project\\fixtures_' + current_date + '.json'

with open(file_name, "w") as outfile:
    json.dump(fixtures.json(), outfile)
    
# venues
url = "https://api-football-v1.p.rapidapi.com/v3/venues"

querystring = {"country":"England"}

headers = {
	"X-RapidAPI-Key": "09024c9128msh264ab4739c90641p185fcajsnb24343c30706",
	"X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
}

venues = requests.get(url, headers=headers, params=querystring)

print(venues.json())

file_name = 'C:\\Users\\Tristan\\OneDrive\\Desktop\\API_football_etl_project\\venues_' + current_date + '.json'

with open(file_name, "w") as outfile:
    json.dump(venues.json(), outfile)

# league
url = "https://api-football-v1.p.rapidapi.com/v3/leagues"

querystring = {"id":"39"}

headers = {
	"X-RapidAPI-Key": "09024c9128msh264ab4739c90641p185fcajsnb24343c30706",
	"X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
}

league = requests.get(url, headers=headers, params=querystring)

print(league.json())

file_name = 'C:\\Users\\Tristan\\OneDrive\\Desktop\\API_football_etl_project\\league_' + current_date + '.json'

with open(file_name, "w") as outfile:
    json.dump(league.json(), outfile)

# players
url = "https://api-football-v1.p.rapidapi.com/v3/players"

querystring = {"league":"39","season":"2023"}

headers = {
	"X-RapidAPI-Key": "09024c9128msh264ab4739c90641p185fcajsnb24343c30706",
	"X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
}

players = requests.get(url, headers=headers, params=querystring)

print(players.json())

file_name = 'C:\\Users\\Tristan\\OneDrive\\Desktop\\API_football_etl_project\\players_' + current_date + '.json'

with open(file_name, "w") as outfile:
    json.dump(players.json(), outfile)

# teams
url = "https://api-football-v1.p.rapidapi.com/v3/teams"

querystring = {"league":"39"}

headers = {
	"X-RapidAPI-Key": "09024c9128msh264ab4739c90641p185fcajsnb24343c30706",
	"X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
}

teams = requests.get(url, headers=headers, params=querystring)

print(teams.json())

file_name = 'C:\\Users\\Tristan\\OneDrive\\Desktop\\API_football_etl_project\\teams_' + current_date + '.json'

with open(file_name, "w") as outfile:
    json.dump(teams.json(), outfile)
    
# fixture stats
url = "https://api-football-v1.p.rapidapi.com/v3/fixtures/statistics"

querystring = {"fixture":"1035037"}

headers = {
	"X-RapidAPI-Key": "09024c9128msh264ab4739c90641p185fcajsnb24343c30706",
	"X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
}

fixture_stats = requests.get(url, headers=headers, params=querystring)

print(fixture_stats.json())

file_name = 'C:\\Users\\Tristan\\OneDrive\\Desktop\\API_football_etl_project\\fixture_stats_' + current_date + '.json'

with open(file_name, "w") as outfile:
    json.dump(fixture_stats.json(), outfile)
    
# fixture events
url = "https://api-football-v1.p.rapidapi.com/v3/fixtures/events"

querystring = {"fixture":"1035037"}

headers = {
	"X-RapidAPI-Key": "09024c9128msh264ab4739c90641p185fcajsnb24343c30706",
	"X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
}

fixture_events = requests.get(url, headers=headers, params=querystring)

print(fixture_events.json())

file_name = 'C:\\Users\\Tristan\\OneDrive\\Desktop\\API_football_etl_project\\fixture_events_' + current_date + '.json'

with open(file_name, "w") as outfile:
    json.dump(fixture_events.json(), outfile)
    
# fixture lineups
url = "https://api-football-v1.p.rapidapi.com/v3/fixtures/lineups"

querystring = {"fixture":"1035037"}

headers = {
	"X-RapidAPI-Key": "09024c9128msh264ab4739c90641p185fcajsnb24343c30706",
	"X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
}

fixture_lineups = requests.get(url, headers=headers, params=querystring)

print(fixture_lineups.json())

file_name = 'C:\\Users\\Tristan\\OneDrive\\Desktop\\API_football_etl_project\\fixture_lineups_' + current_date + '.json'

with open(file_name, "w") as outfile:
    json.dump(fixture_lineups.json(), outfile)

# player stats
url = "https://api-football-v1.p.rapidapi.com/v3/players"

querystring = {"league":"39","season":"2023","page":"10"}

headers = {
	"X-RapidAPI-Key": "09024c9128msh264ab4739c90641p185fcajsnb24343c30706",
	"X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
}

player_stats = requests.get(url, headers=headers, params=querystring)

print(player_stats.json())

file_name = 'C:\\Users\\Tristan\\OneDrive\\Desktop\\API_football_etl_project\\player_stats_' + current_date + '.json'

with open(file_name, "w") as outfile:
    json.dump(player_stats.json(), outfile)