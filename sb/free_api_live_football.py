import requests
import json
from datetime import datetime

current_date = datetime.today().strftime('%Y_%m_%d')

# player stats
url = "https://free-api-live-football-data.p.rapidapi.com/football-get-list-player"

querystring = {"teamid":"8634"}

headers = {
	"X-RapidAPI-Key": "09024c9128msh264ab4739c90641p185fcajsnb24343c30706",
    "X-RapidAPI-Host": "free-api-live-football-data.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

data = json.dumps(response.json()['response']['list']['squad'])
print(data)

"""
file_name = 'C:\\Users\\Tristan\\OneDrive\\Desktop\\API_football_etl_project\\fcb_players_' + current_date + '.json'

with open(file_name, "w") as outfile:
    json.dump(data, outfile)"""