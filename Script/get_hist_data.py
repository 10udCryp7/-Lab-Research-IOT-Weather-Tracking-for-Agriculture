import requests
import json
import pandas as pd

api_key = "d51fcdae91688868faff22223bd3f663"
base_url = "https://history.openweathermap.org/data/2.5/history/city?"
lat = "21.03788790200899"
lon = "105.782678995109"
type = "hour"
start = "0"
end = "1705398180000"

complete_url = base_url + "lat=" + lat + "&lon=" + lon + "&type=" + type + "&start=" + start + "&appid=" + api_key

response = requests.get(complete_url)

data = response.json

print(response.json())

df = pd.read_json()


# df.to_csv("output.csv", index=False)