# THIS CODE CONNECTS TO API THINGSBOARD (GET DATA FROM API OPENWEATHER), COLLECT DATA AND BRING IT TO POSTGRESQL DATABASE


import requests
import json
import psycopg2

# API
url = "http://localhost:8080/api/plugins/telemetry/ASSET/99643c50-7731-11ee-b094-a797de75d9ec/values/timeseries"
querystring = {"keys":"outsideHumidity,outsideMaxTemp,outsideMinTemp,outsideTemp","startTs":"1699002500000","endTs":"1699002520000"}

headers = {
    "accept": "application/json",
    "X-Authorization": "Bearer eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ0ZW5hbnRAdGhpbmdzYm9hcmQub3JnIiwidXNlcklkIjoiMGY3NmRmZDAtNmU0ZC0xMWVlLTk5YjUtZmY0MGI1ZTk4MjUxIiwic2NvcGVzIjpbIlRFTkFOVF9BRE1JTiJdLCJzZXNzaW9uSWQiOiIxZTVhOWUxZS0xOTg1LTRkZDctYTg5ZS03ZGY3ZWYxNzMwN2IiLCJpc3MiOiJ0aGluZ3Nib2FyZC5pbyIsImlhdCI6MTY5OTExNTMxMywiZXhwIjoxNjk5MTI0MzEzLCJlbmFibGVkIjp0cnVlLCJpc1B1YmxpYyI6ZmFsc2UsInRlbmFudElkIjoiMGYyNjc1ZTAtNmU0ZC0xMWVlLTk5YjUtZmY0MGI1ZTk4MjUxIiwiY3VzdG9tZXJJZCI6IjEzODE0MDAwLTFkZDItMTFiMi04MDgwLTgwODA4MDgwODA4MCJ9.9HQrcGCuNpo3g53TPaV14C9-SMl-h6YGqIWpUt8F_hwu15Rs1fwMOw9ZhTuRAYUFFqxppVSg0ieElUt0n5sg4Q"
}

# Get data from API, return json string
response = requests.request("GET", url, headers=headers, params=querystring)

# Convert json string to dict
data = json.loads(response.text)

# Initial database
conn = psycopg2.connect(database="weather", user="postgres", password="postgres", host="localhost", port="5432")
cur = conn.cursor()

# Insert the data into the PostgreSQL table
outsideHumidityArr = data['outsideHumidity']

outsideMaxTempArr = data['outsideMaxTemp']
outsideMinTempArr = data['outsideMinTemp']
outsideTempArr = data['outsideTemp']


for i in range(len(data['outsideHumidity'])):
    
    cur.execute('INSERT INTO "weatherData"("outsideHumidity", "outsideMaxTemp", "outsideMinTemp", "outsideTemp") VALUES (%s,%s,%s,%s)', (outsideHumidityArr[i]['value'], outsideMaxTempArr[i]['value'], outsideMinTempArr[i]['value'], outsideTempArr[i]['value']))
    

# Commit the changes and close the connection
conn.commit()
cur.close()
conn.close()


