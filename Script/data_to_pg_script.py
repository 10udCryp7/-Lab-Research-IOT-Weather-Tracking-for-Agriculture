# THIS CODE CONNECTS TO API THINGSBOARD (GET DATA FROM API OPENWEATHER), COLLECT DATA AND BRING IT TO POSTGRESQL DATABASE
# Tip: use Bing to generate code, as we have api detail
# Must do: change the frequency of data to  hours, as temperature doesn't change so much in seconds.

import requests
import json
import psycopg2

import pandas as pd

def calculateTimestamp(timeseries):

    # Create a sample timestamp


    # Convert timestamp to datetime object
    datetime_object = pd.to_datetime(timeseries, unit='ms')

    # Convert datetime object to date
    return datetime_object

startTs = 0
endTs = 1799315200000

print(calculateTimestamp(startTs))
# API
url = "http://localhost:8080/api/plugins/telemetry/ASSET/99643c50-7731-11ee-b094-a797de75d9ec/values/timeseries"
querystring = {"keys": "outsideHumidity,outsideMaxTemp,outsideMinTemp,outsideTemp",
               "startTs": startTs, "endTs": endTs}

headers = {
    "accept": "application/json",
    "X-Authorization": "Bearer  eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ0ZW5hbnRAdGhpbmdzYm9hcmQub3JnIiwidXNlcklkIjoiMGY3NmRmZDAtNmU0ZC0xMWVlLTk5YjUtZmY0MGI1ZTk4MjUxIiwic2NvcGVzIjpbIlRFTkFOVF9BRE1JTiJdLCJzZXNzaW9uSWQiOiIxZTVhOWUxZS0xOTg1LTRkZDctYTg5ZS03ZGY3ZWYxNzMwN2IiLCJpc3MiOiJ0aGluZ3Nib2FyZC5pbyIsImlhdCI6MTY5OTMyNjc0NSwiZXhwIjoxNjk5MzM1NzQ1LCJlbmFibGVkIjp0cnVlLCJpc1B1YmxpYyI6ZmFsc2UsInRlbmFudElkIjoiMGYyNjc1ZTAtNmU0ZC0xMWVlLTk5YjUtZmY0MGI1ZTk4MjUxIiwiY3VzdG9tZXJJZCI6IjEzODE0MDAwLTFkZDItMTFiMi04MDgwLTgwODA4MDgwODA4MCJ9.uXByHUYVSxBSY9eiWYkRSvKiRMxI_xknF7BjaEcnme3qGF6M0yVAcyjtgI1iM_4_5i8HxNDh94A6cBJYzBdTPg"
}

# Get data from API, return json string
response = requests.request("GET", url, headers=headers, params=querystring)

# Convert json string to dict
data = json.loads(response.text)
        
# Initial database
conn = psycopg2.connect(database="weather", user="postgres",
                        password="postgres", host="localhost", port="5432")
cur = conn.cursor()

# Insert the data into the PostgreSQL table
outsideHumidityArr = data['outsideHumidity']

outsideMaxTempArr = data['outsideMaxTemp']
outsideMinTempArr = data['outsideMinTemp']
outsideTempArr = data['outsideTemp']


for i in range(len(data['outsideHumidity'])):
    print(calculateTimestamp(outsideHumidityArr[i]['ts']))
    cur.execute('INSERT INTO "weatherData"("dateTime","outsideHumidity", "outsideMaxTemp", "outsideMinTemp", "outsideTemp") VALUES (%s,%s,%s,%s,%s)',
                (calculateTimestamp(outsideHumidityArr[i]['ts']),outsideHumidityArr[i]['value'], outsideMaxTempArr[i]['value'], outsideMinTempArr[i]['value'], outsideTempArr[i]['value']))


# Commit the changes and close the connection
conn.commit()
cur.close()
conn.close()
