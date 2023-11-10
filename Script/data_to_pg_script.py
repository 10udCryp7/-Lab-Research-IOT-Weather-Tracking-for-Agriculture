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


startTs = 1599315200000
endTs = 2099315200000

print(calculateTimestamp(startTs))

url = "http://localhost:8080/api/auth/login"
querystring = {'username': 'tenant@thingsboard.org',
                'password': 'tenant',}

headers = {
    'accept': 'application/json',
    'Content-Type': 'application/json',
}

response = requests.post(url, headers = headers, data = json.dumps(querystring))
data = json.loads(response.text)
token = "Bearer " + data['token']

print(token)
# API
url = "http://localhost:8080/api/plugins/telemetry/ASSET/99643c50-7731-11ee-b094-a797de75d9ec/values/timeseries"
querystring = {"keys": "outsideHumidity,outsideMaxTemp,outsideMinTemp,outsideTemp",
               "startTs": startTs, "endTs": endTs}

headers = {
    "accept": "application/json",
    "X-Authorization": token

}

# Get data from API, return json string
response = requests.request("GET", url, headers=headers, params=querystring)

# Convert json string to dict
data = json.loads(response.text)

print(data)
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
                (calculateTimestamp(outsideHumidityArr[i]['ts']), outsideHumidityArr[i]['value'], outsideMaxTempArr[i]['value'], outsideMinTempArr[i]['value'], outsideTempArr[i]['value']))


# Commit the changes and close the connection
conn.commit()
cur.close()
conn.close()
