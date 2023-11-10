import json
import requests
import psycopg2
import time
import pandas as pd


def calculateTimestamp(timeseries):

    # Create a sample timestamp

    # Convert timestamp to datetime object
    datetime_object = pd.to_datetime(timeseries, unit='ms')

    # Convert datetime object to date
    return datetime_object


def calculateTimeseries(dateTime):
    return int(dateTime.timestamp()*1000)


def post_data_to_api(data, api_url, auth_token):
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "X-Authorization": f"Bearer {auth_token}",
    }

    response = requests.post(api_url, headers=headers, data=json.dumps(data))

    if response.status_code != 200:
        raise Exception(
            f"Failed to post data to API: {response.status_code} {response.content}")

url = "http://localhost:8080/api/auth/login"
querystring = {'username': 'tenant@thingsboard.org',
                'password': 'tenant',}

headers = {
    'accept': 'application/json',
    'Content-Type': 'application/json',
}

response = requests.post(url, headers = headers, data = json.dumps(querystring))
data = json.loads(response.text)
token = data['token']

if __name__ == "__main__":
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        database="weather",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432",
    )

    # Query the database to retrieve the data you want to post
    cur = conn.cursor()
    cur.execute(
        """
            SELECT Date("dateTime") as day, avg("outsideTemp") as average_temp
            FROM "weatherData"
            GROUP BY DATE("dateTime");
        """
    )

    data = []
    for row in cur:
        data.append(
            {
                "ts": int(calculateTimeseries(pd.to_datetime(row[0]))),
                "values": {
                    "avgTemp": row[1]
                },
            }
        )
    
    # Format the results of your query as JSON
    # Post the JSON data to the API
    api_url = "http://localhost:8080/api/plugins/telemetry/ASSET/99643c50-7731-11ee-b094-a797de75d9ec/timeseries/ANY?scope=ANY"
    auth_token = token

    post_data_to_api(data, api_url, auth_token)

    cur.execute(
        """
            SELECT Date("dateTime") as day, avg("outsideTemp") as average_temp
            FROM "weatherData"
			WHERE EXTRACT (hour from "dateTime") >= 11 AND EXTRACT (hour from "dateTime") <= 23 
            GROUP BY DATE("dateTime");
        """
    )
    data = []
    for row in cur:
        data.append(
            {
                "ts": int(calculateTimeseries(pd.to_datetime(row[0]))),
                "values": {
                    "avgTempNight": row[1]
                },
            }
        )
    
    post_data_to_api(data, api_url, auth_token)

    cur.execute(
        """
            SELECT Date("dateTime") as day, avg("outsideTemp") as average_temp
            FROM "weatherData"
			WHERE NOT(EXTRACT (hour from "dateTime") >= 11 AND EXTRACT (hour from "dateTime") <= 23)
            GROUP BY DATE("dateTime");
        """
    )
    for row in cur:
        data.append(
            {
                "ts": int(calculateTimeseries(pd.to_datetime(row[0]))),
                "values": {
                    "avgTempDay": row[1]
                },
            }
        )
    post_data_to_api(data, api_url, auth_token)

