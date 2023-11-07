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
                    "avgTemp" : row[1]
                },
            }
        )
    print(data)
    # Format the results of your query as JSON
    # Post the JSON data to the API
    api_url = "http://localhost:8080/api/plugins/telemetry/ASSET/99643c50-7731-11ee-b094-a797de75d9ec/timeseries/ANY?scope=ANY"
    auth_token = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ0ZW5hbnRAdGhpbmdzYm9hcmQub3JnIiwidXNlcklkIjoiMGY3NmRmZDAtNmU0ZC0xMWVlLTk5YjUtZmY0MGI1ZTk4MjUxIiwic2NvcGVzIjpbIlRFTkFOVF9BRE1JTiJdLCJzZXNzaW9uSWQiOiIxZTVhOWUxZS0xOTg1LTRkZDctYTg5ZS03ZGY3ZWYxNzMwN2IiLCJpc3MiOiJ0aGluZ3Nib2FyZC5pbyIsImlhdCI6MTY5OTMyNjc0NSwiZXhwIjoxNjk5MzM1NzQ1LCJlbmFibGVkIjp0cnVlLCJpc1B1YmxpYyI6ZmFsc2UsInRlbmFudElkIjoiMGYyNjc1ZTAtNmU0ZC0xMWVlLTk5YjUtZmY0MGI1ZTk4MjUxIiwiY3VzdG9tZXJJZCI6IjEzODE0MDAwLTFkZDItMTFiMi04MDgwLTgwODA4MDgwODA4MCJ9.uXByHUYVSxBSY9eiWYkRSvKiRMxI_xknF7BjaEcnme3qGF6M0yVAcyjtgI1iM_4_5i8HxNDh94A6cBJYzBdTPg"

    post_data_to_api(data, api_url, auth_token)
