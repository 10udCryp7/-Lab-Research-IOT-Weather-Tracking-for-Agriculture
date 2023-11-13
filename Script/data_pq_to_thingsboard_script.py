import json
import requests
import psycopg2
import pandas as pd
import time
import datetime


# asset id
ASSET_ID = "99643c50-7731-11ee-b094-a797de75d9ec"
# DATABASE
DATABASE = "weather"
USER = "postgres"
PASSWORD = "postgres"
HOST = "localhost"
PORT = "5432"
SCHEDULE_MINUTE = 15


# calculate timeseries -> date time


def calculateTimeseries(dateTime):
    return int(dateTime.timestamp()*1000)

# post data to thingsboard by thingsboard api


def post_data_to_api(data, asset_id, auth_token):

    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "X-Authorization": f"Bearer {auth_token}",
    }
    api_url = "http://localhost:8080/api/plugins/telemetry/ASSET/" + \
        asset_id+"/timeseries/ANY?scope=ANY"
    response = requests.post(api_url, headers=headers, data=json.dumps(data))

    if response.status_code != 200:
        raise Exception(
            f"Failed to post data to API: {response.status_code} {response.content}")

# get access token login to thingsboard


def get_access_token():
    # url of api login
    url_login = "http://localhost:8080/api/auth/login"
    # data account
    data_login = {'username': 'tenant@thingsboard.org',
                  'password': 'tenant', }
    # header of login api
    headers_login = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }
    response = requests.post(
        url_login, headers=headers_login, data=json.dumps(data_login))
    data = json.loads(response.text)
    token = data['token']
    print("TOKEN IS: ")
    print("======================================================================================")
    print(token)
    print("======================================================================================")
    return token

# get avg temp from database


def get_avg_temp(conn):
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
    print("SEND AVG TEMP:")
    print(data)
    print("======================================================================================")
    conn.commit()
    cur.close()
    return data

# get avg temp at night from database


def get_avg_temp_night(conn):
    cur = conn.cursor()
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
    print("SEND AVG TEMP NIGHT:")
    print(data)
    print("======================================================================================")
    conn.commit()
    cur.close()
    return data

# get avg temp at day from database


def get_avg_temp_day(conn):
    cur = conn.cursor()
    cur.execute(
        """
            SELECT Date("dateTime") as day, avg("outsideTemp") as average_temp
            FROM "weatherData"
			WHERE NOT(EXTRACT (hour from "dateTime") >= 11 AND EXTRACT (hour from "dateTime") <= 23)
            GROUP BY DATE("dateTime");
        """
    )
    data = []
    for row in cur:
        data.append(
            {
                "ts": int(calculateTimeseries(pd.to_datetime(row[0]))),
                "values": {
                    "avgTempDay": row[1]
                },
            }
        )
    print("SEND AVG DAY:")
    print(data)
    print("======================================================================================")
    conn.commit()
    cur.close()
    return data


if __name__ == "__main__":
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        database=DATABASE,
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
    )
    # asset id of asset we send data to
    asset_id = ASSET_ID
    while True:
        now = "NOW: " + str(str(datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')))
        minute_left = "DATA WILL BE POSTED IN: " + str(15 - datetime.datetime.now().minute%SCHEDULE_MINUTE) + " minutes"
        print(now + " || " + minute_left, end="\r", flush=True)
        if (datetime.datetime.now().minute%SCHEDULE_MINUTE == 1):
            # access token
            auth_token = get_access_token()

            data_avg_temp = get_avg_temp(conn)

            data_avg_temp_night = get_avg_temp_night(conn)

            data_avg_temp_day = get_avg_temp_day(conn)
            post_data_to_api(data_avg_temp, asset_id=asset_id, auth_token=auth_token)
            post_data_to_api(data_avg_temp_night, asset_id=asset_id, auth_token=auth_token)
            post_data_to_api(data_avg_temp_day, asset_id=asset_id, auth_token=auth_token)
            conn.close()
            time.sleep(60)
   
