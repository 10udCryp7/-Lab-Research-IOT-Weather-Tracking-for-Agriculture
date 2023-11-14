import json
import requests
import psycopg2
import pandas as pd
import time
import datetime

# acount
USERNAME_ACCOUNT = 'tenant@thingsboard.org'
PASSWORD_ACCOUNT = 'tenant'
# id
ENTITY_ID = "99643c50-7731-11ee-b094-a797de75d9ec"
# entity type
TYPE = "ASSET"
# DATABASE
DATABASE = "weather"
USERNAME_DB = "postgres"
PASSWORD_DB = "postgres"
HOST = "localhost"
PORT = "5432"
SCHEDULE_MINUTE = 15


# calculate timeseries -> date time


def calculateTimeseries(dateTime):
    return int(dateTime.timestamp()*1000)

# post data to thingsboard by thingsboard api


def post_data_to_thingsboard(data, type, entity_id, auth_token):

    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "X-Authorization": f"Bearer {auth_token}",
    }
    api_url = "http://localhost:8080/api/plugins/telemetry/"+type+"/" + \
        entity_id+"/timeseries/ANY?scope=ANY"
    response = requests.post(api_url, headers=headers, data=json.dumps(data))

    if response.status_code != 200:
        raise Exception(
            f"Failed to post data to API: {response.status_code} {response.content}")

# get access token login to thingsboard


def get_access_token():
    # url of api login
    url_login = "http://localhost:8080/api/auth/login"
    # data account
    data_login = {'username': USERNAME_ACCOUNT,
                  'password': PASSWORD_ACCOUNT, }
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

# TEMPLATE FUNCTION

# def function_name(conn):
#     # Query the database to retrieve the data you want to post
#     cur = conn.cursor()
#     cur.execute(
#         """
#             Viết câu truy vấn ở đây!!
#             Câu truy vấn nên có cột đầu tiên là dateTime
#         """
#     )

#     data = []
#     for row in cur:
#         data.append(
#             {
#                 "ts": int(calculateTimeseries(pd.to_datetime(row[0]))),
#                 "values": {
#                     "telemetry_key_1": row[1]
#                     "telemetry_key_2": row[2]
#                     ...
#                 },
#             }
#         )
#     print("SEND ... :")
#     print(data)
#     print("======================================================================================")
#     conn.commit()
#     cur.close()
#     return data


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
        user=USERNAME_DB,
        password=PASSWORD_DB,
        host=HOST,
        port=PORT,
    )
    # asset id of asset we send data to
    entity_id = ENTITY_ID
    # entity type
    type = TYPE

    get_access_token()

    while True:
        now = "NOW: " + \
            str(str(datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')))
        minute_left = "DATA WILL BE POSTED IN: " + \
            str((SCHEDULE_MINUTE - 1 - datetime.datetime.now().minute %
                SCHEDULE_MINUTE) % 15) + " minutes"
        print(now + " || " + minute_left, end="\r", flush=True)
        if (datetime.datetime.now().minute % SCHEDULE_MINUTE == 1):
            # access token
            auth_token = get_access_token()

            data_avg_temp = get_avg_temp(conn)

            data_avg_temp_night = get_avg_temp_night(conn)

            data_avg_temp_day = get_avg_temp_day(conn)
            if (len(data_avg_temp) != 0):
                post_data_to_thingsboard(
                    data_avg_temp, type=type, entity_id=entity_id, auth_token=auth_token)

            if (len(data_avg_temp_night) != 0):
                post_data_to_thingsboard(data_avg_temp_night, type=type,
                                         entity_id=entity_id, auth_token=auth_token)

            if (len(data_avg_temp_day) != 0):
                post_data_to_thingsboard(data_avg_temp_day, type=type,
                                         entity_id=entity_id, auth_token=auth_token)
            time.sleep(60)
