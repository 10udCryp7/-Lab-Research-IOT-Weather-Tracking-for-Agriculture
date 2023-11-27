# THIS CODE CONNECTS TO API THINGSBOARD (GET DATA FROM API OPENWEATHER), COLLECT DATA AND BRING IT TO POSTGRESQL DATABASE
# Tip: use Bing to generate code, as we have api detail
# Must do: change the frequency of data to  hours, as temperature doesn't change so much in seconds.


import requests
import json
import psycopg2
import pandas as pd
import time
import datetime

# INFORMATION
# account
USERNAME_ACCOUNT = "tenant@thingsboard.org"
PASSWORD_ACCOUNT = "tenant"
# schedule time to send data hour:minute
SCHEDULE_MINUTE = 23
# range between start_ts and end_ts
TIME_RANGE = SCHEDULE_MINUTE*60
# end of timeseries
END_TS = int(time.time())*1000
# start of timeseries
START_TS = int(END_TS/1000 - TIME_RANGE)*1000
# entity id
ENTITY_ID = "99643c50-7731-11ee-b094-a797de75d9ec"
# type
TYPE = "ASSET"
# DATABASE
DATABASE = "weather"
USERNAME_DB = "postgres"
PASSWORD_DB = "postgres"
HOST = "localhost"
PORT = "5432"


# convert timeseries -> dateTime


def calculate_date_time(timeseries):
    # Convert timestamp to datetime object
    datetime_object = pd.to_datetime(timeseries, unit='ms')
    # Convert datetime object to date
    return datetime_object

# get access token when login


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


# get data from api


def get_data(start_ts, end_ts, entity_id, type, token):
    # API
    url_get_data = "http://localhost:8080/api/plugins/telemetry/"+type+"/" + \
        entity_id + "/values/timeseries"
    querystring = {"keys": "outsideHumidity,outsideMaxTemp,outsideMinTemp,outsideTemp",
                   "startTs": start_ts, "endTs": end_ts}

    headers_get_data = {
        "accept": "application/json",
        "X-Authorization": "Bearer " + token
    }
    # Get data from API, return json string
    response = requests.request(
        "GET", url_get_data, headers=headers_get_data, params=querystring)

    # Convert json string to dict
    data = json.loads(response.text)
    print("DATA IS: ")
    print(data)
    print("======================================================================================")
    return data


# send data to database postgresql


def send_data_to_database(data, database, user, password, host, port):
    # Initial database
    conn = psycopg2.connect(database=database, user=user,
                            password=password, host=host, port=port)
    cur = conn.cursor()

    # Insert the data into the PostgreSQL table
    outsideHumidityArr = data['outsideHumidity']

    outsideMaxTempArr = data['outsideMaxTemp']
    outsideMinTempArr = data['outsideMinTemp']
    outsideTempArr = data['outsideTemp']

    for i in range(len(data['outsideHumidity'])):
        cur.execute("""
                    INSERT INTO "weatherData"("dateTime","outsideHumidity", "outsideMaxTemp", "outsideMinTemp", "outsideTemp") VALUES (%s,%s,%s,%s,%s);
                    """,
                    (calculate_date_time(outsideHumidityArr[i]['ts']), outsideHumidityArr[i]['value'], outsideMaxTempArr[i]['value'], outsideMinTempArr[i]['value'], outsideTempArr[i]['value']))
    # Commit the changes and close the connection
    conn.commit()
    cur.close()
    conn.close()
    print("SUCCESS: SEND DATA TO DATABASE!!!")
    print("======================================================================================")


if __name__ == "__main__":
    # start of timeseries
    start_ts = START_TS
    # end of timeseries
    end_ts = END_TS
    # asset id
    entity_id = ENTITY_ID

    # entity type
    type = TYPE
    # DATABASE
    database = DATABASE
    user = USERNAME_DB
    password = PASSWORD_DB
    host = HOST
    port = PORT
    while True:
        now = "NOW: " + \
            str(str(datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')))
        minute_left = "DATA WILL BE GOT IN: " + \
            str(SCHEDULE_MINUTE - 1 - datetime.datetime.now().minute %
                SCHEDULE_MINUTE) + " minutes"
        print(now + " || " + minute_left, end="\r", flush=True)
        if (datetime.datetime.now().minute % SCHEDULE_MINUTE == 0):
            print("SEND DATA AT: " + str(datetime.datetime.utcnow()))
            # access token
            token = get_access_token()
            # get data
            data = get_data(start_ts=start_ts, end_ts=end_ts,
                            entity_id=entity_id, type=type, token=token)
            # send data to database
            if (len(data) != 0):
                send_data_to_database(data=data, database=database,
                                    user=user, password=password, host=host, port=port)
            time.sleep(60)
