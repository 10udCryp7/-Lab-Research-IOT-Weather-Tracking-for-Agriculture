import requests
import json
import requests
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


url = "http://localhost:8080/api/plugins/telemetry/ASSET/99643c50-7731-11ee-b094-a797de75d9ec/values/attributes?keys=status"


def calculate_date_time(timeseries):
    # Convert timestamp to datetime object
    datetime_object = pd.to_datetime(timeseries, unit='ms')
    # Convert datetime object to date
    return datetime_object


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





headers = {
    "Accept": "application/json",
    "Authorization": "Bearer " + get_access_token()
}

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


response = requests.get(url, headers=headers)
if response.status_code == 200:
    data = response.json()
else:
    print("Error:", response.status_code)

nowStatus = data[0]['value']
statusString = ""
if (nowStatus == True):
    statusString = "TRUE"
else:
    statusString = "FALSE"
print("STATUS NOW IS: " + statusString)
print("======================================================================================")
while True:
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
    else:
        print("Error:", response.status_code)
    statusTS = data[0]['lastUpdateTs']
    status = data[0]['value']
    # Initial database
    conn = psycopg2.connect(database=database, user=user,
                            password=password, host=host, port=port)
    cur = conn.cursor()
    if (nowStatus != status):
        nowStatus = status
        cur.execute("""
                    INSERT INTO "statusData" ("dateTime","status") VALUES (%s,%s);
                    """,
                    (calculate_date_time(statusTS), status))
        # Commit the changes and close the connection
        conn.commit()
        cur.close()
        conn.close()
        nowStatus = status
        if (nowStatus == True):
            statusString = "TRUE"
        else:
            statusString = "FALSE"
        print("SUCCESS: SEND DATA TO DATABASE!!!");
        print("STATUS NOW IS: " + statusString + "; TIME IS: " + str(calculate_date_time(statusTS)))
        print("======================================================================================")
    