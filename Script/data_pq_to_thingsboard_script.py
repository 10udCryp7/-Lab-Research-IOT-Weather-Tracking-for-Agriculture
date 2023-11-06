import json
import requests
import psycopg2

def post_data_to_api(data, api_url, auth_token):
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "X-Authorization": f"Bearer {auth_token}",
    }

    response = requests.post(api_url, headers=headers, data=json.dumps(data))

    if response.status_code != 200:
        raise Exception(f"Failed to post data to API: {response.status_code} {response.content}")

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
        SELECT * FROM "weatherData";
        """
    )

    
    data = []
    for row in cur:
        data.append(
            {
                "ts": 1699260201000,  
                "values": {
                    "outsideHumidity": row[0],
                    "outsideMaxTemp": row[1],
                    "outsideMinTemp": row[2],
                    "outsideTemp": row[3]
                },
            }
        )

    # Format the results of your query as JSON
    json_data = json.dumps(data)

    print(json_data)
    # Post the JSON data to the API
    api_url = "http://localhost:8080/api/plugins/telemetry/ASSET/5e66af30-7c80-11ee-bc01-716cbc946983/timeseries/ANY?scope=ANY"
    auth_token = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ0ZW5hbnRAdGhpbmdzYm9hcmQub3JnIiwidXNlcklkIjoiMGY3NmRmZDAtNmU0ZC0xMWVlLTk5YjUtZmY0MGI1ZTk4MjUxIiwic2NvcGVzIjpbIlRFTkFOVF9BRE1JTiJdLCJzZXNzaW9uSWQiOiIxZTVhOWUxZS0xOTg1LTRkZDctYTg5ZS03ZGY3ZWYxNzMwN2IiLCJpc3MiOiJ0aGluZ3Nib2FyZC5pbyIsImlhdCI6MTY5OTI1ODY0MSwiZXhwIjoxNjk5MjY3NjQxLCJlbmFibGVkIjp0cnVlLCJpc1B1YmxpYyI6ZmFsc2UsInRlbmFudElkIjoiMGYyNjc1ZTAtNmU0ZC0xMWVlLTk5YjUtZmY0MGI1ZTk4MjUxIiwiY3VzdG9tZXJJZCI6IjEzODE0MDAwLTFkZDItMTFiMi04MDgwLTgwODA4MDgwODA4MCJ9.hkATK--56Tb1C92A4tTGq2fjpK1mel2_-D-K9anfG6G9tv9VZsHOuoTZvJIYHaHFQclUwwnwf_Kade42-34vfw"

    post_data_to_api(data, api_url, auth_token)
