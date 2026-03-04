from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta
import requests


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


def return_snowflake_conn(conn_id):
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    conn = hook.get_conn()
    return conn.cursor()


@task
def extract(latitude, longitude):
    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "past_days": 60,
        "forecast_days": 0,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "weather_code"
        ],
        "timezone": "America/Los_Angeles"
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        raise RuntimeError(f"API request failed: {response.status_code}")

    return response.json()


@task
def transform(raw_data, latitude, longitude, city):
    if "daily" not in raw_data:
        raise ValueError("Missing 'daily' key in API response")

    data = raw_data["daily"]

    records = []

    for i in range(len(data["time"])):
        records.append((
            latitude,
            longitude,
            data["time"][i],
            data["temperature_2m_max"][i],
            data["temperature_2m_min"][i],
            str(data["weather_code"][i]),
            city
        ))

    return records


@task
def load(records, target_table):
    cur = return_snowflake_conn("snowflake_conn")

    try:
        cur.execute("BEGIN;")

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                latitude FLOAT,
                longitude FLOAT,
                date DATE,
                temp_max FLOAT,
                temp_min FLOAT,
                weather_code VARCHAR(10),
                city VARCHAR(100),
                PRIMARY KEY (latitude, longitude, date, city)
            );
        """)

        # FULL REFRESH
        cur.execute(f"DELETE FROM {target_table};")

        insert_sql = f"""
            INSERT INTO {target_table} (
                latitude, longitude, date,
                temp_max, temp_min, weather_code, city
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """

        cur.executemany(insert_sql, records)

        cur.execute("COMMIT;")

        print(f"Loaded {len(records)} records into {target_table}")

    except Exception as e:
        cur.execute("ROLLBACK;")
        raise e


with DAG(
    dag_id="WeatherData_ETL",
    start_date=datetime(2026, 3, 2),
    schedule="30 2 * * *",
    catchup=False,
    tags=["ETL"],
    default_args=default_args
) as dag:

    LATITUDE = Variable.get("LATITUDE")
    LONGITUDE = Variable.get("LONGITUDE")
    CITY = "New York"

    target_table = "RAW.WEATHER_DATA_HW5"

    raw_data = extract(LATITUDE, LONGITUDE)
    transformed = transform(raw_data, LATITUDE, LONGITUDE, CITY)
    load(transformed, target_table)