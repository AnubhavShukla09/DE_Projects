from datetime import timedelta, datetime
import json
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.hooks.base import BaseHook


def get_api_key():
    conn = BaseHook.get_connection("weather_api")
    extras = conn.extra_dejson
    return extras["api_key"]


api_key = get_api_key()


def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9 / 5) + 32
    return temp_in_fahrenheit


def get_aws_credentials():
    conn = BaseHook.get_connection("aws_credentials")
    aws_credentials = {
        "key": conn.login,
        "secret": conn.password,
        "token": conn.extra_dejson.get("aws_session_token"),
    }
    return aws_credentials


def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_data_from_api")
    city = data["name"]
    weather_description = data["weather"][0]["description"]
    temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_fahrenheit = kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data["dt"] + data["timezone"])
    sunrise_time = datetime.utcfromtimestamp(data["sys"]["sunrise"] + data["timezone"])
    sunset_time = datetime.utcfromtimestamp(data["sys"]["sunset"] + data["timezone"])

    transformed_data = {
        "City": city,
        "Description": weather_description,
        "Temperature (F)": temp_fahrenheit,
        "Feels Like (F)": feels_like_fahrenheit,
        "Minimum Temp (F)": min_temp_fahrenheit,
        "Maximum Temp (F)": max_temp_fahrenheit,
        "Pressure": pressure,
        "Humidity": humidity,
        "Wind Speed": wind_speed,
        "Time of Record": time_of_record,
        "Sunrise (Local Time)": sunrise_time,
        "Sunset (Local Time)": sunset_time,
    }

    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)

    # Retrieve AWS credentials
    aws_credentials = get_aws_credentials()

    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    file_name = f"current_weather_data_seattle_{dt_string}.csv"

    # Save data to S3
    df_data.to_csv(
        f"s3://weatherapibucket-anubhav/{file_name}",
        index=False,
        storage_options={
            "key": aws_credentials["key"],
            "secret": aws_credentials["secret"],
            "token": aws_credentials["token"],
        },
    )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 15),
    "email": ["anubhav020909@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "weather_dag", default_args=default_args, schedule_interval="@daily", catchup=False
) as dag:

    is_api_working = HttpSensor(
        task_id="is_api_working",
        http_conn_id="weather_api",
        endpoint=f"/data/2.5/weather?q=Seattle&APPID={api_key}",
    )

    extract_data_from_api = SimpleHttpOperator(
        task_id="extract_data_from_api",
        http_conn_id="weather_api",
        endpoint=f"/data/2.5/weather?q=Seattle&APPID={api_key}",
        method="GET",
        response_filter=lambda r: json.loads(r.text),
        log_response=True,
    )

    transform_load_data = PythonOperator(
        task_id="transform_load_data", python_callable=transform_load_data
    )

    is_api_working >> extract_data_from_api >> transform_load_data
