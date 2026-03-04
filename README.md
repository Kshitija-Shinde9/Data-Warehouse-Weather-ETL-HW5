# Weather Data ETL Pipeline  
Apache Airflow + Snowflake + Open-Meteo API  

---


## Project Overview

This project implements an end-to-end ETL pipeline using Apache Airflow TaskFlow API and Snowflake.

The pipeline:
- Extracts historical weather data from the Open-Meteo API
- Transforms the JSON response into structured records
- Loads the data into Snowflake using a transactional full refresh strategy

The DAG runs daily and ensures data consistency using SQL transactions.

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/bceb7676-7173-49c5-abb9-9602336a1f41" />

---


## DAG Configuration

- DAG ID: `WeatherData_ETL`
- Schedule: `30 2 * * *`
- Runs daily at **02:30 UTC**
- Catchup: Disabled
- Tags: ETL

Cron Meaning:
- Minute: 30
- Hour: 2
- Runs every day

<img width="1710" height="438" alt="image" src="https://github.com/user-attachments/assets/d1315275-d294-41a5-a190-a0aaf2bf777e" />

---


## TaskFlow Implementation

This DAG uses the Airflow TaskFlow API with the `@task` decorator.

<img width="1710" height="893" alt="image" src="https://github.com/user-attachments/assets/7cbb7466-4487-428e-932e-01465a50ff0d" />

### Implemented Tasks

### Extract
- Calls Open-Meteo API
- Retrieves 60 days of historical weather data
- Returns raw JSON

### Transform
- Parses JSON response
- Structures data into tuples
- Prepares records for Snowflake insertion

### Load
- Connects to Snowflake
- Creates table if not exists
- Performs full refresh using SQL transaction
- Inserts transformed records

### Task Dependency

Task dependency is defined through function chaining inside the DAG context.

---


## Airflow Variables

Airflow Variables are configured for:

- LATITUDE
- LONGITUDE

Used in python code via:

LATITUDE = Variable.get("LATITUDE")
LONGITUDE = Variable.get("LONGITUDE")

<img width="1710" height="497" alt="image" src="https://github.com/user-attachments/assets/6691b3f7-b66f-425d-bcef-4ce0b1ec233f" />

---


## Snowflake Data Validation

<img width="1710" height="954" alt="image" src="https://github.com/user-attachments/assets/0412a4fd-ccbb-43fd-a76e-4885fc0da052" />




