# Data-Warehouse-Weather-ETL-HW5

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

---

## TaskFlow Implementation

This DAG uses the Airflow TaskFlow API with the `@task` decorator.

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

- `LATITUDE`
- `LONGITUDE`

Used in python code via:

LATITUDE = Variable.get("LATITUDE")
LONGITUDE = Variable.get("LONGITUDE")
