# lifecycle-platform-challenge

This project is based on [Apache Airflow](https://airflow.apache.org/docs/apache-airflow/).

This is a data pipeline that orchestrates audience from a residential rental marketplace for a Marketing campaign.

## Requirements

    - Python 3.14 
    - SQLite

All Python dependencies are listed on `requirements.txt` in the project root.

Ex: `pip install -r requirements.txt`

NOTE: Consider to [create and activate a virtual environment](https://docs.python.org/3/library/venv.html) to install
all the dependencies.

Also, you can run some commands using [Makefile](https://www.gnu.org/software/make/manual/make.html) for convenience.

## Setup

This project requires the following environment variables:

    - AIRFLOW_HOME (ex: "~/airflow-lifecycle")
    - AIRFLOW__CORE__SQL_ALCHEMY_CONN (ex: "sqlite:////tmp/airflow.db")

### Initialize Airflow

To initialize Airflow, run `airflow db init` or `make airflow-init-db`.

## Running locally

To run Airflow locally, follow the steps:

    - webserver: run `airflow webserver --port 8080` or `make airflow-run-webserver`
    - scheduler: in a different session, run `airflow scheduler` or `make airflow-run-scheduler`.

## Design decisions

## What I would make differently with more time

