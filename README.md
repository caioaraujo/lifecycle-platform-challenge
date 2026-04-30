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

- I choose to separate the tasks in different contexts (extract, transform, load) to make the code more organized and easier to maintain
- I choose to use SQLite for Airflow database because it's easy to set up and doesn't require additional configuration
- I choose to use @task decorator for tasks because it's more concise and easier to read, once all operators are simple Python functions
- I choose to keep the scripts out of the DAG file to make it more readable, easier to test and maintain, following the Single Responsibility Principle
- I choose to create a file for query repository to make it more organized and easier to maintain

## What I would make differently with more time

- Provide a Docker image and/or other way (Kubernetes, terraform) to make it easier to run and deploy the project without having to set up manually
- Add unit tests for scripts and integration tests for the client API integration and airflow DAGs
- Add CI/CD pipeline to automate the testing and deployment
- Add monitoring for the Airflow DAGs to track the execution and performance of the tasks, and to be able to alert in case of failures or issues
- Change SQLite for PostgreSQL or MySQL for better performance and scalability, especially for production environments
