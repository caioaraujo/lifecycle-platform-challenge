import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task, get_current_context
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.utils.dates import days_ago
from google.cloud import bigquery

from scripts.campaign_sender import execute_campaign_send
from scripts.client import ESPClient
from scripts.query_repository import AUDIENCE_COUNT_QUERY, AUDIENCE_SEGMENTATION_QUERY, AUDIENCE_STAGE_QUERY

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    print("SLA missed!")
    print(f"DAG: {dag.dag_id}")
    print(f"Tasks: {task_list}")


with DAG(
        "audience-pipeline",
        default_args=default_args,
        description="DAG for audience data pipeline",
        schedule="0 5 * * *",  # Daily at 5AM UTC
        start_date=datetime(2026, 4, 28),
        catchup=False,
        tags=["audience"],
        sla_miss_callback=sla_miss_callback,
) as dag:
    @task(sla=timedelta(hours=3))
    def read_query():
        context = get_current_context()
        execution_date = context["execution_date"]
        bq_client = bigquery.Client()

        job_config = bigquery.QueryJobConfig(
            destination=(
                f"audience_segmentation.staging.audience_segmentation_stage"
                f"${execution_date.strftime('%Y%m%d')}"
            ),
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "run_timestamp",
                    "TIMESTAMP",
                    execution_date,
                ),
                bigquery.ScalarQueryParameter(
                    "run_date",
                    "DATE",
                    execution_date.date(),
                )
            ]
        )
        query_job = bq_client.query(AUDIENCE_SEGMENTATION_QUERY, job_config=job_config)
        query_job.result()


    @task(sla=timedelta(hours=3))
    def validate_campaign():
        context = get_current_context()
        execution_date = context["execution_date"]
        client = bigquery.Client()

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "run_date",
                    "DATE",
                    execution_date.date(),
                )
            ]
        )

        result = list(client.query(AUDIENCE_COUNT_QUERY, job_config=job_config))[0]

        if result.total == 0:
            raise Exception("No audience data found")

        # Apply other validations...


    @task(sla=timedelta(hours=3))
    def send_to_client():
        context = get_current_context()
        execution_date = context["execution_date"]
        client = bigquery.Client()
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "run_date",
                    "DATE",
                    execution_date.date(),
                )
            ]
        )

        result = client.query(AUDIENCE_STAGE_QUERY, job_config=job_config).result()

        audience = []

        for row in result:
            audience_data = {
                "renter_id": row.renter_id,
                "email": row.email,
                "phone": row.phone,
                "last_login": row.last_login,
                "search_count": row.search_count,
                "days_since_login": row.days_since_login,
            }
            audience.append(audience_data)

        return execute_campaign_send(
            campaign_id="CAMPAIGN_ID",
            audience=audience,
            esp_client=ESPClient(),
        )


    @task(sla=timedelta(hours=3))
    def log_and_notify(api_response):
        context = get_current_context()
        bq_client = bigquery.Client()

        table_id = "audience_segmentation.logging.campaign_log"

        rows = [api_response]

        errors = bq_client.insert_rows_json(table_id, rows)

        if errors:
            raise Exception(errors)

        send_slack_notification(
            text=f"```{json.dumps(api_response, indent=2)}```",
            channel="#alerts",
        )(context)


    t1 = read_query()
    t2 = validate_campaign()
    t3 = send_to_client()
    t4 = log_and_notify(t3)

    t1 >> t2 >> t3 >> t4
