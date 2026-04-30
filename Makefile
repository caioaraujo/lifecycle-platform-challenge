airflow-init:
	airflow db migrate

airflow-run-webserver:
	airflow webserver --port 8080

airflow-run-scheduler:
	airflow scheduler
