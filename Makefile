airflow-db-init:
	airflow db migrate

airflow-users-create:
	airflow users create

airflow-init: airflow-db-init airflow-users-create

airflow-run-webserver:
	airflow webserver --port 8080

airflow-run-scheduler:
	airflow scheduler
