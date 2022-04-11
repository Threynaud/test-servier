set-up:
	pipenv install --dev

launch-airflow:
	docker-compose up airflow-init
	docker-compose up

run-airflow-pipeline:
	docker-compose run airflow-worker airflow dags trigger airflow_pipeline

run-backup-pipeline:
	python dags/backup_pipeline.py