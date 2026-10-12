init:
	cp -n .env.example .env || true
	docker compose build
	docker compose up airflow-init
up:
	docker compose up -d
storage-test:
	docker compose --profile test run --rm storage-smoke-test
trigger:
	docker compose exec airflow-scheduler airflow dags trigger chicago_taxi_medallion
pipeline-test:
	bash scripts/test_full_pipeline.sh
logs:
	docker compose logs -f airflow-scheduler spark-worker seaweedfs-s3
status:
	docker compose ps
down:
	docker compose down
reset:
	docker compose down -v --remove-orphans
