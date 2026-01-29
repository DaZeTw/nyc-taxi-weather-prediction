airflow_up:
	docker compose -f airflow-docker-compose.yaml up -d --build
airflow_down:
	docker compose -f airflow-docker-compose.yaml down

batch_up:
	docker compose -f docker-compose.yaml up -d
batch_down:
	docker compose -f docker-compose.yaml down

stream_up:
	docker compose -f stream-docker-compose.yaml up -d
stream_down:
	docker compose -f stream-docker-compose.yaml down

up_all:
	docker compose -f docker-compose.yaml up -d
	docker compose -f stream-docker-compose.yaml up -d
	docker compose -f airflow-docker-compose.yaml up -d

down_all:
	docker compose -f docker-compose.yaml down
	docker compose -f stream-docker-compose.yaml down
	docker compose -f airflow-docker-compose.yaml down

stop_all:
	docker compose -f docker-compose.yaml stop
	docker compose -f stream-docker-compose.yaml stop
	docker compose -f airflow-docker-compose.yaml stop

start_all:
	docker compose -f docker-compose.yaml start
	docker compose -f stream-docker-compose.yaml start
	docker compose -f airflow-docker-compose.yaml start