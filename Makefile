.PHONY: up down restart ps logs logs-producer logs-consumer smoke-test clean

up:
	docker compose up -d --build

down:
	docker compose down

restart:
	docker compose down
	docker compose up -d --build

ps:
	docker compose ps

logs:
	docker compose logs -f

logs-producer:
	docker compose logs -f producer

logs-consumer:
	docker compose logs -f consumer

smoke-test:
	bash scripts/smoke_test.sh

clean:
	docker compose down -v
