SHELL := /bin/bash

.PHONY: run migrate seed envoy envoy-down db-up db-down

run:
	go run ./cmd/server

migrate:
	go run ./cmd/migrate

seed:
	go run ./cmd/seed

envoy:
	docker compose -f deploy/envoy/docker-compose.yaml up -d envoy

envoy-down:
	docker compose -f deploy/envoy/docker-compose.yaml down

db-up:
	docker compose up -d postgres

db-down:
	docker compose down
