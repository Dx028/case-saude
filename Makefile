# Atalhos do projeto — rode "make help" para ver os comandos
SHELL := /bin/bash
.DEFAULT_GOAL := help
COMPOSE := docker compose

.PHONY: help setup check env validate up down restart ps logs psql topics spark-smoke scale-workers airflow db-setup urls clean smoke

help: ## Lista os comandos disponíveis
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

setup: check env ## Verifica pré-requisitos e cria/atualiza o .env

check: ## Verifica pré-requisitos da máquina
	@./scripts/check-prereqs.sh

env: ## Cria o .env ou acrescenta variáveis novas
	@./scripts/gen-env.sh

validate: ## Valida a sintaxe do docker-compose.yml
	@$(COMPOSE) config -q && echo "docker-compose.yml válido."

up: ## Sobe os serviços dos profiles definidos em COMPOSE_PROFILES
	$(COMPOSE) up -d --build

down: ## Para os serviços (mantém os dados)
	$(COMPOSE) down

restart: ## Reinicia os serviços
	$(COMPOSE) restart

ps: ## Lista os containers e seu estado
	$(COMPOSE) ps -a

logs: ## Mostra logs (uso: make logs s=postgres)
	$(COMPOSE) logs -f --tail=100 $(s)

psql: ## Abre o psql (uso: make psql db=dw)
	$(COMPOSE) exec postgres psql -U $$(grep ^POSTGRES_USER= .env | cut -d= -f2) -d $(or $(db),postgres)

topics: ## Lista os tópicos do Kafka
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

spark-smoke: ## Testa as conexões do Spark (Silo/Delta, Kafka e Postgres)
	$(COMPOSE) exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/jobs/common/smoke_test.py

scale-workers: ## Ajusta o número de workers Spark (uso: make scale-workers n=3)
	$(COMPOSE) up -d --no-recreate --scale spark-worker=$(or $(n),2) spark-worker

airflow: ## Executa um comando do Airflow (uso: make airflow cmd="dags list")
	$(COMPOSE) exec airflow-scheduler airflow $(cmd)

db-setup: ## Reaplica schemas, papéis e tabelas de referência do DW
	$(COMPOSE) run --rm db-setup

urls: ## Mostra os endereços das interfaces web
	@echo "  Console do Silo (lakehouse):  http://localhost:$$(grep ^MINIO_CONSOLE_PORT= .env | cut -d= -f2)"
	@echo "  Metabase:                     http://localhost:$$(grep ^METABASE_PORT= .env | cut -d= -f2)"
	@echo "  Airflow:                      http://localhost:$$(grep ^AIRFLOW_PORT= .env | cut -d= -f2)"
	@echo "  Spark Master UI:              http://localhost:$$(grep ^SPARK_MASTER_UI_PORT= .env | cut -d= -f2)"
	@echo "  Kafka UI:                     http://localhost:$$(grep ^KAFKA_UI_PORT= .env | cut -d= -f2)"
	@echo "  Kafka Connect (API REST):      http://localhost:$$(grep ^KAFKA_CONNECT_PORT= .env | cut -d= -f2)"
	@echo "  Kafka (bootstrap no host):     localhost:$$(grep ^KAFKA_EXTERNAL_PORT= .env | cut -d= -f2)"
	@echo "  PostgreSQL:                    localhost:$$(grep ^POSTGRES_PORT= .env | cut -d= -f2)"

clean: ## Remove containers E volumes (apaga os dados!)
	@read -p "Isso apaga TODOS os dados do projeto. Confirmar? [s/N] " r; [[ $$r == s ]] && $(COMPOSE) down -v --remove-orphans || echo "Cancelado."

smoke: ## Testa as conexões de ponta a ponta
	@echo "Smoke tests serão implementados na fase 9."
