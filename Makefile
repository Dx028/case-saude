# Atalhos do projeto — rode "make help" para ver os comandos
SHELL := /bin/bash
.DEFAULT_GOAL := help
COMPOSE := docker compose

.PHONY: help setup check env up down restart ps logs clean smoke

help: ## Lista os comandos disponíveis
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

setup: check env ## Verifica pré-requisitos e cria o .env

check: ## Verifica pré-requisitos da máquina
	@./scripts/check-prereqs.sh

env: ## Cria o .env com senhas aleatórias
	@./scripts/gen-env.sh

up: ## Sobe os serviços dos profiles definidos em COMPOSE_PROFILES
	$(COMPOSE) up -d --build

down: ## Para os serviços (mantém os dados)
	$(COMPOSE) down

restart: ## Reinicia os serviços
	$(COMPOSE) restart

ps: ## Lista os containers e seu estado
	$(COMPOSE) ps

logs: ## Mostra logs (uso: make logs s=postgres)
	$(COMPOSE) logs -f --tail=100 $(s)

clean: ## Remove containers E volumes (apaga os dados!)
	@read -p "Isso apaga TODOS os dados do projeto. Confirmar? [s/N] " r; [[ $$r == s ]] && $(COMPOSE) down -v --remove-orphans || echo "Cancelado."

smoke: ## Testa as conexões de ponta a ponta
	@echo "Smoke tests serão implementados na fase 9."
