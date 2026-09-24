#!/bin/bash
# Executado automaticamente pelo Postgres apenas na PRIMEIRA inicialização
# (volume vazio). Cria um banco e um usuário dedicado para cada finalidade,
# aplicando o princípio do menor privilégio.
set -euo pipefail

create_db() {
  local db="$1" user="$2" pass="$3"
  echo "[init] criando banco '${db}' com owner '${user}'"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<EOSQL
CREATE ROLE ${user} LOGIN PASSWORD '${pass}';
CREATE DATABASE ${db} OWNER ${user};
REVOKE ALL ON DATABASE ${db} FROM PUBLIC;
EOSQL
}

create_db airflow     airflow         "$AIRFLOW_DB_PASSWORD"    # metadados do Airflow
create_db dw          dw_owner        "$DW_DB_PASSWORD"         # data warehouse (serving)
create_db prontuario  prontuario_app  "$OLTP_DB_PASSWORD"       # OLTP simulado (origem do CDC)
create_db metabase    metabase        "$METABASE_DB_PASSWORD"   # metadados do Metabase

# O usuário do OLTP precisa de REPLICATION para o Debezium ler o WAL
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  -c "ALTER ROLE prontuario_app WITH REPLICATION;"

echo "[init] bancos criados com sucesso"
