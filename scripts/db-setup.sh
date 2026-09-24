#!/bin/sh
# Aplica os scripts SQL idempotentes a cada "make up":
#   sql/cluster/*.sql -> objetos globais do servidor (banco postgres)
#   sql/dw/*.sql      -> schemas, papéis e tabelas do data warehouse
set -eu

export PGPASSWORD="$POSTGRES_PASSWORD"
export PGOPTIONS="-c client_min_messages=warning"

aplicar() {
  banco="$1"; shift
  for f in "$@"; do
    [ -f "$f" ] || continue
    echo "[db-setup] ${banco}: aplicando $(basename "$f")"
    psql -h postgres -U "$POSTGRES_USER" -d "$banco" -v ON_ERROR_STOP=1 -q \
         -v bi_pass="$BI_READER_PASSWORD" -v mon_pass="$MONITORING_DB_PASSWORD" -f "$f"
  done
}

aplicar postgres /sql/cluster/*.sql
aplicar dw /sql/dw/*.sql
echo "[db-setup] banco pronto"
