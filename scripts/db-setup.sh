#!/bin/sh
# Aplica os scripts do DW (sql/dw/*.sql) em ordem. Todos são idempotentes,
# então rodam a cada "make up" sem efeitos colaterais.
set -eu

export PGPASSWORD="$POSTGRES_PASSWORD"
export PGOPTIONS="-c client_min_messages=warning"
for f in /sql/dw/*.sql; do
  echo "[db-setup] aplicando $(basename "$f")"
  psql -h postgres -U "$POSTGRES_USER" -d dw -v ON_ERROR_STOP=1 -q \
       -v bi_pass="$BI_READER_PASSWORD" -f "$f"
done
echo "[db-setup] DW pronto"
