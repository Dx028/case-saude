#!/bin/bash
# Registra (ou atualiza) o conector de CDC do prontuário no Kafka Connect.
# Idempotente: PUT /connectors/<nome>/config cria ou atualiza a configuração.
# A senha do banco não está no arquivo: "${env:OLTP_DB_PASSWORD}" é resolvida pelo
# Kafka Connect a partir da variável de ambiente, e não fica gravada em claro.
set -eu

URL=http://kafka-connect:8083
NOME=prontuario-cdc

for i in $(seq 1 60); do
  curl -fs "$URL/connectors" >/dev/null && break
  echo "[connect-init] aguardando o Kafka Connect ($i/60)..."
  sleep 5
done

curl -fsS -X PUT -H "Content-Type: application/json" \
     --data @/config/debezium/prontuario-cdc.json "$URL/connectors/$NOME/config" >/dev/null
echo "[connect-init] conector $NOME registrado/atualizado"

for i in $(seq 1 24); do
  estados=$(curl -fs "$URL/connectors/$NOME/status" | grep -o '"state":"[A-Z]*"' | cut -d'"' -f4 | tr '\n' ' ')
  echo "[connect-init] estado (conector e tarefa): ${estados:-aguardando}"
  case "$estados" in
    "RUNNING RUNNING ") exit 0 ;;
    *FAILED*) curl -s "$URL/connectors/$NOME/status"; echo; exit 1 ;;
  esac
  sleep 5
done
echo "[connect-init] o conector não chegou ao estado RUNNING a tempo"
exit 1
