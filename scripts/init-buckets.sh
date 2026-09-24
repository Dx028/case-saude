#!/bin/sh
# Inicializa o lakehouse (idempotente):
#   1. buckets das camadas
#   2. criptografia em repouso (SSE-S3 com a chave do KMS) em todos os buckets
#   3. versionamento na bronze
#   4. usuários de serviço com políticas de menor privilégio por camada
set -eu

SILO_URL="${SILO_URL:-http://silo:9000}"
POLITICAS="${POLITICAS_DIR:-/politicas}"

mc alias set lake "$SILO_URL" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null

for bucket in landing bronze silver gold; do
  mc mb --ignore-existing "lake/${bucket}"
  mc encrypt set sse-s3 "lake/${bucket}" >/dev/null
done
mc version enable lake/bronze >/dev/null

# usuário|segredo|política
for linha in \
  "svc-pipeline|${SILO_PIPELINE_SECRET}|pipeline" \
  "svc-ingestao|${SILO_INGESTAO_SECRET}|ingestao" \
  "svc-analista|${SILO_ANALISTA_SECRET}|analista"
do
  usuario=$(echo "$linha" | cut -d'|' -f1)
  segredo=$(echo "$linha" | cut -d'|' -f2)
  politica=$(echo "$linha" | cut -d'|' -f3)
  mc admin policy create lake "$politica" "${POLITICAS}/${politica}.json" >/dev/null
  mc admin user add lake "$usuario" "$segredo" >/dev/null
  mc admin policy attach lake "$politica" --user "$usuario" >/dev/null 2>&1 || true
  echo "[silo-init] usuário ${usuario} -> política ${politica}"
done

echo "[silo-init] buckets (todos com SSE-S3):"
for bucket in landing bronze silver gold; do
  printf '  %-8s ' "$bucket"; mc encrypt info "lake/${bucket}" | head -1
done
