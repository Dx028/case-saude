#!/bin/sh
# Cria os buckets do lakehouse (idempotente: pode rodar várias vezes)
set -eu

mc alias set lake http://silo:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null

for bucket in landing bronze silver gold; do
  mc mb --ignore-existing "lake/${bucket}"
done

# Versionamento na bronze: permite recuperar dados brutos sobrescritos
mc version enable lake/bronze

echo "[silo-init] buckets prontos:"
mc ls lake
