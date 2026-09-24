#!/usr/bin/env bash
# Cria o .env a partir do .env.example, gerando senhas aleatórias.
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ -f .env ]]; then
  echo "[gen-env] .env já existe — nada a fazer (apague-o para regerar)."
  exit 0
fi

cp .env.example .env
while grep -q "__GENERATE__" .env; do
  secret=$(openssl rand -hex 16)
  sed -i "0,/__GENERATE__/s//${secret}/" .env
done
chmod 600 .env
echo "[gen-env] .env criado com senhas aleatórias (permissão 600)."
