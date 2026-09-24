#!/usr/bin/env bash
# Cria o .env a partir do .env.example, gerando senhas aleatórias.
# Se o .env já existir, apenas ACRESCENTA as variáveis novas do .env.example,
# sem alterar as existentes.
set -euo pipefail
cd "$(dirname "$0")/.."

gen() { openssl rand -hex 16; }

if [[ ! -f .env ]]; then
  cp .env.example .env
  while grep -q "__GENERATE__" .env; do
    sed -i "0,/__GENERATE__/s//$(gen)/" .env
  done
  chmod 600 .env
  echo "[gen-env] .env criado com senhas aleatórias (permissão 600)."
  exit 0
fi

added=0
while IFS= read -r line; do
  [[ "$line" =~ ^([A-Z0-9_]+)=(.*)$ ]] || continue
  key="${BASH_REMATCH[1]}"; value="${BASH_REMATCH[2]}"
  if ! grep -q "^${key}=" .env; then
    [[ "$value" == "__GENERATE__" ]] && value="$(gen)"
    echo "${key}=${value}" >> .env
    echo "[gen-env] adicionada: ${key}"
    added=$((added + 1))
  fi
done < .env.example
chmod 600 .env
(( added == 0 )) && echo "[gen-env] .env já está completo." || echo "[gen-env] ${added} variável(is) adicionada(s)."
