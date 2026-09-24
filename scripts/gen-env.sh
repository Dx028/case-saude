#!/usr/bin/env bash
# Cria o .env a partir do .env.example, gerando segredos aleatórios:
#   __GENERATE__ -> senha hexadecimal
#   __FERNET__   -> chave Fernet (criptografia de conexões do Airflow)
#   __KMS__      -> chave mestra do KMS do Silo (criptografia em repouso)
# Se o .env já existir, apenas ACRESCENTA as variáveis novas, sem alterar as existentes.
set -euo pipefail
cd "$(dirname "$0")/.."

gen()    { openssl rand -hex 16; }
fernet() { openssl rand -base64 32 | tr '+/' '-_'; }
kms()    { echo "case-saude-kms:$(openssl rand -base64 32)"; }

resolve() {
  case "$1" in
    __GENERATE__) gen ;;
    __FERNET__)   fernet ;;
    __KMS__)      kms ;;
    *)            printf '%s' "$1" ;;
  esac
}

if [[ ! -f .env ]]; then
  : > .env
  while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ "$line" =~ ^([A-Z0-9_]+)=(.*)$ ]]; then
      echo "${BASH_REMATCH[1]}=$(resolve "${BASH_REMATCH[2]}")" >> .env
    else
      echo "$line" >> .env
    fi
  done < .env.example
  chmod 600 .env
  echo "[gen-env] .env criado com segredos aleatórios (permissão 600)."
  exit 0
fi

added=0
while IFS= read -r line || [[ -n "$line" ]]; do
  [[ "$line" =~ ^([A-Z0-9_]+)=(.*)$ ]] || continue
  key="${BASH_REMATCH[1]}"
  if ! grep -q "^${key}=" .env; then
    echo "${key}=$(resolve "${BASH_REMATCH[2]}")" >> .env
    echo "[gen-env] adicionada: ${key}"
    added=$((added + 1))
  fi
done < .env.example
chmod 600 .env
(( added == 0 )) && echo "[gen-env] .env já está completo." || echo "[gen-env] ${added} variável(is) adicionada(s)."
