#!/usr/bin/env bash
# Gera uma autoridade certificadora (CA) local e o certificado TLS do PostgreSQL.
# Idempotente: não sobrescreve certificados existentes. Os arquivos ficam em
# ./certs (fora do Git). Em produção, usar uma CA corporativa ou um serviço
# gerenciado (ex.: AWS Private CA, cert-manager no Kubernetes).
set -euo pipefail
cd "$(dirname "$0")/.."
DIR=certs
mkdir -p "$DIR"

if [[ -f "$DIR/ca.crt" && -f "$DIR/postgres.crt" ]]; then
  echo "[certs] certificados já existem em ./$DIR"
  exit 0
fi

# 1. Autoridade certificadora do projeto
openssl req -x509 -newkey rsa:4096 -nodes -days 825 -sha256 \
  -keyout "$DIR/ca.key" -out "$DIR/ca.crt" \
  -subj "/C=BR/O=Case Saude/CN=Case Saude CA Local" 2>/dev/null

# 2. Certificado do PostgreSQL (nomes válidos: postgres, localhost)
openssl req -newkey rsa:2048 -nodes -sha256 \
  -keyout "$DIR/postgres.key" -out "$DIR/postgres.csr" \
  -subj "/C=BR/O=Case Saude/CN=postgres" 2>/dev/null
cat > "$DIR/postgres.ext" <<EXT
subjectAltName = DNS:postgres, DNS:localhost, IP:127.0.0.1
extendedKeyUsage = serverAuth
EXT
openssl x509 -req -in "$DIR/postgres.csr" -CA "$DIR/ca.crt" -CAkey "$DIR/ca.key" \
  -CAcreateserial -days 825 -sha256 -extfile "$DIR/postgres.ext" \
  -out "$DIR/postgres.crt" 2>/dev/null

rm -f "$DIR"/*.csr "$DIR"/*.ext "$DIR"/*.srl
chmod 600 "$DIR"/*.key
chmod 644 "$DIR"/*.crt
echo "[certs] CA e certificado do PostgreSQL gerados em ./$DIR"
