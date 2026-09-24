#!/bin/bash
# Copia o certificado para dentro do container com o dono e as permissões
# que o PostgreSQL exige (chave privada 0600 do usuário postgres) e segue
# para o entrypoint oficial da imagem.
set -euo pipefail
mkdir -p /etc/postgresql/tls
cp /certs-src/postgres.crt /certs-src/postgres.key /certs-src/ca.crt /etc/postgresql/tls/
chown -R postgres:postgres /etc/postgresql/tls
chmod 600 /etc/postgresql/tls/postgres.key
exec docker-entrypoint.sh "$@"
