#!/usr/bin/env bash
# Verifica, no ambiente em execução, os controles de segurança da plataforma.
# Cada teste informa o resultado esperado; qualquer divergência é FALHA.
set -uo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a

falhas=0
registrar() { # descrição | esperado | obtido
  if [[ "$2" == "$3" ]]; then st="[OK]   "; else st="[FALHA]"; falhas=$((falhas + 1)); fi
  printf '  %s %-62s %s\n' "$st" "$1" "$3"
}
acesso() { # descrição | esperado (PERMITIDO/NEGADO) | comando...
  local d="$1" e="$2"; shift 2
  if "$@" >/dev/null 2>&1; then registrar "$d" "$e" PERMITIDO; else registrar "$d" "$e" NEGADO; fi
}

# psql a partir do container do Postgres, mas pela REDE (host=postgres), como um cliente externo
PSQL() { # usuário senha sslmode sql
  docker compose exec -T -e PGPASSWORD="$2" postgres \
    psql "host=postgres dbname=dw user=$1 sslmode=$3" -v ON_ERROR_STOP=1 -Atc "$4"
}
# mc a partir do container do Silo, autenticado como o usuário informado
MC() { # usuário segredo comando
  docker compose exec -T silo sh -c "mc alias set chk http://localhost:9000 '$1' '$2' >/dev/null 2>&1 && $3"
}

echo
echo "PostgreSQL — criptografia em trânsito e controle de acesso"
acesso "Conexão SEM TLS é recusada"                      NEGADO    PSQL dw_owner "$DW_DB_PASSWORD" disable "SELECT 1"
tls=$(PSQL dw_owner "$DW_DB_PASSWORD" require "SELECT ssl || ' ' || version FROM pg_stat_ssl WHERE pid = pg_backend_pid()" 2>/dev/null)
registrar "Conexão COM TLS é aceita e criptografada"       "true TLSv1.3" "${tls:-sem conexão}"
acesso "BI lê a view mascarada de pacientes"             PERMITIDO PSQL bi_reader "$BI_READER_PASSWORD" require "SELECT count(*) FROM gold.vw_paciente_exemplo_mascarado"
acesso "BI lê a tabela com dados identificáveis"         NEGADO    PSQL bi_reader "$BI_READER_PASSWORD" require "SELECT * FROM restrito.paciente_exemplo"
acesso "BI executa a eliminação de titular"              NEGADO    PSQL bi_reader "$BI_READER_PASSWORD" require "SELECT seguranca.eliminar_titular('00000000000')"
acesso "BI cria tabela na gold"                          NEGADO    PSQL bi_reader "$BI_READER_PASSWORD" require "CREATE TABLE gold.teste_invasao (i int)"
acesso "Usuário de monitoramento lê dados do DW"         NEGADO    PSQL monitoring "$MONITORING_DB_PASSWORD" require "SELECT * FROM gold.dim_data LIMIT 1"
cpf=$(PSQL bi_reader "$BI_READER_PASSWORD" require "SELECT cpf FROM gold.vw_paciente_exemplo_mascarado ORDER BY paciente_id LIMIT 1" 2>/dev/null)
registrar "CPF exibido ao BI está mascarado"               "***.456.789-**" "${cpf:-sem resultado}"

echo
echo "Lakehouse (Silo) — criptografia em repouso e menor privilégio por camada"
MC "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" \
  "echo sonda | mc pipe chk/gold/_seguranca/sonda.txt && echo sonda | mc pipe chk/bronze/_seguranca/sonda.txt" >/dev/null 2>&1
for b in landing bronze silver gold; do
  enc=$(MC "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" "mc encrypt info chk/$b" 2>/dev/null | grep -o "sse-s3" | head -1)
  registrar "Criptografia automática (SSE-S3) no bucket $b" "sse-s3" "${enc:-desativada}"
done
acesso "Pipeline grava na bronze"                        PERMITIDO MC svc-pipeline "$SILO_PIPELINE_SECRET" "echo x | mc pipe chk/bronze/_seguranca/pipeline.txt"
acesso "Ingestão grava na landing"                       PERMITIDO MC svc-ingestao "$SILO_INGESTAO_SECRET" "echo x | mc pipe chk/landing/_seguranca/ingestao.txt"
acesso "Ingestão lê a bronze"                            NEGADO    MC svc-ingestao "$SILO_INGESTAO_SECRET" "mc cat chk/bronze/_seguranca/sonda.txt"
acesso "Analista lê a gold"                              PERMITIDO MC svc-analista "$SILO_ANALISTA_SECRET" "mc cat chk/gold/_seguranca/sonda.txt"
acesso "Analista lê a bronze (dado bruto)"               NEGADO    MC svc-analista "$SILO_ANALISTA_SECRET" "mc cat chk/bronze/_seguranca/sonda.txt"
acesso "Analista grava na gold"                          NEGADO    MC svc-analista "$SILO_ANALISTA_SECRET" "echo x | mc pipe chk/gold/_seguranca/analista.txt"
MC "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" \
  "mc rm --recursive --force chk/gold/_seguranca chk/bronze/_seguranca chk/landing/_seguranca" >/dev/null 2>&1

echo
if (( falhas == 0 )); then echo "Todos os controles de segurança verificados com sucesso."
else echo "${falhas} verificação(ões) com resultado inesperado."; exit 1; fi
