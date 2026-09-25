#!/usr/bin/env bash
# =============================================================
# Smoke test de ponta a ponta da plataforma
#   1. Estado dos containers (profiles ativos)
#   2. Saúde e conexões de cada componente
#   3. Fluxo real: Airflow -> Spark -> Silo / Kafka / PostgreSQL
#   4. Controles de segurança (security-check)
# Uso: make smoke          (completo, ~3 min)
#      make smoke-rapido   (sem o fluxo de ponta a ponta)
# =============================================================
set -uo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a

RAPIDO="${RAPIDO:-0}"
inicio=$(date +%s)
falhas=0
VERDE=$'\033[32m'; VERMELHO=$'\033[31m'; NEGRITO=$'\033[1m'; RESET=$'\033[0m'

ok()    { printf '  %s[OK]%s    %s\n' "$VERDE" "$RESET" "$1"; }
falha() { printf '  %s[FALHA]%s %s\n' "$VERMELHO" "$RESET" "$1"; falhas=$((falhas + 1)); }
secao() { printf '\n%s%s%s\n' "$NEGRITO" "$1" "$RESET"; }

# Executa o teste até N vezes (útil logo após "make up")
checar() { # descrição | tentativas | comando...
  local d="$1" n="$2"; shift 2
  for _ in $(seq 1 "$n"); do
    if "$@" >/dev/null 2>&1; then ok "$d"; return; fi
    sleep 5
  done
  falha "$d"
}
http() { curl -fsS -m 5 "$@"; }

# ---------------------------------------------------------------
secao "1. Containers (profiles: ${COMPOSE_PROFILES})"
UNICA_VEZ="db-setup silo-init kafka-init airflow-init"

# Containers recém-criados passam pelo estado "starting" até o primeiro healthcheck:
# aguarda até 3 minutos antes de avaliar (ex.: logo após um "make up")
for i in $(seq 1 36); do
  iniciando=$(docker compose ps --format json 2>/dev/null | jq -s 'flatten | map(select(.Health == "starting")) | length')
  [[ "$iniciando" == "0" ]] && break
  (( i == 1 )) && printf '  ...  %s container(s) iniciando, aguardando o primeiro healthcheck' "$iniciando"
  printf '.'; sleep 5
done
(( i > 1 )) && echo
estado=$(docker compose ps -a --format json 2>/dev/null | jq -s 'flatten')
for svc in $(docker compose config --services 2>/dev/null | sort); do
  linhas=$(jq -r --arg s "$svc" '.[] | select(.Service == $s) | "\(.State)|\(.Health)|\(.ExitCode)"' <<<"$estado")
  if [[ -z "$linhas" ]]; then falha "$svc: não criado"; continue; fi
  total=0; bons=0
  while IFS='|' read -r st health code; do
    total=$((total + 1))
    if [[ " $UNICA_VEZ " == *" $svc "* ]]; then
      [[ "$st" == "exited" && "$code" == "0" ]] && bons=$((bons + 1))
    else
      [[ "$st" == "running" && ( -z "$health" || "$health" == "healthy" ) ]] && bons=$((bons + 1))
    fi
  done <<<"$linhas"
  if [[ " $UNICA_VEZ " == *" $svc "* ]]; then desc="concluído com sucesso"; else desc="em execução"; fi
  (( total > 1 )) && desc="$desc (${bons}/${total} réplicas)"
  if (( bons == total )); then
    ok "$svc: $desc"
  else
    falha "$svc: $bons de $total saudáveis"
    # mostra a última mensagem do healthcheck, para agilizar o diagnóstico
    nome=$(jq -r --arg s "$svc" '[.[] | select(.Service == $s)][0].Name' <<<"$estado")
    docker inspect --format '{{json .State.Health}}' "$nome" 2>/dev/null \
      | jq -r '.Log[-1].Output // empty' 2>/dev/null | head -3 | sed 's/^/          /'
  fi
done

# ---------------------------------------------------------------
secao "2. Saúde e conexões"
ativo() { [[ ",${COMPOSE_PROFILES}," == *",$1,"* ]]; }

checar "PostgreSQL aceita conexões"            6 docker compose exec -T postgres pg_isready -q
checar "Silo (object storage) saudável"        6 http "http://localhost:${MINIO_API_PORT}/minio/health/live"
checar "DW: dimensão calendário populada"      3 bash -c "docker compose exec -T postgres psql -U '$POSTGRES_USER' -d dw -Atc 'SELECT count(*) > 7000 FROM gold.dim_data' | grep -qx t"

if ativo stream; then
  checar "Kafka: 3 tópicos de negócio criados" 6 bash -c "[[ \$(docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -c '^saude\.') -eq 3 ]]"
  checar "Kafka Connect com conector Debezium" 12 bash -c "curl -fsS -m 5 http://localhost:${KAFKA_CONNECT_PORT}/connector-plugins | jq -e 'any(.[]; .class | test(\"PostgresConnector\"))'"
  checar "Kafka UI respondendo"                6 bash -c "[[ \$(curl -s -m 5 -o /dev/null -w '%{http_code}' http://localhost:${KAFKA_UI_PORT}/) =~ ^(200|302|401)$ ]]"
fi
if ativo processing; then
  checar "Spark: workers registrados no master" 6 bash -c "curl -fsS -m 5 http://localhost:${SPARK_MASTER_UI_PORT}/json/ | jq -e '.aliveworkers >= 1'"
fi
if ativo speed; then
  checar "Speed layer: aplicação de streaming ativa" 12 bash -c "curl -fsS -m 5 http://localhost:${SPARK_STREAMING_UI_PORT}/api/v1/applications | jq -e 'length > 0'"
fi
if ativo orchestration; then
  checar "Airflow: banco de metadados e scheduler" 12 bash -c "curl -fsS -m 5 http://localhost:${AIRFLOW_PORT}/api/v2/monitor/health | jq -e '.metadatabase.status == \"healthy\" and .scheduler.status == \"healthy\"'"
fi
if ativo bi; then
  checar "Metabase saudável"                    12 bash -c "curl -fsS -m 5 http://localhost:${METABASE_PORT}/api/health | jq -e '.status == \"ok\"'"
fi
if ativo observability; then
  checar "Grafana saudável"                     6 bash -c "curl -fsS -m 5 http://localhost:${GRAFANA_PORT}/api/health | jq -e '.database == \"ok\"'"
  checar "Prometheus: todos os alvos coletados" 12 bash -c "curl -fsS -m 5 http://localhost:${PROMETHEUS_PORT}/api/v1/targets | jq -e '[.data.activeTargets[] | select(.health != \"up\")] | length == 0'"
  checar "Loki recebendo logs dos containers"   12 bash -c "curl -fsS -m 5 -u admin:${GRAFANA_ADMIN_PASSWORD} http://localhost:${GRAFANA_PORT}/api/datasources/proxy/uid/loki/loki/api/v1/label/service/values | jq -e '.data | length > 5'"
fi

# ---------------------------------------------------------------
if [[ "$RAPIDO" != "1" ]] && ativo orchestration && ativo processing; then
  secao "3. Fluxo de ponta a ponta: Airflow -> Spark -> Silo / Kafka / PostgreSQL"
  AF() { docker compose exec -T airflow-scheduler airflow "$@" 2>/dev/null; }
  run_id="smoke__$(date +%Y%m%dT%H%M%S)"
  AF dags unpause smoke_test_spark >/dev/null
  if AF dags trigger smoke_test_spark --run-id "$run_id" >/dev/null; then
    printf '  ...  DAG smoke_test_spark disparada (%s), aguardando' "$run_id"
    status="queued"
    for _ in $(seq 1 60); do   # até 5 minutos
      sleep 5; printf '.'
      status=$(AF dags list-runs smoke_test_spark -o json | sed -n '/^\[/,$p' \
               | jq -r --arg r "$run_id" '.[] | select(.run_id == $r) | .state' 2>/dev/null)
      [[ "$status" == "success" || "$status" == "failed" ]] && break
    done
    echo
    if [[ "$status" == "success" ]]; then
      ok "Airflow executou o job no cluster Spark, que gravou Delta no Silo, leu o Kafka e consultou o DW via TLS"
    else
      falha "DAG terminou com estado '${status:-desconhecido}' (veja os logs da task no Airflow)"
    fi
  else
    falha "não foi possível disparar a DAG smoke_test_spark"
  fi
fi

# ---------------------------------------------------------------
if [[ "$RAPIDO" != "1" ]] && ativo speed && ativo stream; then
  secao "3b. Speed layer: evento no Kafka -> Spark Streaming -> alerta no DW"
  id="smoke-$(date +%s)"
  agora=$(date -u +%Y-%m-%dT%H:%M:%S.000+00:00)
  depois=$(date -u -d '+2 seconds' +%Y-%m-%dT%H:%M:%S.000+00:00)
  comum='"cpf":"000.000.001-91","nome":"Paciente Teste","data_nascimento":"1950-01-01","sexo":"F","telefone":"(11) 90000-0000","email":"teste@exemplo.com","endereco":"Rua Teste, 1","cep":"01000-000","municipio":"São Paulo","codigo_ibge":"3550308","uf":"SP","hospital_cnes":"0000000","hospital_nome":"Hospital de Teste (smoke)","hospital_municipio":"São Paulo","hospital_uf":"SP","setor":"UTI","leito":"UTI-00","cid_principal":"J18","gravidade":5'
  PRODUZ() { docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic "$1" >/dev/null 2>&1; }
  printf '{"evento_id":"%s-a","tipo_evento":"ADMISSAO","ocorrido_em":"%s","internacao_id":"%s",%s}\n' "$id" "$agora" "$id" "$comum" | PRODUZ saude.eventos.admissoes
  printf '{"evento_id":"%s-s","internacao_id":"%s","ocorrido_em":"%s","leito":"UTI-00","frequencia_cardiaca":95,"pressao_sistolica":118,"pressao_diastolica":75,"saturacao_o2":85.0,"temperatura":37.0,"frequencia_respiratoria":22}\n' "$id" "$id" "$agora" | PRODUZ saude.eventos.sinais-vitais
  printf '{"evento_id":"%s-b","tipo_evento":"ALTA","ocorrido_em":"%s","internacao_id":"%s",%s}\n' "$id" "$depois" "$id" "$comum" | PRODUZ saude.eventos.admissoes
  printf '  ...  eventos de teste publicados (internação %s), aguardando o alerta no DW' "$id"
  SQL() { docker compose exec -T postgres psql -U "$POSTGRES_USER" -d dw -Atc "$1" 2>/dev/null; }
  latencia=""
  for _ in $(seq 1 24); do   # até 2 minutos
    sleep 5; printf '.'
    latencia=$(SQL "SELECT round(extract(epoch FROM processado_em - ocorrido_em)::numeric, 1) FROM gold.alerta_clinico_rt WHERE internacao_id = '$id' AND tipo_alerta = 'HIPOXEMIA' LIMIT 1")
    [[ -n "$latencia" ]] && break
  done
  echo
  if [[ -n "$latencia" ]]; then
    ok "Alerta de hipoxemia (SpO2 85%) chegou ao DW em ${latencia}s: bronze, silver e serving atualizados"
  else
    falha "o alerta de teste não chegou ao DW em 2 minutos (veja: make speed-logs)"
  fi
  SQL "DELETE FROM gold.alerta_clinico_rt WHERE internacao_id LIKE 'smoke-%'" >/dev/null
fi

# ---------------------------------------------------------------
secao "4. Segurança"
if ./scripts/security-check.sh >/tmp/security-check.log 2>&1; then
  ok "security-check: $(grep -c '\[OK\]' /tmp/security-check.log) controles verificados (detalhes: make security-check)"
else
  falha "security-check encontrou problemas:"
  grep 'FALHA' /tmp/security-check.log | sed 's/^/        /'
fi

# ---------------------------------------------------------------
duracao=$(( $(date +%s) - inicio ))
echo
if (( falhas == 0 )); then
  printf '%s%sPlataforma pronta: todas as verificações passaram (%ds).%s\n' "$NEGRITO" "$VERDE" "$duracao" "$RESET"
  echo; make --no-print-directory urls
else
  printf '%s%s%d verificação(ões) falharam (%ds).%s\n' "$NEGRITO" "$VERMELHO" "$falhas" "$duracao" "$RESET"
  exit 1
fi
