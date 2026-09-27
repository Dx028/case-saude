#!/bin/sh
# Cria os tópicos do projeto (idempotente e tolerante a um broker ainda lento na partida)
# Convenção de nomes: <domínio>.<tipo>.<entidade>
set -eu

BS=kafka:9092
KT=/opt/kafka/bin/kafka-topics.sh
DIA=86400000

# O healthcheck só garante que o broker responde; operações administrativas podem demorar mais
for i in $(seq 1 30); do
  $KT --bootstrap-server "$BS" --list >/dev/null 2>&1 && break
  echo "[kafka-init] aguardando o broker aceitar operações administrativas ($i/30)..."
  sleep 5
done
EXISTENTES=$($KT --bootstrap-server "$BS" --list 2>/dev/null)

create() {
  # $1 tópico | $2 partições | $3 retenção (ms)
  if echo "$EXISTENTES" | grep -qx "$1"; then
    echo "[kafka-init] $1 já existe"
    return 0
  fi
  for i in 1 2 3 4 5; do
    if $KT --bootstrap-server "$BS" --create --if-not-exists \
         --topic "$1" --partitions "$2" --replication-factor 1 \
         --config retention.ms="$3" --config compression.type=producer 2>/dev/null; then
      echo "[kafka-init] $1 criado"
      return 0
    fi
    echo "[kafka-init] falha ao criar $1, nova tentativa em 10 s ($i/5)"
    sleep 10
  done
  return 1
}

create saude.eventos.admissoes     3  $((7 * DIA))   # admissões/altas hospitalares
create saude.eventos.sinais-vitais 6  $((3 * DIA))   # alto volume: mais partições
create saude.eventos.dlq           1  $((14 * DIA))  # dead letter queue (mensagens inválidas)

echo "[kafka-init] tópicos disponíveis:"
$KT --bootstrap-server "$BS" --list 2>/dev/null
