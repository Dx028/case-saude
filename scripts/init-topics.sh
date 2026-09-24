#!/bin/sh
# Cria os tópicos do projeto (idempotente: --if-not-exists)
# Convenção de nomes: <domínio>.<tipo>.<entidade>
set -eu

BS=kafka:9092
KT=/opt/kafka/bin/kafka-topics.sh

create() {
  # $1 tópico | $2 partições | $3 retenção (ms)
  $KT --bootstrap-server "$BS" --create --if-not-exists \
      --topic "$1" --partitions "$2" --replication-factor 1 \
      --config retention.ms="$3" --config compression.type=producer
}

DIA=86400000
create saude.eventos.admissoes     3  $((7 * DIA))   # admissões/altas hospitalares
create saude.eventos.sinais-vitais 6  $((3 * DIA))   # alto volume: mais partições
create saude.eventos.dlq           1  $((14 * DIA))  # dead letter queue (mensagens inválidas)

echo "[kafka-init] tópicos disponíveis:"
$KT --bootstrap-server "$BS" --list
