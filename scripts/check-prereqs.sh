#!/usr/bin/env bash
# Verifica se a máquina está pronta para rodar o projeto.
set -uo pipefail

ok()   { echo "  [OK]    $1"; }
warn() { echo "  [AVISO] $1"; }
fail() { echo "  [ERRO]  $1"; ERR=1; }
ERR=0

echo "Verificando pré-requisitos..."

docker --version >/dev/null 2>&1 && ok "docker $(docker --version | awk '{print $3}' | tr -d ,)" || fail "docker não encontrado"
docker compose version >/dev/null 2>&1 && ok "docker compose $(docker compose version --short)" || fail "docker compose v2 não encontrado"
docker info >/dev/null 2>&1 && ok "Docker Engine em execução" || fail "Docker Engine parado (abra o Docker Desktop)"
command -v git  >/dev/null && ok "git $(git --version | awk '{print $3}')" || fail "git não encontrado"
command -v make >/dev/null && ok "make" || fail "make não encontrado"
command -v openssl >/dev/null && ok "openssl" || fail "openssl não encontrado"

mem_gb=$(awk '/MemTotal/ {printf "%d", $2/1024/1024}' /proc/meminfo)
if (( mem_gb >= 16 )); then ok "Memória: ${mem_gb} GB"
elif (( mem_gb >= 12 )); then warn "Memória: ${mem_gb} GB (suba a stack por profiles)"
else fail "Memória: ${mem_gb} GB (mínimo recomendado: 12 GB)"; fi

disk_gb=$(df -BG --output=avail "$PWD" | tail -1 | tr -dc '0-9')
(( disk_gb >= 40 )) && ok "Disco livre: ${disk_gb} GB" || warn "Disco livre: ${disk_gb} GB (recomendado: 60 GB+)"

case "$PWD" in
  /mnt/*) warn "Projeto em $PWD — mova para ~/projetos (disco do Windows é lento no WSL)";;
  *) ok "Projeto no sistema de arquivos Linux";;
esac

git config core.autocrlf | grep -q input && ok "git core.autocrlf=input" || warn "rode: git config --global core.autocrlf input"

echo
if (( ERR )); then echo "Há pendências acima."; exit 1; else echo "Ambiente pronto."; fi
