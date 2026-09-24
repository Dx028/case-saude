# Case de Engenharia de Dados — Plataforma de Dados de Saúde

> Documento em construção.

## I. Objetivo do case

_A preencher._

## II. Arquitetura de solução e arquitetura técnica

_A preencher (diagramas em `docs/diagrams`)._

## III. Explicação sobre o case desenvolvido

_A preencher._

## IV. Melhorias e considerações finais

_A preencher._

---

## Como executar

### Pré-requisitos

- Docker Desktop (Windows/macOS) ou Docker Engine (Linux) com Docker Compose v2
- No Windows: WSL2 com Ubuntu — clone o projeto **dentro** do Linux (ex.: `~/projetos`)
- `git`, `make` e `openssl`
- Mínimo de 12 GB de RAM disponíveis para o Docker (recomendado: 16 GB+)

### Passo a passo

```bash
git clone <url-do-repositorio>
cd case-saude
make setup     # verifica pré-requisitos e gera o .env com senhas aleatórias
make up        # sobe os serviços
make ps        # confere o estado dos containers
```

Rode `make help` para ver todos os comandos.

## Estrutura do repositório

```
├── config/      # configurações de Prometheus, Grafana, Loki, Alloy e Kafka
├── dags/        # DAGs do Airflow
├── data/        # dados locais (não versionados)
├── docker/      # Dockerfiles customizados (Spark, Airflow, Kafka Connect)
├── docs/        # diagramas e documentação complementar
├── generator/   # gerador de eventos simulados de pacientes
├── jobs/        # jobs Spark (batch e streaming)
├── scripts/     # scripts de setup, inicialização e testes
├── sql/         # scripts de inicialização e modelagem do DW
└── tests/       # testes automatizados
```
