# Case de Engenharia de Dados — Plataforma de Dados de Saúde

> **Status:** ambiente de execução completo e verificado (`make smoke`). Pipelines de dados em desenvolvimento.

Plataforma de dados para o domínio de saúde, construída inteiramente com ferramentas open source e executada em Docker. A solução segue uma arquitetura Lambda sobre um lakehouse (camadas bronze, silver e gold), com ingestão em lote e em tempo real, observabilidade de ponta a ponta e controles de segurança alinhados à LGPD.

## I. Objetivo do case

_Em construção._

## II. Arquitetura de solução e arquitetura técnica

_Em construção (diagramas em `docs/diagrams`)._

## III. Explicação sobre o case desenvolvido

_Em construção._

## IV. Melhorias e considerações finais

_Em construção._

---

## Como executar

### Pré-requisitos

| Requisito | Observação |
|---|---|
| Docker Desktop (Windows/macOS) ou Docker Engine (Linux) | Com Docker Compose v2 |
| WSL2 com Ubuntu (apenas Windows) | O projeto deve ficar **dentro** do Linux (ex.: `~/projetos`), nunca em `/mnt/c` |
| `git`, `make`, `openssl`, `jq`, `curl` | No Ubuntu: `sudo apt install -y git make openssl jq curl` |
| Memória para o Docker | Mínimo de 12 GB; recomendado 16 GB ou mais para a stack completa |
| Disco livre | Cerca de 60 GB (imagens, dados e cache de build) |

No Windows, a memória disponível para o WSL é definida no arquivo `%UserProfile%\.wslconfig` (por exemplo, `memory=20GB`).

### Passo a passo

```bash
git clone <url-do-repositorio>
cd case-saude

make setup    # verifica pré-requisitos, gera o .env (senhas aleatórias) e os certificados TLS
make up       # constrói as imagens e sobe todos os serviços
make smoke    # verifica a plataforma de ponta a ponta
```

O primeiro `make up` leva de 10 a 20 minutos, pois constrói as imagens do Spark e do Airflow. As execuções seguintes usam o cache e levam poucos minutos.

Nenhum segredo é versionado: o `.env` e a pasta `certs/` são gerados localmente por `make setup` e estão no `.gitignore`.

### Configuração inicial do Metabase (manual, feita uma única vez)

O Metabase exige a criação do administrador pela interface no primeiro acesso:

1. Acesse http://localhost:3000, escolha o idioma e crie o usuário administrador (conta local, sem cadastro externo).
2. Na etapa de adicionar dados, escolha **PostgreSQL** e preencha:

| Campo | Valor |
|---|---|
| Nome de exibição | `DW Saúde` |
| Host / Porta | `postgres` / `5432` |
| Banco de dados | `dw` |
| Usuário | `bi_reader` (somente leitura) |
| Senha | valor de `BI_READER_PASSWORD` no `.env` |
| Usar conexão segura (SSL) | **Ativado**, modo `require` |
| Schemas | Apenas `gold` |

Sem SSL a conexão é recusada pelo banco: o PostgreSQL aceita apenas conexões criptografadas.

### Acessos

| Serviço | Endereço | Usuário | Senha |
|---|---|---|---|
| Airflow | http://localhost:8080 | `admin` | `AIRFLOW_ADMIN_PASSWORD` |
| Metabase | http://localhost:3000 | definido no primeiro acesso | definida no primeiro acesso |
| Grafana | http://localhost:3001 | `admin` | `GRAFANA_ADMIN_PASSWORD` |
| Prometheus | http://localhost:9090 | — | — |
| Kafka UI | http://localhost:8082 | `admin` | `KAFKA_UI_PASSWORD` |
| Spark Master | http://localhost:8081 | — | — |
| Console do Silo | http://localhost:9001 | `MINIO_ROOT_USER` | `MINIO_ROOT_PASSWORD` |
| PostgreSQL | `localhost:5432` (TLS obrigatório) | `POSTGRES_USER` | `POSTGRES_PASSWORD` |

As senhas ficam no arquivo `.env`. Exemplo: `grep GRAFANA_ADMIN_PASSWORD .env`.

### Comandos principais

| Comando | Descrição |
|---|---|
| `make setup` | Verifica pré-requisitos, cria o `.env` e os certificados |
| `make up` / `make down` | Sobe / para os serviços (os dados são mantidos) |
| `make ps` | Estado dos containers |
| `make smoke` | Verificação completa, com fluxo real Airflow → Spark → Silo/Kafka/PostgreSQL |
| `make smoke-rapido` | Verificação de containers, conexões e segurança |
| `make security-check` | Testes de TLS, criptografia em repouso, permissões e mascaramento |
| `make mascaramento-demo` | Demonstração das técnicas de mascaramento com o Spark |
| `make spark-smoke` | Testa as conexões do Spark diretamente no cluster |
| `make scale-workers n=3` | Ajusta o número de workers Spark (escala horizontal) |
| `make targets` / `make alerts` | Alvos coletados e alertas ativos no Prometheus |
| `make logs s=<serviço>` | Logs de um serviço |
| `make psql db=<banco>` | Console SQL no PostgreSQL |
| `make urls` | Endereços das interfaces |
| `make clean` | Remove containers **e dados** (pede confirmação) |

A lista completa está em `make help`.

### Perfis de execução

Os serviços são agrupados em *profiles*, definidos pela variável `COMPOSE_PROFILES` no `.env`. Em máquinas com pouca memória, é possível subir apenas parte da stack.

| Profile | Serviços |
|---|---|
| `core` | PostgreSQL, Silo (object storage) e rotinas de inicialização |
| `stream` | Kafka, Kafka Connect (Debezium) e Kafka UI |
| `processing` | Spark master e workers |
| `orchestration` | Airflow e statsd-exporter |
| `bi` | Metabase |
| `observability` | Prometheus, Grafana, Loki, Alloy, cAdvisor e exporters |

### Solução de problemas

| Sintoma | Causa provável e solução |
|---|---|
| `The command 'docker' could not be found in this WSL 2 distro` | Integração com o WSL desativada: Docker Desktop → Settings → Resources → WSL Integration → ativar o Ubuntu |
| `docker-desktop` com estado `Stopped` em `wsl -l -v` | O Docker Desktop não está em execução: abra-o e aguarde "Engine running" |
| Scripts falham com `\r: command not found` | Quebras de linha do Windows: `git config --global core.autocrlf input` e clone novamente |
| Containers reiniciando ou jobs interrompidos | Memória insuficiente: aumente o limite no `.wslconfig` ou suba menos profiles |
| Metabase: `pg_hba.conf rejects connection ... no encryption` | SSL desativado na conexão do DW: ative-o com o modo `require` |
| Senha do administrador do Metabase perdida | `docker compose exec metabase java -jar /app/metabase.jar reset-password <email>` e acesse o link com o token gerado |

## Estrutura do repositório

```
├── certs/       # CA e certificados TLS locais (gerados; fora do Git)
├── config/      # Prometheus, Grafana, Loki, Alloy, StatsD, PostgreSQL e políticas do Silo
├── dags/        # DAGs do Airflow
├── data/        # dados locais (fora do Git)
├── docker/      # Dockerfiles do Spark e do Airflow
├── docs/        # diagramas e documentação complementar
├── generator/   # gerador de eventos simulados de pacientes
├── jobs/        # jobs Spark (batch, streaming e bibliotecas comuns, como o mascaramento)
├── scripts/     # setup, inicialização, smoke test e verificação de segurança
├── sql/         # inicialização do PostgreSQL e objetos do DW (schemas, papéis, segurança)
└── tests/       # testes automatizados
```
