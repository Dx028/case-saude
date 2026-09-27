# Case de Engenharia de Dados — Plataforma de Dados de Saúde

> **Status:** plataforma completa e verificada (`make smoke`): speed layer, batch layer, CDC e painéis do BI em funcionamento.

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
make metabase # cria a conexão com o DW e os painéis do Metabase
```

O primeiro `make up` leva de 10 a 20 minutos, pois constrói as imagens do Spark e do Airflow. As execuções seguintes usam o cache e levam poucos minutos.

Nenhum segredo é versionado: o `.env` e a pasta `certs/` são gerados localmente por `make setup` e estão no `.gitignore`.

### Painéis do Metabase (criados como código)

Os painéis não são montados à mão: o script `scripts/metabase_paineis.py` usa a API do Metabase para criar a conta de administrador (numa instalação nova), a conexão com o DW e todos os painéis. Assim, a camada de visualização é reproduzível como o restante da plataforma.

```bash
make metabase
```

| Painel | Conteúdo |
|---|---|
| 1. Vigilância de SRAG (batch) | Casos, óbitos, letalidade e UTI; curva epidêmica por semana e ano; classificação final; letalidade por faixa etária; casos por UF e municípios com mais casos |
| 2. Operação hospitalar em tempo real | Internações ativas, UTI e pacientes graves; alertas por minuto; ocupação por UF; últimos alertas com a latência do evento até o DW |
| 3. Visão integrada (Lambda) | A visão batch (SRAG no ano) e a visão em tempo real (ocupação atual), lado a lado por UF |
| 4. Qualidade e ingestão | Resultado de cada regra de qualidade e controle das cargas incrementais |

A conexão usa o usuário `bi_reader` (somente leitura, schema `gold` e resultados de qualidade) com TLS obrigatório. O login do Metabase usa `METABASE_ADMIN_EMAIL` e `METABASE_ADMIN_PASSWORD`, do `.env`. A cada execução, os itens da coleção "Case Saúde" são arquivados e recriados, de modo que o código é sempre a fonte da verdade.

### Acessos

| Serviço | Endereço | Usuário | Senha |
|---|---|---|---|
| Airflow | http://localhost:8080 | `admin` | `AIRFLOW_ADMIN_PASSWORD` |
| Metabase | http://localhost:3000 | `METABASE_ADMIN_EMAIL` | `METABASE_ADMIN_PASSWORD` |
| Grafana | http://localhost:3001 | `admin` | `GRAFANA_ADMIN_PASSWORD` |
| Prometheus | http://localhost:9090 | — | — |
| Kafka UI | http://localhost:8082 | `admin` | `KAFKA_UI_PASSWORD` |
| Spark Master | http://localhost:8081 | — | — |
| Speed layer (Spark UI, aba *Structured Streaming*) | http://localhost:4040 | — | — |
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
| `make gerador-start taxa=200` | Liga o gerador de eventos simulados (vazão em eventos/s) |
| `make gerador-stop` | Desliga o gerador |
| `make speed-logs` / `make gerador-logs` | Logs da speed layer / do gerador |
| `make batch-srag` | Dispara a batch layer do SRAG (OpenDataSUS e IBGE) |
| `make batch-status` | Situação das cargas e resultado das verificações de qualidade |
| `make cdc-status` | Estado do conector de CDC e comparação entre a origem e o espelho no DW |
| `make cdc-registrar` | Registra/atualiza o conector do Debezium no Kafka Connect |
| `make metabase` | Cria/atualiza os painéis do Metabase pela API (painéis como código) |
| `make batch-reprocessar` | Reprocessa o SRAG a partir da landing, sem novo download (idempotente) |
| `make test` | Testes automatizados das transformações e do mascaramento |
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
| `speed` | Speed layer (Spark Structured Streaming) |
| `simulation` | Gerador de eventos (ligado sob demanda com `make gerador-start`) |
| `orchestration` | Airflow e statsd-exporter |
| `bi` | Metabase |
| `observability` | Prometheus, Grafana, Loki, Alloy, cAdvisor e exporters |

### Simulação de eventos e speed layer

O gerador simula internações em 12 capitais e publica no Kafka eventos de admissão, transferência e alta (com dados pessoais sintéticos) e sinais vitais periódicos. Cerca de 1% dos eventos são corrompidos de propósito, para exercitar a validação e a fila de mensagens inválidas (DLQ).

```bash
make gerador-start            # 50 eventos/s (padrão)
make gerador-start taxa=500   # teste de carga
make gerador-stop
```

A speed layer (Spark Structured Streaming) roda continuamente e, a cada micro-batch de 30 segundos:

| Etapa | Destino | Conteúdo |
|---|---|---|
| Bronze | `s3a://bronze/eventos_saude` | Evento bruto, com tópico, partição e offset de origem |
| Validação | `saude.eventos.dlq` | Eventos inválidos: motivo e referência ao registro na bronze (sem dados pessoais) |
| Silver | `s3a://silver/admissoes` e `s3a://silver/sinais_vitais` | Eventos válidos, com o CPF pseudonimizado e os dados de contato suprimidos |
| Estado | `s3a://silver/internacoes_estado` | Situação atual de cada internação (`MERGE`) |
| Serving | `gold.alerta_clinico_rt` e `gold.ocupacao_rt` (PostgreSQL) | Alertas por sinais vitais críticos e ocupação por hospital e setor |

O intervalo de 30 segundos foi definido por medição: com lotes de 10 segundos, o processamento levava cerca de 17 segundos e o atraso crescia continuamente; com 30 segundos, cada lote leva cerca de 13 segundos e a latência média, do evento ao alerta no DW, fica em torno de 28 segundos, de forma estável. O custo de cada lote é quase todo fixo (commits transacionais no Delta Lake), por isso lotes maiores o diluem. O intervalo é configurável pela variável `SPEED_INTERVALO_LOTE`.

As gravações nas tabelas Delta são idempotentes por micro-batch: um reprocessamento após falha não gera duplicatas. No Metabase, as views `vw_alertas_clinicos` (com a latência de cada alerta) e `vw_ocupacao_por_uf` ficam disponíveis após sincronizar o esquema do banco.

### CDC: prontuário no PostgreSQL (Debezium)

O banco `prontuario` simula o sistema transacional de um hospital (schema `clinico`, tabelas `pacientes` e `atendimentos`). Com o gerador ligado, ele recebe operações reais: cadastros, agendamentos, mudanças de situação, atualização de contato e eliminação de titulares a pedido (LGPD).

| Etapa | Como funciona |
|---|---|
| Captura | O **Debezium** lê o log de transações (WAL) por replicação lógica (`pgoutput`), com o usuário da aplicação e TLS. A publicação `dbz_prontuario` restringe a captura às duas tabelas do prontuário |
| Transporte | Um tópico por tabela (`prontuario.clinico.pacientes` e `prontuario.clinico.atendimentos`), com o envelope do Debezium: estado anterior, estado novo, operação e posição no log (LSN) |
| Segredos | A senha do banco não aparece na configuração do conector: `${env:OLTP_DB_PASSWORD}` é resolvida pelo Kafka Connect em tempo de execução |
| Processamento | Segunda query de streaming da speed layer (lotes de 60 s): bronze com os eventos brutos e silver como **espelho pseudonimizado**, atualizado por `MERGE` apenas com a mudança mais recente de cada chave |
| Integração | O CPF vira o mesmo pseudônimo usado nos eventos de internação: as duas fontes se cruzam sem expor o dado pessoal |
| Eliminação (LGPD) | Uma exclusão na origem remove o titular da silver (e, em cascata, seus atendimentos) e apaga da bronze os eventos com os dados pessoais dele. A remoção física dos arquivos ocorre no `VACUUM` do Delta Lake, após o período de retenção |
| Serving | `gold.cdc_pacientes` e `gold.cdc_atendimentos`, exibidos no painel de tempo real do Metabase |

O conector é registrado automaticamente pelo serviço `connect-init` a cada `make up`. A verificação de ponta a ponta (`make smoke`, etapa 3c) insere um paciente de teste no prontuário e mede o tempo até a alteração chegar ao DW.

### Batch layer: SRAG (OpenDataSUS) e municípios (IBGE)

A DAG `batch_srag` roda semanalmente (terças, às 6h) e integra duas fontes públicas:

| Fonte | Formato | Conteúdo |
|---|---|---|
| OpenDataSUS — SRAG 2019 a 2026 (SIVEP-Gripe) | CSV (≈ 300 MB por ano, 194 colunas) | Notificações de síndrome respiratória aguda grave |
| IBGE — API de Localidades | JSON | Cadastro dos 5.571 municípios brasileiros |

Etapas e decisões:

1. **Descoberta:** os nomes dos arquivos mudam a cada atualização semanal (ex.: `INFLUD25-14-09-2026.csv`). A DAG lê a página do conjunto de dados e escolhe o recorte mais recente de cada ano.
2. **Carga incremental:** a versão de cada arquivo (`Last-Modified`) é registrada em `auditoria.controle_ingestao`; só o que mudou é baixado e processado.
3. **Landing:** download em streaming direto para o object storage, com o usuário `svc-ingestao`, que só tem permissão de gravar na landing.
4. **Bronze:** as 194 colunas originais, como texto, em Delta Lake particionado por ano.
5. **Silver:** tipagem, tradução dos códigos do dicionário de dados, deduplicação e **minimização (LGPD)**: mesmo sendo dados abertos, a data de nascimento é descartada (fica a faixa etária) e o número da notificação é pseudonimizado. Os códigos de município do Distrito Federal são padronizados: o SIVEP-Gripe registra as Regiões Administrativas (Ceilândia, Taguatinga etc.) com códigos próprios, inexistentes no IBGE, onde o DF tem um único município (Brasília). A verificação de integridade referencial da gold detectou o problema (cerca de 3% dos casos sem município correspondente), e os códigos passaram a ser mapeados para Brasília, preservando o original em `municipio_residencia_sivep`.
6. **Portão de qualidade:** regras críticas e de alerta, gravadas em `qualidade.verificacao`. Se uma regra crítica falhar, a silver daquele ano **não é publicada** e o arquivo fica pendente para a próxima execução.
7. **Gold:** a fato `gold.fato_srag_semanal` é recalculada a partir de toda a silver (princípio da camada batch na arquitetura Lambda), com a dimensão `gold.dim_municipio` e as views `vw_srag_semanal_uf` e `vw_srag_municipio`.

Os anos carregados são definidos pela variável `SRAG_ANOS` (padrão: 2024 a 2026).

Resultado da carga de 2024 a 2026 (disco de dados em HD mecânico, 2 núcleos para o lote):

| Ano | Arquivo | Notificações | Situação da base |
|---|---|---|---|
| 2024 | 302 MB | 267.986 | congelada |
| 2025 | 382 MB | 336.391 | viva (atualização semanal) |
| 2026 | 237 MB | 212.278 | viva (ano corrente) |

- **Total:** 816.655 notificações, agregadas em cerca de 531 mil linhas na fato semanal.
- **Qualidade:** 26 regras aprovadas; nenhuma duplicidade nem notificação sem número.
- **Tempo:** cerca de 30 minutos na carga completa (download dispensado quando o arquivo não mudou), sendo cerca de 15 minutos em bronze e silver e 7 minutos na gold.

### Limitações conhecidas

- **Dados pessoais nos tópicos do CDC:** os eventos do Debezium carregam o registro completo da origem e ficam no Kafka pelo período de retenção (7 dias). O acesso é controlado pela autenticação da interface e pelo mascaramento de campos; em produção, somam-se SASL/ACLs no Kafka ou a pseudonimização já no conector (transformações do Kafka Connect).
- **Gravações concorrentes no Delta Lake sobre S3:** o object storage não oferece a operação atômica "gravar somente se não existir" de que o log de transações do Delta precisa. Por isso, duas aplicações Spark gravando **na mesma tabela** ao mesmo tempo podem falhar. No projeto, cada tabela tem um único gravador (a speed layer ou o job em lote correspondente), as DAGs usam `max_active_runs=1` e o smoke test grava em uma tabela exclusiva por execução. Em produção, a solução é um LogStore com coordenação externa (por exemplo, o baseado em DynamoDB na AWS) ou um formato com catálogo transacional, como o Apache Iceberg.

### Solução de problemas

| Sintoma | Causa provável e solução |
|---|---|
| `The command 'docker' could not be found in this WSL 2 distro` | Integração com o WSL desativada: Docker Desktop → Settings → Resources → WSL Integration → ativar o Ubuntu |
| `docker-desktop` com estado `Stopped` em `wsl -l -v` | O Docker Desktop não está em execução: abra-o e aguarde "Engine running" |
| Scripts falham com `\r: command not found` | Quebras de linha do Windows: `git config --global core.autocrlf input` e clone novamente |
| Containers reiniciando ou jobs interrompidos | Memória insuficiente: aumente o limite no `.wslconfig` ou suba menos profiles |
| Metabase: `pg_hba.conf rejects connection ... no encryption` | SSL desativado na conexão do DW: ative-o com o modo `require` |
| Build ou serviços falham com `read-only file system` | Disco do Windows cheio: o disco virtual do Docker não consegue crescer e passa a somente leitura. Libere espaço ou mova o disco (Docker Desktop → Settings → Resources → Advanced → Disk image location). O alerta `DiscoQuaseCheio` monitora o disco virtual por dentro; o espaço livre no disco do Windows, causa deste incidente, precisa ser monitorado pelo lado do host (ex.: `windows_exporter`) |
| Master do Spark não responde ("All masters are unresponsive") | Memória de serviços ociosos enviada ao swap em disco lento. Mantenha o swap em SSD (`swapfile=` no `.wslconfig`) e reduza o `vm.swappiness` para 10 |
| Serviço saudável, mas a porta não responde no host (`curl` retorna `000`) | Encaminhamento de portas desatualizado após reiniciar o WSL: `docker compose up -d --force-recreate <serviço>` |
| Docker Desktop: erro de integração com o Ubuntu após reiniciar o WSL | Settings → Resources → WSL Integration: desligue e religue o Ubuntu |
| Após religar a máquina, serviço falha com `No such file or directory` em arquivo de configuração | O container foi montado antes de o WSL estar ativo: `docker compose up -d --force-recreate <serviço>`. Os serviços usam `restart: on-failure` justamente para não religarem sozinhos no boot; a rotina é abrir o Docker Desktop, abrir o Ubuntu e rodar `make up` |
| Container com `Exited (127)` depois de reiniciar o Docker | Montagem desatualizada: `docker compose up -d --force-recreate <serviço>` |
| Comandos `docker` travados, sem resposta | Docker Desktop sobrecarregado: `timeout 20 docker info`; se não responder, reinicie o Docker Desktop e rode `wsl --shutdown` |
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
