"""
Portal da plataforma: uma página para o usuário de negócio acompanhar a plataforma
sem abrir as ferramentas técnicas. Responde a quatro perguntas:

  * A plataforma está funcionando?             (Prometheus: alvos e alertas)
  * A carga do SRAG deu certo? Em que etapa?   (Airflow: execuções, etapas e logs)
  * O tempo real está em dia?                  (Prometheus, Kafka Connect e DW)
  * Onde vejo os resultados e os detalhes?     (links para Metabase, Grafana, Airflow...)

Somente leitura: consulta o Airflow com um usuário de papel Viewer e o DW com o
bi_reader (TLS). As credenciais ficam neste serviço, nunca no navegador.
"""
import asyncio
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import httpx
import psycopg
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

AIRFLOW = os.getenv("AIRFLOW_URL", "http://airflow-apiserver:8080")
PROMETHEUS = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
CONNECT = os.getenv("CONNECT_URL", "http://kafka-connect:8083")
DW_DSN = (f"host={os.getenv('DW_HOST', 'postgres')} port=5432 dbname=dw user=bi_reader "
          f"password={os.getenv('BI_READER_PASSWORD', '')} sslmode=require connect_timeout=5")
PAINEIS = Path(os.getenv("PAINEIS_ARQUIVO", "/dados/paineis.json"))
HOST = os.getenv("HOST_PUBLICO", "localhost")
DAG = "batch_srag"
TIMEOUT = httpx.Timeout(8.0)

# Etapas da carga do SRAG, na ordem em que acontecem, com nomes para o usuário
ETAPAS = [
    ("descobrir_arquivos", "Descobrir arquivos"),
    ("baixar_para_landing", "Baixar arquivos"),
    ("ha_pendentes", "Verificar pendências"),
    ("bronze_silver", "Tratar e checar qualidade"),
    ("municipios_ibge", "Atualizar municípios"),
    ("gold", "Calcular indicadores"),
    ("registrar_conclusao", "Registrar conclusão"),
]
ESTADO_RUN = {"queued": "Na fila", "running": "Em andamento", "success": "Concluída", "failed": "Falhou"}
# Quando uma etapa tem várias instâncias (um download por ano), vale o estado mais relevante
PRIORIDADE = ["failed", "running", "up_for_retry", "restarting", "up_for_reschedule", "deferred",
              "queued", "scheduled", "upstream_failed", "success", "skipped", None]


def url_publica(variavel, padrao, caminho=""):
    return f"http://{HOST}:{os.getenv(variavel, padrao)}{caminho}"


def agora():
    return datetime.now(timezone.utc)


def data(texto):
    if not texto:
        return None
    return datetime.fromisoformat(str(texto).replace("Z", "+00:00"))


# ------------------------------------------------------------------ regras puras (testadas)
def estado_da_etapa(estados):
    """Consolida os estados das instâncias de uma etapa (tarefas mapeadas têm várias)."""
    if not estados:
        return None
    return min(estados, key=lambda e: PRIORIDADE.index(e) if e in PRIORIDADE else len(PRIORIDADE))


def consolidar_etapas(instancias):
    por_tarefa = {}
    for ti in instancias:
        por_tarefa.setdefault(ti["task_id"], []).append(ti)
    etapas = []
    for tarefa, nome in ETAPAS:
        tis = por_tarefa.get(tarefa, [])
        inicios = [data(t.get("start_date")) for t in tis if t.get("start_date")]
        fins = [data(t.get("end_date")) for t in tis if t.get("end_date")]
        etapas.append({
            "tarefa": tarefa, "nome": nome,
            "estado": estado_da_etapa([t.get("state") for t in tis]),
            "instancias": len(tis),
            "inicio": min(inicios).isoformat() if inicios else None,
            "fim": max(fins).isoformat() if fins and len(fins) == len(tis) else None,
        })
    return etapas


def trecho_do_log(conteudo, limite=14):
    """Extrai do log da tarefa as linhas que explicam a falha (erros e a exceção final)."""
    if isinstance(conteudo, str):
        itens = conteudo.splitlines()
    else:
        itens = conteudo or []
    linhas = []
    for item in itens:
        if isinstance(item, dict):
            nivel = str(item.get("level", "")).lower()
            texto = str(item.get("event", "")).strip()
            for erro in item.get("error_detail") or []:
                if isinstance(erro, dict) and erro.get("exc_type"):
                    texto += f"\n{erro['exc_type']}: {erro.get('exc_value', '')}"
            if texto:
                linhas.append((nivel, texto))
        elif str(item).strip():
            texto = str(item).strip()
            nivel = "error" if any(p in texto for p in ("ERROR", "Error", "Exception", "Traceback")) else ""
            linhas.append((nivel, texto))
    # Erros técnicos e as linhas que explicam o motivo ao usuário (ex.: regra de qualidade reprovada)
    erros = [t for n, t in linhas if n in ("error", "critical") or "FALHA" in t]
    escolhidas = erros[-limite:] if erros else [t for _, t in linhas][-limite:]
    return "\n".join(escolhidas)[-2500:]


def avaliar_saude(alertas, alvos_fora, srag, tempo_real, indisponiveis):
    """Traduz os sinais técnicos numa situação geral e numa lista de problemas em linguagem simples."""
    problemas, nivel = [], "ok"
    def registrar(texto, gravidade):
        nonlocal nivel
        problemas.append({"texto": texto, "gravidade": gravidade})
        if gravidade == "falha" or (gravidade == "atencao" and nivel == "ok"):
            nivel = gravidade
    for a in alertas:
        registrar(a["resumo"], "falha" if a["severidade"] == "critica" else "atencao")
    if alvos_fora:
        registrar(f"Sem resposta: {', '.join(sorted(alvos_fora))}", "falha")
    if srag and srag.get("estado") == "failed":
        etapa = next((e["nome"] for e in srag.get("etapas", []) if e["estado"] == "failed"), "uma das etapas")
        registrar(f"A última carga do SRAG falhou em \"{etapa}\"", "falha")
    cdc = (tempo_real or {}).get("cdc", {})
    if cdc.get("conector") not in (None, "RUNNING"):
        registrar(f"O conector do prontuário está {cdc['conector']}", "falha")
    for fonte in indisponiveis:
        registrar(f"Não foi possível consultar {fonte}", "atencao")
    titulos = {"ok": "Tudo funcionando", "atencao": "Funcionando, com pontos de atenção",
               "falha": "Há falhas que precisam de ação"}
    return {"nivel": nivel, "titulo": titulos[nivel], "problemas": problemas}


# ------------------------------------------------------------------ fontes
class Airflow:
    def __init__(self):
        self.token, self.validade = None, 0.0

    async def _autenticar(self, cliente):
        if self.token and time.time() < self.validade:
            return
        r = await cliente.post(f"{AIRFLOW}/auth/token", json={
            "username": os.getenv("AIRFLOW_PORTAL_USER", "portal"),
            "password": os.getenv("AIRFLOW_PORTAL_PASSWORD", "")})
        r.raise_for_status()
        self.token, self.validade = r.json()["access_token"], time.time() + 600

    async def get(self, cliente, caminho, **params):
        for tentativa in (1, 2):
            await self._autenticar(cliente)
            r = await cliente.get(f"{AIRFLOW}{caminho}", params=params,
                                  headers={"Authorization": f"Bearer {self.token}", "Accept": "application/json"})
            if r.status_code == 401 and tentativa == 1:
                self.token = None
                continue
            r.raise_for_status()
            return r.json()


airflow = Airflow()


async def carga_srag(cliente):
    dag = await airflow.get(cliente, f"/api/v2/dags/{DAG}")
    try:
        runs = (await airflow.get(cliente, f"/api/v2/dags/{DAG}/dagRuns", limit=1, order_by="-run_after"))["dag_runs"]
    except httpx.HTTPStatusError:
        todas = (await airflow.get(cliente, f"/api/v2/dags/{DAG}/dagRuns", limit=100))["dag_runs"]
        runs = sorted(todas, key=lambda r: r.get("run_after") or r.get("start_date") or "", reverse=True)[:1]
    resultado = {
        "pausada": dag.get("is_paused"),
        "proxima": dag.get("next_dagrun_run_after") or dag.get("next_dagrun_logical_date"),
        "link": url_publica("AIRFLOW_PORT", "8080", f"/dags/{DAG}"),
    }
    if not runs:
        return {**resultado, "estado": None}
    run = runs[0]
    rid = quote(run["dag_run_id"], safe="")
    tis = (await airflow.get(cliente, f"/api/v2/dags/{DAG}/dagRuns/{rid}/taskInstances", limit=100))["task_instances"]
    etapas = consolidar_etapas(tis)
    inicio, fim = data(run.get("start_date")), data(run.get("end_date"))
    falha = None
    ti_falha = next((t for t in tis if t.get("state") == "failed"), None)
    if ti_falha:
        tarefa = ti_falha["task_id"]
        try:
            log = await airflow.get(cliente, f"/api/v2/dags/{DAG}/dagRuns/{rid}/taskInstances/{tarefa}/logs/"
                                             f"{max(ti_falha.get('try_number') or 1, 1)}",
                                    map_index=ti_falha.get("map_index", -1), full_content="true")
            trecho = trecho_do_log(log.get("content"))
        except (httpx.HTTPError, ValueError, KeyError):
            trecho = ""
        falha = {"etapa": dict(ETAPAS).get(tarefa, tarefa), "trecho": trecho,
                 "link": url_publica("AIRFLOW_PORT", "8080", f"/dags/{DAG}/runs/{rid}/tasks/{tarefa}")}
    return {**resultado,
            "estado": run.get("state"), "estado_texto": ESTADO_RUN.get(run.get("state"), run.get("state")),
            "tipo": "Agendada" if run.get("run_type") == "scheduled" else "Manual",
            "inicio": inicio.isoformat() if inicio else None, "fim": fim.isoformat() if fim else None,
            "duracao_s": int(((fim or agora()) - inicio).total_seconds()) if inicio else None,
            "etapas": etapas, "falha": falha,
            "link_execucao": url_publica("AIRFLOW_PORT", "8080", f"/dags/{DAG}/runs/{rid}")}


async def prometheus(cliente):
    async def consulta(expr):
        r = await cliente.get(f"{PROMETHEUS}/api/v1/query", params={"query": expr})
        r.raise_for_status()
        return r.json()["data"]["result"]

    def valor(resultado):
        return float(resultado[0]["value"][1]) if resultado else None

    alertas_r = await cliente.get(f"{PROMETHEUS}/api/v1/alerts")
    alertas_r.raise_for_status()
    alertas = [{"nome": a["labels"].get("alertname"), "severidade": a["labels"].get("severidade", ""),
                "resumo": a.get("annotations", {}).get("resumo") or a["labels"].get("alertname")}
               for a in alertas_r.json()["data"]["alerts"] if a.get("state") == "firing"]
    alvos = await consulta("up")
    fora = sorted({r["metric"].get("job", "?") for r in alvos if r["value"][1] == "0"} - {"gerador"})
    fluxo = {}
    for q in ("speed_layer", "cdc_prontuario"):
        fluxo[q] = {
            "ativo": (valor(await consulta(f'changes(spark_streaming_batch_id{{query="{q}"}}[5m])')) or 0) > 0,
            "atraso": valor(await consulta(f'max(spark_streaming_offsets_behind_latest_max{{query="{q}"}})')),
            "duracao_ms": valor(await consulta(f'max(spark_streaming_batch_duration_ms{{query="{q}"}})')),
            "entrada_s": valor(await consulta(f'max(spark_streaming_input_rows_per_second{{query="{q}"}})')),
        }
    gerador = valor(await consulta('max(up{job="gerador"})'))
    # Mensagens que de fato chegaram aos tópicos (medidas no Kafka, não no gerador)
    chegando = valor(await consulta(
        'sum(rate(kafka_topic_partition_current_offset{topic=~"saude\\\\.eventos\\\\.(admissoes|sinais-vitais)"}[5m]))'))
    return {"alertas": alertas, "fora": fora, "total_alvos": len(alvos), "fluxo": fluxo,
            "gerador_ligado": gerador == 1.0, "eventos_chegando_s": chegando}


async def conector(cliente):
    r = await cliente.get(f"{CONNECT}/connectors/prontuario-cdc/status")
    if r.status_code == 404:
        return "NÃO REGISTRADO"
    r.raise_for_status()
    estados = [r.json()["connector"]["state"]] + [t["state"] for t in r.json().get("tasks", [])]
    return "RUNNING" if all(e == "RUNNING" for e in estados) else next(e for e in estados if e != "RUNNING")


def dw():
    with psycopg.connect(DW_DSN) as conexao, conexao.cursor() as cur:
        cur.execute("""SELECT DISTINCT ON (ano) ano, arquivo, status, round(tamanho_bytes / 1e6), linhas_silver,
                              atualizado_em
                       FROM auditoria.controle_ingestao WHERE fonte = 'srag' ORDER BY ano, atualizado_em DESC""")
        anos = [{"ano": a, "arquivo": arq, "status": st, "mb": int(mb or 0), "notificacoes": n,
                 "atualizado_em": at.isoformat() if at else None} for a, arq, st, mb, n, at in cur.fetchall()]
        cur.execute("""SELECT count(*) FILTER (WHERE aprovado), count(*) FROM qualidade.vw_ultima_verificacao""")
        aprovadas, total = cur.fetchone()
        cur.execute("""SELECT COALESCE(ano::text, 'geral'), regra, severidade, valor, limite
                       FROM qualidade.vw_ultima_verificacao WHERE NOT aprovado ORDER BY severidade, regra""")
        falhas = [{"ano": a, "regra": r, "severidade": s, "valor": float(v), "limite": float(li)}
                  for a, r, s, v, li in cur.fetchall()]
        cur.execute("SELECT max(atualizado_em) FROM gold.ocupacao_rt")
        ocupacao = cur.fetchone()[0]
        cur.execute("SELECT pacientes, eliminados, atualizado_em FROM gold.cdc_pacientes LIMIT 1")
        cdc = cur.fetchone()
    return {"anos": anos, "qualidade": {"aprovadas": aprovadas, "total": total, "falhas": falhas},
            "speed_ultimo": ocupacao.isoformat() if ocupacao else None,
            "cdc": {"pacientes": cdc[0], "eliminados": cdc[1], "ultimo": cdc[2].isoformat()} if cdc else None}


def links():
    ferramentas = [
        {"nome": "Airflow", "descricao": "Execuções das cargas, etapas e logs", "url": url_publica("AIRFLOW_PORT", "8080")},
        {"nome": "Grafana", "descricao": "Métricas, alertas e logs da plataforma",
         "url": url_publica("GRAFANA_PORT", "3001", "/d/case-saude-visao-geral")},
        {"nome": "Kafka UI", "descricao": "Mensagens em trânsito, com dados pessoais mascarados",
         "url": url_publica("KAFKA_UI_PORT", "8082")},
        {"nome": "Spark", "descricao": "Processamento em execução no cluster", "url": url_publica("SPARK_MASTER_UI_PORT", "8081")},
        {"nome": "Prometheus", "descricao": "Alertas ativos e consultas às métricas",
         "url": url_publica("PROMETHEUS_PORT", "9090", "/alerts")},
        {"nome": "Silo", "descricao": "Arquivos do lakehouse", "url": url_publica("MINIO_CONSOLE_PORT", "9001")},
    ]
    try:
        paineis = json.loads(PAINEIS.read_text(encoding="utf-8"))["paineis"]
    except (OSError, ValueError, KeyError):
        paineis = [{"nome": "Painéis do Metabase", "url": url_publica("METABASE_PORT", "3000"),
                    "descricao": "Rode make metabase para listar os painéis aqui"}]
    return {"paineis": paineis, "ferramentas": ferramentas}


# ------------------------------------------------------------------ API
app = FastAPI(title="Portal da plataforma de dados de saúde", docs_url=None, redoc_url=None)
_cache = {"em": 0.0, "dados": None}


async def montar_status():
    async with httpx.AsyncClient(timeout=TIMEOUT) as cliente:
        tarefas = {
            "airflow": carga_srag(cliente),
            "prometheus": prometheus(cliente),
            "conector": conector(cliente),
            "dw": asyncio.to_thread(dw),
        }
        respostas = await asyncio.gather(*tarefas.values(), return_exceptions=True)
    r = dict(zip(tarefas, respostas))
    nomes = {"airflow": "o Airflow", "prometheus": "o Prometheus", "conector": "o Kafka Connect", "dw": "o DW"}
    indisponiveis = [nomes[k] for k, v in r.items() if isinstance(v, Exception)]
    ok = {k: (None if isinstance(v, Exception) else v) for k, v in r.items()}

    prom, base = ok["prometheus"] or {}, ok["dw"] or {}
    fluxo = prom.get("fluxo", {})
    tempo_real = {
        "gerador_ligado": prom.get("gerador_ligado"),
        "eventos_chegando_s": prom.get("eventos_chegando_s"),
        "speed": {**fluxo.get("speed_layer", {}), "ultimo": base.get("speed_ultimo")},
        "cdc": {**fluxo.get("cdc_prontuario", {}), **(base.get("cdc") or {}), "conector": ok["conector"]},
    }
    srag = ok["airflow"]
    if srag is not None:
        srag["anos"] = base.get("anos", [])
        srag["qualidade"] = base.get("qualidade")
    return {
        "gerado_em": agora().isoformat(),
        "saude": avaliar_saude(prom.get("alertas", []), prom.get("fora", []), srag, tempo_real, indisponiveis),
        "carga": srag,
        "tempo_real": tempo_real,
        "links": links(),
    }


@app.get("/api/status")
async def status():
    if time.time() - _cache["em"] > 5:  # várias abas abertas não multiplicam as consultas
        _cache["dados"], _cache["em"] = await montar_status(), time.time()
    return JSONResponse(_cache["dados"])


@app.get("/api/saude")
async def saude():
    return {"status": "ok"}


@app.get("/")
async def pagina():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")
