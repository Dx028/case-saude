"""
Batch layer — SRAG (OpenDataSUS) e municípios (IBGE).

Fluxo semanal (a fonte é atualizada toda semana):

  descobrir_arquivos -> baixar_para_landing (1 por ano) -> ha_pendentes -> bronze_silver ─┐
                         (só baixa o que mudou)                                           ├─> gold -> registrar_conclusao
  municipios_ibge ─────────────────────────────────────────────────────────────────────────┘

* Descoberta: a página do conjunto de dados traz os links diretos dos arquivos
  (o nome muda a cada atualização, ex.: INFLUD25-14-09-2026.csv).
* Carga incremental: um arquivo só é baixado se a versão (Last-Modified) ainda
  não consta em auditoria.controle_ingestao.
* Menor privilégio: o download usa o usuário svc-ingestao, que só grava na landing.
"""
import os
import re
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, get_current_context, task
from boto3.s3.transfer import TransferConfig

PAGINA_SRAG = "https://dadosabertos.saude.gov.br/dataset/srag-2019-a-2026"
PADRAO_ARQUIVO = re.compile(
    r"https://s3\.sa-east-1\.amazonaws\.com/ckan\.saude\.gov\.br/SRAG/(\d{4})/"
    r"(INFLUD\d{2}-(\d{2})-(\d{2})-(\d{4})\.csv)")
API_IBGE = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
ANOS = [int(a) for a in os.getenv("SRAG_ANOS", "2024,2025,2026").split(",") if a.strip()]
# Cabeçalhos HTTP devem ser ASCII: o servidor do IBGE rejeita (400) caracteres acentuados
CABECALHOS = {"User-Agent": "case-saude-pipeline/1.0 (projeto academico de engenharia de dados)"}

SPARK_COMUM = dict(
    conn_id="spark_default",
    py_files="/opt/jobs/common/mascaramento.py,/opt/jobs/batch/srag_transformacoes.py",
    conf={"spark.cores.max": "2", "spark.executor.memory": "1g",
          "spark.databricks.delta.snapshotPartitions": "2"},
)


@dag(
    dag_id="batch_srag",
    description="SRAG (OpenDataSUS) e municípios (IBGE): landing -> bronze -> silver -> gold",
    schedule="0 6 * * 2",            # terças às 6h: a fonte costuma ser atualizada no início da semana
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["batch", "srag", "opendatasus", "ibge"],
)
def batch_srag():

    @task
    def municipios_ibge() -> int:
        """Dimensão de municípios a partir da API de Localidades do IBGE (JSON)."""
        resposta = requests.get(API_IBGE, headers=CABECALHOS, timeout=60)
        resposta.raise_for_status()
        linhas = []
        for m in resposta.json():
            # municípios criados recentemente podem vir sem microrregião: usa a região imediata
            micro = m.get("microrregiao") or {}
            uf = ((micro.get("mesorregiao") or {}).get("UF")
                  or ((m.get("regiao-imediata") or {}).get("regiao-intermediaria") or {}).get("UF"))
            codigo = str(m["id"])
            linhas.append((codigo, codigo[:6], m["nome"], uf["sigla"], uf["nome"], uf["regiao"]["nome"],
                           (m.get("regiao-imediata") or {}).get("nome")))
        # executemany funciona tanto com psycopg2 quanto com psycopg 3 (usado pelo provider atual)
        conexao = PostgresHook(postgres_conn_id="dw").get_conn()
        try:
            with conexao.cursor() as cursor:
                cursor.executemany("""
                    INSERT INTO gold.dim_municipio
                        (codigo_ibge, codigo_ibge6, nome, uf, uf_nome, regiao, regiao_imediata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (codigo_ibge) DO UPDATE SET nome = EXCLUDED.nome, uf = EXCLUDED.uf,
                        uf_nome = EXCLUDED.uf_nome, regiao = EXCLUDED.regiao,
                        regiao_imediata = EXCLUDED.regiao_imediata, atualizado_em = now()""", linhas)
            conexao.commit()
        finally:
            conexao.close()
        print(f"[ibge] {len(linhas)} municípios carregados")
        return len(linhas)

    @task
    def descobrir_arquivos() -> list[dict]:
        """Lê a página do conjunto de dados e escolhe o recorte mais recente de cada ano."""
        html = requests.get(PAGINA_SRAG, headers=CABECALHOS, timeout=60).text
        mais_recente = {}
        for url, ano, arquivo, dia, mes, ano_recorte in set(
                (m.group(0), *m.groups()) for m in PADRAO_ARQUIVO.finditer(html)):
            ano, recorte = int(ano), (int(ano_recorte), int(mes), int(dia))
            if ano in ANOS and (ano not in mais_recente or recorte > mais_recente[ano]["recorte"]):
                mais_recente[ano] = {"ano": ano, "url": url, "arquivo": arquivo, "recorte": recorte}
        faltando = sorted(set(ANOS) - set(mais_recente))
        if faltando:
            raise ValueError(f"arquivos não encontrados na página para os anos {faltando}")

        pendentes = []
        hook = PostgresHook(postgres_conn_id="dw")
        for ano, info in sorted(mais_recente.items()):
            cabecalho = requests.head(info["url"], headers=CABECALHOS, timeout=60)
            cabecalho.raise_for_status()
            modificado = parsedate_to_datetime(cabecalho.headers["Last-Modified"])
            ja_processado = hook.get_first(
                """SELECT 1 FROM auditoria.controle_ingestao
                   WHERE fonte = 'srag' AND arquivo = %s AND last_modified = %s AND status = 'processado'""",
                parameters=(info["arquivo"], modificado))
            print(f"[descoberta] {ano}: {info['arquivo']} ({int(cabecalho.headers['Content-Length']) / 1e6:.0f} MB, "
                  f"modificado em {modificado:%d/%m/%Y}) -> {'já processado' if ja_processado else 'PENDENTE'}")
            if not ja_processado:
                pendentes.append({"ano": ano, "url": info["url"], "arquivo": info["arquivo"],
                                  "tamanho": int(cabecalho.headers["Content-Length"]),
                                  "last_modified": modificado.isoformat()})
        return pendentes

    @task
    def baixar_para_landing(arquivo: dict) -> str:
        """Download em streaming direto para a landing (sem gravar em disco)."""
        chave = f"srag/ano={arquivo['ano']}/{arquivo['arquivo']}"
        s3 = S3Hook(aws_conn_id="silo_ingestao").get_conn()
        existente = s3.list_objects_v2(Bucket="landing", Prefix=chave).get("Contents", [])
        if existente and existente[0]["Size"] == arquivo["tamanho"]:
            print(f"[landing] {chave} já existe com o mesmo tamanho: download dispensado")
        else:
            inicio = datetime.now(timezone.utc)
            with requests.get(arquivo["url"], headers=CABECALHOS, stream=True, timeout=(30, 300)) as resposta:
                resposta.raise_for_status()
                resposta.raw.decode_content = True
                s3.upload_fileobj(resposta.raw, "landing", chave,
                                  Config=TransferConfig(multipart_chunksize=32 * 1024**2, max_concurrency=4))
            segundos = (datetime.now(timezone.utc) - inicio).total_seconds()
            print(f"[landing] {chave}: {arquivo['tamanho'] / 1e6:.0f} MB em {segundos:.0f} s "
                  f"({arquivo['tamanho'] / 1e6 / max(segundos, 1):.1f} MB/s)")
        PostgresHook(postgres_conn_id="dw").run("""
            INSERT INTO auditoria.controle_ingestao
                (fonte, ano, url, arquivo, chave_landing, tamanho_bytes, last_modified, status, dag_run_id)
            VALUES ('srag', %s, %s, %s, %s, %s, %s, 'baixado', %s)
            ON CONFLICT (fonte, arquivo, last_modified)
            DO UPDATE SET status = 'baixado', atualizado_em = now(), dag_run_id = EXCLUDED.dag_run_id""",
            parameters=(arquivo["ano"], arquivo["url"], arquivo["arquivo"], chave, arquivo["tamanho"],
                        arquivo["last_modified"], get_current_context()["run_id"]))
        return chave

    # none_failed: roda mesmo sem downloads nesta execução, para retomar arquivos
    # que ficaram pendentes (ex.: processamento reprovado na semana anterior)
    @task.short_circuit(trigger_rule="none_failed")
    def ha_pendentes(chaves: list | None = None) -> bool:
        """Sem arquivos pendentes, bronze/silver/gold são dispensados."""
        pendentes = PostgresHook(postgres_conn_id="dw").get_first(
            "SELECT count(*) FROM auditoria.controle_ingestao WHERE fonte = 'srag' AND status = 'baixado'")[0]
        print(f"[batch] {pendentes} arquivo(s) aguardando processamento")
        return pendentes > 0

    bronze_silver = SparkSubmitOperator(
        task_id="bronze_silver", name="batch-srag-bronze-silver",
        application="/opt/jobs/batch/srag_bronze_silver.py",
        # Uma retentativa cobre falhas de infraestrutura (ex.: master do Spark lento para registrar
        # a aplicação). Uma reprovação por qualidade é determinística e falha de novo.
        retries=1,
        retry_delay=timedelta(minutes=2),
        **SPARK_COMUM)

    gold = SparkSubmitOperator(
        task_id="gold", name="batch-srag-gold",
        application="/opt/jobs/batch/srag_gold.py", **SPARK_COMUM)

    @task
    def registrar_conclusao():
        """Marca como processados os arquivos cuja silver foi publicada, com as contagens."""
        atualizados = PostgresHook(postgres_conn_id="dw").run("""
            UPDATE auditoria.controle_ingestao c
               SET status = 'processado', atualizado_em = now(),
                   linhas_bronze = r.linhas_bronze, linhas_silver = r.linhas_silver
              FROM (SELECT DISTINCT ON (arquivo) arquivo, linhas_bronze, linhas_silver
                      FROM auditoria.resultado_carga ORDER BY arquivo, processado_em DESC) r
             WHERE c.fonte = 'srag' AND c.status = 'baixado' AND c.arquivo = r.arquivo
            RETURNING c.ano, c.arquivo, c.linhas_silver""", handler=lambda cursor: cursor.fetchall())
        for ano, arquivo, linhas in atualizados or []:
            print(f"[conclusão] {ano}: {arquivo} processado ({linhas:,} notificações na silver)".replace(",", "."))

    # O SRAG não depende do IBGE até a gold: uma falha na API do IBGE não trava a carga principal
    dim = municipios_ibge()
    chaves = baixar_para_landing.expand(arquivo=descobrir_arquivos())
    continuar = ha_pendentes(chaves)
    continuar >> bronze_silver >> gold >> registrar_conclusao()
    dim >> gold


batch_srag()
