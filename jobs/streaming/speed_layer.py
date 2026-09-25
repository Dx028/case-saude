"""
Speed layer: processamento contínuo dos eventos hospitalares (Structured Streaming).

Fluxo de cada micro-batch (foreachBatch):

  Kafka (admissões + sinais vitais)
    ├─> BRONZE  s3a://bronze/eventos_saude           evento bruto + metadados do Kafka
    ├─> validação
    │     ├─ inválidos ─> Kafka saude.eventos.dlq     motivo + referência ao registro na bronze
    │     └─ válidos
    │          ├─> SILVER s3a://silver/admissoes      pseudonimizado (sem nome, CPF, contato)
    │          ├─> SILVER s3a://silver/sinais_vitais
    │          └─> SILVER s3a://silver/internacoes_estado  (MERGE: situação atual de cada internação)
    └─> SERVING (PostgreSQL, schema gold)
          ├─ alerta_clinico_rt   alertas por sinais vitais críticos
          └─ ocupacao_rt         internações ativas por hospital e setor

Garantias: as gravações Delta são idempotentes por micro-batch (txnAppId +
txnVersion), então um reprocessamento após falha não duplica dados. As
gravações no PostgreSQL são "pelo menos uma vez" (o batch_id permite deduplicar).
"""
import json
import os
import socket
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.window import Window

DIR_COMMON = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "common")
sys.path.insert(0, DIR_COMMON)
import mascaramento as m  # noqa: E402

APP = "speed_layer"
TOPICO_ADMISSOES = "saude.eventos.admissoes"
TOPICO_SINAIS = "saude.eventos.sinais-vitais"
TOPICO_DLQ = "saude.eventos.dlq"

BRONZE = "s3a://bronze/eventos_saude"
SILVER_ADMISSOES = "s3a://silver/admissoes"
SILVER_SINAIS = "s3a://silver/sinais_vitais"
SILVER_ESTADO = "s3a://silver/internacoes_estado"
CHECKPOINT = "s3a://bronze/_checkpoints/speed_layer"

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
JDBC_URL = "jdbc:postgresql://postgres:5432/dw?sslmode=require"

CORROMPIDO = "_registro_corrompido"

# ------------------------------------------------------------------ esquemas
ESQUEMA_ADMISSAO = T.StructType([
    T.StructField("evento_id", T.StringType()),
    T.StructField("tipo_evento", T.StringType()),
    T.StructField("ocorrido_em", T.StringType()),
    T.StructField("internacao_id", T.StringType()),
    T.StructField("cpf", T.StringType()),
    T.StructField("nome", T.StringType()),
    T.StructField("data_nascimento", T.StringType()),
    T.StructField("sexo", T.StringType()),
    T.StructField("telefone", T.StringType()),
    T.StructField("email", T.StringType()),
    T.StructField("endereco", T.StringType()),
    T.StructField("cep", T.StringType()),
    T.StructField("municipio", T.StringType()),
    T.StructField("codigo_ibge", T.StringType()),
    T.StructField("uf", T.StringType()),
    T.StructField("hospital_cnes", T.StringType()),
    T.StructField("hospital_nome", T.StringType()),
    T.StructField("hospital_municipio", T.StringType()),
    T.StructField("hospital_uf", T.StringType()),
    T.StructField("setor", T.StringType()),
    T.StructField("leito", T.StringType()),
    T.StructField("cid_principal", T.StringType()),
    T.StructField("gravidade", T.IntegerType()),
    T.StructField(CORROMPIDO, T.StringType()),
])

ESQUEMA_SINAIS = T.StructType([
    T.StructField("evento_id", T.StringType()),
    T.StructField("internacao_id", T.StringType()),
    T.StructField("ocorrido_em", T.StringType()),
    T.StructField("leito", T.StringType()),
    T.StructField("frequencia_cardiaca", T.DoubleType()),
    T.StructField("pressao_sistolica", T.DoubleType()),
    T.StructField("pressao_diastolica", T.DoubleType()),
    T.StructField("saturacao_o2", T.DoubleType()),
    T.StructField("temperatura", T.DoubleType()),
    T.StructField("frequencia_respiratoria", T.DoubleType()),
    T.StructField(CORROMPIDO, T.StringType()),
])

# Faixas fisiologicamente possíveis (fora delas o dado é erro de medição/transmissão)
FAIXAS_SINAIS = {
    "frequencia_cardiaca": (20, 250),
    "pressao_sistolica": (40, 280),
    "pressao_diastolica": (20, 180),
    "saturacao_o2": (40, 100),
    "temperatura": (30, 45),
    "frequencia_respiratoria": (4, 70),
}

# Critérios de alerta clínico (limiares usuais de deterioração): (alerta, campo, operador, limite)
ALERTAS = [
    ("HIPOXEMIA", "saturacao_o2", "<", 90),
    ("TAQUICARDIA", "frequencia_cardiaca", ">", 130),
    ("FEBRE_ALTA", "temperatura", ">=", 39),
    ("HIPOTENSAO", "pressao_sistolica", "<", 90),
]
_OPERADORES = {"<": lambda c, v: c < v, ">": lambda c, v: c > v, ">=": lambda c, v: c >= v}


# ------------------------------------------------------------------ validação
def _com_motivos(df: DataFrame, regras) -> DataFrame:
    """Adiciona a coluna 'motivos' (array) com as regras violadas por cada evento."""
    motivos = F.filter(F.array(*[F.when(cond, F.lit(msg)) for msg, cond in regras]), lambda x: x.isNotNull())
    return df.withColumn(
        "motivos",
        F.when(F.col(f"d.{CORROMPIDO}").isNotNull(), F.array(F.lit("JSON inválido ou campo com tipo incorreto")))
         .otherwise(motivos),
    )


def validar_admissoes(brutos: DataFrame) -> DataFrame:
    d = lambda c: F.col(f"d.{c}")  # noqa: E731
    df = brutos.withColumn("d", F.from_json("valor", ESQUEMA_ADMISSAO, {"columnNameOfCorruptRecord": CORROMPIDO}))
    regras = [
        ("evento_id ausente", d("evento_id").isNull()),
        ("internacao_id ausente", d("internacao_id").isNull()),
        ("tipo_evento inválido", ~F.coalesce(d("tipo_evento").isin("ADMISSAO", "ALTA", "TRANSFERENCIA"), F.lit(False))),
        ("ocorrido_em inválido", F.try_to_timestamp(d("ocorrido_em")).isNull()),
        ("CPF em formato inválido", ~F.coalesce(d("cpf").rlike(r"^\d{3}\.\d{3}\.\d{3}-\d{2}$"), F.lit(False))),
        ("data_nascimento inválida", ~F.coalesce(d("data_nascimento").rlike(r"^\d{4}-\d{2}-\d{2}$"), F.lit(False))),
        ("UF inválida", ~F.coalesce(d("uf").rlike(r"^[A-Z]{2}$"), F.lit(False))),
        ("setor inválido", ~F.coalesce(d("setor").isin("UTI", "ENFERMARIA", "PRONTO_SOCORRO"), F.lit(False))),
    ]
    return _com_motivos(df, regras)


def validar_sinais(brutos: DataFrame) -> DataFrame:
    d = lambda c: F.col(f"d.{c}")  # noqa: E731
    df = brutos.withColumn("d", F.from_json("valor", ESQUEMA_SINAIS, {"columnNameOfCorruptRecord": CORROMPIDO}))
    regras = [
        ("evento_id ausente", d("evento_id").isNull()),
        ("internacao_id ausente", d("internacao_id").isNull()),
        ("ocorrido_em inválido", F.try_to_timestamp(d("ocorrido_em")).isNull()),
    ] + [
        (f"{campo} fora da faixa [{lo}, {hi}]", ~F.coalesce(d(campo).between(lo, hi), F.lit(False)))
        for campo, (lo, hi) in FAIXAS_SINAIS.items()
    ]
    return _com_motivos(df, regras)


# ------------------------------------------------------------------ transformações
def _metadados():
    return [
        F.col("topico").alias("kafka_topico"),
        F.col("particao").alias("kafka_particao"),
        F.col("offset").alias("kafka_offset"),
        F.col("ingerido_em"),
    ]


def silver_admissoes(validos: DataFrame) -> DataFrame:
    """Pseudonimiza o paciente e suprime os dados de contato (LGPD: minimização)."""
    ocorrido = F.try_to_timestamp("d.ocorrido_em")
    nascimento = F.to_date("d.data_nascimento")
    return validos.select(
        F.col("d.evento_id").alias("evento_id"),
        F.col("d.tipo_evento").alias("tipo_evento"),
        ocorrido.alias("ocorrido_em"),
        F.to_date(ocorrido).alias("data_evento"),
        F.col("d.internacao_id").alias("internacao_id"),
        m.pseudonimizar(F.col("d.cpf")).alias("paciente_id"),        # CPF -> pseudônimo (HMAC)
        F.col("d.sexo").alias("sexo"),
        F.floor(F.months_between(F.to_date(ocorrido), nascimento) / 12).cast("int").alias("idade"),
        m.faixa_etaria(nascimento).alias("faixa_etaria"),            # generalização
        m.generalizar_cep(F.col("d.cep")).alias("cep_regiao"),        # generalização
        F.col("d.municipio").alias("municipio"),
        F.col("d.codigo_ibge").alias("codigo_ibge"),
        F.col("d.uf").alias("uf"),
        F.col("d.hospital_cnes").alias("hospital_cnes"),
        F.col("d.hospital_nome").alias("hospital_nome"),
        F.col("d.hospital_municipio").alias("hospital_municipio"),
        F.col("d.hospital_uf").alias("hospital_uf"),
        F.col("d.setor").alias("setor"),
        F.col("d.leito").alias("leito"),
        F.col("d.cid_principal").alias("cid_principal"),
        F.col("d.gravidade").alias("gravidade"),
        *_metadados(),
        # suprimidos: cpf, nome, data_nascimento, telefone, email, endereco
    )


def silver_sinais(validos: DataFrame) -> DataFrame:
    ocorrido = F.try_to_timestamp("d.ocorrido_em")
    return validos.select(
        F.col("d.evento_id").alias("evento_id"),
        F.col("d.internacao_id").alias("internacao_id"),
        ocorrido.alias("ocorrido_em"),
        F.to_date(ocorrido).alias("data_evento"),
        F.col("d.leito").alias("leito"),
        *[F.col(f"d.{c}").alias(c) for c in FAIXAS_SINAIS],
        *_metadados(),
    )


def estado_internacoes(admissoes: DataFrame) -> DataFrame:
    """Último evento de cada internação no lote (base para o MERGE do estado atual)."""
    ultimo = (admissoes
              .withColumn("_ordem", F.row_number().over(
                  Window.partitionBy("internacao_id").orderBy(F.col("ocorrido_em").desc())))
              .filter("_ordem = 1"))
    return ultimo.select(
        "internacao_id", "paciente_id", "sexo", "faixa_etaria", "uf", "municipio",
        "hospital_cnes", "hospital_nome", "hospital_uf", "setor", "leito", "cid_principal", "gravidade",
        F.when(F.col("tipo_evento") == "ALTA", "ENCERRADA").otherwise("ATIVA").alias("situacao"),
        F.col("ocorrido_em").alias("atualizado_em"),
    )


def derivar_alertas(sinais: DataFrame, estado: DataFrame, batch_id: int) -> DataFrame:
    """Um alerta por critério violado, enriquecido com o contexto da internação (sem dado pessoal)."""
    alertas = sinais.select(
        "evento_id", "internacao_id", "ocorrido_em",
        F.explode(F.filter(F.array(*[
            F.when(_OPERADORES[op](F.col(campo), limite),
                   F.struct(F.lit(nome).alias("tipo_alerta"), F.col(campo).cast("double").alias("valor")))
            for nome, campo, op, limite in ALERTAS
        ]), lambda x: x.isNotNull())).alias("alerta"),
    )
    return (alertas.join(estado, "internacao_id", "left")
            .select(
                "evento_id", "internacao_id", "ocorrido_em",
                F.col("alerta.tipo_alerta").alias("tipo_alerta"),
                F.col("alerta.valor").alias("valor"),
                "hospital_cnes", "hospital_nome", "hospital_uf", "setor", "faixa_etaria", "cid_principal",
                F.current_timestamp().alias("processado_em"),
                F.lit(batch_id).cast("long").alias("batch_id"),
            ))


def ocupacao(estado: DataFrame) -> DataFrame:
    return (estado.filter("situacao = 'ATIVA'")
            .groupBy("hospital_cnes", "hospital_nome", "hospital_uf", "setor")
            .agg(F.count("*").alias("internacoes_ativas"),
                 F.sum(F.when(F.col("gravidade") >= 4, 1).otherwise(0)).alias("pacientes_graves"))
            .withColumn("atualizado_em", F.current_timestamp()))


def mensagens_dlq(invalidos: DataFrame) -> DataFrame:
    """A DLQ leva o motivo e a REFERÊNCIA ao registro na bronze, nunca o dado pessoal."""
    return invalidos.select(
        F.col("chave").alias("key"),
        F.to_json(F.struct(
            F.array_join("motivos", "; ").alias("motivo"),
            F.col("topico").alias("topico_origem"),
            F.col("particao").alias("particao"),
            F.col("offset").alias("offset"),
            F.lit(BRONZE).alias("registro_bruto_em"),
            F.date_format("ingerido_em", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("recebido_em"),
        )).alias("value"),
    )


# ------------------------------------------------------------------ gravação
def _delta_append(df: DataFrame, caminho: str, tabela: str, batch_id: int, particao=None):
    w = (df.write.format("delta").mode("append")
         .option("txnAppId", f"{APP}.{tabela}").option("txnVersion", batch_id))
    if particao:
        w = w.partitionBy(particao)
    w.save(caminho)


def _jdbc(df: DataFrame, tabela: str, modo: str):
    (df.write.format("jdbc").mode(modo)
     .option("url", JDBC_URL).option("dbtable", tabela)
     .option("user", "dw_owner").option("password", os.environ["DW_DB_PASSWORD"])
     .option("truncate", "true")          # em overwrite, preserva a tabela e as permissões
     .save())


class Contadores:
    """Totais do último lote, enviados ao StatsD pelo listener."""
    validos = invalidos = alertas = 0


def processar_lote(lote: DataFrame, batch_id: int):
    spark = lote.sparkSession
    brutos = (lote.select(
        F.col("topic").alias("topico"), F.col("partition").alias("particao"), F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("chave"), F.col("value").cast("string").alias("valor"))
        .withColumn("ingerido_em", F.current_timestamp())
        .withColumn("data_ingestao", F.to_date("ingerido_em"))
        .withColumn("batch_id", F.lit(batch_id).cast("long"))
        .persist())

    # 1. Bronze: tudo o que chegou, sem transformação
    _delta_append(brutos, BRONZE, "bronze", batch_id, particao=["data_ingestao", "topico"])

    # 2. Validação
    adm = validar_admissoes(brutos.filter(F.col("topico") == TOPICO_ADMISSOES)).persist()
    sin = validar_sinais(brutos.filter(F.col("topico") == TOPICO_SINAIS)).persist()
    ok = lambda df: df.filter(F.size("motivos") == 0)  # noqa: E731
    nok = lambda df: df.filter(F.size("motivos") > 0)  # noqa: E731

    # 3. DLQ
    invalidos = nok(adm).unionByName(nok(sin), allowMissingColumns=True)
    Contadores.invalidos = invalidos.count()
    if Contadores.invalidos:
        mensagens_dlq(invalidos).write.format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA).option("topic", TOPICO_DLQ).save()

    # 4. Silver
    s_adm = silver_admissoes(ok(adm)).persist()
    s_sin = silver_sinais(ok(sin)).persist()
    _delta_append(s_adm, SILVER_ADMISSOES, "silver_admissoes", batch_id, particao=["data_evento"])
    _delta_append(s_sin, SILVER_SINAIS, "silver_sinais", batch_id, particao=["data_evento"])
    Contadores.validos = s_adm.count() + s_sin.count()

    # 5. Estado atual das internações (MERGE: só aplica eventos mais recentes)
    estado_lote = estado_internacoes(s_adm)
    estado_lote.createOrReplaceTempView("estado_lote")
    spark.sql(f"""
        MERGE INTO delta.`{SILVER_ESTADO}` AS alvo
        USING estado_lote AS novo
        ON alvo.internacao_id = novo.internacao_id
        WHEN MATCHED AND novo.atualizado_em >= alvo.atualizado_em THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    estado = spark.read.format("delta").load(SILVER_ESTADO).persist()

    # 6. Serving no DW (consumo em tempo real pelo BI)
    alertas = derivar_alertas(s_sin, estado, batch_id).persist()
    Contadores.alertas = alertas.count()
    if Contadores.alertas:
        _jdbc(alertas, "gold.alerta_clinico_rt", "append")
    _jdbc(ocupacao(estado), "gold.ocupacao_rt", "overwrite")

    for df in (brutos, adm, sin, s_adm, s_sin, estado, alertas):
        df.unpersist()


# ------------------------------------------------------------------ métricas
class MetricasStatsD(StreamingQueryListener):
    """Envia o progresso da query ao statsd-exporter (Prometheus).
    Necessário porque o Structured Streaming guarda offsets no checkpoint, e não
    no Kafka: o consumer lag tradicional não enxerga esta aplicação."""

    def __init__(self, host, porta=9125):
        self.destino = (host, porta)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def _enviar(self, linhas):
        try:
            self.sock.sendto("\n".join(linhas).encode(), self.destino)
        except OSError:
            pass  # observabilidade nunca derruba o pipeline

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        p = event.progress
        atraso = [s.metrics.get("avgOffsetsBehindLatest") for s in p.sources if s.metrics]
        atraso_max = [s.metrics.get("maxOffsetsBehindLatest") for s in p.sources if s.metrics]
        g = lambda nome, v: f"spark_streaming.{APP}.{nome}:{float(v or 0)}|g"  # noqa: E731
        c = lambda nome, v: f"{APP}.{nome}:{int(v or 0)}|c"  # noqa: E731
        self._enviar([
            g("input_rows_per_second", p.inputRowsPerSecond),
            g("processed_rows_per_second", p.processedRowsPerSecond),
            g("num_input_rows", p.numInputRows),
            g("batch_duration_ms", (p.durationMs or {}).get("triggerExecution")),
            g("batch_id", p.batchId),
            g("offsets_behind_latest_avg", sum(float(x) for x in atraso if x) if atraso else 0),
            g("offsets_behind_latest_max", max((float(x) for x in atraso_max if x), default=0)),
            c("eventos_validos", Contadores.validos),
            c("eventos_invalidos", Contadores.invalidos),
            c("alertas_clinicos", Contadores.alertas),
        ])

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        pass


# ------------------------------------------------------------------ execução
def preparar_tabelas(spark: SparkSession):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS delta.`{SILVER_ESTADO}` (
            internacao_id STRING, paciente_id STRING, sexo STRING, faixa_etaria STRING,
            uf STRING, municipio STRING, hospital_cnes STRING, hospital_nome STRING,
            hospital_uf STRING, setor STRING, leito STRING, cid_principal STRING,
            gravidade INT, situacao STRING, atualizado_em TIMESTAMP
        ) USING DELTA
    """)


def main():
    spark = SparkSession.builder.appName("speed-layer-eventos-saude").getOrCreate()
    # O módulo de mascaramento chega aos executores via --py-files (ver docker-compose.yml)
    preparar_tabelas(spark)
    spark.streams.addListener(MetricasStatsD(os.getenv("STATSD_HOST", "statsd-exporter")))

    fonte = (spark.readStream.format("kafka")
             .option("kafka.bootstrap.servers", KAFKA)
             .option("subscribe", f"{TOPICO_ADMISSOES},{TOPICO_SINAIS}")
             .option("startingOffsets", "earliest")
             .option("maxOffsetsPerTrigger", int(os.getenv("MAX_EVENTOS_POR_LOTE", "20000")))
             .option("failOnDataLoss", "false")
             .load())

    consulta = (fonte.writeStream
                .queryName(APP)
                .foreachBatch(processar_lote)
                .option("checkpointLocation", CHECKPOINT)
                .trigger(processingTime=os.getenv("INTERVALO_LOTE", "30 seconds"))
                .start())
    print(f"[{datetime.now(timezone.utc).isoformat()}] speed layer iniciada: {json.dumps({'id': str(consulta.id)})}",
          flush=True)
    consulta.awaitTermination()


if __name__ == "__main__":
    main()
