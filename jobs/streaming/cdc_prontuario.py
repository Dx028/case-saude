"""
CDC do prontuário: Debezium (Kafka) -> bronze -> silver (MERGE) -> serving no DW.

Cada evento do Debezium traz o estado anterior ("before"), o novo ("after"), a
posição no log de transações ("source.lsn") e a operação ("op"):
  r = leitura do snapshot inicial | c = inserção | u = atualização | d = exclusão

A silver é um ESPELHO pseudonimizado da origem: aplicam-se, por chave, apenas as
mudanças mais recentes (maior LSN). Uma exclusão na origem — como a eliminação de
um titular a pedido (LGPD, art. 18) — remove o registro da silver e também os
eventos com dados pessoais daquele titular na bronze.
"""
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

import mascaramento as m

QUERY = "cdc_prontuario"
TOPICO_PACIENTES = "prontuario.clinico.pacientes"
TOPICO_ATENDIMENTOS = "prontuario.clinico.atendimentos"
BRONZE = "s3a://bronze/prontuario_cdc"
SILVER_PACIENTES = "s3a://silver/prontuario_pacientes"
SILVER_ATENDIMENTOS = "s3a://silver/prontuario_atendimentos"
GERACAO = os.getenv("STREAMING_GERACAO", "1")
SUFIXO = "" if GERACAO == "1" else f".g{GERACAO}"
CHECKPOINT = f"s3a://bronze/_checkpoints/cdc_prontuario{SUFIXO}"

LINHA_PACIENTE = T.StructType([
    T.StructField("paciente_id", T.LongType()), T.StructField("cpf", T.StringType()),
    T.StructField("nome", T.StringType()), T.StructField("data_nascimento", T.IntegerType()),  # dias desde 1970
    T.StructField("sexo", T.StringType()), T.StructField("telefone", T.StringType()),
    T.StructField("email", T.StringType()), T.StructField("municipio_ibge6", T.StringType()),
    T.StructField("uf", T.StringType()), T.StructField("criado_em", T.StringType()),
    T.StructField("atualizado_em", T.StringType()),
])
LINHA_ATENDIMENTO = T.StructType([
    T.StructField("atendimento_id", T.LongType()), T.StructField("paciente_id", T.LongType()),
    T.StructField("tipo", T.StringType()), T.StructField("cid_principal", T.StringType()),
    T.StructField("status", T.StringType()), T.StructField("unidade_cnes", T.StringType()),
    T.StructField("iniciado_em", T.StringType()), T.StructField("encerrado_em", T.StringType()),
    T.StructField("atualizado_em", T.StringType()),
])


def envelope(linha: T.StructType) -> T.StructType:
    return T.StructType([
        T.StructField("before", linha), T.StructField("after", linha),
        T.StructField("source", T.StructType([T.StructField("lsn", T.LongType()),
                                              T.StructField("ts_ms", T.LongType()),
                                              T.StructField("snapshot", T.StringType())])),
        T.StructField("op", T.StringType()), T.StructField("ts_ms", T.LongType()),
    ])


def mudancas(brutos: DataFrame, linha: T.StructType, chave: str) -> DataFrame:
    """Última mudança de cada chave no lote (maior LSN; em empate, maior offset)."""
    e = brutos.withColumn("e", F.from_json("valor", envelope(linha)))
    ultima = Window.partitionBy("_id").orderBy(F.col("_lsn").desc_nulls_last(), F.col("offset").desc())
    return (e.select("offset",
                     F.coalesce(F.col(f"e.after.{chave}"), F.col(f"e.before.{chave}")).alias("_id"),
                     F.col("e.op").alias("_op"), F.col("e.source.lsn").alias("_lsn"),
                     F.col("e.after").alias("a"))
             .where(F.col("_id").isNotNull())
             .withColumn("_ordem", F.row_number().over(ultima)).where("_ordem = 1").drop("_ordem"))


def _ts(coluna):
    return F.try_to_timestamp(F.col(coluna))


def silver_pacientes(mud: DataFrame) -> DataFrame:
    """Minimização: nome, CPF, contato e data de nascimento não entram na silver."""
    nascimento = F.date_add(F.lit("1970-01-01").cast("date"), F.col("a.data_nascimento"))
    return mud.select(
        F.col("_id").alias("paciente_id"),
        m.pseudonimizar(F.col("a.cpf")).alias("paciente_pseudo"),  # mesmo pseudônimo da speed layer
        F.col("a.sexo").alias("sexo"),
        m.faixa_etaria(nascimento).alias("faixa_etaria"),
        F.col("a.municipio_ibge6").alias("municipio_ibge6"),
        F.col("a.uf").alias("uf"),
        _ts("a.criado_em").alias("criado_em"),
        _ts("a.atualizado_em").alias("atualizado_em"),
        "_op", "_lsn", F.current_timestamp().alias("_processado_em"),
    )


def silver_atendimentos(mud: DataFrame) -> DataFrame:
    return mud.select(
        F.col("_id").alias("atendimento_id"),
        F.col("a.paciente_id").alias("paciente_id"),
        F.col("a.tipo").alias("tipo"), F.col("a.cid_principal").alias("cid_principal"),
        F.col("a.status").alias("status"), F.col("a.unidade_cnes").alias("unidade_cnes"),
        _ts("a.iniciado_em").alias("iniciado_em"), _ts("a.encerrado_em").alias("encerrado_em"),
        _ts("a.atualizado_em").alias("atualizado_em"),
        "_op", "_lsn", F.current_timestamp().alias("_processado_em"),
    )


DDL = {
    SILVER_PACIENTES: """paciente_id BIGINT, paciente_pseudo STRING, sexo STRING, faixa_etaria STRING,
                         municipio_ibge6 STRING, uf STRING, criado_em TIMESTAMP, atualizado_em TIMESTAMP,
                         _op STRING, _lsn BIGINT, _processado_em TIMESTAMP""",
    SILVER_ATENDIMENTOS: """atendimento_id BIGINT, paciente_id BIGINT, tipo STRING, cid_principal STRING,
                            status STRING, unidade_cnes STRING, iniciado_em TIMESTAMP, encerrado_em TIMESTAMP,
                            atualizado_em TIMESTAMP, _op STRING, _lsn BIGINT, _processado_em TIMESTAMP""",
}


def _merge(spark, df: DataFrame, caminho: str, chave: str, visao: str):
    df.createOrReplaceTempView(visao)
    spark.sql(f"""
        MERGE INTO delta.`{caminho}` AS alvo
        USING {visao} AS novo
        ON alvo.{chave} = novo.{chave}
        WHEN MATCHED AND novo._op = 'd' THEN DELETE
        WHEN MATCHED AND novo._lsn >= alvo._lsn THEN UPDATE SET *
        WHEN NOT MATCHED AND novo._op <> 'd' THEN INSERT *
    """)


class Estado:
    """Total acumulado de titulares eliminados, carregado do DW na primeira vez."""
    eliminados = None


def processar_lote(lote: DataFrame, batch_id: int, jdbc_gravar, metricas: dict, jdbc_ler=None):
    spark = lote.sparkSession
    brutos = (lote.select(
        F.col("topic").alias("topico"), F.col("partition").alias("particao"), "offset",
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("chave"), F.col("value").cast("string").alias("valor"))
        .where(F.col("valor").isNotNull())
        .withColumn("operacao", F.get_json_object("valor", "$.op"))
        .withColumn("entidade_id", F.coalesce(F.get_json_object("chave", "$.paciente_id"),
                                              F.get_json_object("chave", "$.atendimento_id")).cast("long"))
        .withColumn("ingerido_em", F.current_timestamp())
        .withColumn("data_ingestao", F.to_date("ingerido_em"))
        .withColumn("batch_id", F.lit(batch_id).cast("long"))
        .persist())
    total = brutos.count()
    if total == 0:
        brutos.unpersist()
        return

    # 1. Bronze: eventos brutos do Debezium (idempotente por micro-batch)
    (brutos.write.format("delta").mode("append").partitionBy("data_ingestao", "topico")
     .option("txnAppId", f"{QUERY}.bronze{SUFIXO}").option("txnVersion", batch_id).save(BRONZE))

    # 2. Silver: espelho pseudonimizado, aplicando só a mudança mais recente de cada chave
    pac = brutos.where(F.col("topico") == TOPICO_PACIENTES)
    ate = brutos.where(F.col("topico") == TOPICO_ATENDIMENTOS)
    mud_pac = mudancas(pac, LINHA_PACIENTE, "paciente_id").persist()
    if mud_pac.take(1):
        _merge(spark, silver_pacientes(mud_pac), SILVER_PACIENTES, "paciente_id", "cdc_pacientes_lote")
    if ate.take(1):
        _merge(spark, silver_atendimentos(mudancas(ate, LINHA_ATENDIMENTO, "atendimento_id")),
               SILVER_ATENDIMENTOS, "atendimento_id", "cdc_atendimentos_lote")

    # 3. Direito de eliminação: remove da bronze os eventos com dados pessoais dos titulares excluídos
    #    (o evento de exclusão fica, pois só contém a chave). A remoção física ocorre no VACUUM.
    eliminados = [r._id for r in mud_pac.where("_op = 'd'").select("_id").collect()]
    if eliminados:
        ids = ",".join(str(int(i)) for i in eliminados)
        spark.sql(f"""DELETE FROM delta.`{BRONZE}` WHERE topico = '{TOPICO_PACIENTES}'
                      AND entidade_id IN ({ids}) AND operacao <> 'd'""")

    # 4. Serving no DW: situação atual do espelho (sem dados pessoais)
    atend = spark.read.format("delta").load(SILVER_ATENDIMENTOS)
    jdbc_gravar(atend.groupBy("tipo", "status").agg(F.count("*").alias("total"))
                .withColumn("atualizado_em", F.current_timestamp()), "gold.cdc_atendimentos")
    pacientes = spark.read.format("delta").load(SILVER_PACIENTES).count()
    # Contagem incremental: total anterior (do DW) + eliminações deste lote. Reler a bronze a cada
    # lote deixaria o processamento mais lento conforme ela cresce.
    if Estado.eliminados is None:
        anterior = jdbc_ler("SELECT COALESCE(max(eliminados), 0) AS n FROM gold.cdc_pacientes").first() if jdbc_ler else None
        Estado.eliminados = int(anterior.n) if anterior else 0
    Estado.eliminados += len(eliminados)
    jdbc_gravar(spark.createDataFrame([(pacientes, Estado.eliminados)], "pacientes long, eliminados long")
                .withColumn("atualizado_em", F.current_timestamp()), "gold.cdc_pacientes")

    metricas.update({"mudancas": total, "eliminacoes": len(eliminados)})
    mud_pac.unpersist(); brutos.unpersist()


def iniciar(spark, kafka, jdbc_gravar, metricas: dict, jdbc_ler=None):
    for caminho, colunas in DDL.items():
        spark.sql(f"CREATE TABLE IF NOT EXISTS delta.`{caminho}` ({colunas}) USING DELTA")
    fonte = (spark.readStream.format("kafka")
             .option("kafka.bootstrap.servers", kafka)
             .option("subscribePattern", r"prontuario\.clinico\..*")   # tópicos criados pelo Debezium
             .option("startingOffsets", "earliest")
             .option("maxOffsetsPerTrigger", int(os.getenv("CDC_MAX_EVENTOS_POR_LOTE", "3000")))
             .option("failOnDataLoss", "false")
             .load())
    return (fonte.writeStream.queryName(QUERY)
            .foreachBatch(lambda lote, bid: processar_lote(lote, bid, jdbc_gravar, metricas, jdbc_ler))
            .option("checkpointLocation", CHECKPOINT)
            .trigger(processingTime=os.getenv("CDC_INTERVALO_LOTE", "60 seconds"))
            .start())
