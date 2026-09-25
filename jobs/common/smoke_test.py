"""
Smoke test das conexões do Spark:
  1. Silo (S3A) + Delta Lake: escrita, leitura e histórico (time travel)
  2. Kafka: leitura em lote de um tópico
  3. PostgreSQL: consulta via JDBC no DW

Uso: make spark-smoke
"""
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("smoke-test-conexoes").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

resultados = {}


def checar(nome, fn):
    try:
        detalhe = fn()
        resultados[nome] = ("OK", detalhe)
    except Exception as exc:  # noqa: BLE001 - queremos reportar qualquer falha
        resultados[nome] = ("ERRO", " | ".join([l.strip() for l in str(exc).splitlines() if "Exception" in l or "Caused by" in l][:4])[:600] or str(exc)[:300])


# 1. Lakehouse: Delta Lake no Silo --------------------------------------
def teste_delta():
    caminho = "s3a://bronze/_smoke/teste_delta"
    df = spark.range(1000).withColumn("gerado_em", F.lit(datetime.now().isoformat()))
    df.write.format("delta").mode("overwrite").save(caminho)
    total = spark.read.format("delta").load(caminho).count()
    versoes = spark.sql(f"DESCRIBE HISTORY delta.`{caminho}`").count()
    assert total == 1000, f"esperava 1000 linhas, li {total}"
    return f"{total} linhas gravadas/lidas em {caminho} ({versoes} versão(ões) no histórico)"


# 2. Kafka ----------------------------------------------------------------
def teste_kafka():
    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "saude.eventos.dlq")
        .option("startingOffsets", "earliest")
        .load()
    )
    return f"{df.count()} mensagem(ns) lida(s) do tópico saude.eventos.dlq"


# 3. PostgreSQL (DW) via JDBC ---------------------------------------------
def teste_postgres():
    df = (
        spark.read.format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/dw?sslmode=require")
        .option("user", "dw_owner")
        .option("password", os.environ["DW_DB_PASSWORD"])
        .option("query", "SELECT current_database() AS banco, current_user AS usuario, (SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()) AS tls")
        .load()
    )
    linha = df.first()
    return f"conectado ao banco '{linha.banco}' como '{linha.usuario}' (TLS: {'sim' if linha.tls else 'não'})"


checar("Delta Lake no Silo (S3A)", teste_delta)
checar("Kafka", teste_kafka)
checar("PostgreSQL (JDBC)", teste_postgres)

workers = spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1

print("\n" + "=" * 70)
print(f" Smoke test do Spark {spark.version} | executores ativos: {workers}")
print("=" * 70)
for nome, (status, detalhe) in resultados.items():
    print(f" [{status:4}] {nome}: {detalhe}")
print("=" * 70 + "\n")

spark.stop()
sys.exit(0 if all(s == "OK" for s, _ in resultados.values()) else 1)
