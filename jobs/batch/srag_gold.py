"""
Batch layer — SRAG: silver -> gold (DW PostgreSQL).

Recalcula a fato semanal a partir de TODA a silver (princípio da arquitetura
Lambda: a camada batch recomputa as visões a partir do conjunto mestre) e
verifica a integridade referencial com a dimensão de municípios (IBGE).
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import srag_transformacoes as t

SILVER = "s3a://silver/srag"
JDBC = {"url": "jdbc:postgresql://postgres:5432/dw?sslmode=require",
        "user": "dw_owner", "password": os.environ.get("DW_DB_PASSWORD", "")}


def main():
    spark = SparkSession.builder.appName("batch-srag-gold").getOrCreate()
    execucao = spark.sparkContext.applicationId

    fato = t.para_fato_semanal(spark.read.format("delta").load(SILVER)).persist()
    (fato.write.format("jdbc").options(**JDBC).option("dbtable", "gold.fato_srag_semanal")
     .option("truncate", "true").mode("overwrite").save())

    # Integridade referencial: casos com município de residência fora do cadastro do IBGE
    municipios = (spark.read.format("jdbc").options(**JDBC)
                  .option("query", "SELECT codigo_ibge6 FROM gold.dim_municipio").load())
    total = fato.agg(F.sum("casos")).first()[0] or 0
    orfaos = (fato.join(municipios, fato.municipio_residencia_ibge6 == municipios.codigo_ibge6, "left_anti")
                  .agg(F.sum("casos")).first()[0] or 0)
    pct = round(100.0 * orfaos / total, 3) if total else 0.0
    regras = [
        ("gold: fato com registros", "critica", float(fato.count()), 1.0, fato.count() > 0),
        ("gold: % de casos sem município no cadastro do IBGE", "alerta", pct, 2.0, pct <= 2.0),
    ]
    (spark.createDataFrame(
        [(execucao, "gold", "gold.fato_srag_semanal", None, r, sev, float(v), float(lim), bool(ok))
         for r, sev, v, lim, ok in regras],
        "execucao string, camada string, tabela string, ano int, regra string, severidade string, "
        "valor double, limite double, aprovado boolean")
     .withColumn("verificado_em", F.current_timestamp())
     .write.format("jdbc").options(**JDBC).option("dbtable", "qualidade.verificacao").mode("append").save())
    for r, sev, v, lim, ok in regras:
        print(f"[qualidade] {'OK   ' if ok else 'FALHA'} [{sev}] {r}: {v} (limite {lim})")
    print(f"[batch-srag] gold atualizada: {fato.count()} linhas agregadas, {total} casos")
    return 1 if any(sev == "critica" and not ok for _, sev, _, _, ok in regras) else 0


if __name__ == "__main__":
    sys.exit(main())
