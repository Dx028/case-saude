"""
Batch layer — SRAG: landing (CSV) -> bronze (Delta) -> silver (Delta), com portão de qualidade.

Processa os arquivos registrados com status 'baixado' na tabela de controle
(auditoria.controle_ingestao). Cada ano é uma partição: reprocessar um arquivo
substitui apenas a partição daquele ano (replaceWhere), o que torna a carga
idempotente. Se uma regra CRÍTICA de qualidade falhar, a silver daquele ano
não é publicada e o job termina com erro (o arquivo fica pendente).
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import srag_transformacoes as t

BRONZE = "s3a://bronze/srag"
SILVER = "s3a://silver/srag"
JDBC = {"url": "jdbc:postgresql://postgres:5432/dw?sslmode=require",
        "user": "dw_owner", "password": os.environ.get("DW_DB_PASSWORD", "")}


def jdbc_ler(spark, consulta):
    return spark.read.format("jdbc").options(**JDBC).option("query", consulta).load()


def jdbc_gravar(df, tabela):
    df.write.format("jdbc").options(**JDBC).option("dbtable", tabela).mode("append").save()


def main():
    spark = SparkSession.builder.appName("batch-srag-bronze-silver").getOrCreate()
    execucao = spark.sparkContext.applicationId

    pendentes = jdbc_ler(spark, """
        SELECT ano, arquivo, chave_landing FROM auditoria.controle_ingestao
        WHERE fonte = 'srag' AND status = 'baixado' ORDER BY ano""").collect()
    if not pendentes:
        print("[batch-srag] nenhum arquivo pendente")
        return 0

    reprovados = []
    for p in pendentes:
        print(f"[batch-srag] {p.ano}: lendo s3a://landing/{p.chave_landing}")
        csv = (spark.read
               .option("header", True).option("sep", ";").option("quote", '"').option("escape", '"')
               .option("encoding", "UTF-8").option("mode", "PERMISSIVE")
               .csv(f"s3a://landing/{p.chave_landing}"))          # todas as colunas como texto
        bronze = (csv.withColumn("ano", F.lit(p.ano).cast("int"))
                     .withColumn("_arquivo", F.lit(p.arquivo))
                     .withColumn("_ingerido_em", F.current_timestamp()))

        # BRONZE: dado bruto, substituindo apenas a partição do ano
        (bronze.write.format("delta").mode("overwrite").partitionBy("ano")
         .option("replaceWhere", f"ano = {p.ano}").option("mergeSchema", "true").save(BRONZE))
        bronze = spark.read.format("delta").load(BRONZE).where(F.col("ano") == p.ano).persist()

        silver = t.para_silver(bronze).persist()
        regras = t.verificar_silver(bronze, silver, p.ano)
        resultado = spark.createDataFrame(
            [(execucao, "silver", "silver.srag", p.ano, r, sev, float(v), float(lim), bool(ok))
             for r, sev, v, lim, ok in regras],
            "execucao string, camada string, tabela string, ano int, regra string, severidade string, "
            "valor double, limite double, aprovado boolean")
        jdbc_gravar(resultado.withColumn("verificado_em", F.current_timestamp()), "qualidade.verificacao")
        for r, sev, v, lim, ok in regras:
            print(f"[qualidade] {p.ano} {'OK   ' if ok else 'FALHA'} [{sev}] {r}: {v} (limite {lim})")

        criticas = [r for r, sev, v, lim, ok in regras if sev == "critica" and not ok]
        if criticas:
            reprovados.append((p.ano, criticas))
        else:
            # SILVER: publicada somente se as regras críticas passaram (portão de qualidade)
            (silver.write.format("delta").mode("overwrite").partitionBy("ano")
             .option("replaceWhere", f"ano = {p.ano}").option("mergeSchema", "true").save(SILVER))
            contagem = spark.createDataFrame(
                [("srag", p.ano, p.arquivo, bronze.count(), silver.count(), execucao)],
                "fonte string, ano int, arquivo string, linhas_bronze long, linhas_silver long, execucao string")
            jdbc_gravar(contagem.withColumn("processado_em", F.current_timestamp()), "auditoria.resultado_carga")
            print(f"[batch-srag] {p.ano}: silver publicada")
        bronze.unpersist(); silver.unpersist()

    if reprovados:
        print(f"[batch-srag] silver NÃO publicada por falha crítica de qualidade: {reprovados}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
