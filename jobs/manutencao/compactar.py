"""
Compactação das tabelas Delta alimentadas pelo streaming (manutenção manual).

Cada micro-batch grava arquivos pequenos. Com o tempo, as tabelas acumulam milhares
deles, e os lotes ficam mais lentos. O OPTIMIZE reúne esses arquivos em poucos
arquivos grandes. Na bronze do CDC, o ZORDER pelo identificador do titular permite
que a remoção de um titular eliminado leia só os arquivos que o contêm.

Uso: make lakehouse-compactar (a speed layer é parada antes e religada depois,
para que haja um único gravador em cada tabela durante a compactação).
"""
import time

from pyspark.sql import SparkSession

TABELAS = [
    ("s3a://bronze/eventos_saude", None),
    ("s3a://silver/admissoes", None),
    ("s3a://silver/sinais_vitais", None),
    ("s3a://bronze/prontuario_cdc", "entidade_id"),
]


def arquivos(spark, caminho):
    return spark.sql("DESCRIBE DETAIL delta.`%s`" % caminho).first()["numFiles"]


def main():
    spark = SparkSession.builder.appName("manutencao-compactar").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print("%-34s%12s%12s%12s" % ("tabela", "antes", "depois", "segundos"))
    for caminho, ordem in TABELAS:
        try:
            antes, inicio = arquivos(spark, caminho), time.time()
            comando = "OPTIMIZE delta.`%s`" % caminho
            if ordem:
                comando += " ZORDER BY (%s)" % ordem
            spark.sql(comando)
            print("%-34s%12d%12d%12.0f" % (caminho, antes, arquivos(spark, caminho), time.time() - inicio), flush=True)
        except Exception as erro:  # uma tabela com problema não impede as demais
            print("%-34s erro: %s" % (caminho, str(erro).splitlines()[0][:80]), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
