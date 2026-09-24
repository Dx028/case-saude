#!/usr/bin/env bash
# Instala no $SPARK_HOME/jars os conectores do projeto: Delta Lake, S3A
# (hadoop-aws), Kafka e JDBC do PostgreSQL. Usado pelas imagens do Spark
# e do Airflow, garantindo as MESMAS versões no driver e nos executores.
set -euo pipefail

DELTA_VERSION="${DELTA_VERSION:-4.4.0}"
DELTA_ARTIFACT="${DELTA_ARTIFACT:-delta-spark_4.1_2.13}"
POSTGRES_JDBC_VERSION="${POSTGRES_JDBC_VERSION:-42.7.4}"
SPARK_HOME="${SPARK_HOME:-/opt/spark}"
HERE="$(cd "$(dirname "$0")" && pwd)"

# Versões lidas dos JARs instalados: hadoop-aws e spark-sql-kafka
# precisam bater exatamente com o Hadoop e o Spark da imagem
SPARK_VER=$(ls "$SPARK_HOME/jars" | sed -n 's/^spark-core_2\.13-\(.*\)\.jar$/\1/p')
HADOOP_VER=$(ls "$SPARK_HOME/jars" | sed -n 's/^hadoop-client-api-\(.*\)\.jar$/\1/p')
echo "[extras] Spark ${SPARK_VER} | Hadoop ${HADOOP_VER} | Delta ${DELTA_VERSION}"

PKGS="io.delta:${DELTA_ARTIFACT}:${DELTA_VERSION}"
PKGS+=",org.apache.spark:spark-sql-kafka-0-10_2.13:${SPARK_VER}"
PKGS+=",org.apache.hadoop:hadoop-aws:${HADOOP_VER}"
PKGS+=",org.postgresql:postgresql:${POSTGRES_JDBC_VERSION}"

"$SPARK_HOME/bin/spark-submit" --master 'local[1]' \
  --conf spark.jars.ivy=/tmp/ivy --packages "$PKGS" "$HERE/resolve-jars.py"

cp -n /tmp/ivy/jars/*.jar "$SPARK_HOME/jars/"
chmod 644 "$SPARK_HOME"/jars/*.jar
rm -rf /tmp/ivy
echo "[extras] JARs instalados em $SPARK_HOME/jars"
