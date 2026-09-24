# Imagem Spark do projeto: Spark oficial + Delta Lake + conectores
# (S3A para o Silo, Kafka e JDBC do PostgreSQL)
ARG SPARK_IMAGE=spark:4.1.2-scala2.13-java17-python3-ubuntu
FROM ${SPARK_IMAGE}

USER root

ARG DELTA_VERSION=4.4.0
ARG DELTA_ARTIFACT=delta-spark_4.1_2.13
ARG POSTGRES_JDBC_VERSION=42.7.4

COPY resolve-jars.py /tmp/resolve-jars.py

# As versões do Spark e do Hadoop são lidas dos próprios JARs da imagem,
# garantindo que hadoop-aws e spark-sql-kafka sejam sempre compatíveis.
# O Ivy resolve as dependências transitivas e os JARs são copiados para
# /opt/spark/jars — assim nada é baixado em tempo de execução.
RUN set -eux; \
    SPARK_VER=$(ls /opt/spark/jars | sed -n 's/^spark-core_2\.13-\(.*\)\.jar$/\1/p'); \
    HADOOP_VER=$(ls /opt/spark/jars | sed -n 's/^hadoop-client-api-\(.*\)\.jar$/\1/p'); \
    echo "Spark ${SPARK_VER} | Hadoop ${HADOOP_VER}"; \
    PKGS="io.delta:${DELTA_ARTIFACT}:${DELTA_VERSION},org.apache.spark:spark-sql-kafka-0-10_2.13:${SPARK_VER},org.apache.hadoop:hadoop-aws:${HADOOP_VER},org.postgresql:postgresql:${POSTGRES_JDBC_VERSION}"; \
    /opt/spark/bin/spark-submit --master 'local[1]' \
        --conf spark.jars.ivy=/tmp/ivy --packages "${PKGS}" /tmp/resolve-jars.py; \
    cp -n /tmp/ivy/jars/*.jar /opt/spark/jars/; \
    chmod 644 /opt/spark/jars/*.jar; \
    rm -rf /tmp/ivy /tmp/resolve-jars.py /root/.ivy2*

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY metrics.properties /opt/spark/conf/metrics.properties

RUN mkdir -p /opt/jobs && chown -R spark:spark /opt/spark/work-dir /opt/jobs

USER spark
WORKDIR /opt/spark/work-dir
