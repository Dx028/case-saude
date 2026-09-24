# Imagem Airflow do projeto: Airflow oficial + cliente Spark completo
# O driver dos jobs roda no Airflow (client mode) e os executores no cluster.
# Python 3.10 = mesma versão dos workers Spark (exigência do PySpark).
# (build context: ./docker)
ARG SPARK_IMAGE=spark:4.1.2-scala2.13-java17-python3-ubuntu
ARG AIRFLOW_IMAGE=apache/airflow:3.3.2-python3.10

FROM ${SPARK_IMAGE} AS spark

FROM ${AIRFLOW_IMAGE}

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless procps \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

# Distribuição do Spark copiada da imagem oficial (mesma versão do cluster)
COPY --from=spark /opt/spark /opt/spark
ENV SPARK_HOME=/opt/spark \
    JAVA_HOME=/usr/lib/jvm/default-java \
    PYSPARK_PYTHON=python3
ENV PATH="${PATH}:/opt/spark/bin"

# Mesmos conectores e configurações do cluster
COPY spark/install-spark-extras.sh spark/resolve-jars.py /tmp/build/
RUN bash /tmp/build/install-spark-extras.sh && rm -rf /tmp/build /root/.ivy2*
COPY spark/spark-defaults.conf /opt/spark/conf/

USER airflow
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      "apache-airflow-providers-apache-spark==6.3.2" \
      "pyspark-client==4.1.2" \
      "statsd"
