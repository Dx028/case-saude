# Imagem Spark do projeto: Spark oficial + Delta Lake + conectores
# (build context: ./docker)
ARG SPARK_IMAGE=spark:4.1.2-scala2.13-java17-python3-ubuntu
FROM ${SPARK_IMAGE}

USER root

COPY spark/install-spark-extras.sh spark/resolve-jars.py /tmp/build/
RUN bash /tmp/build/install-spark-extras.sh && rm -rf /tmp/build /root/.ivy2*

COPY spark/spark-defaults.conf spark/metrics.properties /opt/spark/conf/

RUN mkdir -p /opt/jobs && chown -R spark:spark /opt/spark/work-dir /opt/jobs

USER spark
WORKDIR /opt/spark/work-dir
