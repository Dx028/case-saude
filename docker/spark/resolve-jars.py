# Usado apenas no build: força o download das dependências via --packages
from pyspark.sql import SparkSession

SparkSession.builder.appName("resolve-jars").getOrCreate().stop()
