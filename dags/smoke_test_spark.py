"""
Smoke test da integração Airflow -> Spark.

Executa o mesmo job de verificação de conexões (Silo/Delta, Kafka e
PostgreSQL) submetido pelo Airflow ao cluster Spark.
"""
from datetime import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import dag


@dag(
    dag_id="smoke_test_spark",
    description="Valida a integração Airflow -> Spark -> Silo/Kafka/Postgres",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["infra", "smoke-test"],
)
def smoke_test_spark():
    SparkSubmitOperator(
        task_id="spark_smoke_test",
        conn_id="spark_default",
        application="/opt/jobs/common/smoke_test.py",
        name="airflow-smoke-test",
        conf={"spark.executor.memory": "1g"},
    )


smoke_test_spark()
