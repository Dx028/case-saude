"""
Demonstração das técnicas de mascaramento sobre pacientes sintéticos.
Mostra o dado original (bronze), o pseudonimizado (silver) e o
anonimizado/agregado (gold), e verifica o k-anonimato da saída.

Uso: make mascaramento-demo
"""
import os
import sys
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import mascaramento as m  # noqa: E402

spark = SparkSession.builder.appName("demo-mascaramento").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.sparkContext.addPyFile(os.path.join(os.path.dirname(os.path.abspath(__file__)), "mascaramento.py"))

pacientes = spark.createDataFrame(
    [
        ("Maria Aparecida Souza", "123.456.789-01", date(1958, 3, 14), "(11) 98765-4321", "maria.souza@email.com", "01310-100", "São Paulo", "SP", "J18"),
        ("João Pedro Oliveira", "234.567.890-12", date(1990, 7, 22), "(21) 99876-5432", "joao.pedro@email.com", "20040-002", "Rio de Janeiro", "RJ", "J11"),
        ("Ana Beatriz Lima", "345.678.901-23", date(2015, 11, 2), "(31) 98765-1234", "ana.lima@email.com", "30130-010", "Belo Horizonte", "MG", "J06"),
        ("Carlos Eduardo Santos", "456.789.012-34", date(1975, 1, 30), "(41) 99654-1234", "carlos.santos@email.com", "80010-000", "Curitiba", "PR", "U07"),
        ("Francisca das Chagas", "567.890.123-45", date(1943, 9, 9), "(85) 99123-4567", "francisca@email.com", "60060-000", "Fortaleza", "CE", "J18"),
        ("Maria Aparecida Souza", "123.456.789-01", date(1958, 3, 14), "(11) 98765-4321", "maria.souza@email.com", "01310-100", "São Paulo", "SP", "J11"),
    ],
    ["nome", "cpf", "data_nascimento", "telefone", "email", "cep", "municipio", "uf", "cid"],
)

print("\n=== BRONZE: dado bruto (acesso restrito) ===")
pacientes.show(truncate=False)

silver = pacientes.select(
    m.pseudonimizar(F.col("cpf")).alias("paciente_id"),  # pseudônimo estável
    m.mascarar_nome(F.col("nome")).alias("nome"),
    m.mascarar_cpf(F.col("cpf")).alias("cpf"),
    m.faixa_etaria(F.col("data_nascimento")).alias("faixa_etaria"),
    m.mascarar_telefone(F.col("telefone")).alias("telefone"),
    m.mascarar_email(F.col("email")).alias("email"),
    m.generalizar_cep(F.col("cep")).alias("cep"),
    "municipio", "uf", "cid",
)
print("=== SILVER: pseudonimizado e mascarado ===")
print("(o mesmo CPF gera o mesmo paciente_id: as duas internações da Maria continuam ligadas)")
silver.show(truncate=18)

gold = silver.groupBy("uf", "faixa_etaria", "cid").agg(F.count("*").alias("internacoes"))
print("=== GOLD: agregado e anonimizado (sem identificadores) ===")
gold.orderBy("uf").show(truncate=False)

print("=== Verificação de k-anonimato (k=5) sobre UF + faixa etária ===")
riscos = m.verificar_k_anonimato(silver, ["uf", "faixa_etaria"], k=5)
print(f"{riscos.count()} grupo(s) com menos de 5 pessoas: risco de reidentificação;")
print("na publicação real, esses grupos seriam suprimidos ou generalizados (ex.: região em vez de UF).")
riscos.show(truncate=False)

spark.stop()
