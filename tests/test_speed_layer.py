"""
Testes das transformações da speed layer e das regras de mascaramento.
Rodam com um Spark local, sem Kafka, Silo ou PostgreSQL.

Uso: make test
"""
import json
import os
import sys
import unittest
from datetime import date

RAIZ = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "jobs")
sys.path.insert(0, os.path.join(RAIZ, "streaming"))
sys.path.insert(0, os.path.join(RAIZ, "common"))
os.environ.setdefault("PII_HMAC_KEY", "chave-de-teste")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

import mascaramento as m  # noqa: E402
import speed_layer as sl  # noqa: E402

ADMISSAO = {
    "evento_id": "e1", "tipo_evento": "ADMISSAO", "ocorrido_em": "2026-09-24T12:00:00.000+00:00",
    "internacao_id": "i1", "cpf": "123.456.789-01", "nome": "Maria Souza", "data_nascimento": "1958-03-14",
    "sexo": "F", "telefone": "(11) 98765-4321", "email": "maria@email.com", "endereco": "Rua A, 10",
    "cep": "01310-100", "municipio": "São Paulo", "codigo_ibge": "3550308", "uf": "SP",
    "hospital_cnes": "1234567", "hospital_nome": "Hospital Municipal de São Paulo",
    "hospital_municipio": "São Paulo", "hospital_uf": "SP", "setor": "UTI", "leito": "UTI-01",
    "cid_principal": "J18", "gravidade": 4,
}
SINAL = {
    "evento_id": "s1", "internacao_id": "i1", "ocorrido_em": "2026-09-24T12:00:10.000+00:00", "leito": "UTI-01",
    "frequencia_cardiaca": 142, "pressao_sistolica": 85, "pressao_diastolica": 60,
    "saturacao_o2": 88.5, "temperatura": 38.2, "frequencia_respiratoria": 26,
}


def linha(topico, valor, offset=0):
    return (topico, 0, offset, None, "i1", valor if isinstance(valor, str) else json.dumps(valor, ensure_ascii=False))


class TesteSpeedLayer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]").appName("testes")
                     .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.sparkContext.addPyFile(os.path.join(RAIZ, "common", "mascaramento.py"))

    def brutos(self, linhas):
        return (self.spark.createDataFrame(
            linhas, "topico string, particao int, offset long, kafka_timestamp timestamp, chave string, valor string")
            .withColumn("ingerido_em", F.current_timestamp()))

    # ---------------------------------------------------------- validação
    def test_admissao_valida(self):
        df = sl.validar_admissoes(self.brutos([linha(sl.TOPICO_ADMISSOES, ADMISSAO)]))
        self.assertEqual(df.first().motivos, [])

    def test_admissao_invalida_aponta_motivos(self):
        ruim = dict(ADMISSAO, cpf="000.000.000-00x", setor="COZINHA")
        ruim.pop("internacao_id")
        motivos = sl.validar_admissoes(self.brutos([linha(sl.TOPICO_ADMISSOES, ruim)])).first().motivos
        self.assertIn("internacao_id ausente", motivos)
        self.assertIn("CPF em formato inválido", motivos)
        self.assertIn("setor inválido", motivos)

    def test_json_truncado_e_tipo_errado(self):
        casos = [json.dumps(SINAL)[:-7], json.dumps(dict(SINAL, saturacao_o2="alta"))]
        df = sl.validar_sinais(self.brutos([linha(sl.TOPICO_SINAIS, c, i) for i, c in enumerate(casos)]))
        for r in df.collect():
            self.assertEqual(r.motivos, ["JSON inválido ou campo com tipo incorreto"])

    def test_sinal_fora_da_faixa(self):
        df = sl.validar_sinais(self.brutos([linha(sl.TOPICO_SINAIS, dict(SINAL, saturacao_o2=250.0))]))
        self.assertEqual(df.first().motivos, ["saturacao_o2 fora da faixa [40, 100]"])

    # ---------------------------------------------------------- silver (LGPD)
    def test_silver_admissoes_sem_dado_pessoal(self):
        validos = sl.validar_admissoes(self.brutos([linha(sl.TOPICO_ADMISSOES, ADMISSAO)]))
        silver = sl.silver_admissoes(validos)
        for proibida in ("cpf", "nome", "data_nascimento", "telefone", "email", "endereco"):
            self.assertNotIn(proibida, silver.columns)
        r = silver.first()
        self.assertEqual(r.paciente_id, m.pseudonimizar_py("123.456.789-01", "chave-de-teste"))
        self.assertEqual(r.cep_regiao, "01310-***")
        self.assertEqual(r.idade, 68)
        self.assertNotIn("Maria", json.dumps(r.asDict(), default=str))

    def test_pseudonimo_estavel_para_o_mesmo_paciente(self):
        a2 = dict(ADMISSAO, evento_id="e2", internacao_id="i2", cpf="12345678901")  # outro formato, mesmo CPF
        ids = {r.paciente_id for r in sl.silver_admissoes(sl.validar_admissoes(self.brutos(
            [linha(sl.TOPICO_ADMISSOES, ADMISSAO, 0), linha(sl.TOPICO_ADMISSOES, dict(a2, cpf="123.456.789-01"), 1)]
        ))).collect()}
        self.assertEqual(len(ids), 1)

    # ---------------------------------------------------------- estado, alertas, ocupação
    def test_estado_considera_o_evento_mais_recente(self):
        alta = dict(ADMISSAO, evento_id="e9", tipo_evento="ALTA", ocorrido_em="2026-09-24T18:00:00.000+00:00")
        adm = sl.silver_admissoes(sl.validar_admissoes(self.brutos(
            [linha(sl.TOPICO_ADMISSOES, alta, 1), linha(sl.TOPICO_ADMISSOES, ADMISSAO, 0)])))
        estado = sl.estado_internacoes(adm).collect()
        self.assertEqual(len(estado), 1)
        self.assertEqual(estado[0].situacao, "ENCERRADA")

    def test_alertas_por_criterio(self):
        adm = sl.silver_admissoes(sl.validar_admissoes(self.brutos([linha(sl.TOPICO_ADMISSOES, ADMISSAO)])))
        sinais = sl.silver_sinais(sl.validar_sinais(self.brutos([linha(sl.TOPICO_SINAIS, SINAL)])))
        alertas = sl.derivar_alertas(sinais, sl.estado_internacoes(adm), batch_id=7).collect()
        tipos = sorted(a.tipo_alerta for a in alertas)
        self.assertEqual(tipos, ["HIPOTENSAO", "HIPOXEMIA", "TAQUICARDIA"])  # febre 38.2 < 39
        self.assertTrue(all(a.hospital_uf == "SP" and a.batch_id == 7 for a in alertas))

    def test_ocupacao_conta_apenas_ativas(self):
        outra = dict(ADMISSAO, evento_id="e2", internacao_id="i2")
        alta = dict(outra, evento_id="e3", tipo_evento="ALTA", ocorrido_em="2026-09-24T13:00:00.000+00:00")
        adm = sl.silver_admissoes(sl.validar_admissoes(self.brutos(
            [linha(sl.TOPICO_ADMISSOES, e, i) for i, e in enumerate([ADMISSAO, outra, alta])])))
        r = sl.ocupacao(sl.estado_internacoes(adm)).first()
        self.assertEqual((r.internacoes_ativas, r.pacientes_graves), (1, 1))

    def test_dlq_nao_carrega_dado_pessoal(self):
        ruim = dict(ADMISSAO, setor="COZINHA")
        inv = sl.validar_admissoes(self.brutos([linha(sl.TOPICO_ADMISSOES, ruim, 42)]))
        msg = json.loads(sl.mensagens_dlq(inv).first().value)
        self.assertEqual((msg["motivo"], msg["offset"]), ("setor inválido", 42))
        self.assertNotIn("123.456.789-01", json.dumps(msg))
        self.assertNotIn("Maria", json.dumps(msg))


class TesteMascaramento(unittest.TestCase):
    def test_regras(self):
        self.assertEqual(m.mascarar_cpf_py("123.456.789-01"), "***.456.789-**")
        self.assertEqual(m.mascarar_nome_py("Maria Souza"), "M**** S****")
        self.assertEqual(m.faixa_etaria_py(date(1943, 9, 9), date(2026, 9, 24)), "80+")
        self.assertNotEqual(m.pseudonimizar_py("12345678901", "a"), m.pseudonimizar_py("12345678901", "b"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
