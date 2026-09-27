"""
Testes do CDC do prontuário com eventos no formato do Debezium
(conversor JSON sem schema: {"before", "after", "source", "op", "ts_ms"}).
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

import cdc_prontuario as cdc  # noqa: E402
import mascaramento as m  # noqa: E402

DIAS_1958 = (date(1958, 3, 14) - date(1970, 1, 1)).days
PACIENTE = {"paciente_id": 1, "cpf": "12345678901", "nome": "Maria Souza", "data_nascimento": DIAS_1958,
            "sexo": "F", "telefone": "(11) 98765-4321", "email": "maria@email.com", "municipio_ibge6": "355030",
            "uf": "SP", "criado_em": "2026-09-27T03:00:00.123456Z", "atualizado_em": "2026-09-27T03:00:00.123456Z"}
ATENDIMENTO = {"atendimento_id": 10, "paciente_id": 1, "tipo": "EXAME", "cid_principal": "J18", "status": "AGENDADO",
               "unidade_cnes": "1234567", "iniciado_em": "2026-09-27T03:05:00Z", "encerrado_em": None,
               "atualizado_em": "2026-09-27T03:05:00Z"}


def evento(op, antes, depois, lsn):
    return json.dumps({"before": antes, "after": depois, "op": op, "ts_ms": 1790000000000,
                       "source": {"connector": "postgresql", "name": "prontuario", "lsn": lsn, "ts_ms": 1790000000000,
                                  "snapshot": "true" if op == "r" else "false", "schema": "clinico"},
                       "transaction": None}, ensure_ascii=False)


class TesteCdc(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]").appName("testes-cdc")
                     .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.sparkContext.addPyFile(os.path.join(RAIZ, "common", "mascaramento.py"))

    def brutos(self, *valores):
        return self.spark.createDataFrame([(i, v) for i, v in enumerate(valores)], "offset long, valor string")

    def test_ultima_mudanca_por_chave(self):
        atualizado = dict(PACIENTE, email="novo@email.com", atualizado_em="2026-09-27T04:00:00Z")
        mud = cdc.mudancas(self.brutos(evento("r", None, PACIENTE, 100), evento("u", None, atualizado, 900),
                                       evento("c", None, dict(PACIENTE, paciente_id=2, cpf="98765432100"), 500)),
                           cdc.LINHA_PACIENTE, "paciente_id")
        linhas = {r._id: r for r in mud.collect()}
        self.assertEqual(set(linhas), {1, 2})
        self.assertEqual((linhas[1]._op, linhas[1]._lsn, linhas[1].a.email), ("u", 900, "novo@email.com"))

    def test_silver_sem_dados_pessoais_e_pseudonimo_da_speed_layer(self):
        silver = cdc.silver_pacientes(cdc.mudancas(self.brutos(evento("c", None, PACIENTE, 100)),
                                                   cdc.LINHA_PACIENTE, "paciente_id"))
        for proibida in ("cpf", "nome", "telefone", "email", "data_nascimento"):
            self.assertNotIn(proibida, silver.columns)
        r = silver.first()
        # a speed layer recebe o CPF formatado; o pseudônimo precisa ser o mesmo (junção sem expor o CPF)
        self.assertEqual(r.paciente_pseudo, m.pseudonimizar_py("123.456.789-01", "chave-de-teste"))
        self.assertEqual((r.faixa_etaria, r.uf, str(r.criado_em)[:19]), ("60-69", "SP", "2026-09-27 03:00:00"))

    def test_exclusao_do_titular(self):
        # Com REPLICA IDENTITY padrão, o evento de exclusão traz apenas a chave em "before"
        antes = {k: (1 if k == "paciente_id" else None) for k in PACIENTE}
        mud = cdc.mudancas(self.brutos(evento("c", None, PACIENTE, 100), evento("d", antes, None, 700)),
                           cdc.LINHA_PACIENTE, "paciente_id").collect()
        self.assertEqual([(r._id, r._op) for r in mud], [(1, "d")])
        self.assertNotIn("Maria", json.dumps(evento("d", antes, None, 700)))

    def test_atendimento_tipado(self):
        concluido = dict(ATENDIMENTO, status="REALIZADO", encerrado_em="2026-09-27T05:00:00Z")
        r = cdc.silver_atendimentos(cdc.mudancas(
            self.brutos(evento("c", None, ATENDIMENTO, 100), evento("u", None, concluido, 200)),
            cdc.LINHA_ATENDIMENTO, "atendimento_id")).first()
        self.assertEqual((r.atendimento_id, r.paciente_id, r.status, r._op), (10, 1, "REALIZADO", "u"))
        self.assertEqual(str(r.encerrado_em)[:16], "2026-09-27 05:00")

    def test_evento_sem_chave_e_ignorado(self):
        mud = cdc.mudancas(self.brutos('{"op":"c"}', "lixo"), cdc.LINHA_PACIENTE, "paciente_id")
        self.assertEqual(mud.count(), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
