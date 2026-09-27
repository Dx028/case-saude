"""
Testes das transformações da batch layer (SRAG). Rodam com Spark local.
A linha-base reproduz um registro real do arquivo INFLUD24 (OpenDataSUS).
"""
import os
import sys
import unittest

RAIZ = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "jobs")
sys.path.insert(0, os.path.join(RAIZ, "batch"))
sys.path.insert(0, os.path.join(RAIZ, "common"))
os.environ.setdefault("PII_HMAC_KEY", "chave-de-teste")

from pyspark.sql import SparkSession  # noqa: E402

import mascaramento as m  # noqa: E402
import srag_transformacoes as t  # noqa: E402

BASE = {
    "NU_NOTIFIC": "31725016651631", "DT_NOTIFIC": "2024-08-29T00:00:00.000Z", "SEM_NOT": "35",
    "DT_SIN_PRI": "2024-08-28T00:00:00.000Z", "SEM_PRI": "35", "SG_UF_NOT": "PR", "CO_MUN_NOT": "410830",
    "SG_UF": "PR", "CO_MUN_RES": "410830", "CS_SEXO": "F", "DT_NASC": "1942-01-01T00:00:00.000Z",
    "NU_IDADE_N": "82", "TP_IDADE": "3", "CS_RACA": "4", "FATOR_RISC": "1", "HOSPITAL": "1",
    "DT_INTERNA": "2024-08-29T00:00:00.000Z", "UTI": "2", "DT_ENTUTI": "", "DT_SAIDUTI": "",
    "SUPORT_VEN": "3", "VACINA_COV": "1", "VACINA": "1", "CLASSI_FIN": "5", "EVOLUCAO": "1",
    "DT_EVOLUCA": "2024-09-10T00:00:00.000Z", "DT_ENCERRA": "2024-09-12T00:00:00.000Z",
    "DT_DIGITA": "2024-08-30T00:00:00.000Z", "PAC_DSCBO": "APOSENTADA", "_arquivo": "INFLUD24-23-03-2026.csv",
    "ano": "2024",
}
COLUNAS = list(BASE)


class TesteBatchSrag(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]").appName("testes-batch")
                     .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.sparkContext.addPyFile(os.path.join(RAIZ, "common", "mascaramento.py"))

    def bronze(self, *registros):
        return self.spark.createDataFrame([tuple({**BASE, **r}[c] for c in COLUNAS) for r in registros], COLUNAS)

    def test_registro_real_tipado_e_traduzido(self):
        r = t.para_silver(self.bronze({})).first()
        self.assertEqual((r.ano, r.semana_sintomas, r.uf_residencia, r.municipio_residencia_ibge6), (2024, 35, "PR", "410830"))
        self.assertEqual((r.sexo, r.idade, r.faixa_etaria, r.raca_cor), ("Feminino", 82, "80+", "Parda"))
        self.assertEqual((r.classificacao_final, r.evolucao, r.hospitalizado, r.internado_uti),
                         ("SRAG por covid-19", "Cura", "Sim", "Não"))
        self.assertEqual(str(r.dt_primeiros_sintomas), "2024-08-28")
        self.assertIsNone(r.dt_entrada_uti)  # campo vazio vira nulo, sem erro

    def test_minimizacao_lgpd(self):
        silver = t.para_silver(self.bronze({}))
        for proibida in ("NU_NOTIFIC", "DT_NASC", "PAC_DSCBO", "data_nascimento", "ocupacao"):
            self.assertNotIn(proibida, silver.columns)
        self.assertEqual(silver.first().notificacao_id, m.pseudonimizar_py("31725016651631", "chave-de-teste"))

    def test_idade_em_dias_e_meses(self):
        rs = {r.idade: r.faixa_etaria for r in t.para_silver(self.bronze(
            {"NU_NOTIFIC": "1", "NU_IDADE_N": "200", "TP_IDADE": "1"},   # 200 dias
            {"NU_NOTIFIC": "2", "NU_IDADE_N": "30", "TP_IDADE": "2"},    # 30 meses
            {"NU_NOTIFIC": "3", "NU_IDADE_N": "", "TP_IDADE": ""},       # sem idade
        )).collect()}
        self.assertEqual(rs, {0: "0-9", 2: "0-9", None: "não informado"})

    def test_codigos_desconhecidos_e_datas_invalidas(self):
        r = t.para_silver(self.bronze({"CLASSI_FIN": "", "EVOLUCAO": "7", "CS_SEXO": "X",
                                       "DT_EVOLUCA": "31/02/2024"})).first()
        self.assertEqual((r.classificacao_final, r.evolucao, r.sexo), ("Em investigação", "Sem desfecho informado", "Ignorado"))
        self.assertIsNone(r.dt_evolucao)

    def test_deduplicacao_mantem_digitacao_mais_recente(self):
        silver = t.para_silver(self.bronze(
            {"EVOLUCAO": "", "DT_DIGITA": "2024-08-30T00:00:00.000Z"},
            {"EVOLUCAO": "2", "DT_DIGITA": "2024-09-15T00:00:00.000Z"},  # mesma notificação, atualizada
        ))
        linhas = silver.collect()
        self.assertEqual(len(linhas), 1)
        self.assertEqual(linhas[0].evolucao, "Óbito")

    def test_qualidade_aprova_dado_bom_e_reprova_critico(self):
        b = self.bronze({}, {"NU_NOTIFIC": "9"})
        regras = {r[0]: r for r in t.verificar_silver(b, t.para_silver(b), 2024)}
        self.assertTrue(all(r[4] for r in regras.values()))
        ruim = self.bronze({"DT_SIN_PRI": ""}, {"NU_NOTIFIC": "9", "DT_SIN_PRI": ""})
        regras = {r[0]: r for r in t.verificar_silver(ruim, t.para_silver(ruim), 2024)}
        critica = regras["silver: % sem data de primeiros sintomas"]
        self.assertEqual((critica[1], critica[2], critica[4]), ("critica", 100.0, False))

    def test_regioes_administrativas_do_df_viram_brasilia(self):
        rs = t.para_silver(self.bronze(
            {"NU_NOTIFIC": "1", "SG_UF": "DF", "CO_MUN_RES": "530040", "CO_MUN_NOT": "530140"},  # Ceilândia etc.
            {"NU_NOTIFIC": "2", "SG_UF": "DF", "CO_MUN_RES": "530010", "CO_MUN_NOT": "530010"},  # Brasília
            {"NU_NOTIFIC": "3"},                                                                 # Foz do Iguaçu (PR)
        )).orderBy("municipio_residencia_sivep").collect()
        self.assertEqual([(r.municipio_residencia_ibge6, r.municipio_residencia_sivep, r.municipio_notificacao_ibge6) for r in rs],
                         [("410830", "410830", "410830"), ("530010", "530010", "530010"), ("530010", "530040", "530010")])

    def test_fato_semanal(self):
        silver = t.para_silver(self.bronze(
            {}, {"NU_NOTIFIC": "2", "EVOLUCAO": "2", "UTI": "1", "SUPORT_VEN": "1"}))
        fatos = {r.evolucao: r for r in t.para_fato_semanal(silver).collect()}
        self.assertEqual(fatos["Cura"].casos, 1)
        obito = fatos["Óbito"]
        self.assertEqual((obito.casos, obito.obitos, obito.internados_uti, obito.ventilacao_invasiva, obito.semana_epidemiologica),
                         (1, 1, 1, 1, 35))


if __name__ == "__main__":
    unittest.main(verbosity=2)
