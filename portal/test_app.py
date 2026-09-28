"""Testes das regras do portal (sem rede nem banco): python -m unittest test_app"""
import unittest

import app


class TestePortal(unittest.TestCase):
    def test_estado_da_etapa_prioriza_falha_e_andamento(self):
        self.assertEqual(app.estado_da_etapa(["success", "failed", "success"]), "failed")
        self.assertEqual(app.estado_da_etapa(["success", "running"]), "running")
        self.assertEqual(app.estado_da_etapa(["success", "success"]), "success")
        self.assertIsNone(app.estado_da_etapa([]))

    def test_consolida_tarefas_mapeadas_na_ordem_das_etapas(self):
        tis = [{"task_id": "baixar_para_landing", "state": "success", "map_index": i,
                "start_date": f"2026-09-28T10:0{i}:00Z", "end_date": f"2026-09-28T10:1{i}:00Z"} for i in range(3)]
        tis.append({"task_id": "bronze_silver", "state": "running", "start_date": "2026-09-28T10:20:00Z", "end_date": None})
        etapas = app.consolidar_etapas(tis)
        self.assertEqual([e["tarefa"] for e in etapas], [t for t, _ in app.ETAPAS])
        baixar = etapas[1]
        self.assertEqual((baixar["estado"], baixar["instancias"]), ("success", 3))
        self.assertEqual((baixar["inicio"][:16], baixar["fim"][:16]), ("2026-09-28T10:00", "2026-09-28T10:12"))
        self.assertIsNone(etapas[3]["fim"])            # etapa em andamento não tem fim
        self.assertIsNone(etapas[0]["estado"])         # etapa ainda não iniciada

    def test_trecho_do_log_mostra_o_motivo_e_a_excecao(self):
        conteudo = [{"event": "DAG bundles loaded", "level": "info"},
                    {"event": "[qualidade] 2026 FALHA [critica] silver: % sem data: 12.4 (limite 5.0)", "level": "info"},
                    {"event": "Task failed with exception", "level": "error",
                     "error_detail": [{"exc_type": "AirflowException", "exc_value": "Exit code 1"}]}]
        trecho = app.trecho_do_log(conteudo)
        self.assertIn("12.4 (limite 5.0)", trecho)
        self.assertIn("AirflowException: Exit code 1", trecho)
        self.assertNotIn("DAG bundles", trecho)
        self.assertIn("Traceback", app.trecho_do_log("linha comum\nTraceback (most recent call last):\nValueError: x"))

    def test_saude_traduz_sinais_em_linguagem_simples(self):
        ok = app.avaliar_saude([], [], {"estado": "success", "etapas": []}, {"cdc": {"conector": "RUNNING"}}, [])
        self.assertEqual((ok["nivel"], ok["problemas"]), ("ok", []))
        alerta = app.avaliar_saude([{"resumo": "Memória alta", "severidade": "alta"}], [], None, {}, [])
        self.assertEqual(alerta["nivel"], "atencao")
        falha = app.avaliar_saude([], ["kafka"], {"estado": "failed", "etapas": [{"nome": "Calcular indicadores", "estado": "failed"}]},
                                  {"cdc": {"conector": "FAILED"}}, ["o Airflow"])
        self.assertEqual(falha["nivel"], "falha")
        textos = " | ".join(p["texto"] for p in falha["problemas"])
        for esperado in ("Sem resposta: kafka", 'falhou em "Calcular indicadores"', "conector do prontuário está FAILED",
                         "Não foi possível consultar o Airflow"):
            self.assertIn(esperado, textos)


if __name__ == "__main__":
    unittest.main(verbosity=2)
