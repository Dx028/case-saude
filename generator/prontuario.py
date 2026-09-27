"""
Simulador de uso do sistema de prontuário (fonte OLTP do CDC).

Executa operações reais no PostgreSQL, como um sistema hospitalar faria:
cadastros, agendamentos, mudanças de situação, atualização de contato e
eliminação de titulares (LGPD, art. 18). O Debezium captura cada alteração
no log de transações (WAL) e a publica no Kafka.
"""
import logging
import random
import threading
import time

import psycopg

log = logging.getLogger("prontuario")

# (operação, peso)
OPERACOES = [("agendar", 40), ("concluir", 30), ("cadastrar", 15), ("atualizar_contato", 12), ("eliminar", 3)]
TIPOS = [("CONSULTA", 55), ("EXAME", 30), ("INTERNACAO", 15)]
CIDS = ["J18", "J11", "U07", "J44", "J06", "A90", "I21", "I50", "N39", "R50", "E11", "I10"]


class SimuladorProntuario:
    def __init__(self, gerador, dsn, ops_por_segundo, metricas=None):
        self.gerador = gerador            # reaproveita o Faker e as capitais do gerador de eventos
        self.dsn = dsn
        self.intervalo = 1.0 / ops_por_segundo
        self.metricas = metricas
        self.rnd = random.Random()
        self.rodando = True

    def _paciente(self):
        capital = self.rnd.choices(self.gerador.capitais, weights=[c[3] for c in self.gerador.capitais], k=1)[0]
        p = self.gerador._novo_paciente(capital)
        return (p["cpf"].replace(".", "").replace("-", ""), p["nome"], p["data_nascimento"], p["sexo"],
                p["telefone"], p["email"], capital[1][:6], capital[2])

    def _executar(self, cur, operacao):
        if operacao == "cadastrar":
            cur.execute("""INSERT INTO clinico.pacientes
                           (cpf, nome, data_nascimento, sexo, telefone, email, municipio_ibge6, uf)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (cpf) DO NOTHING""", self._paciente())
        elif operacao == "agendar":
            cur.execute("""INSERT INTO clinico.atendimentos (paciente_id, tipo, cid_principal, status, unidade_cnes)
                           SELECT paciente_id, %s, %s, 'AGENDADO', %s FROM clinico.pacientes
                           ORDER BY random() LIMIT 1""",
                        (self.gerador._escolher(TIPOS), self.rnd.choice(CIDS),
                         f"{self.rnd.randint(1000000, 9999999)}"))
        elif operacao == "concluir":
            cur.execute("""UPDATE clinico.atendimentos
                           SET status = %s, encerrado_em = now(), atualizado_em = now()
                           WHERE atendimento_id = (SELECT atendimento_id FROM clinico.atendimentos
                                                   WHERE status = 'AGENDADO' ORDER BY random() LIMIT 1)""",
                        ("REALIZADO" if self.rnd.random() < 0.85 else "CANCELADO",))
        elif operacao == "atualizar_contato":
            p = self._paciente()
            cur.execute("""UPDATE clinico.pacientes SET telefone = %s, email = %s, atualizado_em = now()
                           WHERE paciente_id = (SELECT paciente_id FROM clinico.pacientes
                                                ORDER BY random() LIMIT 1)""", (p[4], p[5]))
        elif operacao == "eliminar":
            # Solicitação de eliminação do titular: os atendimentos saem junto (ON DELETE CASCADE)
            cur.execute("""DELETE FROM clinico.pacientes
                           WHERE paciente_id = (SELECT paciente_id FROM clinico.pacientes
                                                ORDER BY random() LIMIT 1)""")
        return cur.rowcount

    def popular(self, conexao, minimo=200):
        with conexao.cursor() as cur:
            cur.execute("SELECT count(*) FROM clinico.pacientes")
            existentes = cur.fetchone()[0]
            for _ in range(max(0, minimo - existentes)):
                self._executar(cur, "cadastrar")
            for _ in range(max(0, minimo - existentes)):
                self._executar(cur, "agendar")
        conexao.commit()
        log.info("prontuário com %d pacientes (mínimo %d)", max(existentes, minimo), minimo)

    def executar(self):
        while self.rodando:
            try:
                with psycopg.connect(self.dsn, connect_timeout=10) as conexao:
                    self.popular(conexao)
                    while self.rodando:
                        operacao = self.gerador._escolher(OPERACOES)
                        with conexao.cursor() as cur:
                            afetadas = self._executar(cur, operacao)
                        conexao.commit()
                        if self.metricas and afetadas:
                            self.metricas.labels(operacao).inc()
                        time.sleep(self.intervalo)
            except psycopg.Error as erro:
                log.warning("prontuário indisponível (%s); nova tentativa em 10 s", str(erro).splitlines()[0])
                time.sleep(10)

    def iniciar(self):
        threading.Thread(target=self.executar, name="prontuario", daemon=True).start()
        log.info("simulador do prontuário iniciado: %.1f operações/s", 1 / self.intervalo)
