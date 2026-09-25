"""
Gerador de eventos hospitalares simulados (dados 100% sintéticos).

Simula internações em capitais brasileiras e publica no Kafka:
  * saude.eventos.admissoes     -> admissões, transferências e altas (com dados pessoais)
  * saude.eventos.sinais-vitais -> sinais vitais periódicos de cada internação

Uma fração configurável de eventos é corrompida de propósito para exercitar
a validação e a dead letter queue (DLQ) da speed layer.

Configuração por variáveis de ambiente:
  KAFKA_BOOTSTRAP        endereço do Kafka              (padrão: kafka:9092)
  EVENTOS_POR_SEGUNDO    vazão total aproximada         (padrão: 50)
  TAXA_INVALIDOS         fração de eventos inválidos    (padrão: 0.01)
  INTERNACOES_INICIAIS   internações abertas no início  (padrão: 150)
  MAX_INTERNACOES        limite de internações ativas   (padrão: 3000)
  SEMENTE                semente aleatória (reprodutibilidade)
  DRY_RUN=1              imprime eventos em vez de publicar (testes)
"""
import json
import logging
import os
import random
import re
import signal
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

from faker import Faker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("gerador")

TOPICO_ADMISSOES = "saude.eventos.admissoes"
TOPICO_SINAIS = "saude.eventos.sinais-vitais"

# Capitais: (município, código IBGE, UF, peso populacional, faixa de CEP, DDD, preposição)
CAPITAIS = [
    ("São Paulo", "3550308", "SP", 11.5, (1000, 5999), "11", "de"),
    ("Rio de Janeiro", "3304557", "RJ", 6.2, (20000, 23799), "21", "do"),
    ("Brasília", "5300108", "DF", 2.8, (70000, 72799), "61", "de"),
    ("Fortaleza", "2304400", "CE", 2.4, (60000, 61599), "85", "de"),
    ("Salvador", "2927408", "BA", 2.4, (40000, 42599), "71", "de"),
    ("Belo Horizonte", "3106200", "MG", 2.3, (30000, 31999), "31", "de"),
    ("Manaus", "1302603", "AM", 2.1, (69000, 69099), "92", "de"),
    ("Curitiba", "4106902", "PR", 1.8, (80000, 82999), "41", "de"),
    ("Recife", "2611606", "PE", 1.5, (50000, 52999), "81", "do"),
    ("Goiânia", "5208707", "GO", 1.4, (74000, 74899), "62", "de"),
    ("Porto Alegre", "4314902", "RS", 1.3, (90000, 91999), "51", "de"),
    ("Belém", "1501402", "PA", 1.3, (66000, 66999), "91", "de"),
]

# CIDs de doenças infecciosas: febre é mais frequente
CIDS_FEBRIS = {"J18", "J11", "U07", "A90", "R50"}

# CID-10 principal e peso relativo (quadros respiratórios predominam)
CIDS = [("J18", 30), ("J11", 15), ("U07", 10), ("J44", 10), ("J06", 8),
        ("A90", 7), ("I21", 6), ("I50", 6), ("N39", 5), ("R50", 3)]

SETORES = [("PRONTO_SOCORRO", 45), ("ENFERMARIA", 40), ("UTI", 15)]
TIPOS_HOSPITAL = ["Hospital Municipal", "Hospital Estadual", "Hospital Regional", "Santa Casa", "Hospital Universitário"]


@dataclass
class Internacao:
    internacao_id: str
    paciente: dict
    hospital: dict
    setor: str
    cid: str
    gravidade: int                      # 1 (leve) a 5 (crítico)
    leito: str
    iniciada_em: datetime
    # sinais vitais atuais (evoluem por passeio aleatório)
    vitais: dict = field(default_factory=dict)


class Gerador:
    def __init__(self, semente=None):
        self.rnd = random.Random(semente)
        self.fake = Faker("pt_BR")
        if semente is not None:
            self.fake.seed_instance(semente)
        self.hospitais = self._criar_hospitais()
        self.ativas: dict[str, Internacao] = {}

    # ---------------------------------------------------------------- cadastros
    def _criar_hospitais(self):
        hospitais = []
        for n, (municipio, ibge, uf, peso, _, _, prep) in enumerate(CAPITAIS):
            for i in range(2 if peso < 2 else 3):
                tipo = TIPOS_HOSPITAL[(n + i) % len(TIPOS_HOSPITAL)]  # tipos distintos na mesma cidade
                hospitais.append({
                    "hospital_cnes": f"{self.rnd.randint(1000000, 9999999)}",
                    "hospital_nome": f"{tipo} {prep} {municipio}",
                    "municipio": municipio, "codigo_ibge": ibge, "uf": uf, "peso": peso,
                })
        return hospitais

    def _escolher(self, pares):
        itens, pesos = zip(*pares)
        return self.rnd.choices(itens, weights=pesos, k=1)[0]

    def _novo_paciente(self, capital):
        municipio, ibge, uf, _, (cep_ini, cep_fim), ddd, _ = capital
        nascimento = date.today() - timedelta(days=self.rnd.randint(0, 95 * 365))
        cep = f"{self.rnd.randint(cep_ini, cep_fim):05d}-{self.rnd.randint(0, 999):03d}"
        sexo = self.rnd.choice(["F", "M"])
        nome = self.fake.name_female() if sexo == "F" else self.fake.name_male()
        nome = re.sub(r"^(Sr|Sra|Srta|Dr|Dra)\.\s+", "", nome)  # remove pronomes de tratamento
        return {
            "cpf": self.fake.cpf(),
            "nome": nome,
            "data_nascimento": nascimento.isoformat(),
            "sexo": sexo,
            "telefone": f"({ddd}) 9{self.rnd.randint(1000, 9999)}-{self.rnd.randint(0, 9999):04d}",
            "email": self.fake.free_email(),
            "endereco": self.fake.street_address(),
            "cep": cep,
            "municipio": municipio, "codigo_ibge": ibge, "uf": uf,
        }

    def _vitais_base(self, gravidade, cid=None):
        g = gravidade - 1
        febre = 1.0 if cid in CIDS_FEBRIS else 0.0
        return {
            "frequencia_cardiaca": self.rnd.gauss(80 + 8 * g, 8),
            "pressao_sistolica": self.rnd.gauss(120 - 5 * g, 10),
            "pressao_diastolica": self.rnd.gauss(78 - 3 * g, 7),
            "saturacao_o2": min(99.5, self.rnd.gauss(97 - 1.5 * g, 1.2)),
            "temperatura": self.rnd.gauss(36.6 + 0.35 * g + febre * (0.4 + 0.25 * g), 0.3),
            "frequencia_respiratoria": self.rnd.gauss(16 + 2 * g, 2),
        }

    # ---------------------------------------------------------------- eventos
    @staticmethod
    def _agora():
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    def _evento_internacao(self, it: Internacao, tipo):
        return {
            "evento_id": str(uuid.uuid4()),
            "tipo_evento": tipo,
            "ocorrido_em": self._agora(),
            "internacao_id": it.internacao_id,
            **it.paciente,
            "hospital_cnes": it.hospital["hospital_cnes"],
            "hospital_nome": it.hospital["hospital_nome"],
            "hospital_municipio": it.hospital["municipio"],
            "hospital_uf": it.hospital["uf"],
            "setor": it.setor,
            "leito": it.leito,
            "cid_principal": it.cid,
            "gravidade": it.gravidade,
        }

    def admitir(self):
        capital = self.rnd.choices(CAPITAIS, weights=[c[3] for c in CAPITAIS], k=1)[0]
        hospital = self.rnd.choice([h for h in self.hospitais if h["uf"] == capital[2]])
        setor = self._escolher(SETORES)
        gravidade = {"UTI": self.rnd.randint(3, 5), "ENFERMARIA": self.rnd.randint(2, 4),
                     "PRONTO_SOCORRO": self.rnd.randint(1, 3)}[setor]
        it = Internacao(
            internacao_id=str(uuid.uuid4()),
            paciente=self._novo_paciente(capital),
            hospital=hospital, setor=setor, cid=self._escolher(CIDS), gravidade=gravidade,
            leito=f"{setor[:3]}-{self.rnd.randint(1, 60):02d}",
            iniciada_em=datetime.now(timezone.utc),
        )
        it.vitais = self._vitais_base(gravidade, it.cid)
        self.ativas[it.internacao_id] = it
        return TOPICO_ADMISSOES, it.internacao_id, self._evento_internacao(it, "ADMISSAO")

    def alta_ou_transferencia(self):
        it = self.ativas[self.rnd.choice(list(self.ativas))]
        if it.setor != "UTI" and it.gravidade >= 4 and self.rnd.random() < 0.5:
            it.setor, it.leito = "UTI", f"UTI-{self.rnd.randint(1, 60):02d}"
            return TOPICO_ADMISSOES, it.internacao_id, self._evento_internacao(it, "TRANSFERENCIA")
        del self.ativas[it.internacao_id]
        return TOPICO_ADMISSOES, it.internacao_id, self._evento_internacao(it, "ALTA")

    def sinais_vitais(self):
        it = self.ativas[self.rnd.choice(list(self.ativas))]
        v = it.vitais
        # evolução clínica: pacientes graves tendem a piorar, os demais a estabilizar
        tendencia = 0.03 * (it.gravidade - 3)
        v["frequencia_cardiaca"] += self.rnd.gauss(tendencia * 10, 3)
        v["pressao_sistolica"] += self.rnd.gauss(-tendencia * 8, 3)
        v["pressao_diastolica"] += self.rnd.gauss(-tendencia * 4, 2)
        v["saturacao_o2"] = min(100.0, v["saturacao_o2"] + self.rnd.gauss(-tendencia, 0.5))
        v["temperatura"] += self.rnd.gauss(tendencia * 0.1, 0.1)
        v["frequencia_respiratoria"] += self.rnd.gauss(tendencia * 2, 1)
        # limites fisiológicos + reversão à média (evita derivas irreais)
        base = self._vitais_base(it.gravidade, it.cid)
        for k in v:
            v[k] = 0.9 * v[k] + 0.1 * base[k]
        evento = {
            "evento_id": str(uuid.uuid4()),
            "internacao_id": it.internacao_id,
            "ocorrido_em": self._agora(),
            "leito": it.leito,
            "frequencia_cardiaca": round(v["frequencia_cardiaca"]),
            "pressao_sistolica": round(v["pressao_sistolica"]),
            "pressao_diastolica": round(v["pressao_diastolica"]),
            "saturacao_o2": round(v["saturacao_o2"], 1),
            "temperatura": round(v["temperatura"], 1),
            "frequencia_respiratoria": round(v["frequencia_respiratoria"]),
        }
        return TOPICO_SINAIS, it.internacao_id, evento

    def corromper(self, evento: dict):
        """Gera um evento inválido: campo obrigatório ausente, valor impossível ou JSON quebrado."""
        tipo = self.rnd.choice(["sem_campo", "valor_impossivel", "json_invalido"])
        e = dict(evento)
        if tipo == "sem_campo":
            e.pop("internacao_id", None)
        elif tipo == "valor_impossivel":
            if "saturacao_o2" in e:
                e["saturacao_o2"] = 250.0
            else:
                e["cpf"] = "000.000.000-00x"
        else:
            return json.dumps(e, ensure_ascii=False)[:-7]  # JSON truncado
        return json.dumps(e, ensure_ascii=False)

    def proximo_evento(self, max_internacoes):
        """Sorteia o próximo evento mantendo o número de internações estável."""
        n = len(self.ativas)
        if n == 0:
            return self.admitir()
        r = self.rnd.random()
        p_admissao = 0.06 if n < max_internacoes else 0.0
        if r < p_admissao:
            return self.admitir()
        if r < p_admissao + 0.05 and n > 10:
            return self.alta_ou_transferencia()
        return self.sinais_vitais()


# ------------------------------------------------------------------------ execução
def main():
    taxa = float(os.getenv("EVENTOS_POR_SEGUNDO", "50"))
    taxa_invalidos = float(os.getenv("TAXA_INVALIDOS", "0.01"))
    iniciais = int(os.getenv("INTERNACOES_INICIAIS", "150"))
    max_internacoes = int(os.getenv("MAX_INTERNACOES", "3000"))
    semente = int(os.environ["SEMENTE"]) if os.getenv("SEMENTE") else None
    dry_run = os.getenv("DRY_RUN") == "1"

    gerador = Gerador(semente)

    if dry_run:
        eventos = [gerador.admitir() for _ in range(3)] + [gerador.sinais_vitais() for _ in range(3)]
        for topico, chave, evento in eventos:
            print(f"--- {topico} (chave={chave[:8]}...)")
            print(json.dumps(evento, ensure_ascii=False, indent=2))
        print("--- exemplo de evento inválido")
        print(gerador.corromper(eventos[-1][2]))
        return

    from confluent_kafka import Producer
    from prometheus_client import Counter, Gauge, Histogram, start_http_server

    produzidos = Counter("gerador_eventos_produzidos", "Eventos publicados", ["topico", "valido"])
    erros = Counter("gerador_erros_entrega", "Falhas de entrega ao Kafka", ["topico"])
    ativas = Gauge("gerador_internacoes_ativas", "Internações abertas na simulação")
    vazao_alvo = Gauge("gerador_vazao_alvo_eventos_por_segundo", "Vazão configurada")
    latencia = Histogram("gerador_latencia_entrega_segundos", "Tempo até a confirmação do Kafka",
                         buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5))
    start_http_server(8000)
    vazao_alvo.set(taxa)

    producer = Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
        "client.id": "gerador-eventos-saude",
        "acks": "all",                 # confirmação de todas as réplicas em sincronia
        "enable.idempotence": True,    # sem duplicatas em caso de reenvio
        "compression.type": "lz4",
        "linger.ms": 20,               # agrupa mensagens em lotes (vazão)
    })

    def confirmacao(topico, enviado_em):
        def _cb(err, _msg):
            if err is not None:
                erros.labels(topico).inc()
                log.warning("falha na entrega (%s): %s", topico, err)
            else:
                latencia.observe(time.monotonic() - enviado_em)
        return _cb

    rodando = True

    def parar(*_):
        nonlocal rodando
        rodando = False
    signal.signal(signal.SIGTERM, parar)
    signal.signal(signal.SIGINT, parar)

    def publicar(topico, chave, evento):
        invalido = gerador.rnd.random() < taxa_invalidos
        valor = gerador.corromper(evento) if invalido else json.dumps(evento, ensure_ascii=False)
        while True:
            try:
                producer.produce(topico, key=chave, value=valor.encode("utf-8"),
                                 on_delivery=confirmacao(topico, time.monotonic()))
                break
            except BufferError:          # fila local cheia: aguarda o Kafka escoar
                producer.poll(0.1)
        produzidos.labels(topico, "nao" if invalido else "sim").inc()

    for _ in range(iniciais):
        publicar(*gerador.admitir())
    producer.flush(10)
    log.info("simulação iniciada: %d internações, %.0f eventos/s, %.1f%% inválidos",
             iniciais, taxa, taxa_invalidos * 100)

    # controle de vazão por "fatias" de 100 ms
    fatia = 0.1
    por_fatia = taxa * fatia
    acumulado = 0.0
    ultimo_log = time.monotonic()
    while rodando:
        inicio = time.monotonic()
        acumulado += por_fatia
        n = int(acumulado)
        acumulado -= n
        for _ in range(n):
            publicar(*gerador.proximo_evento(max_internacoes))
        producer.poll(0)
        ativas.set(len(gerador.ativas))
        if time.monotonic() - ultimo_log > 30:
            log.info("internações ativas: %d", len(gerador.ativas))
            ultimo_log = time.monotonic()
        time.sleep(max(0.0, fatia - (time.monotonic() - inicio)))

    log.info("encerrando: aguardando confirmação das mensagens pendentes...")
    producer.flush(15)


if __name__ == "__main__":
    sys.exit(main())
