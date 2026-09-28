#!/usr/bin/env python3
"""
Painéis do Metabase como código.

Cria (ou recria) pela API do Metabase:
  * a conta de administrador, se a instalação for nova (dados do .env);
  * a conexão "DW Saúde" com o DW (usuário somente leitura, TLS obrigatório);
  * a coleção "Case Saúde" com as perguntas e os painéis.

Idempotente: a cada execução, os itens anteriores da coleção são arquivados e
recriados, então alterações feitas aqui sempre prevalecem.

Uso: make metabase   (usa apenas a biblioteca padrão do Python)
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

RAIZ = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
COLECAO = "Case Saúde"
BANCO = "DW Saúde"


# ------------------------------------------------------------------ utilitários
def ler_env():
    env = {}
    with open(os.path.join(RAIZ, ".env"), encoding="utf-8") as f:
        for linha in f:
            linha = linha.strip()
            if linha and not linha.startswith("#") and "=" in linha:
                chave, valor = linha.split("=", 1)
                env[chave.strip()] = valor.strip().strip('"').strip("'")
    return env


class Metabase:
    def __init__(self, url):
        self.url = url.rstrip("/")
        self.sessao = None

    def chamar(self, metodo, caminho, corpo=None):
        dados = json.dumps(corpo).encode() if corpo is not None else None
        req = urllib.request.Request(self.url + caminho, data=dados, method=metodo)
        req.add_header("Content-Type", "application/json")
        if self.sessao:
            req.add_header("X-Metabase-Session", self.sessao)
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                texto = resp.read().decode()
                return json.loads(texto) if texto else None
        except urllib.error.HTTPError as erro:
            detalhe = erro.read().decode()[:500]
            raise RuntimeError(f"{metodo} {caminho} -> HTTP {erro.code}: {detalhe}") from None


def pergunta_sql(nome, sql, display, viz=None, descricao=None):
    return {"nome": nome, "sql": sql.strip(), "display": display, "viz": viz or {}, "descricao": descricao}


def texto(markdown):
    return {"texto": markdown.strip()}


# ------------------------------------------------------------------ conteúdo dos painéis
# Cada painel: lista de (item, coluna, linha, largura, altura) numa grade de 24 colunas
LETALIDADE = "round(100.0 * sum(obitos) / NULLIF(sum(casos), 0), 1)"
ORDEM_FAIXA = ("CASE WHEN faixa_etaria = '80+' THEN 80 WHEN faixa_etaria = 'não informado' THEN 999 "
               "ELSE split_part(faixa_etaria, '-', 1)::int END")
HORA_LOCAL = "AT TIME ZONE 'America/Sao_Paulo'"

PAINEIS = [
    {
        "nome": "1. Vigilância de SRAG (batch — OpenDataSUS)",
        "descricao": "Notificações de síndrome respiratória aguda grave do SIVEP-Gripe (OpenDataSUS). "
                     "Camada batch: atualização semanal.",
        "itens": [
            (texto("## Vigilância de SRAG\nNotificações reais do **SIVEP-Gripe** (OpenDataSUS), processadas pela "
                   "**camada batch** (atualização semanal). Semana epidemiológica de início dos sintomas; "
                   "dados pessoais minimizados na silver."), 0, 0, 24, 2),
            (pergunta_sql("SRAG — casos notificados", "SELECT sum(casos) AS casos FROM gold.fato_srag_semanal",
                          "scalar"), 0, 2, 6, 3),
            (pergunta_sql("SRAG — óbitos", "SELECT sum(obitos) AS obitos FROM gold.fato_srag_semanal",
                          "scalar"), 6, 2, 6, 3),
            (pergunta_sql("SRAG — letalidade (%)", f"SELECT {LETALIDADE} AS letalidade FROM gold.fato_srag_semanal",
                          "scalar", {"scalar.suffix": " %"}), 12, 2, 6, 3),
            (pergunta_sql("SRAG — internações em UTI",
                          "SELECT sum(internados_uti) AS internados_uti FROM gold.fato_srag_semanal",
                          "scalar"), 18, 2, 6, 3),
            (pergunta_sql("SRAG — curva epidêmica por ano", """
                SELECT semana_epidemiologica AS semana, ano::text AS ano, sum(casos) AS casos
                FROM gold.fato_srag_semanal
                WHERE semana_epidemiologica BETWEEN 1 AND 53
                GROUP BY 1, 2 ORDER BY 1, 2""", "line",
                          {"graph.dimensions": ["semana", "ano"], "graph.metrics": ["casos"],
                           "graph.x_axis.title_text": "Semana epidemiológica (início dos sintomas)",
                           "graph.y_axis.title_text": "Casos"}), 0, 5, 24, 7),
            (pergunta_sql("SRAG — casos por classificação final", """
                SELECT ano::text AS ano, classificacao_final, sum(casos) AS casos
                FROM gold.fato_srag_semanal GROUP BY 1, 2 ORDER BY 1, 3 DESC""", "bar",
                          {"graph.dimensions": ["ano", "classificacao_final"], "graph.metrics": ["casos"],
                           "stackable.stack_type": "stacked"}), 0, 12, 12, 7),
            (pergunta_sql("SRAG — letalidade por faixa etária (%)", f"""
                SELECT faixa_etaria, {LETALIDADE} AS letalidade_pct
                FROM gold.fato_srag_semanal GROUP BY 1 ORDER BY {ORDEM_FAIXA}""", "bar",
                          {"graph.dimensions": ["faixa_etaria"], "graph.metrics": ["letalidade_pct"],
                           "graph.y_axis.title_text": "Letalidade (%)"}), 12, 12, 12, 7),
            (pergunta_sql("SRAG — casos por UF de residência", """
                SELECT uf_residencia AS uf, sum(casos) AS casos
                FROM gold.fato_srag_semanal WHERE uf_residencia IS NOT NULL
                GROUP BY 1 ORDER BY 2 DESC""", "row",
                          {"graph.dimensions": ["uf"], "graph.metrics": ["casos"]}), 0, 19, 10, 10),
            (pergunta_sql("SRAG — municípios com mais casos", f"""
                SELECT municipio AS "Município", uf AS "UF", sum(casos) AS "Casos", sum(obitos) AS "Óbitos",
                       {LETALIDADE} AS "Letalidade (%)"
                FROM gold.vw_srag_municipio GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 20""", "table"), 10, 19, 14, 10),
        ],
    },
    {
        "nome": "2. Operação hospitalar em tempo real (speed layer)",
        "descricao": "Eventos simulados de internação e sinais vitais processados pela speed layer "
                     "(Spark Structured Streaming, micro-batches de 30 s).",
        "itens": [
            (texto("## Operação hospitalar em tempo real\nEventos **simulados** de internação e sinais vitais, "
                   "processados continuamente pela **speed layer** (micro-batches de 30 s). Para atualização "
                   "automática, abra o painel com `#refresh=60` no fim do endereço."), 0, 0, 24, 2),
            (pergunta_sql("Tempo real — internações ativas",
                          "SELECT COALESCE(sum(internacoes_ativas), 0) AS internacoes FROM gold.ocupacao_rt",
                          "scalar"), 0, 2, 5, 3),
            (pergunta_sql("Tempo real — em UTI", """
                SELECT COALESCE(sum(internacoes_ativas) FILTER (WHERE setor = 'UTI'), 0) AS uti
                FROM gold.ocupacao_rt""", "scalar"), 5, 2, 5, 3),
            (pergunta_sql("Tempo real — pacientes graves",
                          "SELECT COALESCE(sum(pacientes_graves), 0) AS graves FROM gold.ocupacao_rt",
                          "scalar"), 10, 2, 5, 3),
            (pergunta_sql("Tempo real — alertas na última hora", """
                SELECT count(*) AS alertas FROM gold.vw_alertas_clinicos
                WHERE ocorrido_em > now() - interval '1 hour'""", "scalar"), 15, 2, 5, 3),
            (pergunta_sql("Tempo real — latência média evento → DW", """
                SELECT round(avg(extract(epoch FROM latencia))::numeric, 1) AS latencia
                FROM gold.vw_alertas_clinicos WHERE processado_em > now() - interval '1 hour'""", "scalar",
                          {"scalar.suffix": " s"}), 20, 2, 4, 3),
            (pergunta_sql("Tempo real — alertas clínicos por minuto", """
                SELECT date_trunc('minute', ocorrido_em) AS minuto, tipo_alerta, count(*) AS alertas
                FROM gold.vw_alertas_clinicos WHERE ocorrido_em > now() - interval '2 hours'
                GROUP BY 1, 2 ORDER BY 1""", "line",
                          {"graph.dimensions": ["minuto", "tipo_alerta"], "graph.metrics": ["alertas"]}),
             0, 5, 14, 7),
            (pergunta_sql("Tempo real — ocupação por UF", """
                SELECT uf, internacoes_ativas - internacoes_uti AS "Fora da UTI", internacoes_uti AS "UTI"
                FROM gold.vw_ocupacao_por_uf ORDER BY internacoes_ativas DESC""", "bar",
                          {"graph.dimensions": ["uf"], "graph.metrics": ["Fora da UTI", "UTI"],
                           "stackable.stack_type": "stacked"}), 14, 5, 10, 7),
            (pergunta_sql("Tempo real — últimos alertas clínicos", f"""
                SELECT to_char(ocorrido_em {HORA_LOCAL}, 'DD/MM HH24:MI:SS') AS "Ocorrido em",
                       tipo_alerta AS "Alerta", valor AS "Valor", hospital_nome AS "Hospital",
                       hospital_uf AS "UF", setor AS "Setor", faixa_etaria AS "Faixa etária",
                       cid_principal AS "CID", round(extract(epoch FROM latencia)::numeric, 1) AS "Latência (s)"
                FROM gold.vw_alertas_clinicos ORDER BY ocorrido_em DESC LIMIT 25""", "table"), 0, 12, 24, 9),
            (texto("### Prontuário (CDC)\nEspelho do sistema de prontuário, alimentado pelo **Debezium** a partir do "
                   "log de transações do PostgreSQL. Eliminações de titulares na origem (LGPD) são propagadas até o "
                   "lakehouse."), 0, 21, 24, 2),
            (pergunta_sql("CDC — pacientes no espelho",
                          "SELECT COALESCE(max(pacientes), 0) AS pacientes FROM gold.cdc_pacientes", "scalar"),
             0, 23, 6, 4),
            (pergunta_sql("CDC — titulares eliminados (LGPD)",
                          "SELECT COALESCE(max(eliminados), 0) AS eliminados FROM gold.cdc_pacientes", "scalar"),
             0, 27, 6, 4),
            (pergunta_sql("CDC — atendimentos por tipo e situação", """
                SELECT tipo, status, total FROM gold.cdc_atendimentos ORDER BY 1, 2""", "bar",
                          {"graph.dimensions": ["tipo", "status"], "graph.metrics": ["total"],
                           "stackable.stack_type": "stacked"}), 6, 23, 18, 8),
        ],
    },
    {
        "nome": "3. Visão integrada (arquitetura Lambda)",
        "descricao": "Camada de serving: combina as visões da camada batch e da speed layer.",
        "itens": [
            (texto("## Visão integrada — camada de serving\nNa arquitetura **Lambda**, a camada de serving combina "
                   "a **visão batch** (completa e recalculada periodicamente, aqui o SRAG do OpenDataSUS) com a "
                   "**visão em tempo real** (incremental e de baixa latência, aqui a ocupação hospitalar simulada). "
                   "Cada linha abaixo junta as duas visões para a mesma UF."), 0, 0, 24, 3),
            (pergunta_sql("Lambda — batch e tempo real por UF", """
                WITH ano_atual AS (SELECT max(ano) AS ano FROM gold.fato_srag_semanal),
                batch AS (
                    SELECT uf_residencia AS uf, sum(casos) AS casos, sum(obitos) AS obitos,
                           sum(internados_uti) AS uti
                    FROM gold.fato_srag_semanal WHERE ano = (SELECT ano FROM ano_atual) GROUP BY 1
                ),
                tempo_real AS (SELECT uf, internacoes_ativas, internacoes_uti, pacientes_graves
                               FROM gold.vw_ocupacao_por_uf)
                SELECT COALESCE(b.uf, t.uf) AS "UF",
                       b.casos AS "SRAG no ano: casos (batch)", b.obitos AS "SRAG no ano: óbitos (batch)",
                       b.uti AS "SRAG no ano: UTI (batch)",
                       t.internacoes_ativas AS "Agora: internações (tempo real)",
                       t.internacoes_uti AS "Agora: em UTI (tempo real)",
                       t.pacientes_graves AS "Agora: graves (tempo real)"
                FROM batch b FULL JOIN tempo_real t ON t.uf = b.uf
                WHERE COALESCE(b.uf, t.uf) IS NOT NULL
                ORDER BY 2 DESC NULLS LAST""", "table"), 0, 3, 24, 12),
        ],
    },
    {
        "nome": "4. Qualidade e ingestão de dados",
        "descricao": "Governança: resultado das regras de qualidade e controle das cargas da camada batch.",
        "itens": [
            (texto("## Qualidade e ingestão\nResultado mais recente de cada **regra de qualidade** (críticas "
                   "bloqueiam a publicação da silver; alertas apenas registram) e o **controle de ingestão** "
                   "que sustenta a carga incremental."), 0, 0, 24, 2),
            (pergunta_sql("Qualidade — regras aprovadas", """
                SELECT count(*) FILTER (WHERE aprovado) || ' de ' || count(*) AS regras
                FROM qualidade.vw_ultima_verificacao""", "scalar"), 0, 2, 8, 3),
            (pergunta_sql("Ingestão — notificações na silver", """
                SELECT COALESCE(sum(linhas_silver), 0) AS notificacoes FROM (
                    SELECT DISTINCT ON (ano) ano, linhas_silver FROM auditoria.controle_ingestao
                    WHERE fonte = 'srag' AND status = 'processado' ORDER BY ano, atualizado_em DESC) ultimos""",
                          "scalar"), 8, 2, 8, 3),
            (pergunta_sql("Ingestão — última carga concluída", f"""
                SELECT to_char(max(atualizado_em) {HORA_LOCAL}, 'DD/MM/YYYY HH24:MI') AS ultima_carga
                FROM auditoria.controle_ingestao WHERE status = 'processado'""", "scalar"), 16, 2, 8, 3),
            (pergunta_sql("Qualidade — resultado por regra", f"""
                SELECT COALESCE(ano::text, '-') AS "Ano", tabela AS "Tabela", regra AS "Regra",
                       severidade AS "Severidade", valor AS "Valor", limite AS "Limite",
                       CASE WHEN aprovado THEN 'OK' ELSE 'FALHA' END AS "Resultado",
                       to_char(verificado_em {HORA_LOCAL}, 'DD/MM/YYYY HH24:MI') AS "Verificado em"
                FROM qualidade.vw_ultima_verificacao ORDER BY tabela DESC, ano, regra""", "table"), 0, 5, 24, 10),
            (pergunta_sql("Ingestão — controle das cargas", f"""
                SELECT ano AS "Ano", arquivo AS "Arquivo", status AS "Situação",
                       round(tamanho_bytes / 1e6) AS "MB", linhas_silver AS "Notificações (silver)",
                       to_char(last_modified {HORA_LOCAL}, 'DD/MM/YYYY') AS "Versão da fonte",
                       to_char(atualizado_em {HORA_LOCAL}, 'DD/MM/YYYY HH24:MI') AS "Atualizado em"
                FROM auditoria.controle_ingestao ORDER BY ano, atualizado_em DESC""", "table"), 0, 15, 24, 6),
        ],
    },
]


# ------------------------------------------------------------------ execução
def autenticar(mb, env):
    email, senha = env.get("METABASE_ADMIN_EMAIL"), env.get("METABASE_ADMIN_PASSWORD")
    if not email or not senha:
        sys.exit("[metabase] defina METABASE_ADMIN_EMAIL e METABASE_ADMIN_PASSWORD no .env")
    props = mb.chamar("GET", "/api/session/properties")
    if props.get("setup-token") and not props.get("has-user-setup"):
        print("[metabase] instalação nova: criando o administrador")
        resp = mb.chamar("POST", "/api/setup", {
            "token": props["setup-token"],
            "user": {"email": email, "password": senha, "password_confirm": senha,
                     "first_name": "Admin", "last_name": "Case Saúde", "site_name": "Case Saúde"},
            "prefs": {"site_name": "Case Saúde", "site_locale": "pt_BR", "allow_tracking": False},
        })
        mb.sessao = resp["id"]
    else:
        try:
            mb.sessao = mb.chamar("POST", "/api/session", {"username": email, "password": senha})["id"]
        except RuntimeError as erro:
            sys.exit(f"[metabase] falha no login com {email}: confira METABASE_ADMIN_EMAIL e "
                     f"METABASE_ADMIN_PASSWORD no .env ({erro})")
    print(f"[metabase] autenticado como {email}")


def garantir_banco(mb, env):
    for banco in mb.chamar("GET", "/api/database").get("data", []):
        if banco["name"] == BANCO:
            print(f"[metabase] conexão '{BANCO}' já existe (id {banco['id']})")
            return banco["id"]
    banco = mb.chamar("POST", "/api/database", {
        "engine": "postgres", "name": BANCO,
        "details": {"host": "postgres", "port": 5432, "dbname": "dw", "user": "bi_reader",
                    "password": env["BI_READER_PASSWORD"], "ssl": True, "ssl-mode": "require",
                    "schema-filters-type": "inclusion", "schema-filters-patterns": "gold",
                    "tunnel-enabled": False, "advanced-options": False},
    })
    print(f"[metabase] conexão '{BANCO}' criada (id {banco['id']}): usuário bi_reader, TLS obrigatório")
    return banco["id"]


def garantir_colecao(mb):
    for colecao in mb.chamar("GET", "/api/collection"):
        if colecao.get("name") == COLECAO and not colecao.get("archived"):
            itens = mb.chamar("GET", f"/api/collection/{colecao['id']}/items").get("data", [])
            for item in itens:
                if item["model"] in ("card", "dashboard"):
                    rota = "card" if item["model"] == "card" else "dashboard"
                    mb.chamar("PUT", f"/api/{rota}/{item['id']}", {"archived": True})
            if itens:
                print(f"[metabase] {len(itens)} item(ns) anterior(es) da coleção arquivado(s)")
            return colecao["id"]
    return mb.chamar("POST", "/api/collection", {"name": COLECAO,
                                                  "description": "Painéis do case de engenharia de dados"})["id"]


def criar_painel(mb, banco_id, colecao_id, painel):
    painel_id = mb.chamar("POST", "/api/dashboard", {
        "name": painel["nome"], "description": painel["descricao"], "collection_id": colecao_id})["id"]
    dashcards = []
    for n, (item, col, linha, largura, altura) in enumerate(painel["itens"], start=1):
        base = {"id": -n, "row": linha, "col": col, "size_x": largura, "size_y": altura,
                "series": [], "parameter_mappings": []}
        if "texto" in item:
            dashcards.append({**base, "card_id": None, "visualization_settings": {
                "virtual_card": {"name": None, "display": "text", "visualization_settings": {},
                                 "dataset_query": {}, "archived": False},
                "text": item["texto"]}})
            continue
        card = mb.chamar("POST", "/api/card", {
            "name": item["nome"], "type": "question", "display": item["display"],
            "description": item["descricao"], "collection_id": colecao_id,
            "visualization_settings": item["viz"],
            "dataset_query": {"type": "native", "database": banco_id, "native": {"query": item["sql"]}},
        })
        dashcards.append({**base, "card_id": card["id"], "visualization_settings": {}})
    mb.chamar("PUT", f"/api/dashboard/{painel_id}", {"dashcards": dashcards})
    return painel_id


def gravar_links_do_portal(mb, painel_ids, colecao_id):
    """O portal lê este arquivo para listar os painéis (os IDs mudam a cada recriação)."""
    pasta = os.path.join(RAIZ, "data", "portal")
    os.makedirs(pasta, exist_ok=True)
    descricoes = {1: "Casos, óbitos, UTI e curva epidêmica do SRAG", 2: "Internações, alertas clínicos e prontuário agora",
                  3: "Histórico do ano e situação atual, lado a lado", 4: "Regras de qualidade e controle das cargas"}
    paineis = [{"nome": nome.split(" (")[0], "descricao": descricoes.get(i), "url": f"{mb.url}/dashboard/{pid}"}
               for i, (nome, pid) in enumerate(painel_ids, start=1)]
    with open(os.path.join(pasta, "paineis.json"), "w", encoding="utf-8") as f:
        json.dump({"colecao": f"{mb.url}/collection/{colecao_id}", "paineis": paineis}, f, ensure_ascii=False, indent=2)


def main():
    env = ler_env()
    mb = Metabase(f"http://localhost:{env.get('METABASE_PORT', '3000')}")
    for tentativa in range(30):  # aguarda o Metabase ficar pronto
        try:
            if mb.chamar("GET", "/api/health").get("status") == "ok":
                break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(5)
    else:
        sys.exit("[metabase] o Metabase não respondeu em http://localhost:"
                 f"{env.get('METABASE_PORT', '3000')}")

    autenticar(mb, env)
    banco_id = garantir_banco(mb, env)
    colecao_id = garantir_colecao(mb)
    criados = []
    for painel in PAINEIS:
        painel_id = criar_painel(mb, banco_id, colecao_id, painel)
        criados.append((painel["nome"], painel_id))
        perguntas = sum(1 for item, *_ in painel["itens"] if "sql" in item)
        print(f"[metabase] painel '{painel['nome']}' criado com {perguntas} pergunta(s): {mb.url}/dashboard/{painel_id}")
    gravar_links_do_portal(mb, criados, colecao_id)
    print(f"[metabase] pronto: coleção '{COLECAO}' em {mb.url}/collection/{colecao_id} (links enviados ao portal)")


if __name__ == "__main__":
    main()
