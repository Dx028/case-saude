-- =============================================================
-- Batch layer — SRAG (idempotente)
--   auditoria: controle da ingestão (carga incremental e rastreabilidade)
--   qualidade: resultados das verificações de qualidade
--   gold     : dimensão de municípios (IBGE) e fato semanal de SRAG
-- =============================================================
SET ROLE dw_owner;

-- ---------------- Controle da ingestão ----------------
CREATE TABLE IF NOT EXISTS auditoria.controle_ingestao (
    id              BIGSERIAL PRIMARY KEY,
    fonte           TEXT        NOT NULL,
    ano             SMALLINT    NOT NULL,
    url             TEXT        NOT NULL,
    arquivo         TEXT        NOT NULL,
    chave_landing   TEXT        NOT NULL,
    tamanho_bytes   BIGINT,
    last_modified   TIMESTAMPTZ,
    status          TEXT        NOT NULL CHECK (status IN ('baixado', 'processado', 'falhou')),
    dag_run_id      TEXT,
    registrado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
    linhas_bronze   BIGINT,
    linhas_silver   BIGINT,
    UNIQUE (fonte, arquivo, last_modified)
);
COMMENT ON TABLE auditoria.controle_ingestao IS
  'Um registro por versão de arquivo ingerida: base da carga incremental e trilha de auditoria.';

CREATE TABLE IF NOT EXISTS auditoria.resultado_carga (
    fonte          TEXT, ano SMALLINT, arquivo TEXT,
    linhas_bronze  BIGINT, linhas_silver BIGINT,
    execucao       TEXT, processado_em TIMESTAMPTZ
);

-- ---------------- Qualidade ----------------
CREATE TABLE IF NOT EXISTS qualidade.verificacao (
    execucao       TEXT,
    camada         TEXT,
    tabela         TEXT,
    ano            SMALLINT,
    regra          TEXT,
    severidade     TEXT CHECK (severidade IN ('critica', 'alerta')),
    valor          NUMERIC,
    limite         NUMERIC,
    aprovado       BOOLEAN,
    verificado_em  TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_verificacao_data ON qualidade.verificacao (verificado_em DESC);

CREATE OR REPLACE VIEW qualidade.vw_ultima_verificacao AS
SELECT DISTINCT ON (tabela, ano, regra)
       tabela, ano, regra, severidade, valor, limite, aprovado, verificado_em
FROM qualidade.verificacao
ORDER BY tabela, ano, regra, verificado_em DESC;
COMMENT ON VIEW qualidade.vw_ultima_verificacao IS 'Resultado mais recente de cada regra de qualidade.';

-- ---------------- Dimensão de municípios (IBGE) ----------------
CREATE TABLE IF NOT EXISTS gold.dim_municipio (
    codigo_ibge      CHAR(7) PRIMARY KEY,
    codigo_ibge6     CHAR(6) NOT NULL UNIQUE,   -- formato usado pelo SIVEP-Gripe/DATASUS
    nome             TEXT    NOT NULL,
    uf               CHAR(2) NOT NULL,
    uf_nome          TEXT    NOT NULL,
    regiao           TEXT    NOT NULL,
    regiao_imediata  TEXT,
    atualizado_em    TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE gold.dim_municipio IS 'Municípios brasileiros (API de Localidades do IBGE).';

-- ---------------- Fato semanal de SRAG ----------------
CREATE TABLE IF NOT EXISTS gold.fato_srag_semanal (
    ano                         INTEGER,
    semana_epidemiologica       INTEGER,
    uf_residencia               TEXT,
    municipio_residencia_ibge6  TEXT,
    faixa_etaria                TEXT,
    sexo                        TEXT,
    classificacao_final         TEXT,
    evolucao                    TEXT,
    casos                       BIGINT,
    hospitalizados              BIGINT,
    internados_uti              BIGINT,
    ventilacao_invasiva         BIGINT,
    obitos                      BIGINT,
    vacinados_covid             BIGINT,
    atualizado_em               TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_fato_srag_tempo ON gold.fato_srag_semanal (ano, semana_epidemiologica);
CREATE INDEX IF NOT EXISTS ix_fato_srag_municipio ON gold.fato_srag_semanal (municipio_residencia_ibge6);
COMMENT ON TABLE gold.fato_srag_semanal IS
  'Casos de SRAG (OpenDataSUS) agregados por semana epidemiológica de início dos sintomas e perfil. Recalculada a cada carga.';

-- ---------------- Visões para o BI ----------------
CREATE OR REPLACE VIEW gold.vw_srag_semanal_uf AS
SELECT f.ano, f.semana_epidemiologica, f.uf_residencia AS uf, f.classificacao_final,
       sum(f.casos) AS casos, sum(f.internados_uti) AS internados_uti, sum(f.obitos) AS obitos,
       round(100.0 * sum(f.obitos) / NULLIF(sum(f.casos), 0), 2) AS letalidade_pct
FROM gold.fato_srag_semanal f
GROUP BY 1, 2, 3, 4;

CREATE OR REPLACE VIEW gold.vw_srag_municipio AS
SELECT f.ano, m.codigo_ibge, m.nome AS municipio, m.uf, m.regiao,
       sum(f.casos) AS casos, sum(f.obitos) AS obitos, sum(f.internados_uti) AS internados_uti
FROM gold.fato_srag_semanal f
JOIN gold.dim_municipio m ON m.codigo_ibge6 = f.municipio_residencia_ibge6
GROUP BY 1, 2, 3, 4, 5;

RESET ROLE;

-- BI: leitura dos resultados de qualidade e do controle da ingestão (sem dados pessoais)
GRANT USAGE ON SCHEMA qualidade, auditoria TO bi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA qualidade TO bi_reader;
GRANT SELECT ON auditoria.controle_ingestao TO bi_reader;
