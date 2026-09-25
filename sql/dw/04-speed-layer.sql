-- =============================================================
-- Serving da speed layer (idempotente)
-- Tabelas alimentadas continuamente pelo Spark Structured Streaming
-- =============================================================
SET ROLE dw_owner;

-- Alertas clínicos por sinais vitais críticos (sem dados pessoais)
CREATE TABLE IF NOT EXISTS gold.alerta_clinico_rt (
    evento_id      TEXT        NOT NULL,
    internacao_id  TEXT        NOT NULL,
    ocorrido_em    TIMESTAMPTZ NOT NULL,
    tipo_alerta    TEXT        NOT NULL,
    valor          NUMERIC(6,1),
    hospital_cnes  TEXT,
    hospital_nome  TEXT,
    hospital_uf    CHAR(2),
    setor          TEXT,
    faixa_etaria   TEXT,
    cid_principal  TEXT,
    processado_em  TIMESTAMPTZ NOT NULL,
    batch_id       BIGINT      NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_alerta_rt_ocorrido ON gold.alerta_clinico_rt (ocorrido_em DESC);
COMMENT ON TABLE gold.alerta_clinico_rt IS
  'Speed layer: alertas por sinais vitais críticos. Gravação "pelo menos uma vez": use a view deduplicada.';

-- Ocupação atual por hospital e setor (substituída a cada micro-batch)
CREATE TABLE IF NOT EXISTS gold.ocupacao_rt (
    hospital_cnes       TEXT,
    hospital_nome       TEXT,
    hospital_uf         CHAR(2),
    setor               TEXT,
    internacoes_ativas  BIGINT,
    pacientes_graves    BIGINT,
    atualizado_em       TIMESTAMPTZ
);
COMMENT ON TABLE gold.ocupacao_rt IS 'Speed layer: internações ativas por hospital e setor (tempo real).';

-- Alertas deduplicados (um registro por evento e tipo de alerta)
CREATE OR REPLACE VIEW gold.vw_alertas_clinicos AS
SELECT DISTINCT ON (evento_id, tipo_alerta)
       evento_id, internacao_id, ocorrido_em, tipo_alerta, valor,
       hospital_cnes, hospital_nome, hospital_uf, setor, faixa_etaria, cid_principal, processado_em,
       processado_em - ocorrido_em AS latencia
FROM gold.alerta_clinico_rt
ORDER BY evento_id, tipo_alerta, processado_em;
COMMENT ON VIEW gold.vw_alertas_clinicos IS 'Alertas clínicos sem duplicatas, com a latência ponta a ponta.';

-- Ocupação consolidada por UF
CREATE OR REPLACE VIEW gold.vw_ocupacao_por_uf AS
SELECT hospital_uf AS uf,
       sum(internacoes_ativas) AS internacoes_ativas,
       COALESCE(sum(internacoes_ativas) FILTER (WHERE setor = 'UTI'), 0) AS internacoes_uti,
       COALESCE(sum(pacientes_graves), 0) AS pacientes_graves,
       max(atualizado_em) AS atualizado_em
FROM gold.ocupacao_rt
GROUP BY hospital_uf;

RESET ROLE;
