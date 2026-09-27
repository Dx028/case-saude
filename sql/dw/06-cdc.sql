-- =============================================================
-- Serving do CDC do prontuário (idempotente)
-- =============================================================
SET ROLE dw_owner;

CREATE TABLE IF NOT EXISTS gold.cdc_atendimentos (
    tipo           TEXT,
    status         TEXT,
    total          BIGINT,
    atualizado_em  TIMESTAMPTZ
);
COMMENT ON TABLE gold.cdc_atendimentos IS
  'CDC do prontuário: atendimentos por tipo e situação (espelho atual da fonte, sem dados pessoais).';

CREATE TABLE IF NOT EXISTS gold.cdc_pacientes (
    pacientes      BIGINT,
    eliminados     BIGINT,
    atualizado_em  TIMESTAMPTZ
);
COMMENT ON TABLE gold.cdc_pacientes IS
  'CDC do prontuário: pacientes no espelho e titulares eliminados (LGPD) propagados da origem.';

RESET ROLE;
