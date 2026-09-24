-- =============================================================
-- Dimensão de datas (calendário) — base de todo o modelo dimensional
-- Inclui a semana epidemiológica (SE), usada nas bases do DATASUS
-- =============================================================
SET ROLE dw_owner;

CREATE TABLE IF NOT EXISTS gold.dim_data (
    data_sk             INTEGER     PRIMARY KEY,  -- formato AAAAMMDD
    data                DATE        NOT NULL UNIQUE,
    ano                 SMALLINT    NOT NULL,
    trimestre           SMALLINT    NOT NULL,
    mes                 SMALLINT    NOT NULL,
    nome_mes            VARCHAR(10) NOT NULL,
    dia                 SMALLINT    NOT NULL,
    dia_semana          SMALLINT    NOT NULL,     -- 0 = domingo
    nome_dia_semana     VARCHAR(10) NOT NULL,
    fim_de_semana       BOOLEAN     NOT NULL,
    ano_epidemiologico  SMALLINT    NOT NULL,
    semana_epidemiologica SMALLINT  NOT NULL      -- SE: domingo a sábado
);

COMMENT ON TABLE gold.dim_data IS 'Dimensão calendário (2015-2035) com semana epidemiológica';

-- Semana epidemiológica: começa no domingo; a SE 1 é a que contém 4 de janeiro
INSERT INTO gold.dim_data
SELECT
    to_char(d, 'YYYYMMDD')::INTEGER,
    d,
    EXTRACT(YEAR FROM d),
    EXTRACT(QUARTER FROM d),
    EXTRACT(MONTH FROM d),
    (ARRAY['janeiro','fevereiro','março','abril','maio','junho','julho',
           'agosto','setembro','outubro','novembro','dezembro'])[EXTRACT(MONTH FROM d)],
    EXTRACT(DAY FROM d),
    EXTRACT(DOW FROM d),
    (ARRAY['domingo','segunda','terça','quarta','quinta','sexta','sábado'])[EXTRACT(DOW FROM d) + 1],
    EXTRACT(DOW FROM d) IN (0, 6),
    ano_se,
    ((inicio_semana - (make_date(ano_se, 1, 4) - EXTRACT(DOW FROM make_date(ano_se, 1, 4))::INT)) / 7) + 1
FROM (
    SELECT
        d::DATE AS d,
        (d::DATE - EXTRACT(DOW FROM d)::INT) AS inicio_semana,
        EXTRACT(YEAR FROM (d::DATE - EXTRACT(DOW FROM d)::INT + 3))::INT AS ano_se
    FROM generate_series('2015-01-01'::DATE, '2035-12-31'::DATE, INTERVAL '1 day') AS d
) calendario
ON CONFLICT (data_sk) DO NOTHING;

RESET ROLE;
