-- =============================================================
-- Prontuário (fonte OLTP do CDC) — idempotente
-- Tabelas pertencem ao usuário da aplicação (prontuario_app); a publicação
-- limita a replicação lógica às tabelas necessárias (menor privilégio).
-- =============================================================
CREATE SCHEMA IF NOT EXISTS clinico AUTHORIZATION prontuario_app;
SET ROLE prontuario_app;

CREATE TABLE IF NOT EXISTS clinico.pacientes (
    paciente_id      BIGSERIAL PRIMARY KEY,
    cpf              CHAR(11)    NOT NULL UNIQUE,
    nome             TEXT        NOT NULL,
    data_nascimento  DATE        NOT NULL,
    sexo             CHAR(1)     NOT NULL CHECK (sexo IN ('F', 'M')),
    telefone         TEXT,
    email            TEXT,
    municipio_ibge6  CHAR(6)     NOT NULL,
    uf               CHAR(2)     NOT NULL,
    criado_em        TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS clinico.atendimentos (
    atendimento_id   BIGSERIAL PRIMARY KEY,
    paciente_id      BIGINT      NOT NULL REFERENCES clinico.pacientes ON DELETE CASCADE,
    tipo             TEXT        NOT NULL CHECK (tipo IN ('CONSULTA', 'EXAME', 'INTERNACAO')),
    cid_principal    VARCHAR(5),
    status           TEXT        NOT NULL CHECK (status IN ('AGENDADO', 'REALIZADO', 'CANCELADO')),
    unidade_cnes     CHAR(7)     NOT NULL,
    iniciado_em      TIMESTAMPTZ NOT NULL DEFAULT now(),
    encerrado_em     TIMESTAMPTZ,
    atualizado_em    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_atendimentos_paciente ON clinico.atendimentos (paciente_id);
CREATE INDEX IF NOT EXISTS ix_atendimentos_status ON clinico.atendimentos (status);

-- Publicação para o Debezium (pgoutput): somente as tabelas do prontuário
SELECT 'CREATE PUBLICATION dbz_prontuario FOR TABLE clinico.pacientes, clinico.atendimentos'
WHERE NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'dbz_prontuario')
\gexec

RESET ROLE;
