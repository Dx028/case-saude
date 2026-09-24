-- =============================================================
-- Segurança e LGPD no DW (idempotente)
--   * schema "restrito": dados pessoais identificáveis (acesso mínimo)
--   * schema "seguranca": funções de mascaramento
--   * views mascaradas na gold para consumo do BI
--   * auditoria de alterações e função de eliminação do titular
-- =============================================================
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Papel de grupo para quem precisa ver dado identificável (ex.: equipe clínica)
SELECT 'CREATE ROLE analista_clinico NOLOGIN'
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analista_clinico')
\gexec

CREATE SCHEMA IF NOT EXISTS restrito  AUTHORIZATION dw_owner;
CREATE SCHEMA IF NOT EXISTS seguranca AUTHORIZATION dw_owner;
REVOKE ALL ON SCHEMA restrito FROM PUBLIC;

SET ROLE dw_owner;

-- ---------------- Funções de mascaramento ----------------

-- CPF no padrão do Portal da Transparência: ***.456.789-**
CREATE OR REPLACE FUNCTION seguranca.mascarar_cpf(cpf TEXT) RETURNS TEXT
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE
    WHEN length(regexp_replace(cpf, '\D', '', 'g')) = 11 THEN
      '***.' || substr(regexp_replace(cpf, '\D', '', 'g'), 4, 3) || '.'
             || substr(regexp_replace(cpf, '\D', '', 'g'), 7, 3) || '-**'
    ELSE NULL
  END
$$;

-- Nome: mantém só a inicial de cada parte ("Maria Souza" -> "M**** S****")
CREATE OR REPLACE FUNCTION seguranca.mascarar_nome(nome TEXT) RETURNS TEXT
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT string_agg(left(parte, 1) || repeat('*', greatest(length(parte) - 1, 0)), ' ')
  FROM regexp_split_to_table(trim(nome), '\s+') AS parte
$$;

-- Telefone: mantém DDD e 4 últimos dígitos ("(11) *****-4321")
CREATE OR REPLACE FUNCTION seguranca.mascarar_telefone(tel TEXT) RETURNS TEXT
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT '(' || left(regexp_replace(tel, '\D', '', 'g'), 2) || ') *****-'
         || right(regexp_replace(tel, '\D', '', 'g'), 4)
$$;

-- Generalização: data de nascimento -> faixa etária de 10 anos
CREATE OR REPLACE FUNCTION seguranca.faixa_etaria(nascimento DATE) RETURNS TEXT
LANGUAGE sql STABLE PARALLEL SAFE AS $$
  SELECT CASE
    WHEN nascimento IS NULL THEN 'não informado'
    WHEN date_part('year', age(nascimento)) >= 80 THEN '80+'
    ELSE (floor(date_part('year', age(nascimento)) / 10) * 10)::INT || '-'
         || (floor(date_part('year', age(nascimento)) / 10) * 10 + 9)::INT
  END
$$;

-- Generalização: CEP -> apenas os 5 primeiros dígitos (região)
CREATE OR REPLACE FUNCTION seguranca.generalizar_cep(cep TEXT) RETURNS TEXT
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT left(regexp_replace(cep, '\D', '', 'g'), 5) || '-***'
$$;

-- ---------------- Dados identificáveis (exemplo sintético) ----------------
CREATE TABLE IF NOT EXISTS restrito.paciente_exemplo (
    paciente_id      INTEGER PRIMARY KEY,
    nome             TEXT NOT NULL,
    cpf              CHAR(11) NOT NULL UNIQUE,
    data_nascimento  DATE,
    telefone         TEXT,
    cep              CHAR(8),
    municipio        TEXT,
    uf               CHAR(2),
    cid_principal    VARCHAR(5)
);
COMMENT ON TABLE restrito.paciente_exemplo IS
  'Dados SINTÉTICOS de demonstração. Acesso restrito ao papel analista_clinico.';

INSERT INTO restrito.paciente_exemplo VALUES
  (1, 'Maria Aparecida Souza',   '12345678901', '1958-03-14', '11987654321', '01310100', 'São Paulo',      'SP', 'J18'),
  (2, 'João Pedro Oliveira',     '23456789012', '1990-07-22', '21998765432', '20040002', 'Rio de Janeiro', 'RJ', 'J11'),
  (3, 'Ana Beatriz Lima',        '34567890123', '2015-11-02', '31987651234', '30130010', 'Belo Horizonte', 'MG', 'J06'),
  (4, 'Carlos Eduardo Santos',   '45678901234', '1975-01-30', '41996541234', '80010000', 'Curitiba',       'PR', 'U07'),
  (5, 'Francisca das Chagas',    '56789012345', '1943-09-09', '85991234567', '60060000', 'Fortaleza',      'CE', 'J18')
ON CONFLICT (paciente_id) DO NOTHING;

-- ---------------- Auditoria (LGPD: rastreabilidade) ----------------
CREATE TABLE IF NOT EXISTS auditoria.log_alteracoes (
    log_id        BIGSERIAL PRIMARY KEY,
    ocorrido_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
    usuario       TEXT NOT NULL DEFAULT current_user,
    tabela        TEXT NOT NULL,
    operacao      TEXT NOT NULL,
    chave         TEXT,
    detalhe       TEXT
);

CREATE OR REPLACE FUNCTION auditoria.registrar_alteracao() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, auditoria AS $$
BEGIN
  INSERT INTO auditoria.log_alteracoes (usuario, tabela, operacao, chave)
  VALUES (session_user, TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME, TG_OP,
          COALESCE(NEW.paciente_id, OLD.paciente_id)::TEXT);
  RETURN COALESCE(NEW, OLD);
END $$;

DROP TRIGGER IF EXISTS trg_auditoria ON restrito.paciente_exemplo;
CREATE TRIGGER trg_auditoria
  AFTER INSERT OR UPDATE OR DELETE ON restrito.paciente_exemplo
  FOR EACH ROW EXECUTE FUNCTION auditoria.registrar_alteracao();

-- LGPD art. 18: eliminação dos dados do titular, com registro na auditoria
CREATE OR REPLACE FUNCTION seguranca.eliminar_titular(p_cpf TEXT) RETURNS INTEGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE removidos INTEGER;
BEGIN
  DELETE FROM restrito.paciente_exemplo WHERE cpf = regexp_replace(p_cpf, '\D', '', 'g');
  GET DIAGNOSTICS removidos = ROW_COUNT;
  INSERT INTO auditoria.log_alteracoes (usuario, tabela, operacao, detalhe)
  VALUES (session_user, 'restrito.paciente_exemplo', 'ELIMINACAO_TITULAR',
          'CPF ' || seguranca.mascarar_cpf(p_cpf) || ': ' || removidos || ' registro(s)');
  RETURN removidos;
END $$;
REVOKE EXECUTE ON FUNCTION seguranca.eliminar_titular(TEXT) FROM PUBLIC;

-- ---------------- Visão mascarada para o BI ----------------
CREATE OR REPLACE VIEW gold.vw_paciente_exemplo_mascarado
WITH (security_barrier = true) AS
SELECT
    paciente_id,
    seguranca.mascarar_nome(nome)          AS nome,
    seguranca.mascarar_cpf(cpf)            AS cpf,
    seguranca.faixa_etaria(data_nascimento) AS faixa_etaria,
    seguranca.mascarar_telefone(telefone)  AS telefone,
    seguranca.generalizar_cep(cep)         AS cep,
    municipio,
    uf,
    cid_principal
FROM restrito.paciente_exemplo;
COMMENT ON VIEW gold.vw_paciente_exemplo_mascarado IS
  'Pacientes com dados pessoais mascarados/generalizados (consumo do BI).';

RESET ROLE;

-- Permissões: BI lê só a view; equipe clínica lê o dado identificável
GRANT SELECT ON gold.vw_paciente_exemplo_mascarado TO bi_reader;
GRANT USAGE ON SCHEMA restrito TO analista_clinico;
GRANT SELECT ON ALL TABLES IN SCHEMA restrito TO analista_clinico;
GRANT USAGE ON SCHEMA seguranca TO bi_reader, analista_clinico;
