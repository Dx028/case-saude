-- =============================================================
-- DW: schemas e papéis de acesso (idempotente — roda a cada "make up")
-- Variável psql esperada: bi_pass (senha do usuário de BI)
-- =============================================================

-- Usuário de leitura para o BI (Metabase): só enxerga a camada gold
SELECT format('CREATE ROLE bi_reader LOGIN PASSWORD %L', :'bi_pass')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bi_reader')
\gexec
ALTER ROLE bi_reader WITH LOGIN PASSWORD :'bi_pass' NOSUPERUSER NOCREATEDB NOCREATEROLE;

-- Schemas do DW, todos pertencentes ao dono do DW
CREATE SCHEMA IF NOT EXISTS gold      AUTHORIZATION dw_owner;  -- modelo dimensional (consumo do BI)
CREATE SCHEMA IF NOT EXISTS qualidade AUTHORIZATION dw_owner;  -- resultados de data quality
CREATE SCHEMA IF NOT EXISTS auditoria AUTHORIZATION dw_owner;  -- trilhas de auditoria (LGPD)

-- Ninguém cria objetos no schema public
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

-- BI: conecta ao DW e lê apenas a gold (tabelas atuais e futuras)
GRANT CONNECT ON DATABASE dw TO bi_reader;
GRANT USAGE ON SCHEMA gold TO bi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO bi_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE dw_owner IN SCHEMA gold GRANT SELECT ON TABLES TO bi_reader;
