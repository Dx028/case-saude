-- =============================================================
-- Usuário do postgres-exporter (idempotente)
-- pg_monitor: papel nativo que só permite LER estatísticas do servidor
-- Variável psql esperada: mon_pass
-- =============================================================
SELECT format('CREATE ROLE monitoring LOGIN PASSWORD %L', :'mon_pass')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'monitoring')
\gexec
ALTER ROLE monitoring WITH LOGIN PASSWORD :'mon_pass' NOSUPERUSER NOCREATEDB NOCREATEROLE;
GRANT pg_monitor TO monitoring;
