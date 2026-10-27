-- pipelines must not run user-supplied code with elevated privileges
CREATE EXTENSION pg_incremental CASCADE;
CREATE ROLE incremental_attacker;
CREATE SCHEMA attacker_schema AUTHORIZATION incremental_attacker;
CREATE TABLE secret_table (id bigserial primary key);
INSERT INTO secret_table DEFAULT VALUES;
SET ROLE incremental_attacker;
-- a list function that tries to escalate privileges
CREATE FUNCTION attacker_schema.evil_list(pattern text)
RETURNS SETOF text LANGUAGE plpgsql AS $$
BEGIN
  ALTER ROLE incremental_attacker SUPERUSER;
  RETURN NEXT 'file';
END;
$$;
SELECT incremental.create_file_list_pipeline('evil-files', 'pattern', 'SELECT 1',
  list_function := 'attacker_schema.evil_list', schedule := NULL,
  execute_immediately := false);
CALL incremental.execute_pipeline('evil-files');
-- cannot read the state of a sequence without privileges on it
SELECT incremental.create_sequence_pipeline('evil-sequence', 'secret_table', 'SELECT 1',
  schedule := NULL);
RESET ROLE;
SELECT rolsuper FROM pg_roles WHERE rolname = 'incremental_attacker';
-- permission can be revoked after the pipeline is created
CREATE TABLE shared_table (id bigserial primary key);
GRANT SELECT ON SEQUENCE shared_table_id_seq TO incremental_attacker;
SET ROLE incremental_attacker;
SELECT incremental.create_sequence_pipeline('revoked-sequence', 'shared_table', 'SELECT 1',
  schedule := NULL);
RESET ROLE;
REVOKE SELECT ON SEQUENCE shared_table_id_seq FROM incremental_attacker;
INSERT INTO shared_table DEFAULT VALUES;
SET ROLE incremental_attacker;
CALL incremental.execute_pipeline('revoked-sequence');
RESET ROLE;
SELECT incremental.drop_pipeline('evil-files');
SELECT incremental.drop_pipeline('revoked-sequence');
DROP TABLE shared_table;
DROP TABLE secret_table;
DROP SCHEMA attacker_schema CASCADE;
DROP ROLE incremental_attacker;
DROP EXTENSION pg_incremental;
