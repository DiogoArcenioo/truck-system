BEGIN;

ALTER TABLE app.multas ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.multas FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS pol_multas_empresa ON app.multas;
DROP POLICY IF EXISTS multas_rls_empresa ON app.multas;
CREATE POLICY pol_multas_empresa
  ON app.multas
  FOR ALL
  TO app_user
  USING (
    id_empresa = app.current_empresa_id()
  )
  WITH CHECK (
    id_empresa = app.current_empresa_id()
  );

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE app.multas TO app_user;

DO $$
DECLARE
  nome_sequence text;
BEGIN
  nome_sequence := pg_get_serial_sequence('app.multas', 'id_multa');

  IF nome_sequence IS NOT NULL THEN
    EXECUTE format('GRANT USAGE, SELECT ON SEQUENCE %s TO app_user', nome_sequence);
  END IF;
END $$;

COMMIT;
