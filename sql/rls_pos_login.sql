BEGIN;

CREATE OR REPLACE FUNCTION app.current_empresa_id()
RETURNS bigint
LANGUAGE sql
STABLE
AS $function$
  SELECT NULLIF(current_setting('app.empresa_id', true), '')::BIGINT
$function$;

ALTER TABLE app.viagens ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.viagens FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS pol_viagens_empresa ON app.viagens;
DROP POLICY IF EXISTS viagens_rls_empresa ON app.viagens;
CREATE POLICY pol_viagens_empresa
  ON app.viagens
  FOR ALL
  TO app_user
  USING (
    id_empresa = app.current_empresa_id()
  )
  WITH CHECK (
    id_empresa = app.current_empresa_id()
  );

ALTER TABLE app.veiculo ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.veiculo FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS pol_veiculo_empresa ON app.veiculo;
DROP POLICY IF EXISTS veiculo_rls_empresa ON app.veiculo;
CREATE POLICY pol_veiculo_empresa
  ON app.veiculo
  FOR ALL
  TO app_user
  USING (
    id_empresa = app.current_empresa_id()
  )
  WITH CHECK (
    id_empresa = app.current_empresa_id()
  );

ALTER TABLE app.motoristas ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.motoristas FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS pol_motoristas_empresa ON app.motoristas;
DROP POLICY IF EXISTS motoristas_rls_empresa ON app.motoristas;
CREATE POLICY pol_motoristas_empresa
  ON app.motoristas
  FOR ALL
  TO app_user
  USING (
    id_empresa = app.current_empresa_id()
  )
  WITH CHECK (
    id_empresa = app.current_empresa_id()
  );

ALTER TABLE app.abastecimentos ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.abastecimentos FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS pol_abastecimentos_empresa ON app.abastecimentos;
DROP POLICY IF EXISTS abastecimentos_rls_empresa ON app.abastecimentos;
CREATE POLICY pol_abastecimentos_empresa
  ON app.abastecimentos
  FOR ALL
  TO app_user
  USING (
    id_empresa = app.current_empresa_id()
  )
  WITH CHECK (
    id_empresa = app.current_empresa_id()
  );

ALTER TABLE app.fornecedor ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.fornecedor FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS pol_fornecedor_empresa ON app.fornecedor;
DROP POLICY IF EXISTS fornecedor_rls_empresa ON app.fornecedor;
CREATE POLICY pol_fornecedor_empresa
  ON app.fornecedor
  FOR ALL
  TO app_user
  USING (
    id_empresa = app.current_empresa_id()
  )
  WITH CHECK (
    id_empresa = app.current_empresa_id()
  );

COMMIT;
