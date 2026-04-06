BEGIN;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'app'
      AND p.proname = 'current_empresa_id'
      AND pg_get_function_identity_arguments(p.oid) = ''
  ) THEN
    EXECUTE $fn$
      CREATE FUNCTION app.current_empresa_id()
      RETURNS bigint
      LANGUAGE sql
      STABLE
      AS $function$
        SELECT NULLIF(current_setting('app.empresa_id', true), '')::BIGINT
      $function$
    $fn$;
  END IF;
END $$;

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

ALTER TABLE app.produto ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.produto FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS pol_produto_empresa ON app.produto;
DROP POLICY IF EXISTS produto_rls_empresa ON app.produto;
CREATE POLICY pol_produto_empresa
  ON app.produto
  FOR ALL
  TO app_user
  USING (
    id_empresa = app.current_empresa_id()
  )
  WITH CHECK (
    id_empresa = app.current_empresa_id()
  );

ALTER TABLE app.ordem_servico ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.ordem_servico FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS pol_ordem_servico_empresa ON app.ordem_servico;
DROP POLICY IF EXISTS ordem_servico_rls_empresa ON app.ordem_servico;
CREATE POLICY pol_ordem_servico_empresa
  ON app.ordem_servico
  FOR ALL
  TO app_user
  USING (
    id_empresa = app.current_empresa_id()
  )
  WITH CHECK (
    id_empresa = app.current_empresa_id()
  );

ALTER TABLE app.requisicao ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.requisicao FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS pol_requisicao_empresa ON app.requisicao;
DROP POLICY IF EXISTS requisicao_rls_empresa ON app.requisicao;
CREATE POLICY pol_requisicao_empresa
  ON app.requisicao
  FOR ALL
  TO app_user
  USING (
    id_empresa = app.current_empresa_id()
  )
  WITH CHECK (
    id_empresa = app.current_empresa_id()
  );

ALTER TABLE app.requisicao_itens ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.requisicao_itens FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS pol_requisicao_itens_empresa ON app.requisicao_itens;
DROP POLICY IF EXISTS requisicao_itens_rls_empresa ON app.requisicao_itens;
CREATE POLICY pol_requisicao_itens_empresa
  ON app.requisicao_itens
  FOR ALL
  TO app_user
  USING (
    id_empresa = app.current_empresa_id()
  )
  WITH CHECK (
    id_empresa = app.current_empresa_id()
  );

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

ALTER TABLE app.motorista_enderecos ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.motorista_enderecos FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS pol_motorista_enderecos_empresa ON app.motorista_enderecos;
DROP POLICY IF EXISTS motorista_enderecos_rls_empresa ON app.motorista_enderecos;
CREATE POLICY pol_motorista_enderecos_empresa
  ON app.motorista_enderecos
  FOR ALL
  TO app_user
  USING (
    id_empresa = app.current_empresa_id()
  )
  WITH CHECK (
    id_empresa = app.current_empresa_id()
  );

COMMIT;
