BEGIN;

CREATE TABLE IF NOT EXISTS app.notas_fiscais_abastecimento (
  id_nota_fiscal BIGSERIAL PRIMARY KEY,
  id_empresa BIGINT NOT NULL,
  id_fornecedor INTEGER,
  id_veiculo INTEGER,
  id_motorista INTEGER,
  chave_acesso VARCHAR(44),
  numero_nf VARCHAR(20),
  serie_nf VARCHAR(10),
  modelo_nf VARCHAR(4),
  origem_lancamento VARCHAR(20) NOT NULL DEFAULT 'MANUAL',
  status_documento VARCHAR(20) NOT NULL DEFAULT 'PENDENTE',
  data_emissao TIMESTAMPTZ NOT NULL,
  data_abastecimento TIMESTAMPTZ,
  data_entrada TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  protocolo_autorizacao VARCHAR(40),
  cnpj_emitente VARCHAR(14),
  razao_social_emitente VARCHAR(180) NOT NULL,
  nome_fantasia_emitente VARCHAR(180),
  uf_emitente VARCHAR(2),
  placa_veiculo_informada VARCHAR(10),
  km_informado INTEGER,
  total_litros NUMERIC(14,3) NOT NULL DEFAULT 0,
  valor_total_produtos NUMERIC(14,2) NOT NULL DEFAULT 0,
  valor_total_desconto NUMERIC(14,2) NOT NULL DEFAULT 0,
  valor_total_nota NUMERIC(14,2) NOT NULL DEFAULT 0,
  valor_total_icms NUMERIC(14,2) NOT NULL DEFAULT 0,
  observacoes TEXT,
  xml_original TEXT,
  xml_extraido_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  usuario_atualizacao TEXT NOT NULL DEFAULT 'SISTEMA',
  criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT ck_nf_abastecimento_origem
    CHECK (origem_lancamento IN ('XML', 'MANUAL')),
  CONSTRAINT ck_nf_abastecimento_status
    CHECK (status_documento IN ('PENDENTE', 'PROCESSADA', 'CANCELADA', 'ERRO')),
  CONSTRAINT ck_nf_abastecimento_chave
    CHECK (chave_acesso IS NULL OR chave_acesso ~ '^[0-9]{44}$'),
  CONSTRAINT ck_nf_abastecimento_cnpj_emitente
    CHECK (cnpj_emitente IS NULL OR cnpj_emitente ~ '^[0-9]{14}$'),
  CONSTRAINT ck_nf_abastecimento_uf_emitente
    CHECK (uf_emitente IS NULL OR uf_emitente ~ '^[A-Z]{2}$'),
  CONSTRAINT ck_nf_abastecimento_valores
    CHECK (
      total_litros >= 0
      AND valor_total_produtos >= 0
      AND valor_total_desconto >= 0
      AND valor_total_nota >= 0
      AND valor_total_icms >= 0
    ),
  CONSTRAINT fk_nf_abastecimento_empresa
    FOREIGN KEY (id_empresa)
    REFERENCES app.empresas(id_empresa)
    ON UPDATE RESTRICT
    ON DELETE RESTRICT,
  CONSTRAINT fk_nf_abastecimento_fornecedor
    FOREIGN KEY (id_fornecedor)
    REFERENCES app.fornecedor(id_fornecedor)
    ON UPDATE RESTRICT
    ON DELETE SET NULL,
  CONSTRAINT fk_nf_abastecimento_veiculo
    FOREIGN KEY (id_veiculo)
    REFERENCES app.veiculo(id_veiculo)
    ON UPDATE RESTRICT
    ON DELETE SET NULL,
  CONSTRAINT fk_nf_abastecimento_motorista
    FOREIGN KEY (id_motorista)
    REFERENCES app.motoristas(id_motorista)
    ON UPDATE RESTRICT
    ON DELETE SET NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_nf_abastecimento_empresa_chave_acesso
ON app.notas_fiscais_abastecimento (id_empresa, chave_acesso)
WHERE chave_acesso IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_nf_abastecimento_empresa_numero_serie_fornecedor
ON app.notas_fiscais_abastecimento (id_empresa, id_fornecedor, numero_nf, serie_nf)
WHERE numero_nf IS NOT NULL
  AND serie_nf IS NOT NULL
  AND id_fornecedor IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_nf_abastecimento_empresa_data_emissao
ON app.notas_fiscais_abastecimento (id_empresa, data_emissao DESC, id_nota_fiscal DESC);

CREATE INDEX IF NOT EXISTS ix_nf_abastecimento_empresa_veiculo
ON app.notas_fiscais_abastecimento (id_empresa, id_veiculo, data_emissao DESC);

CREATE INDEX IF NOT EXISTS ix_nf_abastecimento_empresa_fornecedor
ON app.notas_fiscais_abastecimento (id_empresa, id_fornecedor, data_emissao DESC);

CREATE INDEX IF NOT EXISTS ix_nf_abastecimento_empresa_status
ON app.notas_fiscais_abastecimento (id_empresa, status_documento, origem_lancamento);

CREATE TABLE IF NOT EXISTS app.notas_fiscais_abastecimento_itens (
  id_item_nota_fiscal BIGSERIAL PRIMARY KEY,
  id_nota_fiscal BIGINT NOT NULL,
  id_empresa BIGINT NOT NULL,
  id_abastecimento INTEGER,
  numero_item INTEGER NOT NULL,
  codigo_produto VARCHAR(60),
  descricao_produto VARCHAR(180) NOT NULL,
  codigo_anp VARCHAR(20),
  ncm VARCHAR(20),
  cfop VARCHAR(10),
  unidade VARCHAR(10),
  quantidade NUMERIC(14,3) NOT NULL DEFAULT 0,
  litros NUMERIC(14,3) NOT NULL DEFAULT 0,
  valor_unitario NUMERIC(14,6) NOT NULL DEFAULT 0,
  valor_desconto NUMERIC(14,2) NOT NULL DEFAULT 0,
  valor_total NUMERIC(14,2) NOT NULL DEFAULT 0,
  combustivel VARCHAR(40),
  observacoes TEXT,
  usuario_atualizacao TEXT NOT NULL DEFAULT 'SISTEMA',
  criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT ck_nf_abastecimento_item_numero
    CHECK (numero_item > 0),
  CONSTRAINT ck_nf_abastecimento_item_valores
    CHECK (
      quantidade >= 0
      AND litros >= 0
      AND valor_unitario >= 0
      AND valor_desconto >= 0
      AND valor_total >= 0
    ),
  CONSTRAINT fk_nf_abastecimento_item_nota
    FOREIGN KEY (id_nota_fiscal)
    REFERENCES app.notas_fiscais_abastecimento(id_nota_fiscal)
    ON UPDATE RESTRICT
    ON DELETE CASCADE,
  CONSTRAINT fk_nf_abastecimento_item_empresa
    FOREIGN KEY (id_empresa)
    REFERENCES app.empresas(id_empresa)
    ON UPDATE RESTRICT
    ON DELETE RESTRICT,
  CONSTRAINT fk_nf_abastecimento_item_abastecimento
    FOREIGN KEY (id_abastecimento)
    REFERENCES app.abastecimentos(id_abastecimento)
    ON UPDATE RESTRICT
    ON DELETE SET NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_nf_abastecimento_item_empresa_nota_numero
ON app.notas_fiscais_abastecimento_itens (id_empresa, id_nota_fiscal, numero_item);

CREATE UNIQUE INDEX IF NOT EXISTS ux_nf_abastecimento_item_abastecimento
ON app.notas_fiscais_abastecimento_itens (id_abastecimento)
WHERE id_abastecimento IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_nf_abastecimento_item_empresa_nota
ON app.notas_fiscais_abastecimento_itens (id_empresa, id_nota_fiscal);

CREATE INDEX IF NOT EXISTS ix_nf_abastecimento_item_empresa_combustivel
ON app.notas_fiscais_abastecimento_itens (id_empresa, combustivel);

CREATE OR REPLACE FUNCTION app.fn_validar_nf_abastecimento_empresa()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.id_fornecedor IS NOT NULL AND NOT EXISTS (
    SELECT 1
    FROM app.fornecedor f
    WHERE f.id_fornecedor = NEW.id_fornecedor
      AND f.id_empresa = NEW.id_empresa
  ) THEN
    RAISE EXCEPTION 'Fornecedor % nao pertence a empresa %.', NEW.id_fornecedor, NEW.id_empresa;
  END IF;

  IF NEW.id_veiculo IS NOT NULL AND NOT EXISTS (
    SELECT 1
    FROM app.veiculo v
    WHERE v.id_veiculo = NEW.id_veiculo
      AND v.id_empresa = NEW.id_empresa
  ) THEN
    RAISE EXCEPTION 'Veiculo % nao pertence a empresa %.', NEW.id_veiculo, NEW.id_empresa;
  END IF;

  IF NEW.id_motorista IS NOT NULL AND NOT EXISTS (
    SELECT 1
    FROM app.motoristas m
    WHERE m.id_motorista = NEW.id_motorista
      AND m.id_empresa = NEW.id_empresa
  ) THEN
    RAISE EXCEPTION 'Motorista % nao pertence a empresa %.', NEW.id_motorista, NEW.id_empresa;
  END IF;

  NEW.atualizado_em := NOW();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_validar_nf_abastecimento_empresa ON app.notas_fiscais_abastecimento;
CREATE TRIGGER trg_validar_nf_abastecimento_empresa
BEFORE INSERT OR UPDATE ON app.notas_fiscais_abastecimento
FOR EACH ROW
EXECUTE FUNCTION app.fn_validar_nf_abastecimento_empresa();

CREATE OR REPLACE FUNCTION app.fn_validar_nf_abastecimento_item_empresa()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  v_id_empresa_nota BIGINT;
BEGIN
  SELECT nf.id_empresa
  INTO v_id_empresa_nota
  FROM app.notas_fiscais_abastecimento nf
  WHERE nf.id_nota_fiscal = NEW.id_nota_fiscal;

  IF v_id_empresa_nota IS NULL THEN
    RAISE EXCEPTION 'Nota fiscal % nao encontrada.', NEW.id_nota_fiscal;
  END IF;

  IF NEW.id_empresa <> v_id_empresa_nota THEN
    RAISE EXCEPTION 'Item com empresa % diferente da nota fiscal %.', NEW.id_empresa, NEW.id_nota_fiscal;
  END IF;

  IF NEW.id_abastecimento IS NOT NULL AND NOT EXISTS (
    SELECT 1
    FROM app.abastecimentos a
    WHERE a.id_abastecimento = NEW.id_abastecimento
      AND a.id_empresa = NEW.id_empresa
  ) THEN
    RAISE EXCEPTION 'Abastecimento % nao pertence a empresa %.', NEW.id_abastecimento, NEW.id_empresa;
  END IF;

  NEW.atualizado_em := NOW();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_validar_nf_abastecimento_item_empresa ON app.notas_fiscais_abastecimento_itens;
CREATE TRIGGER trg_validar_nf_abastecimento_item_empresa
BEFORE INSERT OR UPDATE ON app.notas_fiscais_abastecimento_itens
FOR EACH ROW
EXECUTE FUNCTION app.fn_validar_nf_abastecimento_item_empresa();

ALTER TABLE app.notas_fiscais_abastecimento ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.notas_fiscais_abastecimento FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS pol_nf_abastecimento_empresa ON app.notas_fiscais_abastecimento;
CREATE POLICY pol_nf_abastecimento_empresa
  ON app.notas_fiscais_abastecimento
  FOR ALL
  TO app_user
  USING (id_empresa = app.current_empresa_id())
  WITH CHECK (id_empresa = app.current_empresa_id());

ALTER TABLE app.notas_fiscais_abastecimento_itens ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.notas_fiscais_abastecimento_itens FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS pol_nf_abastecimento_itens_empresa ON app.notas_fiscais_abastecimento_itens;
CREATE POLICY pol_nf_abastecimento_itens_empresa
  ON app.notas_fiscais_abastecimento_itens
  FOR ALL
  TO app_user
  USING (id_empresa = app.current_empresa_id())
  WITH CHECK (id_empresa = app.current_empresa_id());

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE app.notas_fiscais_abastecimento TO app_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE app.notas_fiscais_abastecimento_itens TO app_user;

GRANT USAGE, SELECT ON SEQUENCE app.notas_fiscais_abastecimento_id_nota_fiscal_seq TO app_user;
GRANT USAGE, SELECT ON SEQUENCE app.notas_fiscais_abastecimento_itens_id_item_nota_fiscal_seq TO app_user;

COMMENT ON TABLE app.notas_fiscais_abastecimento IS 'Cabecalho das notas fiscais de abastecimento, importadas via XML ou cadastradas manualmente.';
COMMENT ON TABLE app.notas_fiscais_abastecimento_itens IS 'Itens das notas fiscais de abastecimento com vinculo opcional ao abastecimento do sistema.';
COMMENT ON COLUMN app.notas_fiscais_abastecimento.xml_original IS 'XML bruto da NF-e/NFC-e quando o documento for importado.';
COMMENT ON COLUMN app.notas_fiscais_abastecimento.xml_extraido_json IS 'Payload normalizado do XML para facilitar consultas e auditoria.';

COMMIT;
