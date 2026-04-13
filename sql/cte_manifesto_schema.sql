CREATE TABLE IF NOT EXISTS app.ctes (
  id_cte BIGSERIAL PRIMARY KEY,
  id_empresa BIGINT NOT NULL,
  numero_cte INTEGER NOT NULL,
  serie INTEGER NOT NULL DEFAULT 0,
  chave_cte VARCHAR(44) NOT NULL,
  status_documento VARCHAR(20) NOT NULL DEFAULT 'AUTORIZADO',
  cstat INTEGER,
  motivo_status TEXT,
  protocolo VARCHAR(30),
  data_emissao TIMESTAMPTZ NOT NULL,
  data_autorizacao TIMESTAMPTZ,
  cfop VARCHAR(4),
  natureza_operacao VARCHAR(180),
  municipio_inicio VARCHAR(120),
  uf_inicio CHAR(2),
  municipio_fim VARCHAR(120),
  uf_fim CHAR(2),
  remetente_nome VARCHAR(180),
  remetente_cnpj VARCHAR(14),
  destinatario_nome VARCHAR(180),
  destinatario_cnpj VARCHAR(14),
  tomador_nome VARCHAR(180),
  tomador_cnpj VARCHAR(14),
  motorista_nome VARCHAR(180),
  motorista_cpf VARCHAR(11),
  placa_tracao VARCHAR(12),
  placa_reboque1 VARCHAR(12),
  placa_reboque2 VARCHAR(12),
  placa_reboque3 VARCHAR(12),
  valor_total_prestacao NUMERIC(14,2) NOT NULL DEFAULT 0,
  valor_receber NUMERIC(14,2) NOT NULL DEFAULT 0,
  valor_icms NUMERIC(14,2) NOT NULL DEFAULT 0,
  valor_carga NUMERIC(14,2) NOT NULL DEFAULT 0,
  peso_bruto NUMERIC(14,3) NOT NULL DEFAULT 0,
  quantidade_volumes NUMERIC(14,3) NOT NULL DEFAULT 0,
  chave_mdfe VARCHAR(44),
  qr_code_url TEXT,
  observacao TEXT,
  ativo BOOLEAN NOT NULL DEFAULT TRUE,
  usuario_atualizacao TEXT NOT NULL DEFAULT 'SISTEMA',
  criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT ck_ctes_status_documento
    CHECK (status_documento IN ('AUTORIZADO', 'CANCELADO', 'DENEGADO', 'PENDENTE'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_ctes_empresa_chave
ON app.ctes (id_empresa, chave_cte);

CREATE UNIQUE INDEX IF NOT EXISTS ux_ctes_empresa_numero_serie
ON app.ctes (id_empresa, numero_cte, serie);

CREATE INDEX IF NOT EXISTS ix_ctes_empresa_data
ON app.ctes (id_empresa, data_emissao DESC, id_cte DESC);

CREATE INDEX IF NOT EXISTS ix_ctes_empresa_status
ON app.ctes (id_empresa, status_documento, ativo);

CREATE TABLE IF NOT EXISTS app.manifestos (
  id_manifesto BIGSERIAL PRIMARY KEY,
  id_empresa BIGINT NOT NULL,
  numero_manifesto INTEGER NOT NULL,
  serie INTEGER NOT NULL DEFAULT 0,
  chave_mdfe VARCHAR(44) NOT NULL,
  status_documento VARCHAR(20) NOT NULL DEFAULT 'AUTORIZADO',
  cstat INTEGER,
  motivo_status TEXT,
  protocolo VARCHAR(30),
  data_emissao TIMESTAMPTZ NOT NULL,
  data_autorizacao TIMESTAMPTZ,
  data_inicio_viagem TIMESTAMPTZ,
  uf_inicio CHAR(2),
  uf_fim CHAR(2),
  municipio_carregamento VARCHAR(120),
  percurso_ufs TEXT,
  rntrc VARCHAR(20),
  placa_tracao VARCHAR(12),
  placa_reboque1 VARCHAR(12),
  placa_reboque2 VARCHAR(12),
  placa_reboque3 VARCHAR(12),
  condutor_nome VARCHAR(180),
  condutor_cpf VARCHAR(11),
  quantidade_cte INTEGER NOT NULL DEFAULT 0,
  chaves_cte TEXT,
  valor_carga NUMERIC(14,2) NOT NULL DEFAULT 0,
  quantidade_carga NUMERIC(14,3) NOT NULL DEFAULT 0,
  produto_predominante VARCHAR(180),
  seguradora_nome VARCHAR(180),
  apolice_numero VARCHAR(80),
  averbacao_numero VARCHAR(80),
  qr_code_url TEXT,
  observacao TEXT,
  ativo BOOLEAN NOT NULL DEFAULT TRUE,
  usuario_atualizacao TEXT NOT NULL DEFAULT 'SISTEMA',
  criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT ck_manifestos_status_documento
    CHECK (status_documento IN ('AUTORIZADO', 'CANCELADO', 'ENCERRADO', 'PENDENTE'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_manifestos_empresa_chave
ON app.manifestos (id_empresa, chave_mdfe);

CREATE UNIQUE INDEX IF NOT EXISTS ux_manifestos_empresa_numero_serie
ON app.manifestos (id_empresa, numero_manifesto, serie);

CREATE INDEX IF NOT EXISTS ix_manifestos_empresa_data
ON app.manifestos (id_empresa, data_emissao DESC, id_manifesto DESC);

CREATE INDEX IF NOT EXISTS ix_manifestos_empresa_status
ON app.manifestos (id_empresa, status_documento, ativo);

ALTER TABLE app.ctes ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.ctes FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS pol_ctes_empresa ON app.ctes;
CREATE POLICY pol_ctes_empresa
  ON app.ctes
  FOR ALL
  TO app_user
  USING (id_empresa = app.current_empresa_id())
  WITH CHECK (id_empresa = app.current_empresa_id());

ALTER TABLE app.manifestos ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.manifestos FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS pol_manifestos_empresa ON app.manifestos;
CREATE POLICY pol_manifestos_empresa
  ON app.manifestos
  FOR ALL
  TO app_user
  USING (id_empresa = app.current_empresa_id())
  WITH CHECK (id_empresa = app.current_empresa_id());

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE app.ctes TO app_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE app.manifestos TO app_user;
GRANT USAGE, SELECT ON SEQUENCE app.ctes_id_cte_seq TO app_user;
GRANT USAGE, SELECT ON SEQUENCE app.manifestos_id_manifesto_seq TO app_user;
