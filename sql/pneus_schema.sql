CREATE TABLE IF NOT EXISTS app.pneus (
  id_pneu BIGSERIAL PRIMARY KEY,
  id_empresa BIGINT NOT NULL,
  numero_fogo VARCHAR(60) NOT NULL,
  marca VARCHAR(80),
  modelo VARCHAR(80),
  medida VARCHAR(40),
  tipo VARCHAR(40),
  valor NUMERIC(14,2) NOT NULL DEFAULT 0,
  status_local VARCHAR(20) NOT NULL DEFAULT 'ESTOQUE',
  ativo BOOLEAN NOT NULL DEFAULT TRUE,
  observacoes TEXT,
  usuario_atualizacao TEXT NOT NULL DEFAULT 'SISTEMA',
  criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT ck_pneus_status_local
    CHECK (status_local IN ('ESTOQUE', 'EM_USO', 'CONSERTO', 'BAIXA', 'DESCARTE'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_pneus_empresa_numero_fogo
ON app.pneus (id_empresa, numero_fogo);

CREATE INDEX IF NOT EXISTS ix_pneus_empresa_status_local
ON app.pneus (id_empresa, status_local);

CREATE TABLE IF NOT EXISTS app.pneu_vinculos_veiculo (
  id_vinculo BIGSERIAL PRIMARY KEY,
  id_empresa BIGINT NOT NULL,
  id_pneu BIGINT NOT NULL,
  id_veiculo BIGINT NOT NULL,
  posicao VARCHAR(30) NOT NULL,
  ativo BOOLEAN NOT NULL DEFAULT TRUE,
  usuario_atualizacao TEXT NOT NULL DEFAULT 'SISTEMA',
  criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_pneu_vinculo_ativo_por_pneu
ON app.pneu_vinculos_veiculo (id_empresa, id_pneu)
WHERE ativo = true;

CREATE UNIQUE INDEX IF NOT EXISTS ux_pneu_vinculo_ativo_por_posicao
ON app.pneu_vinculos_veiculo (id_empresa, id_veiculo, posicao)
WHERE ativo = true;

CREATE INDEX IF NOT EXISTS ix_pneu_vinculos_veiculo_empresa_veiculo
ON app.pneu_vinculos_veiculo (id_empresa, id_veiculo);

CREATE TABLE IF NOT EXISTS app.pneu_movimentacoes (
  id_movimentacao BIGSERIAL PRIMARY KEY,
  id_empresa BIGINT NOT NULL,
  id_pneu BIGINT NOT NULL,
  id_veiculo_origem BIGINT,
  posicao_origem VARCHAR(30),
  destino VARCHAR(20) NOT NULL,
  id_veiculo_destino BIGINT,
  posicao_destino VARCHAR(30),
  motivo VARCHAR(120),
  observacoes TEXT,
  usuario_atualizacao TEXT NOT NULL DEFAULT 'SISTEMA',
  data_movimentacao TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT ck_pneu_movimentacoes_destino
    CHECK (destino IN ('ESTOQUE', 'CONSERTO', 'BAIXA', 'DESCARTE', 'VEICULO'))
);

CREATE INDEX IF NOT EXISTS ix_pneu_movimentacoes_empresa_data
ON app.pneu_movimentacoes (id_empresa, data_movimentacao DESC);

CREATE INDEX IF NOT EXISTS ix_pneu_movimentacoes_empresa_pneu
ON app.pneu_movimentacoes (id_empresa, id_pneu);

ALTER TABLE app.pneus ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.pneus FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS pol_pneus_empresa ON app.pneus;
CREATE POLICY pol_pneus_empresa
  ON app.pneus
  FOR ALL
  TO app_user
  USING (id_empresa = app.current_empresa_id())
  WITH CHECK (id_empresa = app.current_empresa_id());

ALTER TABLE app.pneu_vinculos_veiculo ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.pneu_vinculos_veiculo FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS pol_pneu_vinculos_veiculo_empresa ON app.pneu_vinculos_veiculo;
CREATE POLICY pol_pneu_vinculos_veiculo_empresa
  ON app.pneu_vinculos_veiculo
  FOR ALL
  TO app_user
  USING (id_empresa = app.current_empresa_id())
  WITH CHECK (id_empresa = app.current_empresa_id());

ALTER TABLE app.pneu_movimentacoes ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.pneu_movimentacoes FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS pol_pneu_movimentacoes_empresa ON app.pneu_movimentacoes;
CREATE POLICY pol_pneu_movimentacoes_empresa
  ON app.pneu_movimentacoes
  FOR ALL
  TO app_user
  USING (id_empresa = app.current_empresa_id())
  WITH CHECK (id_empresa = app.current_empresa_id());

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE app.pneus TO app_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE app.pneu_vinculos_veiculo TO app_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE app.pneu_movimentacoes TO app_user;

GRANT USAGE, SELECT ON SEQUENCE app.pneus_id_pneu_seq TO app_user;
GRANT USAGE, SELECT ON SEQUENCE app.pneu_vinculos_veiculo_id_vinculo_seq TO app_user;
GRANT USAGE, SELECT ON SEQUENCE app.pneu_movimentacoes_id_movimentacao_seq TO app_user;
