BEGIN;

CREATE TABLE IF NOT EXISTS app.motorista_enderecos (
  id_endereco BIGSERIAL PRIMARY KEY,
  id_motorista INTEGER NOT NULL,
  id_empresa INTEGER NOT NULL,
  logradouro VARCHAR(180) NOT NULL,
  numero VARCHAR(20),
  complemento VARCHAR(120),
  bairro VARCHAR(120),
  cidade VARCHAR(120),
  estado VARCHAR(2),
  cep VARCHAR(9),
  principal BOOLEAN NOT NULL DEFAULT false,
  usuario_atualizacao TEXT,
  criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT fk_motorista_endereco_motorista
    FOREIGN KEY (id_motorista)
    REFERENCES app.motoristas(id_motorista)
    ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_motorista_enderecos_motorista
  ON app.motorista_enderecos (id_motorista);

CREATE INDEX IF NOT EXISTS idx_motorista_enderecos_empresa
  ON app.motorista_enderecos (id_empresa);

CREATE INDEX IF NOT EXISTS idx_motorista_enderecos_motorista_principal
  ON app.motorista_enderecos (id_motorista, principal DESC, id_endereco ASC);

CREATE UNIQUE INDEX IF NOT EXISTS uq_motorista_endereco_principal
  ON app.motorista_enderecos (id_motorista)
  WHERE principal = true;

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

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE app.motorista_enderecos TO app_user;

DO $$
DECLARE
  nome_sequence text;
BEGIN
  nome_sequence := pg_get_serial_sequence('app.motorista_enderecos', 'id_endereco');

  IF nome_sequence IS NOT NULL THEN
    EXECUTE format('GRANT USAGE, SELECT ON SEQUENCE %s TO app_user', nome_sequence);
  END IF;
END $$;

COMMIT;
