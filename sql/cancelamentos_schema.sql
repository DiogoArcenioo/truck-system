CREATE TABLE IF NOT EXISTS app.cancelamento_motivos (
  id_motivo BIGSERIAL PRIMARY KEY,
  id_empresa BIGINT NOT NULL,
  codigo VARCHAR(30),
  descricao VARCHAR(180) NOT NULL,
  ativo BOOLEAN NOT NULL DEFAULT TRUE,
  usuario_atualizacao TEXT NOT NULL DEFAULT 'SISTEMA',
  criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_cancelamento_motivos_empresa_codigo
ON app.cancelamento_motivos (id_empresa, UPPER(codigo))
WHERE codigo IS NOT NULL AND BTRIM(codigo) <> '';

CREATE INDEX IF NOT EXISTS ix_cancelamento_motivos_empresa_ativo
ON app.cancelamento_motivos (id_empresa, ativo, descricao);

CREATE TABLE IF NOT EXISTS app.cancelamento_documentos (
  id_cancelamento BIGSERIAL PRIMARY KEY,
  id_empresa BIGINT NOT NULL,
  tipo_documento VARCHAR(40) NOT NULL,
  id_documento BIGINT NOT NULL,
  id_motivo BIGINT,
  motivo_descricao TEXT NOT NULL DEFAULT '',
  usuario_cancelamento TEXT NOT NULL DEFAULT 'SISTEMA',
  usuario_solicitante TEXT,
  observacao TEXT,
  status_anterior VARCHAR(40),
  status_novo VARCHAR(40),
  data_cancelamento TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  referencia_documento_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_cancelamento_documentos_empresa_data
ON app.cancelamento_documentos (id_empresa, data_cancelamento DESC, id_cancelamento DESC);

CREATE INDEX IF NOT EXISTS ix_cancelamento_documentos_empresa_documento
ON app.cancelamento_documentos (id_empresa, tipo_documento, id_documento);

CREATE INDEX IF NOT EXISTS ix_cancelamento_documentos_empresa_motivo
ON app.cancelamento_documentos (id_empresa, id_motivo);
