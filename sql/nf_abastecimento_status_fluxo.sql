BEGIN;

ALTER TABLE app.notas_fiscais_abastecimento
  ADD COLUMN IF NOT EXISTS status_fiscal VARCHAR(30) NOT NULL DEFAULT 'PENDENTE';

ALTER TABLE app.notas_fiscais_abastecimento
  ADD COLUMN IF NOT EXISTS status_operacional VARCHAR(30) NOT NULL DEFAULT 'IMPORTADA';

ALTER TABLE app.notas_fiscais_abastecimento
  ADD COLUMN IF NOT EXISTS motivo_status TEXT;

ALTER TABLE app.notas_fiscais_abastecimento
  ADD COLUMN IF NOT EXISTS efetivada_em TIMESTAMPTZ;

ALTER TABLE app.notas_fiscais_abastecimento
  ADD COLUMN IF NOT EXISTS efetivada_por TEXT;

UPDATE app.notas_fiscais_abastecimento
SET
  status_fiscal = CASE COALESCE(status_documento, 'PENDENTE')
    WHEN 'PROCESSADA' THEN 'AUTORIZADA'
    WHEN 'CANCELADA' THEN 'CANCELADA'
    WHEN 'ERRO' THEN 'ERRO_IMPORTACAO'
    ELSE 'PENDENTE'
  END,
  status_operacional = CASE COALESCE(status_documento, 'PENDENTE')
    WHEN 'PROCESSADA' THEN 'LANCADA'
    WHEN 'CANCELADA' THEN 'BLOQUEADA'
    WHEN 'ERRO' THEN 'DIVERGENCIA'
    ELSE 'IMPORTADA'
  END
WHERE
  COALESCE(status_fiscal, '') = ''
  OR COALESCE(status_operacional, '') = ''
  OR status_fiscal = 'PENDENTE'
  OR status_operacional = 'IMPORTADA';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'ck_nf_abastecimento_status_fiscal'
      AND conrelid = 'app.notas_fiscais_abastecimento'::regclass
  ) THEN
    ALTER TABLE app.notas_fiscais_abastecimento
      ADD CONSTRAINT ck_nf_abastecimento_status_fiscal
      CHECK (
        status_fiscal IN (
          'PENDENTE',
          'AUTORIZADA',
          'CANCELADA',
          'ERRO_IMPORTACAO'
        )
      );
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'ck_nf_abastecimento_status_operacional'
      AND conrelid = 'app.notas_fiscais_abastecimento'::regclass
  ) THEN
    ALTER TABLE app.notas_fiscais_abastecimento
      ADD CONSTRAINT ck_nf_abastecimento_status_operacional
      CHECK (
        status_operacional IN (
          'IMPORTADA',
          'AGUARDANDO_VINCULOS',
          'PRONTA',
          'LANCADA',
          'DIVERGENCIA',
          'BLOQUEADA'
        )
      );
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS ix_nf_abastecimento_empresa_status_fiscal
ON app.notas_fiscais_abastecimento (id_empresa, status_fiscal, data_emissao DESC, id_nota_fiscal DESC);

CREATE INDEX IF NOT EXISTS ix_nf_abastecimento_empresa_status_operacional
ON app.notas_fiscais_abastecimento (id_empresa, status_operacional, data_emissao DESC, id_nota_fiscal DESC);

CREATE INDEX IF NOT EXISTS ix_nf_abastecimento_empresa_efetivada_em
ON app.notas_fiscais_abastecimento (id_empresa, efetivada_em DESC, id_nota_fiscal DESC);

COMMENT ON COLUMN app.notas_fiscais_abastecimento.status_fiscal IS
  'Situacao fiscal da nota: PENDENTE, AUTORIZADA, CANCELADA ou ERRO_IMPORTACAO.';

COMMENT ON COLUMN app.notas_fiscais_abastecimento.status_operacional IS
  'Situacao operacional da nota no sistema: IMPORTADA, AGUARDANDO_VINCULOS, PRONTA, LANCADA, DIVERGENCIA ou BLOQUEADA.';

COMMENT ON COLUMN app.notas_fiscais_abastecimento.motivo_status IS
  'Motivo complementar para bloqueio, divergencia ou observacao de fluxo da nota fiscal.';

COMMENT ON COLUMN app.notas_fiscais_abastecimento.efetivada_em IS
  'Data e hora em que a nota fiscal foi efetivada no fluxo operacional.';

COMMENT ON COLUMN app.notas_fiscais_abastecimento.efetivada_por IS
  'Usuario que efetivou a nota fiscal no sistema.';

COMMIT;
