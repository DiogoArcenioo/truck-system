BEGIN;

ALTER TABLE app.abastecimentos
  ADD COLUMN IF NOT EXISTS origem_lancamento VARCHAR(20) NOT NULL DEFAULT 'MANUAL';

ALTER TABLE app.abastecimentos
  ADD COLUMN IF NOT EXISTS id_nota_fiscal BIGINT;

DO $$
DECLARE
  v_coluna_data TEXT;
BEGIN
  SELECT column_name
  INTO v_coluna_data
  FROM information_schema.columns
  WHERE table_schema = 'app'
    AND table_name = 'abastecimentos'
    AND column_name IN ('data_abastecimento', 'data', 'data_lancamento', 'dt_abastecimento')
  ORDER BY CASE column_name
    WHEN 'data_abastecimento' THEN 1
    WHEN 'data' THEN 2
    WHEN 'data_lancamento' THEN 3
    WHEN 'dt_abastecimento' THEN 4
    ELSE 99
  END
  LIMIT 1;

  IF v_coluna_data IS NULL THEN
    RAISE EXCEPTION 'Nenhuma coluna de data compativel foi encontrada em app.abastecimentos.';
  END IF;

  EXECUTE format(
    'CREATE INDEX IF NOT EXISTS ix_abastecimentos_empresa_origem ON app.abastecimentos (id_empresa, origem_lancamento, %I DESC)',
    v_coluna_data
  );
END $$;

CREATE INDEX IF NOT EXISTS ix_abastecimentos_empresa_id_nota_fiscal
  ON app.abastecimentos (id_empresa, id_nota_fiscal);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fk_abastecimentos_id_nota_fiscal'
      AND conrelid = 'app.abastecimentos'::regclass
  ) THEN
    ALTER TABLE app.abastecimentos
      ADD CONSTRAINT fk_abastecimentos_id_nota_fiscal
      FOREIGN KEY (id_nota_fiscal)
      REFERENCES app.notas_fiscais_abastecimento(id_nota_fiscal)
      ON UPDATE RESTRICT
      ON DELETE SET NULL;
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'ck_abastecimentos_origem_lancamento'
      AND conrelid = 'app.abastecimentos'::regclass
  ) THEN
    ALTER TABLE app.abastecimentos
      ADD CONSTRAINT ck_abastecimentos_origem_lancamento
      CHECK (origem_lancamento IN ('MANUAL', 'NOTA_FISCAL'));
  END IF;
END $$;

COMMENT ON COLUMN app.abastecimentos.origem_lancamento IS 'Origem do lancamento do abastecimento: manual ou gerado por nota fiscal.';
COMMENT ON COLUMN app.abastecimentos.id_nota_fiscal IS 'Nota fiscal de abastecimento que originou o lancamento.';

COMMIT;
