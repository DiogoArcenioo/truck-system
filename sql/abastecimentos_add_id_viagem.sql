BEGIN;

ALTER TABLE app.abastecimentos
  ADD COLUMN IF NOT EXISTS id_viagem INTEGER;

CREATE INDEX IF NOT EXISTS ix_abastecimentos_id_viagem
  ON app.abastecimentos (id_viagem);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fk_abastecimentos_id_viagem'
      AND conrelid = 'app.abastecimentos'::regclass
  ) THEN
    ALTER TABLE app.abastecimentos
      ADD CONSTRAINT fk_abastecimentos_id_viagem
      FOREIGN KEY (id_viagem)
      REFERENCES app.viagens(id_viagem)
      ON UPDATE RESTRICT
      ON DELETE SET NULL;
  END IF;
END $$;

COMMENT ON COLUMN app.abastecimentos.id_viagem IS 'Viagem vinculada ao abastecimento.';

COMMIT;
