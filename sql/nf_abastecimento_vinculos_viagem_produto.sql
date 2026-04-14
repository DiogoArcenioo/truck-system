BEGIN;

ALTER TABLE app.notas_fiscais_abastecimento
  ADD COLUMN IF NOT EXISTS id_viagem INTEGER;

CREATE INDEX IF NOT EXISTS ix_nf_abastecimento_empresa_viagem
ON app.notas_fiscais_abastecimento (id_empresa, id_viagem, data_emissao DESC);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fk_nf_abastecimento_viagem'
      AND conrelid = 'app.notas_fiscais_abastecimento'::regclass
  ) THEN
    ALTER TABLE app.notas_fiscais_abastecimento
      ADD CONSTRAINT fk_nf_abastecimento_viagem
      FOREIGN KEY (id_viagem)
      REFERENCES app.viagens(id_viagem)
      ON UPDATE RESTRICT
      ON DELETE SET NULL;
  END IF;
END $$;

ALTER TABLE app.notas_fiscais_abastecimento_itens
  ADD COLUMN IF NOT EXISTS id_produto INTEGER;

ALTER TABLE app.notas_fiscais_abastecimento_itens
  ADD COLUMN IF NOT EXISTS codigo_produto_xml VARCHAR(60);

CREATE INDEX IF NOT EXISTS ix_nf_abastecimento_item_empresa_produto
ON app.notas_fiscais_abastecimento_itens (id_empresa, id_produto);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fk_nf_abastecimento_item_produto'
      AND conrelid = 'app.notas_fiscais_abastecimento_itens'::regclass
  ) THEN
    ALTER TABLE app.notas_fiscais_abastecimento_itens
      ADD CONSTRAINT fk_nf_abastecimento_item_produto
      FOREIGN KEY (id_produto)
      REFERENCES app.produto(id_produto)
      ON UPDATE RESTRICT
      ON DELETE SET NULL;
  END IF;
END $$;

COMMENT ON COLUMN app.notas_fiscais_abastecimento.id_viagem IS 'Viagem vinculada automaticamente ou manualmente a nota fiscal de abastecimento.';
COMMENT ON COLUMN app.notas_fiscais_abastecimento_itens.id_produto IS 'Produto do cadastro interno vinculado ao item da nota fiscal.';
COMMENT ON COLUMN app.notas_fiscais_abastecimento_itens.codigo_produto_xml IS 'Codigo do item conforme veio no XML original.';

COMMIT;
