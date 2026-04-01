BEGIN;

-- Remove constraints unicas antigas de CNPJ/CPF sem escopo por empresa.
DO $$
DECLARE
  item RECORD;
BEGIN
  FOR item IN
    SELECT c.conname
    FROM pg_constraint c
    INNER JOIN pg_class t ON t.oid = c.conrelid
    INNER JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = 'app'
      AND t.relname = 'fornecedor'
      AND c.contype = 'u'
      AND pg_get_constraintdef(c.oid) ILIKE '%(cnpj)%'
      AND pg_get_constraintdef(c.oid) NOT ILIKE '%id_empresa%'
  LOOP
    EXECUTE format('ALTER TABLE app.fornecedor DROP CONSTRAINT %I', item.conname);
  END LOOP;
END $$;

DO $$
DECLARE
  item RECORD;
BEGIN
  FOR item IN
    SELECT c.conname
    FROM pg_constraint c
    INNER JOIN pg_class t ON t.oid = c.conrelid
    INNER JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = 'app'
      AND t.relname = 'fornecedor'
      AND c.contype = 'u'
      AND pg_get_constraintdef(c.oid) ILIKE '%(cpf)%'
      AND pg_get_constraintdef(c.oid) NOT ILIKE '%id_empresa%'
  LOOP
    EXECUTE format('ALTER TABLE app.fornecedor DROP CONSTRAINT %I', item.conname);
  END LOOP;
END $$;

-- Remove indexes unicos antigos de CNPJ/CPF sem id_empresa.
DO $$
DECLARE
  item RECORD;
BEGIN
  FOR item IN
    SELECT indexname
    FROM pg_indexes
    WHERE schemaname = 'app'
      AND tablename = 'fornecedor'
      AND indexdef ILIKE 'CREATE UNIQUE INDEX%'
      AND indexdef ILIKE '%(cnpj)%'
      AND indexdef NOT ILIKE '%id_empresa%'
  LOOP
    EXECUTE format('DROP INDEX IF EXISTS app.%I', item.indexname);
  END LOOP;
END $$;

DO $$
DECLARE
  item RECORD;
BEGIN
  FOR item IN
    SELECT indexname
    FROM pg_indexes
    WHERE schemaname = 'app'
      AND tablename = 'fornecedor'
      AND indexdef ILIKE 'CREATE UNIQUE INDEX%'
      AND indexdef ILIKE '%(cpf)%'
      AND indexdef NOT ILIKE '%id_empresa%'
  LOOP
    EXECUTE format('DROP INDEX IF EXISTS app.%I', item.indexname);
  END LOOP;
END $$;

-- Regras finais: unico por empresa para documentos.
CREATE UNIQUE INDEX IF NOT EXISTS uq_fornecedor_empresa_cnpj
  ON app.fornecedor (id_empresa, cnpj)
  WHERE cnpj IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_fornecedor_empresa_cpf
  ON app.fornecedor (id_empresa, cpf)
  WHERE cpf IS NOT NULL;

COMMIT;
