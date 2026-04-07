BEGIN;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname = 'app'
      AND t.typname = 'tipo_despesa_enum'
      AND t.typtype = 'd'
  ) THEN
    EXECUTE 'ALTER DOMAIN app.tipo_despesa_enum DROP CONSTRAINT IF EXISTS tipo_despesa_enum_check';
    EXECUTE $ddl$
      ALTER DOMAIN app.tipo_despesa_enum
      ADD CONSTRAINT tipo_despesa_enum_check
      CHECK (
        VALUE = ANY (
          ARRAY[
            'P'::bpchar,
            'A'::bpchar,
            'E'::bpchar,
            'L'::bpchar,
            'O'::bpchar,
            'H'::bpchar,
            'M'::bpchar
          ]
        )
      )
    $ddl$;
  END IF;
END $$;

COMMIT;
