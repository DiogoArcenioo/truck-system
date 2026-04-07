BEGIN;

CREATE OR REPLACE FUNCTION app.fn_despesas_descricao_uppercase()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.descricao IS NULL THEN
    RETURN NEW;
  END IF;

  NEW.descricao := NULLIF(UPPER(BTRIM(NEW.descricao)), '');
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_despesas_descricao_uppercase ON app.despesas;

CREATE TRIGGER trg_despesas_descricao_uppercase
BEFORE INSERT OR UPDATE OF descricao
ON app.despesas
FOR EACH ROW
EXECUTE FUNCTION app.fn_despesas_descricao_uppercase();

COMMIT;
