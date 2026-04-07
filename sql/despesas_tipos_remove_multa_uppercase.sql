BEGIN;

UPDATE app.despesas
SET
  descricao = UPPER(descricao),
  tipo = UPPER(TRIM(tipo::text))
WHERE
  (descricao IS NOT NULL AND descricao <> UPPER(descricao))
  OR (tipo IS NOT NULL AND TRIM(tipo::text) <> UPPER(TRIM(tipo::text)));

UPDATE app.tipo_despesas
SET
  descricao = UPPER(descricao),
  aliases = COALESCE(
    ARRAY(
      SELECT UPPER(BTRIM(alias_item))
      FROM UNNEST(COALESCE(aliases, ARRAY[]::text[])) AS alias_item
      WHERE BTRIM(alias_item) <> ''
    ),
    ARRAY[]::text[]
  );

DELETE FROM app.tipo_despesas
WHERE UPPER(BTRIM(codigo)) = 'M';

COMMIT;
