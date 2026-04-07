BEGIN;

-- Normaliza descricoes ja existentes para UPPERCASE.
UPDATE app.despesas
SET descricao = UPPER(descricao)
WHERE descricao IS NOT NULL
  AND descricao <> UPPER(descricao);

-- Normaliza variacoes de "lavacao/lavagem" para o codigo canonico "L".
UPDATE app.despesas
SET tipo = 'L'
WHERE tipo IS NOT NULL
  AND TRIM(tipo::text) <> ''
  AND UPPER(TRIM(tipo::text)) IN ('LAVACAO', 'LAVAGEM', 'L');

COMMIT;
