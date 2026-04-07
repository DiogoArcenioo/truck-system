BEGIN;

CREATE TABLE IF NOT EXISTS app.tipo_despesas (
  codigo character varying(8) PRIMARY KEY,
  descricao character varying(120) NOT NULL,
  aliases text[] NOT NULL DEFAULT ARRAY[]::text[],
  ordem integer NOT NULL DEFAULT 0,
  ativo boolean NOT NULL DEFAULT true
);

INSERT INTO app.tipo_despesas (codigo, descricao, aliases, ordem, ativo)
VALUES
  ('P', 'PEDAGIO', ARRAY['P', 'PEDAGIO', 'PEDAGIOS'], 10, true),
  ('A', 'ALIMENTACAO', ARRAY['A', 'ALIMENTACAO', 'ALIMENTACAO_MOTORISTA', 'ALIMENTOS'], 30, true),
  ('E', 'ESTADIA', ARRAY['E', 'ESTADIA', 'HOSPEDAGEM'], 40, true),
  ('L', 'LAVACAO', ARRAY['L', 'LAVACAO', 'LAVAGEM'], 50, true),
  ('O', 'OUTROS', ARRAY['O', 'OUTRO', 'OUTROS'], 60, true)
ON CONFLICT (codigo) DO UPDATE
SET
  descricao = EXCLUDED.descricao,
  aliases = EXCLUDED.aliases,
  ordem = EXCLUDED.ordem,
  ativo = EXCLUDED.ativo;

DELETE FROM app.tipo_despesas
WHERE UPPER(BTRIM(codigo)) = 'M';

GRANT SELECT ON TABLE app.tipo_despesas TO app_user;

COMMIT;
