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
  ('P', 'Pedagio', ARRAY['P', 'PEDAGIO', 'PEDAGIOS'], 10, true),
  ('M', 'Multa', ARRAY['M', 'MULTA', 'MULTAS'], 20, true),
  ('A', 'Alimentacao', ARRAY['A', 'ALIMENTACAO', 'ALIMENTACAO_MOTORISTA', 'ALIMENTOS'], 30, true),
  ('E', 'Estadia', ARRAY['E', 'ESTADIA', 'HOSPEDAGEM'], 40, true),
  ('L', 'Lavacao', ARRAY['L', 'LAVACAO', 'LAVAGEM'], 50, true),
  ('O', 'Outros', ARRAY['O', 'OUTRO', 'OUTROS'], 60, true)
ON CONFLICT (codigo) DO UPDATE
SET
  descricao = EXCLUDED.descricao,
  aliases = EXCLUDED.aliases,
  ordem = EXCLUDED.ordem,
  ativo = EXCLUDED.ativo;

GRANT SELECT ON TABLE app.tipo_despesas TO app_user;

COMMIT;
