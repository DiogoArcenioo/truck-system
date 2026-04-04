ALTER TABLE IF EXISTS app.viagens
ADD COLUMN IF NOT EXISTS peso numeric(14,3);

COMMENT ON COLUMN app.viagens.peso IS 'Peso da carga da viagem (kg).';
