export const STATUS_MOTORISTA_OPCOES = [
  { codigo: 'A', label: 'Ativo' },
  { codigo: 'I', label: 'Inativo' },
  { codigo: 'F', label: 'Ferias' },
] as const;

export const STATUS_MOTORISTA_CODIGOS = STATUS_MOTORISTA_OPCOES.map(
  (item) => item.codigo,
) as Array<(typeof STATUS_MOTORISTA_OPCOES)[number]['codigo']>;

export const CATEGORIAS_CNH_OPCOES = [
  { codigo: 'A', label: 'A (Moto)' },
  { codigo: 'B', label: 'B (Carro)' },
  { codigo: 'C', label: 'C (Caminhao)' },
  { codigo: 'D', label: 'D (Onibus)' },
  { codigo: 'E', label: 'E (Carreta)' },
  { codigo: 'AB', label: 'AB (Moto e carro)' },
  { codigo: 'AC', label: 'AC (Moto e caminhao)' },
  { codigo: 'AD', label: 'AD (Moto e onibus)' },
  { codigo: 'AE', label: 'AE (Moto e carreta)' },
] as const;

export const CATEGORIAS_CNH_CODIGOS = CATEGORIAS_CNH_OPCOES.map(
  (item) => item.codigo,
) as Array<(typeof CATEGORIAS_CNH_OPCOES)[number]['codigo']>;

export const STATUS_MOTORISTA_LABEL_POR_CODIGO = Object.fromEntries(
  STATUS_MOTORISTA_OPCOES.map((item) => [item.codigo, item.label]),
) as Record<(typeof STATUS_MOTORISTA_CODIGOS)[number], string>;
