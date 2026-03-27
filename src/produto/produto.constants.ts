export const SITUACAO_PRODUTO_OPCOES = [
  { label: 'Ativo', value: 'A' },
  { label: 'Inativo', value: 'I' },
] as const;

export const SITUACAO_PRODUTO_CODIGOS = SITUACAO_PRODUTO_OPCOES.map(
  (item) => item.value,
) as Array<(typeof SITUACAO_PRODUTO_OPCOES)[number]['value']>;

export const TIPO_PRODUTO_OPCOES = [
  { label: 'Produto', value: 'P' },
  { label: 'Servico', value: 'S' },
] as const;

export const TIPO_PRODUTO_CODIGOS = TIPO_PRODUTO_OPCOES.map(
  (item) => item.value,
) as Array<(typeof TIPO_PRODUTO_OPCOES)[number]['value']>;
