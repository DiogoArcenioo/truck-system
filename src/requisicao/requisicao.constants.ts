export const SITUACAO_REQUISICAO_OPCOES = [
  { label: 'Aberta', value: 'A' },
  { label: 'Fechada', value: 'F' },
  { label: 'Cancelada', value: 'C' },
] as const;

export const SITUACAO_REQUISICAO_CODIGOS = SITUACAO_REQUISICAO_OPCOES.map(
  (item) => item.value,
) as Array<(typeof SITUACAO_REQUISICAO_OPCOES)[number]['value']>;
