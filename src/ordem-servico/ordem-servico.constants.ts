export const SITUACAO_OS_OPCOES = [
  { label: 'Aberta', value: 'A' },
  { label: 'Fechada', value: 'F' },
  { label: 'Cancelada', value: 'C' },
] as const;

export const SITUACAO_OS_CODIGOS = SITUACAO_OS_OPCOES.map(
  (item) => item.value,
) as Array<(typeof SITUACAO_OS_OPCOES)[number]['value']>;

export const TIPO_SERVICO_OPCOES = [
  { label: 'Corretiva', value: 'C' },
  { label: 'Preventiva', value: 'P' },
  { label: 'Emergente', value: 'E' },
] as const;

export const TIPO_SERVICO_CODIGOS = TIPO_SERVICO_OPCOES.map(
  (item) => item.value,
) as Array<(typeof TIPO_SERVICO_OPCOES)[number]['value']>;
