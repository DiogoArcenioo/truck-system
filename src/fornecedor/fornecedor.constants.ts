export const TIPO_PESSOA_OPCOES = [
  { label: 'Pessoa jurídica', value: 'J' },
  { label: 'Pessoa física', value: 'F' },
] as const;

export const TIPO_PESSOA_CODIGOS = TIPO_PESSOA_OPCOES.map(
  (item) => item.value,
) as Array<(typeof TIPO_PESSOA_OPCOES)[number]['value']>;
