export const TIPO_PESSOA_OPCOES = [
  { label: 'Pessoa juridica', value: 'J' },
  { label: 'Pessoa fisica', value: 'F' },
] as const;

export const TIPO_PESSOA_CODIGOS = TIPO_PESSOA_OPCOES.map(
  (item) => item.value,
) as Array<(typeof TIPO_PESSOA_OPCOES)[number]['value']>;
