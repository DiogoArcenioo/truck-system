export const TIPOS_DOCUMENTO_CANCELAMENTO = [
  { value: 'VIAGEM', label: 'Viagem', statusReaberto: 'A' },
  { value: 'ORDEM_SERVICO', label: 'Ordem de servico', statusReaberto: 'A' },
  { value: 'REQUISICAO', label: 'Requisicao', statusReaberto: 'A' },
] as const;

export const TIPOS_DOCUMENTO_CANCELAMENTO_CODIGOS =
  TIPOS_DOCUMENTO_CANCELAMENTO.map((item) => item.value) as Array<
    (typeof TIPOS_DOCUMENTO_CANCELAMENTO)[number]['value']
  >;

export type TipoDocumentoCancelamento =
  (typeof TIPOS_DOCUMENTO_CANCELAMENTO_CODIGOS)[number];
