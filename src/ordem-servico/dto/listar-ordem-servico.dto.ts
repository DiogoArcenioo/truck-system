export class ListarOrdemServicoDto {
  idOs!: number;
  idVeiculo!: number;
  idFornecedor!: number | null;
  dataCadastro!: string;
  dataFechamento!: string | null;
  tempoOsMin!: number | null;
  situacaoOs!: string;
  observacao!: string | null;
  valorTotal!: number;
  kmVeiculo!: number | null;
  chaveNfe!: string | null;
  usuarioAtualizacao!: string | null;
  tipoServico!: string | null;
  dataAtualizacao!: string;
  atualizadoEm!: string | null;
  qtdRequisicoes!: number;
}
