export class ListarOrdemServicoItemDto {
  idItem!: number;
  idRequisicao!: number;
  idProduto!: number;
  descricaoProduto!: string | null;
  qtdProduto!: number;
  valorUn!: number;
  valorTotalItem!: number;
  observacao!: string | null;
  usuarioAtualizacao!: string | null;
  criadoEm!: string;
  atualizadoEm!: string;
}

export class ListarOrdemServicoRequisicaoDto {
  idRequisicao!: number;
  idOs!: number;
  dataRequisicao!: string;
  situacao!: string;
  observacao!: string | null;
  usuarioAtualizacao!: string | null;
  criadoEm!: string;
  atualizadoEm!: string;
  itens!: ListarOrdemServicoItemDto[];
}

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
  requisicao!: ListarOrdemServicoRequisicaoDto | null;
  itens!: ListarOrdemServicoItemDto[];
}
