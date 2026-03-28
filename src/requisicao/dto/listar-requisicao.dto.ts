export class ListarRequisicaoItemDto {
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

export class ListarRequisicaoDto {
  idRequisicao!: number;
  idOs!: number;
  dataRequisicao!: string;
  situacao!: string;
  observacao!: string | null;
  usuarioAtualizacao!: string | null;
  criadoEm!: string;
  atualizadoEm!: string;
  valorTotal!: number;
  itens!: ListarRequisicaoItemDto[];
}
