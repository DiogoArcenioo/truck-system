export class ListarProdutoDto {
  idProduto!: number;
  descricaoProduto!: string;
  dataCadastro!: string;
  idGrupoProduto!: number | null;
  idSubgrupo!: number | null;
  observacao!: string | null;
  situacao!: string;
  idUn!: number | null;
  idMarca!: number | null;
  usuarioAtualizacao!: string | null;
  tipoProduto!: string | null;
  atualizadoEm!: string;
}
