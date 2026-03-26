export class ListarVeiculoDto {
  idVeiculo!: number;
  placa!: string;
  placasAdicionais!: string[];
  idMotoristaAtual!: number | null;
  kmAtual!: number | null;
  anoFabricacao!: number | null;
  anoModelo!: number | null;
  dataVencimento!: string | null;
}
