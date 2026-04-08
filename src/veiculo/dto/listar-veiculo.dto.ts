export class ListarVeiculoDto {
  idVeiculo!: number;
  idEmpresa!: number | null;
  idFornecedor!: number | null;
  idMarca!: number | null;
  idModelo!: number | null;
  idCombustivel!: number | null;
  idTipo!: number | null;
  idCor!: number | null;
  placa!: string;
  placasAdicionais!: string[];
  numeroMotor!: string | null;
  renavam!: string | null;
  chassi!: string | null;
  vencimentoDocumento!: string | null;
  observacao!: string | null;
  status!: string | null;
  idMotoristaAtual!: number | null;
  km!: number | null;
  kmAtual!: number | null;
  anoFabricacao!: number | null;
  anoModelo!: number | null;
  dataVencimento!: string | null;
  criadoEm!: string | null;
}
