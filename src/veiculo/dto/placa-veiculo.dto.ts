export class PlacaVeiculoDto {
  idVeiculo!: number;
  placa!: string;
  tipo!: 'principal' | 'adicional';
  origemCampo!: 'placa' | 'placa2' | 'placa3' | 'placa4';
}
