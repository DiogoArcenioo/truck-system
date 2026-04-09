export class PlacaVeiculoDto {
  idVeiculo!: number;
  idMotoristaAtual!: number | null;
  kmAtual!: number | null;
  placa!: string;
  tipo!: 'principal' | 'adicional';
  origemCampo!: 'placa' | 'placa2' | 'placa3' | 'placa4';
}
