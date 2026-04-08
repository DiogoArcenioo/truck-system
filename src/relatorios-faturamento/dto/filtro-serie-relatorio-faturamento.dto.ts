import { IsOptional, Matches } from 'class-validator';

export class FiltroSerieRelatorioFaturamentoDto {
  @IsOptional()
  @Matches(/^\d{4}-(0[1-9]|1[0-2])$/)
  inicioMes?: string;

  @IsOptional()
  @Matches(/^\d{4}-(0[1-9]|1[0-2])$/)
  fimMes?: string;
}
