import { Type } from 'class-transformer';
import { IsInt, IsOptional, Matches, Min } from 'class-validator';

export class FiltroSerieRelatorioFaturamentoDto {
  @IsOptional()
  @Matches(/^\d{4}-(0[1-9]|1[0-2])$/)
  inicioMes?: string;

  @IsOptional()
  @Matches(/^\d{4}-(0[1-9]|1[0-2])$/)
  fimMes?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idVeiculo?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idMotorista?: number;
}
