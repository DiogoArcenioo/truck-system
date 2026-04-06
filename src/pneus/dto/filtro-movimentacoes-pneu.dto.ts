import { Transform, Type } from 'class-transformer';
import {
  IsDateString,
  IsIn,
  IsInt,
  IsOptional,
  Max,
  Min,
} from 'class-validator';

const destinosPermitidos = [
  'ESTOQUE',
  'CONSERTO',
  'BAIXA',
  'DESCARTE',
  'VEICULO',
] as const;

export class FiltroMovimentacoesPneuDto {
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idPneu?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idVeiculo?: number;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsIn(destinosPermitidos)
  destino?: (typeof destinosPermitidos)[number];

  @IsOptional()
  @IsDateString()
  dataDe?: string;

  @IsOptional()
  @IsDateString()
  dataAte?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(200)
  limite?: number;
}

