import { Transform, Type } from 'class-transformer';
import {
  IsDateString,
  IsIn,
  IsInt,
  IsNumber,
  IsOptional,
  IsString,
  Max,
  MaxLength,
  Min,
} from 'class-validator';

const ordenacaoPermitida = [
  'id_abastecimento',
  'data_abastecimento',
  'litros',
  'valor_litro',
  'valor_total',
  'km',
  'criado_em',
  'atualizado_em',
] as const;

export class FiltroAbastecimentosDto {
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idAbastecimento?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idVeiculo?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idFornecedor?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idViagem?: number;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(200)
  texto?: string;

  @IsOptional()
  @IsDateString()
  dataDe?: string;

  @IsOptional()
  @IsDateString()
  dataAte?: string;

  @IsOptional()
  @Type(() => Number)
  @IsNumber({ maxDecimalPlaces: 3 })
  @Min(0)
  litrosMin?: number;

  @IsOptional()
  @Type(() => Number)
  @IsNumber({ maxDecimalPlaces: 3 })
  @Min(0)
  litrosMax?: number;

  @IsOptional()
  @Type(() => Number)
  @IsNumber({ maxDecimalPlaces: 6 })
  @Min(0)
  valorLitroMin?: number;

  @IsOptional()
  @Type(() => Number)
  @IsNumber({ maxDecimalPlaces: 6 })
  @Min(0)
  valorLitroMax?: number;

  @IsOptional()
  @Type(() => Number)
  @IsNumber({ maxDecimalPlaces: 2 })
  @Min(0)
  valorTotalMin?: number;

  @IsOptional()
  @Type(() => Number)
  @IsNumber({ maxDecimalPlaces: 2 })
  @Min(0)
  valorTotalMax?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  kmMin?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  kmMax?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  pagina?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(100)
  limite?: number;

  @IsOptional()
  @IsIn(ordenacaoPermitida)
  ordenarPor?: (typeof ordenacaoPermitida)[number];

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsIn(['ASC', 'DESC'])
  ordem?: 'ASC' | 'DESC';
}
