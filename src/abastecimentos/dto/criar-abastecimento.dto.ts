import { Transform, Type } from 'class-transformer';
import {
  IsDateString,
  IsInt,
  IsNumber,
  IsOptional,
  IsString,
  MaxLength,
  Min,
} from 'class-validator';

export class CriarAbastecimentoDto {
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idVeiculo!: number;

  @Type(() => Number)
  @IsInt()
  @Min(1)
  idFornecedor!: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idViagem?: number;

  @IsDateString()
  dataAbastecimento!: string;

  @Type(() => Number)
  @IsNumber({ maxDecimalPlaces: 3 })
  @Min(0.001)
  litros!: number;

  @Type(() => Number)
  @IsNumber({ maxDecimalPlaces: 6 })
  @Min(0)
  valorLitro!: number;

  @IsOptional()
  @Type(() => Number)
  @IsNumber({ maxDecimalPlaces: 2 })
  @Min(0)
  valorTotal?: number;

  @Type(() => Number)
  @IsInt()
  @Min(0)
  km!: number;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(2000)
  observacao?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(120)
  usuarioAtualizacao?: string;
}
