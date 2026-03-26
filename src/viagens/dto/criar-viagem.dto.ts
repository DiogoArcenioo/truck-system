import { Transform, Type } from 'class-transformer';
import {
  IsDateString,
  IsInt,
  IsNumber,
  IsOptional,
  IsString,
  Length,
  MaxLength,
  Min,
} from 'class-validator';

export class CriarViagemDto {
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idVeiculo!: number;

  @Type(() => Number)
  @IsInt()
  @Min(1)
  idMotorista!: number;

  @IsDateString()
  dataInicio!: string;

  @IsOptional()
  @IsDateString()
  dataFim?: string;

  @Type(() => Number)
  @IsInt()
  @Min(0)
  kmInicial!: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  kmFinal?: number;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @Length(1, 1)
  status?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(2000)
  observacao?: string;

  @IsOptional()
  @Type(() => Number)
  @IsNumber({ maxDecimalPlaces: 2 })
  @Min(0)
  valorFrete?: number;

  @IsOptional()
  @Type(() => Number)
  @IsNumber({ maxDecimalPlaces: 2 })
  @Min(0)
  media?: number;

  @IsOptional()
  @Type(() => Number)
  @IsNumber({ maxDecimalPlaces: 2 })
  @Min(0)
  totalDespesas?: number;

  @IsOptional()
  @Type(() => Number)
  @IsNumber({ maxDecimalPlaces: 2 })
  @Min(0)
  totalAbastecimentos?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  totalKm?: number;

  @IsOptional()
  @Type(() => Number)
  @IsNumber({ maxDecimalPlaces: 2 })
  totalLucro?: number;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @MaxLength(150)
  usuarioAtualizacao?: string;
}
