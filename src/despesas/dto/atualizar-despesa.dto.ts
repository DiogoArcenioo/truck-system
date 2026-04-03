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

export class AtualizarDespesaDto {
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idVeiculo?: number;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => {
    if (value === null || value === undefined || value === '') {
      return value === '' ? undefined : value;
    }
    if (typeof value === 'number') {
      return value;
    }
    if (typeof value === 'string') {
      const numero = Number(value);
      return Number.isFinite(numero) ? numero : value;
    }
    return value;
  })
  @IsInt()
  @Min(1)
  idViagem?: number | null;

  @IsOptional()
  @IsDateString()
  data?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(40)
  tipo?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(2000)
  descricao?: string | null;

  @IsOptional()
  @Type(() => Number)
  @IsNumber({ maxDecimalPlaces: 2 })
  @Min(0)
  valor?: number;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => {
    if (value === null || value === undefined || value === '') {
      return value === '' ? undefined : value;
    }
    if (typeof value === 'number') {
      return value;
    }
    if (typeof value === 'string') {
      const numero = Number(value);
      return Number.isFinite(numero) ? numero : value;
    }
    return value;
  })
  @IsInt()
  @Min(0)
  kmRegistro?: number | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(120)
  usuarioAtualizacao?: string;
}
