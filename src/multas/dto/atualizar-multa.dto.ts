import { Transform, Type } from 'class-transformer';
import {
  IsDateString,
  IsInt,
  IsNumber,
  IsOptional,
  IsString,
  IsUppercase,
  Matches,
  MaxLength,
  Min,
} from 'class-validator';

function transformarNumeroNullable(value: unknown) {
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
}

export class AtualizarMultaDto {
  @IsOptional()
  @Transform(({ value }: { value: unknown }) => transformarNumeroNullable(value))
  @IsInt()
  @Min(1)
  idMotorista?: number;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => transformarNumeroNullable(value))
  @IsInt()
  @Min(1)
  idVeiculo?: number;

  @IsOptional()
  @IsDateString()
  dataMulta?: string;

  @IsOptional()
  @IsDateString()
  dataVencimento?: string | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @Matches(/^([01]\d|2[0-3]):[0-5]\d(:[0-5]\d)?$/)
  horaMulta?: string | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(255)
  local?: string | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(255)
  logradouro?: string | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(100)
  cidade?: string | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @IsUppercase()
  @MaxLength(2)
  estado?: string | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(10)
  cep?: string | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(50)
  rodovia?: string | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(20)
  kmRodovia?: string | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(4000)
  descricao?: string | null;

  @IsOptional()
  @Type(() => Number)
  @IsNumber({ maxDecimalPlaces: 2 })
  @Min(0)
  valor?: number;

  @IsOptional()
  @IsDateString()
  dataPagamento?: string | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => transformarNumeroNullable(value))
  @IsNumber({ maxDecimalPlaces: 2 })
  @Min(0)
  desconto?: number | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => transformarNumeroNullable(value))
  @IsNumber({ maxDecimalPlaces: 2 })
  @Min(0)
  juros?: number | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => transformarNumeroNullable(value))
  @IsNumber({ maxDecimalPlaces: 2 })
  @Min(0)
  valorPago?: number | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => transformarNumeroNullable(value))
  @IsInt()
  @Min(0)
  pontos?: number | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @MaxLength(20)
  status?: string | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(100)
  orgaoAutuador?: string | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(50)
  numeroAuto?: string | null;
}
