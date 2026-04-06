import { Transform, Type } from 'class-transformer';
import {
  IsDateString,
  IsIn,
  IsInt,
  IsOptional,
  IsString,
  MaxLength,
  Min,
} from 'class-validator';

const destinosPermitidos = [
  'ESTOQUE',
  'CONSERTO',
  'BAIXA',
  'DESCARTE',
  'VEICULO',
] as const;

export class MovimentarPneuDto {
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idPneu!: number;

  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsIn(destinosPermitidos)
  destino!: (typeof destinosPermitidos)[number];

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idVeiculoDestino?: number;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @MaxLength(30)
  posicaoDestino?: string;

  @IsOptional()
  @IsDateString()
  dataMovimentacao?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(120)
  motivo?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(2000)
  observacoes?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(120)
  usuarioAtualizacao?: string;
}

