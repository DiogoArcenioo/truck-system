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
  'id_despesa',
  'id_veiculo',
  'id_motorista',
  'id_viagem',
  'data',
  'tipo',
  'valor',
  'km_registro',
  'criado_em',
  'atualizado_em',
] as const;

export class FiltroDespesasDto {
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idDespesa?: number;

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
  @MaxLength(30)
  tipo?: string;

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
  @IsNumber({ maxDecimalPlaces: 2 })
  @Min(0)
  valorMin?: number;

  @IsOptional()
  @Type(() => Number)
  @IsNumber({ maxDecimalPlaces: 2 })
  @Min(0)
  valorMax?: number;

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
  @Max(200)
  limite?: number;

  @IsOptional()
  @IsIn(ordenacaoPermitida)
  ordenarPor?: (typeof ordenacaoPermitida)[number];

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsIn(['ATIVO', 'INATIVO', 'TODOS'])
  situacao?: 'ATIVO' | 'INATIVO' | 'TODOS';

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsIn(['ASC', 'DESC'])
  ordem?: 'ASC' | 'DESC';
}
