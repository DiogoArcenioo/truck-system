import { Transform, Type } from 'class-transformer';
import {
  IsBoolean,
  IsDateString,
  IsIn,
  IsInt,
  IsNumber,
  IsOptional,
  IsString,
  Length,
  Max,
  MaxLength,
  Min,
} from 'class-validator';

const ordenacaoPermitida = [
  'id_viagem',
  'data_inicio',
  'data_fim',
  'criado_em',
  'atualizado_em',
  'km_inicial',
  'km_final',
  'valor_frete',
  'total_despesas',
  'total_lucro',
] as const;

export class FiltroViagensDto {
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idViagem?: number;

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
  @MaxLength(200)
  texto?: string;

  @IsOptional()
  @IsDateString()
  dataInicioDe?: string;

  @IsOptional()
  @IsDateString()
  dataInicioAte?: string;

  @IsOptional()
  @IsDateString()
  dataFimDe?: string;

  @IsOptional()
  @IsDateString()
  dataFimAte?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  kmInicialMin?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  kmInicialMax?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  kmFinalMin?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  kmFinalMax?: number;

  @IsOptional()
  @Type(() => Number)
  @IsNumber({ maxDecimalPlaces: 2 })
  valorFreteMin?: number;

  @IsOptional()
  @Type(() => Number)
  @IsNumber({ maxDecimalPlaces: 2 })
  valorFreteMax?: number;

  @IsOptional()
  @Type(() => Number)
  @IsNumber({ maxDecimalPlaces: 2 })
  totalDespesasMin?: number;

  @IsOptional()
  @Type(() => Number)
  @IsNumber({ maxDecimalPlaces: 2 })
  totalDespesasMax?: number;

  @IsOptional()
  @Type(() => Number)
  @IsNumber({ maxDecimalPlaces: 2 })
  totalLucroMin?: number;

  @IsOptional()
  @Type(() => Number)
  @IsNumber({ maxDecimalPlaces: 2 })
  totalLucroMax?: number;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => {
    if (value === undefined || value === null || value === '') {
      return undefined;
    }

    const textoBruto =
      typeof value === 'string' ||
      typeof value === 'number' ||
      typeof value === 'boolean'
        ? `${value}`
        : '';
    const texto = textoBruto.trim().toLowerCase();

    if (!texto) {
      return value;
    }
    if (texto === 'true' || texto === '1' || texto === 'sim') {
      return true;
    }

    if (texto === 'false' || texto === '0' || texto === 'nao') {
      return false;
    }

    return value;
  })
  @IsBoolean()
  apenasAbertas?: boolean;

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
