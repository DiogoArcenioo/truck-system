import { Transform, Type } from 'class-transformer';
import { IsIn, IsInt, IsOptional, IsString, Max, MaxLength, Min } from 'class-validator';

const statusLocalPermitidos = [
  'ESTOQUE',
  'EM_USO',
  'CONSERTO',
  'BAIXA',
  'DESCARTE',
  'TODOS',
] as const;

const ordenarPorPermitido = [
  'id_pneu',
  'numero_fogo',
  'valor',
  'status_local',
  'criado_em',
  'atualizado_em',
] as const;

export class FiltroPneusDto {
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
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(200)
  texto?: string;

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
  @IsIn(statusLocalPermitidos)
  statusLocal?: (typeof statusLocalPermitidos)[number];

  @IsOptional()
  @IsIn(ordenarPorPermitido)
  ordenarPor?: (typeof ordenarPorPermitido)[number];

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsIn(['ASC', 'DESC'])
  ordem?: 'ASC' | 'DESC';

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
}

