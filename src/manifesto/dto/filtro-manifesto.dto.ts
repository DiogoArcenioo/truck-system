import { Transform, Type } from 'class-transformer';
import {
  IsBoolean,
  IsDateString,
  IsIn,
  IsInt,
  IsOptional,
  IsString,
  Max,
  MaxLength,
  Min,
} from 'class-validator';

const ordenacaoPermitida = [
  'id_manifesto',
  'numero_manifesto',
  'serie',
  'data_emissao',
  'data_inicio_viagem',
  'atualizado_em',
] as const;

export class FiltroManifestoDto {
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idManifesto?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  numeroManifesto?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  serie?: number;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(44)
  chaveMdfe?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @IsIn(['AUTORIZADO', 'CANCELADO', 'ENCERRADO', 'PENDENTE'])
  statusDocumento?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(200)
  texto?: string;

  @IsOptional()
  @IsDateString()
  dataEmissaoDe?: string;

  @IsOptional()
  @IsDateString()
  dataEmissaoAte?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => {
    if (value === undefined || value === null || value === '') return undefined;
    const texto = String(value).trim().toLowerCase();
    if (texto === 'true' || texto === '1' || texto === 'sim') return true;
    if (texto === 'false' || texto === '0' || texto === 'nao') return false;
    return value;
  })
  @IsBoolean()
  incluirInativos?: boolean;

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
