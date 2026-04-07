import { Transform, Type } from 'class-transformer';
import {
  IsDateString,
  IsIn,
  IsInt,
  IsOptional,
  IsString,
  Max,
  MaxLength,
  Min,
} from 'class-validator';
import { TIPOS_DOCUMENTO_CANCELAMENTO_CODIGOS } from '../cancelamentos.constants';

export class FiltroCancelamentosDto {
  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsIn(TIPOS_DOCUMENTO_CANCELAMENTO_CODIGOS)
  tipoDocumento?: (typeof TIPOS_DOCUMENTO_CANCELAMENTO_CODIGOS)[number];

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idDocumento?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idMotivo?: number;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(120)
  usuario?: string;

  @IsOptional()
  @IsDateString()
  dataDe?: string;

  @IsOptional()
  @IsDateString()
  dataAte?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(200)
  limite?: number;
}
