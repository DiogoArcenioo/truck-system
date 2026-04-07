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
import { TIPOS_DOCUMENTO_CANCELAMENTO_CODIGOS } from '../cancelamentos.constants';

export class CriarCancelamentoDto {
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsIn(TIPOS_DOCUMENTO_CANCELAMENTO_CODIGOS)
  tipoDocumento!: (typeof TIPOS_DOCUMENTO_CANCELAMENTO_CODIGOS)[number];

  @Type(() => Number)
  @IsInt()
  @Min(1)
  idDocumento!: number;

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
  @MaxLength(200)
  motivoDescricao?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(120)
  usuarioSolicitante?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(2000)
  observacao?: string;

  @IsOptional()
  @IsDateString()
  dataCancelamento?: string;
}
