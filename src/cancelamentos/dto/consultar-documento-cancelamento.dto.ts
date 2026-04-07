import { Transform, Type } from 'class-transformer';
import { IsIn, IsInt, Min } from 'class-validator';
import { TIPOS_DOCUMENTO_CANCELAMENTO_CODIGOS } from '../cancelamentos.constants';

export class ConsultarDocumentoCancelamentoDto {
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsIn(TIPOS_DOCUMENTO_CANCELAMENTO_CODIGOS)
  tipoDocumento!: (typeof TIPOS_DOCUMENTO_CANCELAMENTO_CODIGOS)[number];

  @Type(() => Number)
  @IsInt()
  @Min(1)
  idDocumento!: number;
}
