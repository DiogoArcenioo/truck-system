import { Transform } from 'class-transformer';
import { IsIn, IsOptional } from 'class-validator';

export class FiltroMotivosCancelamentoDto {
  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsIn(['ATIVO', 'INATIVO', 'TODOS'])
  situacao?: 'ATIVO' | 'INATIVO' | 'TODOS';
}
