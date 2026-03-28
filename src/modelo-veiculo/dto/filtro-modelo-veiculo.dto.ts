import { Type } from 'class-transformer';
import { IsInt, IsOptional, Min } from 'class-validator';

export class FiltroModeloVeiculoDto {
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idMarca?: number;
}
