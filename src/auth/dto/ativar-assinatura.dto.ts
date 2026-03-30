import { Type } from 'class-transformer';
import { IsInt, IsOptional, IsPositive, IsString, MaxLength } from 'class-validator';

export class AtivarAssinaturaDto {
  @Type(() => Number)
  @IsInt()
  @IsPositive()
  idEmpresa!: number;

  @IsOptional()
  @IsString()
  @MaxLength(20)
  plano?: string;

  @IsOptional()
  @IsString()
  @MaxLength(120)
  usuarioAtualizacao?: string;
}
