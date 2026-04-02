import { IsObject, IsOptional, IsString, MaxLength } from 'class-validator';

export class AtualizarPermissoesDto {
  @IsObject()
  permissoes!: Record<string, unknown>;

  @IsOptional()
  @IsString()
  @MaxLength(120)
  usuarioAtualizacao?: string;
}

