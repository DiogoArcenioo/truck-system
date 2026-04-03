import {
  IsBoolean,
  IsIn,
  IsObject,
  IsOptional,
  IsString,
  MaxLength,
} from 'class-validator';
import { PERFIS_BASE } from '../permissoes.constants';

export class AtualizarPerfilSistemaDto {
  @IsOptional()
  @IsString()
  @MaxLength(120)
  nome?: string;

  @IsOptional()
  @IsString()
  @MaxLength(255)
  descricao?: string;

  @IsOptional()
  @IsBoolean()
  ativo?: boolean;

  @IsOptional()
  @IsString()
  @IsIn(PERFIS_BASE)
  perfilBase?: (typeof PERFIS_BASE)[number];

  @IsOptional()
  @IsObject()
  permissoes?: Record<string, unknown>;

  @IsOptional()
  @IsString()
  @MaxLength(120)
  usuarioAtualizacao?: string;
}

