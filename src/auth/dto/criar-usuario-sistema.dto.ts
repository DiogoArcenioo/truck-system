import {
  IsBoolean,
  IsEmail,
  IsIn,
  IsNotEmpty,
  IsObject,
  IsOptional,
  IsString,
  MaxLength,
  MinLength,
} from 'class-validator';
import { PERFIS_USUARIO } from '../permissoes.constants';

export class CriarUsuarioSistemaDto {
  @IsString()
  @IsNotEmpty()
  @MaxLength(150)
  nome!: string;

  @IsEmail()
  @MaxLength(150)
  email!: string;

  @IsString()
  @MinLength(8)
  @MaxLength(120)
  senha!: string;

  @IsOptional()
  @IsString()
  @IsIn(PERFIS_USUARIO)
  perfil?: (typeof PERFIS_USUARIO)[number];

  @IsOptional()
  @IsBoolean()
  ativo?: boolean;

  @IsOptional()
  @IsString()
  @MaxLength(120)
  usuarioAtualizacao?: string;

  @IsOptional()
  @IsObject()
  permissoesUsuario?: Record<string, unknown>;
}
