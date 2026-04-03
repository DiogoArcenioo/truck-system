import {
  IsBoolean,
  IsEmail,
  IsObject,
  IsOptional,
  IsString,
  Matches,
  MaxLength,
  MinLength,
} from 'class-validator';
import { PERFIL_CODIGO_REGEX } from '../permissoes.constants';

export class AtualizarUsuarioSistemaDto {
  @IsOptional()
  @IsString()
  @MaxLength(150)
  nome?: string;

  @IsOptional()
  @IsEmail()
  @MaxLength(150)
  email?: string;

  @IsOptional()
  @IsString()
  @MinLength(8)
  @MaxLength(120)
  senha?: string;

  @IsOptional()
  @IsString()
  @Matches(PERFIL_CODIGO_REGEX, {
    message:
      'Perfil invalido. Use de 2 a 40 caracteres com letras, numeros, "_" ou "-".',
  })
  perfil?: string;

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
