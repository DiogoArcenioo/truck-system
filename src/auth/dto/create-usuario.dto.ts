import {
  IsBoolean,
  IsEmail,
  IsInt,
  IsNotEmpty,
  IsOptional,
  IsString,
  MaxLength,
  Min,
  MinLength,
} from 'class-validator';

export class CreateUsuarioDto {
  @IsInt()
  @Min(1)
  idEmpresa!: number;

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
  @MaxLength(20)
  perfil?: string;

  @IsOptional()
  @IsBoolean()
  ativo?: boolean;

  @IsOptional()
  @IsString()
  usuarioAtualizacao?: string;
}
