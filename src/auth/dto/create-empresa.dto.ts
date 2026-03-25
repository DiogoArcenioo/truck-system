import {
  IsBoolean,
  IsEmail,
  IsInt,
  IsNotEmpty,
  IsOptional,
  IsString,
  Length,
  MaxLength,
  Min,
} from 'class-validator';

export class CreateEmpresaDto {
  @IsString()
  @IsNotEmpty()
  @MaxLength(150)
  nomeEmpresa!: string;

  @IsString()
  @IsNotEmpty()
  @MaxLength(180)
  razaoSocial!: string;

  @IsString()
  @IsNotEmpty()
  @Length(14, 18)
  cnpj!: string;

  @IsEmail()
  @MaxLength(150)
  emailEmpresa!: string;

  @IsString()
  @IsNotEmpty()
  @MaxLength(25)
  telefoneEmpresa!: string;

  @IsOptional()
  @IsBoolean()
  ativo?: boolean;

  @IsOptional()
  @IsString()
  @MaxLength(20)
  status?: string;

  @IsOptional()
  @IsString()
  @MaxLength(20)
  plano?: string;

  @IsOptional()
  @IsString()
  usuarioAtualizacao?: string;

  @IsOptional()
  @IsInt()
  @Min(1)
  idEmpresa?: number;

  @IsOptional()
  @IsString()
  @MaxLength(150)
  nomeAdministrador?: string;

  @IsOptional()
  @IsEmail()
  @MaxLength(150)
  emailAdministrador?: string;

  @IsOptional()
  @IsString()
  @MaxLength(120)
  senha?: string;
}
