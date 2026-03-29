import { Transform } from 'class-transformer';
import { IsBoolean, IsEmail, IsOptional, IsString, MaxLength } from 'class-validator';

function limparTexto(value: unknown) {
  return typeof value === 'string' ? value.trim() : value;
}

function limparTextoMaiusculo(value: unknown) {
  return typeof value === 'string' ? value.trim().toUpperCase() : value;
}

function transformarBoolean(value: unknown) {
  if (typeof value === 'boolean') {
    return value;
  }

  if (typeof value === 'string') {
    const normalizado = value.trim().toLowerCase();
    if (normalizado === 'true' || normalizado === '1') {
      return true;
    }
    if (normalizado === 'false' || normalizado === '0') {
      return false;
    }
  }

  return value;
}

export class CriarFornecedorContatoDto {
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsString()
  @MaxLength(120)
  nomeContato!: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsString()
  @MaxLength(20)
  telefone?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsString()
  @MaxLength(20)
  celular?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsEmail()
  @MaxLength(180)
  email?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => transformarBoolean(value))
  @IsBoolean()
  principal?: boolean;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTextoMaiusculo(value))
  @IsString()
  @MaxLength(120)
  usuarioAtualizacao?: string;
}

export class AtualizarFornecedorContatoDto {
  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsString()
  @MaxLength(120)
  nomeContato?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsString()
  @MaxLength(20)
  telefone?: string | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsString()
  @MaxLength(20)
  celular?: string | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsEmail()
  @MaxLength(180)
  email?: string | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => transformarBoolean(value))
  @IsBoolean()
  principal?: boolean;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTextoMaiusculo(value))
  @IsString()
  @MaxLength(120)
  usuarioAtualizacao?: string;
}
