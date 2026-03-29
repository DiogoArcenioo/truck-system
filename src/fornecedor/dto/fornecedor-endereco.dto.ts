import { Transform } from 'class-transformer';
import { IsBoolean, IsOptional, IsString, Length, MaxLength } from 'class-validator';

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

export class CriarFornecedorEnderecoDto {
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsString()
  @MaxLength(180)
  logradouro!: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsString()
  @MaxLength(20)
  numero?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsString()
  @MaxLength(120)
  complemento?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsString()
  @MaxLength(120)
  bairro?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsString()
  @MaxLength(120)
  cidade?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTextoMaiusculo(value))
  @IsString()
  @Length(2, 2)
  estado?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsString()
  @MaxLength(9)
  cep?: string;

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

export class AtualizarFornecedorEnderecoDto {
  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsString()
  @MaxLength(180)
  logradouro?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsString()
  @MaxLength(20)
  numero?: string | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsString()
  @MaxLength(120)
  complemento?: string | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsString()
  @MaxLength(120)
  bairro?: string | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsString()
  @MaxLength(120)
  cidade?: string | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTextoMaiusculo(value))
  @IsString()
  @Length(2, 2)
  estado?: string | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsString()
  @MaxLength(9)
  cep?: string | null;

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
