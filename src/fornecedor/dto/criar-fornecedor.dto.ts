import { Transform } from 'class-transformer';
import { IsBoolean, IsIn, IsOptional, IsString, MaxLength } from 'class-validator';
import { TIPO_PESSOA_CODIGOS } from '../fornecedor.constants';

function limparTexto(value: unknown) {
  return typeof value === 'string' ? value.trim() : value;
}

function limparTextoMaiusculo(value: unknown) {
  return typeof value === 'string' ? value.trim().toUpperCase() : value;
}

export class CriarFornecedorDto {
  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTextoMaiusculo(value))
  @IsIn(TIPO_PESSOA_CODIGOS)
  tipoPessoa?: (typeof TIPO_PESSOA_CODIGOS)[number];

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsString()
  @MaxLength(180)
  razaoSocial?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsString()
  @MaxLength(180)
  nomeFantasia?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsString()
  @MaxLength(180)
  nomePessoa?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsString()
  @MaxLength(14)
  cpf?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsString()
  @MaxLength(18)
  cnpj?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsString()
  @MaxLength(30)
  inscricaoEstadual?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsString()
  @MaxLength(30)
  inscricaoMunicipal?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTexto(value))
  @IsString()
  @MaxLength(2000)
  observacoes?: string;

  @IsOptional()
  @IsBoolean()
  ativo?: boolean;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => limparTextoMaiusculo(value))
  @IsString()
  @MaxLength(120)
  usuarioAtualizacao?: string;
}
