import { Transform } from 'class-transformer';
import { IsDateString, IsEmail, IsIn, IsOptional, IsString, Length, MaxLength } from 'class-validator';
import {
  CATEGORIAS_CNH_CODIGOS,
  STATUS_MOTORISTA_CODIGOS,
} from '../motoristas.constants';

const TIPOS_CONTRATO_CODIGOS = ['CLT', 'PJ', 'AUTONOMO'] as const;

export class AtualizarMotoristaDto {
  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @MaxLength(160)
  nome?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.replace(/\D/g, '').trim() : value,
  )
  @IsString()
  @Length(11, 11)
  cpf?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @MaxLength(40)
  cnh?: string;

  @IsOptional()
  @IsDateString()
  dataNascimento?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toLowerCase() : value,
  )
  @IsEmail()
  @MaxLength(150)
  email?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.replace(/\D/g, '').trim() : value,
  )
  @IsString()
  @MaxLength(20)
  telefone1?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.replace(/\D/g, '').trim() : value,
  )
  @IsString()
  @MaxLength(20)
  telefone2?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @IsIn(CATEGORIAS_CNH_CODIGOS)
  categoriaCnh?: (typeof CATEGORIAS_CNH_CODIGOS)[number];

  @IsOptional()
  @IsDateString()
  validadeCnh?: string;

  @IsOptional()
  @IsDateString()
  dataAdmissao?: string;

  @IsOptional()
  @IsDateString()
  dataDemissao?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @MaxLength(20)
  @IsIn(TIPOS_CONTRATO_CODIGOS)
  tipoContrato?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @IsIn(STATUS_MOTORISTA_CODIGOS)
  status?: (typeof STATUS_MOTORISTA_CODIGOS)[number];
}
