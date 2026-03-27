import { Transform } from 'class-transformer';
import { IsDateString, IsIn, IsOptional, IsString, Length, MaxLength } from 'class-validator';
import {
  CATEGORIAS_CNH_CODIGOS,
  STATUS_MOTORISTA_CODIGOS,
} from '../motoristas.constants';

export class CriarMotoristaDto {
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @MaxLength(160)
  nome!: string;

  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.replace(/\D/g, '').trim() : value,
  )
  @IsString()
  @Length(11, 11)
  cpf!: string;

  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @MaxLength(40)
  cnh!: string;

  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @IsIn(CATEGORIAS_CNH_CODIGOS)
  categoriaCnh!: (typeof CATEGORIAS_CNH_CODIGOS)[number];

  @IsDateString()
  validadeCnh!: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @IsIn(STATUS_MOTORISTA_CODIGOS)
  status?: (typeof STATUS_MOTORISTA_CODIGOS)[number];
}
