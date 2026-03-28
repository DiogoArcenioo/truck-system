import { Transform, Type } from 'class-transformer';
import {
  IsIn,
  IsInt,
  IsNumber,
  IsOptional,
  IsString,
  MaxLength,
  Min,
} from 'class-validator';
import { SITUACAO_OS_CODIGOS, TIPO_SERVICO_CODIGOS } from '../ordem-servico.constants';

function transformarNumero(valor: unknown) {
  if (typeof valor === 'number') {
    return valor;
  }

  if (typeof valor === 'string') {
    const normalizado = valor.trim().replace(',', '.');
    if (!normalizado) {
      return valor;
    }

    return Number(normalizado);
  }

  return valor;
}

export class AtualizarOrdemServicoDto {
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idVeiculo?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idFornecedor?: number;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  dataCadastro?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  dataFechamento?: string | null;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  tempoOsMin?: number;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @IsIn(SITUACAO_OS_CODIGOS)
  situacaoOs?: (typeof SITUACAO_OS_CODIGOS)[number];

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(2000)
  observacao?: string | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) => transformarNumero(value))
  @IsNumber({ maxDecimalPlaces: 2 })
  @Min(0)
  valorTotal?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  kmVeiculo?: number | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @MaxLength(120)
  chaveNfe?: string | null;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @IsIn(TIPO_SERVICO_CODIGOS)
  tipoServico?: (typeof TIPO_SERVICO_CODIGOS)[number];

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @MaxLength(120)
  usuarioAtualizacao?: string;
}
