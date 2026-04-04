import { Transform, Type } from 'class-transformer';
import {
  IsIn,
  IsInt,
  IsOptional,
  IsString,
  MaxLength,
  Min,
} from 'class-validator';
import {
  SITUACAO_PRODUTO_CODIGOS,
  TIPO_PRODUTO_CODIGOS,
} from '../produto.constants';

export class AtualizarProdutoDto {
  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @MaxLength(2000)
  descricaoProduto?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @MaxLength(100)
  referencia?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @MaxLength(100)
  codigoOriginal?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idGrupoProduto?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idSubgrupo?: number;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(2000)
  observacao?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @IsIn(SITUACAO_PRODUTO_CODIGOS)
  situacao?: (typeof SITUACAO_PRODUTO_CODIGOS)[number];

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idUn?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idMarca?: number;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @IsIn(TIPO_PRODUTO_CODIGOS)
  tipoProduto?: (typeof TIPO_PRODUTO_CODIGOS)[number];

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @MaxLength(120)
  usuarioAtualizacao?: string;
}
