import { Transform, Type } from 'class-transformer';
import {
  IsIn,
  IsInt,
  IsOptional,
  IsString,
  Max,
  MaxLength,
  Min,
} from 'class-validator';
import {
  SITUACAO_PRODUTO_CODIGOS,
  TIPO_PRODUTO_CODIGOS,
} from '../produto.constants';

const ordenacaoPermitida = [
  'id_produto',
  'descricao_produto',
  'data_cadastro',
  'atualizado_em',
  'situacao',
  'tipo_produto',
] as const;

export class FiltroProdutosDto {
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idProduto?: number;

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
  @IsIn(SITUACAO_PRODUTO_CODIGOS)
  situacao?: (typeof SITUACAO_PRODUTO_CODIGOS)[number];

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @IsIn(TIPO_PRODUTO_CODIGOS)
  tipoProduto?: (typeof TIPO_PRODUTO_CODIGOS)[number];

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(2000)
  texto?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  pagina?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(100)
  limite?: number;

  @IsOptional()
  @IsIn(ordenacaoPermitida)
  ordenarPor?: (typeof ordenacaoPermitida)[number];

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsIn(['ASC', 'DESC'])
  ordem?: 'ASC' | 'DESC';
}
