import { Transform, Type } from 'class-transformer';
import {
  IsDateString,
  IsIn,
  IsInt,
  IsOptional,
  IsString,
  Max,
  MaxLength,
  Min,
} from 'class-validator';

const ordenacaoPermitida = [
  'id_veiculo',
  'placa',
  'id_fornecedor',
  'id_marca',
  'id_modelo',
  'id_combustivel',
  'id_tipo',
  'id_cor',
  'ano_fabricacao',
  'ano_modelo',
  'km',
  'vencimento_documento',
] as const;

export class FiltroVeiculosDto {
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
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idMarca?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idModelo?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idCombustivel?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idTipo?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idCor?: number;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @MaxLength(10)
  placa?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @MaxLength(20)
  renavam?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @MaxLength(40)
  chassi?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(200)
  texto?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1900)
  @Max(2100)
  anoFabricacaoDe?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1900)
  @Max(2100)
  anoFabricacaoAte?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1900)
  @Max(2100)
  anoModeloDe?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1900)
  @Max(2100)
  anoModeloAte?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  kmMin?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  kmMax?: number;

  @IsOptional()
  @IsDateString()
  vencimentoDocumentoDe?: string;

  @IsOptional()
  @IsDateString()
  vencimentoDocumentoAte?: string;

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
