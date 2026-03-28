import { Transform } from 'class-transformer';
import { IsInt, IsNumber, IsOptional, IsString, MaxLength, Min } from 'class-validator';

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

export class ItemRequisicaoDto {
  @Transform(({ value }: { value: unknown }) => transformarNumero(value))
  @IsInt()
  @Min(1)
  idProduto!: number;

  @Transform(({ value }: { value: unknown }) => transformarNumero(value))
  @IsNumber({ maxDecimalPlaces: 3 })
  @Min(0.001)
  qtdProduto!: number;

  @Transform(({ value }: { value: unknown }) => transformarNumero(value))
  @IsNumber({ maxDecimalPlaces: 4 })
  @Min(0)
  valorUn!: number;

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
  @MaxLength(120)
  usuarioAtualizacao?: string;
}
