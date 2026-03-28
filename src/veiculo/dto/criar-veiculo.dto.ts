import { Transform, Type } from 'class-transformer';
import {
  IsDateString,
  IsInt,
  IsOptional,
  IsString,
  Length,
  Max,
  MaxLength,
  Min,
} from 'class-validator';

export class CriarVeiculoDto {
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idFornecedor!: number;

  @Type(() => Number)
  @IsInt()
  @Min(1)
  idMarca!: number;

  @Type(() => Number)
  @IsInt()
  @Min(1)
  idModelo!: number;

  @Type(() => Number)
  @IsInt()
  @Min(1)
  idCombustivel!: number;

  @Type(() => Number)
  @IsInt()
  @Min(1)
  idTipo!: number;

  @Type(() => Number)
  @IsInt()
  @Min(1)
  idCor!: number;

  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @Length(7, 8)
  placa!: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @Length(7, 8)
  placa2?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @Length(7, 8)
  placa3?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @Length(7, 8)
  placa4?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @MaxLength(60)
  numeroMotor?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @MaxLength(20)
  renavam?: string;

  @Type(() => Number)
  @IsInt()
  @Min(1900)
  @Max(2100)
  anoFabricacao!: number;

  @Type(() => Number)
  @IsInt()
  @Min(1900)
  @Max(2100)
  anoModelo!: number;

  @Type(() => Number)
  @IsInt()
  @Min(0)
  km!: number;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @MaxLength(40)
  chassi?: string;

  @IsOptional()
  @IsDateString()
  vencimentoDocumento?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @MaxLength(255)
  observacao?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idMotoristaAtual?: number;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @MaxLength(20)
  status?: string;
}
