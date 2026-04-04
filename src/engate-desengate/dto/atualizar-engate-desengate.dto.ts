import { Transform, Type } from 'class-transformer';
import {
  IsBoolean,
  IsDateString,
  IsIn,
  IsInt,
  IsOptional,
  IsString,
  MaxLength,
  Min,
} from 'class-validator';

export class AtualizarEngateDesengateDto {
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idVeiculo?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  idMotorista?: number;

  @IsOptional()
  @IsDateString()
  dataInclusao?: string;

  @IsOptional()
  @IsDateString()
  dataMovi?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsIn(['E', 'D'])
  tipoEngate?: 'E' | 'D';

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @MaxLength(10)
  placa2?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @MaxLength(10)
  placa3?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toUpperCase() : value,
  )
  @IsString()
  @MaxLength(10)
  placa4?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsString()
  @MaxLength(120)
  usuarioAtualizacao?: string;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    value === true || value === 'true',
  )
  @IsBoolean()
  desengatarMotorista?: boolean;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    value === true || value === 'true',
  )
  @IsBoolean()
  desengatarPlaca2?: boolean;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    value === true || value === 'true',
  )
  @IsBoolean()
  desengatarPlaca3?: boolean;

  @IsOptional()
  @Transform(({ value }: { value: unknown }) =>
    value === true || value === 'true',
  )
  @IsBoolean()
  desengatarPlaca4?: boolean;
}
