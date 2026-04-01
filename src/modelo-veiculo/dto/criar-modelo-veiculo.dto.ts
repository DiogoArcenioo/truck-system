import { IsInt, IsOptional, IsString, MaxLength, Min } from 'class-validator';

export class CriarModeloVeiculoDto {
  @IsInt()
  @Min(1)
  idMarca!: number;

  @IsString()
  @MaxLength(120)
  descricao!: string;

  @IsOptional()
  @IsString()
  @MaxLength(1)
  status?: string;

  @IsOptional()
  @IsString()
  @MaxLength(120)
  usuarioAtualizacao?: string;
}
