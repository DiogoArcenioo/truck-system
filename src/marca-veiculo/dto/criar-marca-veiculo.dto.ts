import { IsOptional, IsString, MaxLength } from 'class-validator';

export class CriarMarcaVeiculoDto {
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
