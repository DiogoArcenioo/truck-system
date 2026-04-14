import { IsString, MinLength } from 'class-validator';

export class ImportarXmlNotaFiscalAbastecimentoDto {
  @IsString()
  @MinLength(20)
  xml!: string;
}

