import { Transform, Type } from 'class-transformer';
import { IsIn, IsInt, Max, Min } from 'class-validator';
import { FiltroRelatorioFaturamentoDto } from './filtro-relatorio-faturamento.dto';

const indicadoresPermitidos = ['faturamento', 'despesas', 'lucro_real'] as const;

export type IndicadorRelatorioFaturamentoId =
  (typeof indicadoresPermitidos)[number];

export class DetalheRelatorioFaturamentoDto extends FiltroRelatorioFaturamentoDto {
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim().toLowerCase() : value,
  )
  @IsIn(indicadoresPermitidos)
  indicador!: IndicadorRelatorioFaturamentoId;

  @Type(() => Number)
  @IsInt()
  @Min(1)
  pagina = 1;

  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(100)
  limite = 20;
}
