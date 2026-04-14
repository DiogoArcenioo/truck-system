import { Injectable } from '@nestjs/common';
import { FiltroRelatorioFaturamentoDto } from '../relatorios-faturamento/dto/filtro-relatorio-faturamento.dto';
import { FiltroSerieRelatorioFaturamentoDto } from '../relatorios-faturamento/dto/filtro-serie-relatorio-faturamento.dto';
import { RelatoriosFaturamentoService } from '../relatorios-faturamento/relatorios-faturamento.service';
import { FiltroRelatorioResultadoDto } from './dto/filtro-relatorio-resultado.dto';

type ResumoRelatorioFaturamento = {
  periodo?: {
    mes?: number;
    ano?: number;
    inicio?: string;
    fim?: string;
    descricao?: string;
  };
  indicadores?: {
    faturamento?: {
      valor?: number;
      quantidadeRegistros?: number;
    };
    despesas?: {
      valor?: number;
      quantidadeRegistros?: number;
    };
    lucroReal?: {
      valor?: number;
    };
  };
};

type SerieRelatorioFaturamento = {
  periodo?: {
    inicioMes?: string;
    fimMes?: string;
    totalMeses?: number;
  };
  serie?: Array<{
    mes?: string;
    faturamento?: number;
    custos?: number;
    lucroLiquido?: number;
  }>;
};

type PeriodoSelecionado = {
  mes: number;
  ano: number;
  inicioSerie: string;
  fimSerie: string;
};

const MESES: string[] = [
  'Jan',
  'Fev',
  'Mar',
  'Abr',
  'Mai',
  'Jun',
  'Jul',
  'Ago',
  'Set',
  'Out',
  'Nov',
  'Dez',
];

@Injectable()
export class RelatoriosResultadoService {
  constructor(
    private readonly relatoriosFaturamentoService: RelatoriosFaturamentoService,
  ) {}

  async obterRelatorio(
    idEmpresa: number,
    filtro: FiltroRelatorioResultadoDto,
  ) {
    const periodoSelecionado = this.resolverPeriodoSelecionado(filtro);

    const filtroResumo: FiltroRelatorioFaturamentoDto = {
      mes: periodoSelecionado.mes,
      ano: periodoSelecionado.ano,
      idVeiculo: filtro.idVeiculo,
      idMotorista: filtro.idMotorista,
    };

    const filtroSerie: FiltroSerieRelatorioFaturamentoDto = {
      inicioMes: periodoSelecionado.inicioSerie,
      fimMes: periodoSelecionado.fimSerie,
      idVeiculo: filtro.idVeiculo,
      idMotorista: filtro.idMotorista,
    };

    const [resumoRaw, serieRaw] = await Promise.all([
      this.relatoriosFaturamentoService.obterResumo(idEmpresa, filtroResumo),
      this.relatoriosFaturamentoService.obterSerieMensal(idEmpresa, filtroSerie),
    ]);

    const resumo = resumoRaw as ResumoRelatorioFaturamento;
    const serie = serieRaw as SerieRelatorioFaturamento;

    const faturamento = this.arredondar(
      this.converterNumero(resumo.indicadores?.faturamento?.valor),
    );
    const despesas = this.arredondar(
      this.converterNumero(resumo.indicadores?.despesas?.valor),
    );
    const resultado = this.arredondar(
      this.converterNumero(resumo.indicadores?.lucroReal?.valor),
    );
    const totalViagens = this.converterInteiro(
      resumo.indicadores?.faturamento?.quantidadeRegistros,
    );
    const totalDespesas = this.converterInteiro(
      resumo.indicadores?.despesas?.quantidadeRegistros,
    );
    const margemPercentual =
      faturamento > 0
        ? this.arredondar((resultado / faturamento) * 100)
        : 0;
    const ticketMedioViagem =
      totalViagens > 0 ? this.arredondar(faturamento / totalViagens) : 0;
    const custoMedioViagem =
      totalViagens > 0 ? this.arredondar(despesas / totalViagens) : 0;

    const itensSerie = (serie.serie ?? []).map((item) => {
      const mes = this.normalizarMes(item.mes);
      const valorFaturamento = this.arredondar(
        this.converterNumero(item.faturamento),
      );
      const valorDespesas = this.arredondar(
        this.converterNumero(item.custos),
      );
      const valorResultado = this.arredondar(
        this.converterNumero(
          item.lucroLiquido ?? valorFaturamento - valorDespesas,
        ),
      );

      return {
        mes,
        descricao: this.formatarMesDescricao(mes),
        faturamento: valorFaturamento,
        despesas: valorDespesas,
        resultado: valorResultado,
      };
    });

    const inicioMesSerie =
      this.normalizarMes(serie.periodo?.inicioMes) ||
      periodoSelecionado.inicioSerie;
    const fimMesSerie =
      this.normalizarMes(serie.periodo?.fimMes) || periodoSelecionado.fimSerie;
    const totalMesesSerie = Math.max(
      1,
      this.converterInteiro(serie.periodo?.totalMeses) || itensSerie.length || 12,
    );

    return {
      sucesso: true,
      periodo: {
        mes:
          this.converterInteiro(resumo.periodo?.mes) || periodoSelecionado.mes,
        ano:
          this.converterInteiro(resumo.periodo?.ano) || periodoSelecionado.ano,
        inicio: resumo.periodo?.inicio ?? null,
        fim: resumo.periodo?.fim ?? null,
        descricao:
          resumo.periodo?.descricao ??
          `${String(periodoSelecionado.mes).padStart(2, '0')}/${periodoSelecionado.ano}`,
      },
      filtrosAplicados: {
        idVeiculo:
          this.converterInteiro(filtro.idVeiculo) > 0
            ? this.converterInteiro(filtro.idVeiculo)
            : null,
        idMotorista:
          this.converterInteiro(filtro.idMotorista) > 0
            ? this.converterInteiro(filtro.idMotorista)
            : null,
      },
      indicadores: {
        faturamento,
        despesas,
        resultado,
        margemPercentual,
        totalViagens,
        totalDespesas,
        ticketMedioViagem,
        custoMedioViagem,
      },
      serie: {
        inicioMes: inicioMesSerie,
        fimMes: fimMesSerie,
        totalMeses: totalMesesSerie,
        itens: itensSerie,
      },
    };
  }

  private resolverPeriodoSelecionado(
    filtro: FiltroRelatorioResultadoDto,
  ): PeriodoSelecionado {
    const agora = new Date();
    const mesAtual = agora.getUTCMonth() + 1;
    const anoAtual = agora.getUTCFullYear();
    const mes = this.converterInteiro(filtro.mes) || mesAtual;
    const ano = this.converterInteiro(filtro.ano) || anoAtual;

    const fimSerieDate = new Date(Date.UTC(ano, mes - 1, 1, 0, 0, 0, 0));
    const inicioSerieDate = new Date(fimSerieDate);
    inicioSerieDate.setUTCMonth(inicioSerieDate.getUTCMonth() - 11);

    return {
      mes,
      ano,
      inicioSerie: this.formatarMesAno(inicioSerieDate),
      fimSerie: this.formatarMesAno(fimSerieDate),
    };
  }

  private formatarMesAno(data: Date): string {
    return `${data.getUTCFullYear()}-${String(data.getUTCMonth() + 1).padStart(2, '0')}`;
  }

  private normalizarMes(valor: unknown): string {
    if (typeof valor !== 'string') {
      return '';
    }

    const texto = valor.trim();
    const match = /^(\d{4})-(\d{2})$/.exec(texto);
    if (!match) {
      return '';
    }

    const ano = Number(match[1]);
    const mes = Number(match[2]);
    if (!Number.isFinite(ano) || !Number.isFinite(mes) || mes < 1 || mes > 12) {
      return '';
    }

    return `${ano}-${String(mes).padStart(2, '0')}`;
  }

  private formatarMesDescricao(mesAno: string): string {
    const match = /^(\d{4})-(\d{2})$/.exec(mesAno);
    if (!match) {
      return mesAno;
    }

    const ano = Number(match[1]);
    const mes = Number(match[2]);
    const nomeMes = MESES[mes - 1] ?? String(mes).padStart(2, '0');
    return `${nomeMes}/${ano}`;
  }

  private converterNumero(valor: unknown): number {
    const numero = Number(valor);
    return Number.isFinite(numero) ? numero : 0;
  }

  private converterInteiro(valor: unknown): number {
    const numero = Number(valor);
    if (!Number.isFinite(numero)) {
      return 0;
    }

    return Math.max(0, Math.trunc(numero));
  }

  private arredondar(valor: number, casas = 2): number {
    const fator = 10 ** casas;
    return Math.round((valor + Number.EPSILON) * fator) / fator;
  }
}
