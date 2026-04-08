import { Injectable } from '@nestjs/common';
import { DespesasService } from '../despesas/despesas.service';
import { FiltroRelatorioDespesaDto } from './dto/filtro-relatorio-despesa.dto';

type TipoColuna = 'moeda' | 'numero' | 'data' | 'texto';

type ColunaTabela = {
  chave: string;
  label: string;
  tipo?: TipoColuna;
};

type DespesaRelatorio = {
  idDespesa: number;
  idVeiculo: number | null;
  idMotorista: number | null;
  idViagem: number | null;
  data: Date | string;
  tipo: string;
  tipoDescricao: string;
  descricao: string | null;
  valor: number;
};

type ResultadoPaginaDespesas = {
  paginaAtual: number;
  limite: number;
  total: number;
  totalPaginas: number;
  despesas: DespesaRelatorio[];
};

type PeriodoRelatorio = {
  mes: number;
  ano: number;
  inicio: Date;
  fim: Date;
  inicioIso: string;
  fimIso: string;
  inicioData: string;
  fimData: string;
  descricao: string;
};

type AcumuladorTipo = {
  tipo: string;
  tipoDescricao: string;
  total: number;
  quantidade: number;
};

type FiltrosRelacionados = {
  idVeiculo?: number;
  idMotorista?: number;
};

const colunasLancamentos: ColunaTabela[] = [
  { chave: 'idDespesa', label: 'Despesa', tipo: 'numero' },
  { chave: 'data', label: 'Data', tipo: 'data' },
  { chave: 'tipoDescricao', label: 'Tipo', tipo: 'texto' },
  { chave: 'descricao', label: 'Descricao', tipo: 'texto' },
  { chave: 'idVeiculo', label: 'Veiculo', tipo: 'numero' },
  { chave: 'idMotorista', label: 'Motorista', tipo: 'numero' },
  { chave: 'idViagem', label: 'Viagem', tipo: 'numero' },
  { chave: 'valor', label: 'Valor', tipo: 'moeda' },
];

@Injectable()
export class RelatoriosDespesaService {
  private readonly limiteConsulta = 200;
  private readonly maxPaginasConsulta = 5000;

  constructor(private readonly despesasService: DespesasService) {}

  async obterRelatorio(
    idEmpresa: number,
    filtro: FiltroRelatorioDespesaDto,
  ) {
    const periodo = this.resolverPeriodo(filtro);
    const filtrosRelacionados = this.extrairFiltrosRelacionados(filtro);
    const despesas = await this.carregarDespesasPeriodo(
      idEmpresa,
      periodo,
      filtrosRelacionados,
    );

    const totalGasto = this.arredondar(
      despesas.reduce((acumulado, despesa) => {
        return acumulado + this.converterNumero(despesa.valor);
      }, 0),
    );
    const totalLancamentos = despesas.length;
    const ticketMedio =
      totalLancamentos > 0
        ? this.arredondar(totalGasto / totalLancamentos)
        : 0;

    const acumuladoresPorTipo = new Map<string, AcumuladorTipo>();

    for (const despesa of despesas) {
      const tipo = this.normalizarTipo(despesa.tipo);
      const tipoDescricao =
        this.normalizarTexto(despesa.tipoDescricao) ?? tipo;
      const chave = `${tipo}::${tipoDescricao}`;

      const atual = acumuladoresPorTipo.get(chave) ?? {
        tipo,
        tipoDescricao,
        total: 0,
        quantidade: 0,
      };

      atual.total += this.converterNumero(despesa.valor);
      atual.quantidade += 1;
      acumuladoresPorTipo.set(chave, atual);
    }

    const distribuicaoPorTipo = Array.from(acumuladoresPorTipo.values())
      .map((item) => {
        const total = this.arredondar(item.total);
        const percentual =
          totalGasto > 0
            ? this.arredondar(item.total / totalGasto, 6)
            : 0;

        return {
          tipo: item.tipo,
          tipoDescricao: item.tipoDescricao,
          total,
          quantidade: item.quantidade,
          percentual,
        };
      })
      .sort((a, b) => b.total - a.total);

    const maiorTipo = distribuicaoPorTipo[0] ?? null;

    return {
      sucesso: true,
      periodo: {
        mes: periodo.mes,
        ano: periodo.ano,
        inicio: periodo.inicioData,
        fim: periodo.fimData,
        descricao: periodo.descricao,
      },
      resumo: {
        totalGasto,
        totalLancamentos,
        ticketMedio,
        tiposComGasto: distribuicaoPorTipo.length,
        maiorTipo: maiorTipo
          ? {
              tipo: maiorTipo.tipo,
              tipoDescricao: maiorTipo.tipoDescricao,
              total: maiorTipo.total,
              percentual: maiorTipo.percentual,
            }
          : null,
      },
      distribuicaoPorTipo,
      lancamentos: {
        totalRegistros: despesas.length,
        colunas: colunasLancamentos,
        linhas: despesas.map((despesa) => ({
          idDespesa: this.converterInteiro(despesa.idDespesa),
          data: this.converterDataParaIso(despesa.data),
          tipo: this.normalizarTipo(despesa.tipo),
          tipoDescricao:
            this.normalizarTexto(despesa.tipoDescricao) ??
            this.normalizarTipo(despesa.tipo),
          descricao: this.normalizarTexto(despesa.descricao),
          idVeiculo: this.converterInteiroOuNulo(despesa.idVeiculo),
          idMotorista: this.converterInteiroOuNulo(despesa.idMotorista),
          idViagem: this.converterInteiroOuNulo(despesa.idViagem),
          valor: this.arredondar(this.converterNumero(despesa.valor)),
        })),
      },
    };
  }

  private async carregarDespesasPeriodo(
    idEmpresa: number,
    periodo: PeriodoRelatorio,
    filtrosRelacionados: FiltrosRelacionados,
  ): Promise<DespesaRelatorio[]> {
    const despesas: DespesaRelatorio[] = [];
    let pagina = 1;
    let totalPaginas = 1;

    while (pagina <= totalPaginas && pagina <= this.maxPaginasConsulta) {
      const resultado = (await this.despesasService.listarComFiltro(idEmpresa, {
        dataDe: periodo.inicioIso,
        dataAte: periodo.fimIso,
        idVeiculo: filtrosRelacionados.idVeiculo,
        idMotorista: filtrosRelacionados.idMotorista,
        situacao: 'ATIVO',
        pagina,
        limite: this.limiteConsulta,
        ordenarPor: 'data',
        ordem: 'DESC',
      })) as ResultadoPaginaDespesas;

      despesas.push(...(Array.isArray(resultado.despesas) ? resultado.despesas : []));
      totalPaginas = Math.max(1, this.converterInteiro(resultado.totalPaginas));
      pagina += 1;
    }

    return despesas;
  }

  private resolverPeriodo(filtro: FiltroRelatorioDespesaDto): PeriodoRelatorio {
    const agora = new Date();
    const mes = filtro.mes ?? agora.getUTCMonth() + 1;
    const ano = filtro.ano ?? agora.getUTCFullYear();

    const inicio = new Date(Date.UTC(ano, mes - 1, 1, 0, 0, 0, 0));
    const fim = new Date(Date.UTC(ano, mes, 0, 23, 59, 59, 999));

    return {
      mes,
      ano,
      inicio,
      fim,
      inicioIso: inicio.toISOString(),
      fimIso: fim.toISOString(),
      inicioData: inicio.toISOString().slice(0, 10),
      fimData: fim.toISOString().slice(0, 10),
      descricao: `${String(mes).padStart(2, '0')}/${ano}`,
    };
  }

  private extrairFiltrosRelacionados(filtro: {
    idVeiculo?: number;
    idMotorista?: number;
  }): FiltrosRelacionados {
    const idVeiculo = this.converterInteiro(filtro.idVeiculo);
    const idMotorista = this.converterInteiro(filtro.idMotorista);

    return {
      idVeiculo: idVeiculo > 0 ? idVeiculo : undefined,
      idMotorista: idMotorista > 0 ? idMotorista : undefined,
    };
  }

  private converterDataParaIso(valor: Date | string | null | undefined) {
    if (!valor) {
      return null;
    }

    const data = valor instanceof Date ? valor : new Date(valor);
    if (Number.isNaN(data.getTime())) {
      return null;
    }

    return data.toISOString();
  }

  private normalizarTexto(valor: unknown) {
    if (typeof valor !== 'string') {
      return null;
    }

    const texto = valor.trim();
    return texto.length > 0 ? texto : null;
  }

  private normalizarTipo(valor: unknown) {
    const tipo = this.normalizarTexto(valor);
    return tipo?.toUpperCase() ?? 'O';
  }

  private converterNumero(valor: unknown) {
    const numero = Number(valor);
    return Number.isFinite(numero) ? numero : 0;
  }

  private converterInteiro(valor: unknown) {
    const numero = Number(valor);
    if (!Number.isFinite(numero)) {
      return 0;
    }

    return Math.max(0, Math.trunc(numero));
  }

  private converterInteiroOuNulo(valor: unknown) {
    const inteiro = this.converterInteiro(valor);
    return inteiro > 0 ? inteiro : null;
  }

  private arredondar(valor: number, casas = 2) {
    const fator = 10 ** casas;
    return Math.round((valor + Number.EPSILON) * fator) / fator;
  }
}
