import { Injectable } from '@nestjs/common';
import { DespesasService } from '../despesas/despesas.service';
import { ViagensService } from '../viagens/viagens.service';
import { DetalheRelatorioFaturamentoDto } from './dto/detalhe-relatorio-faturamento.dto';
import { FiltroRelatorioFaturamentoDto } from './dto/filtro-relatorio-faturamento.dto';

type TipoValorColuna = 'moeda' | 'numero' | 'data' | 'texto';

type ColunaTabelaDetalhe = {
  chave: string;
  label: string;
  tipo?: TipoValorColuna;
};

type PaginacaoTabelaDetalhe = {
  pagina: number;
  limite: number;
  totalRegistros: number;
  totalPaginas: number;
};

type TabelaDetalhe = {
  id: string;
  titulo: string;
  descricao: string;
  paginacao: PaginacaoTabelaDetalhe;
  colunas: ColunaTabelaDetalhe[];
  linhas: Array<Record<string, unknown>>;
};

type ViagemRelatorio = {
  idViagem: number;
  idVeiculo: number;
  idMotorista: number;
  dataInicio: Date | string;
  dataFim: Date | string | null;
  status: string;
  observacao: string | null;
  valorFrete: number | null;
  totalDespesas: number | null;
  totalLucro: number | null;
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

type ResultadoPaginaViagens = {
  pagina: number;
  limite: number;
  total: number;
  paginas: number;
  viagens: ViagemRelatorio[];
};

type ResultadoPaginaDespesas = {
  paginaAtual: number;
  limite: number;
  total: number;
  totalPaginas: number;
  despesas: DespesaRelatorio[];
};

type TotaisPeriodo = {
  totalFaturado: number;
  totalDespesas: number;
  lucroReal: number;
  quantidadeViagens: number;
  quantidadeDespesas: number;
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

@Injectable()
export class RelatoriosFaturamentoService {
  private readonly limiteViagens = 100;
  private readonly limiteDespesas = 200;
  private readonly limiteMaximoPaginas = 5000;

  constructor(
    private readonly viagensService: ViagensService,
    private readonly despesasService: DespesasService,
  ) {}

  async obterResumo(
    idEmpresa: number,
    filtro: FiltroRelatorioFaturamentoDto,
  ) {
    const periodo = this.resolverPeriodo(filtro);
    const totais = await this.carregarTotaisPeriodo(idEmpresa, periodo);

    return {
      sucesso: true,
      periodo: this.montarPeriodoResposta(periodo),
      indicadores: {
        faturamento: {
          id: 'faturamento',
          titulo: 'Total faturado',
          valor: totais.totalFaturado,
          descricao: 'Soma do valor de frete das viagens iniciadas no periodo.',
          quantidadeRegistros: totais.quantidadeViagens,
          tipoRegistro: 'viagens',
        },
        despesas: {
          id: 'despesas',
          titulo: 'Despesas do periodo',
          valor: totais.totalDespesas,
          descricao:
            'Soma dos lancamentos ativos de despesas com data dentro do periodo.',
          quantidadeRegistros: totais.quantidadeDespesas,
          tipoRegistro: 'despesas',
        },
        lucroReal: {
          id: 'lucro_real',
          titulo: 'Lucro real',
          valor: totais.lucroReal,
          descricao: 'Resultado real do periodo (faturamento - despesas).',
          quantidadeRegistros:
            totais.quantidadeViagens + totais.quantidadeDespesas,
          tipoRegistro: 'viagens_despesas',
        },
      },
    };
  }

  async obterDetalhes(
    idEmpresa: number,
    filtro: DetalheRelatorioFaturamentoDto,
  ) {
    const periodo = this.resolverPeriodo(filtro);

    if (filtro.indicador === 'faturamento') {
      const [totaisViagens, paginaViagens] = await Promise.all([
        this.carregarTotaisViagensPeriodo(idEmpresa, periodo),
        this.carregarPaginaViagens(idEmpresa, periodo, filtro.pagina, filtro.limite),
      ]);

      return {
        sucesso: true,
        periodo: this.montarPeriodoResposta(periodo),
        indicador: {
          id: 'faturamento',
          titulo: 'Total faturado',
          descricao: 'Soma do valor de frete das viagens iniciadas no periodo.',
          valorIndicador: totaisViagens.valor,
        },
        composicao: [
          {
            chave: 'total_faturado',
            label: 'Total faturado',
            valor: totaisViagens.valor,
            tipo: 'moeda' as const,
          },
          {
            chave: 'qtd_viagens',
            label: 'Quantidade de viagens consideradas',
            valor: totaisViagens.quantidade,
            tipo: 'numero' as const,
          },
        ],
        tabelas: [this.montarTabelaViagens(periodo, paginaViagens)],
      };
    }

    if (filtro.indicador === 'despesas') {
      const [totaisDespesas, paginaDespesas] = await Promise.all([
        this.carregarTotaisDespesasPeriodo(idEmpresa, periodo),
        this.carregarPaginaDespesas(
          idEmpresa,
          periodo,
          filtro.pagina,
          filtro.limite,
        ),
      ]);

      return {
        sucesso: true,
        periodo: this.montarPeriodoResposta(periodo),
        indicador: {
          id: 'despesas',
          titulo: 'Despesas do periodo',
          descricao:
            'Soma dos lancamentos ativos de despesas com data dentro do periodo.',
          valorIndicador: totaisDespesas.valor,
        },
        composicao: [
          {
            chave: 'total_despesas',
            label: 'Total de despesas',
            valor: totaisDespesas.valor,
            tipo: 'moeda' as const,
          },
          {
            chave: 'qtd_despesas',
            label: 'Quantidade de despesas consideradas',
            valor: totaisDespesas.quantidade,
            tipo: 'numero' as const,
          },
        ],
        tabelas: [this.montarTabelaDespesas(periodo, paginaDespesas)],
      };
    }

    const [totais, paginaViagens, paginaDespesas] = await Promise.all([
      this.carregarTotaisPeriodo(idEmpresa, periodo),
      this.carregarPaginaViagens(idEmpresa, periodo, filtro.pagina, filtro.limite),
      this.carregarPaginaDespesas(idEmpresa, periodo, filtro.pagina, filtro.limite),
    ]);

    return {
      sucesso: true,
      periodo: this.montarPeriodoResposta(periodo),
      indicador: {
        id: 'lucro_real',
        titulo: 'Lucro real',
        descricao: 'Resultado real do periodo (faturamento - despesas).',
        valorIndicador: totais.lucroReal,
      },
      composicao: [
        {
          chave: 'faturamento',
          label: 'Total faturado',
          valor: totais.totalFaturado,
          tipo: 'moeda' as const,
        },
        {
          chave: 'despesas',
          label: 'Total de despesas',
          valor: totais.totalDespesas,
          tipo: 'moeda' as const,
        },
        {
          chave: 'lucro_real',
          label: 'Lucro real',
          valor: totais.lucroReal,
          tipo: 'moeda' as const,
        },
      ],
      tabelas: [
        this.montarTabelaViagens(periodo, paginaViagens),
        this.montarTabelaDespesas(periodo, paginaDespesas),
      ],
    };
  }

  private async carregarTotaisPeriodo(
    idEmpresa: number,
    periodo: PeriodoRelatorio,
  ): Promise<TotaisPeriodo> {
    const [totaisViagens, totaisDespesas] = await Promise.all([
      this.carregarTotaisViagensPeriodo(idEmpresa, periodo),
      this.carregarTotaisDespesasPeriodo(idEmpresa, periodo),
    ]);

    const totalFaturado = this.arredondar(totaisViagens.valor);
    const totalDespesas = this.arredondar(totaisDespesas.valor);

    return {
      totalFaturado,
      totalDespesas,
      lucroReal: this.arredondar(totalFaturado - totalDespesas),
      quantidadeViagens: totaisViagens.quantidade,
      quantidadeDespesas: totaisDespesas.quantidade,
    };
  }

  private async carregarTotaisViagensPeriodo(
    idEmpresa: number,
    periodo: PeriodoRelatorio,
  ): Promise<{ valor: number; quantidade: number }> {
    let pagina = 1;
    let totalPaginas = 1;
    let totalValor = 0;
    let totalQuantidade = 0;

    while (pagina <= totalPaginas && pagina <= this.limiteMaximoPaginas) {
      const resultado = (await this.viagensService.listarComFiltro(idEmpresa, {
        dataInicioDe: periodo.inicioIso,
        dataInicioAte: periodo.fimIso,
        pagina,
        limite: this.limiteViagens,
        ordenarPor: 'data_inicio',
        ordem: 'DESC',
      })) as ResultadoPaginaViagens;

      const viagens = Array.isArray(resultado.viagens) ? resultado.viagens : [];

      for (const viagem of viagens) {
        totalValor += this.converterNumero(viagem.valorFrete);
      }

      totalQuantidade += viagens.length;
      totalPaginas = Math.max(1, this.converterInteiro(resultado.paginas));
      pagina += 1;
    }

    return {
      valor: this.arredondar(totalValor),
      quantidade: totalQuantidade,
    };
  }

  private async carregarTotaisDespesasPeriodo(
    idEmpresa: number,
    periodo: PeriodoRelatorio,
  ): Promise<{ valor: number; quantidade: number }> {
    let pagina = 1;
    let totalPaginas = 1;
    let totalValor = 0;
    let totalQuantidade = 0;

    while (pagina <= totalPaginas && pagina <= this.limiteMaximoPaginas) {
      const resultado = (await this.despesasService.listarComFiltro(idEmpresa, {
        dataDe: periodo.inicioIso,
        dataAte: periodo.fimIso,
        situacao: 'ATIVO',
        pagina,
        limite: this.limiteDespesas,
        ordenarPor: 'data',
        ordem: 'DESC',
      })) as ResultadoPaginaDespesas;

      const despesas = Array.isArray(resultado.despesas)
        ? resultado.despesas
        : [];

      for (const despesa of despesas) {
        totalValor += this.converterNumero(despesa.valor);
      }

      totalQuantidade += despesas.length;
      totalPaginas = Math.max(1, this.converterInteiro(resultado.totalPaginas));
      pagina += 1;
    }

    return {
      valor: this.arredondar(totalValor),
      quantidade: totalQuantidade,
    };
  }

  private async carregarPaginaViagens(
    idEmpresa: number,
    periodo: PeriodoRelatorio,
    pagina: number,
    limite: number,
  ) {
    const resultado = (await this.viagensService.listarComFiltro(idEmpresa, {
      dataInicioDe: periodo.inicioIso,
      dataInicioAte: periodo.fimIso,
      pagina,
      limite,
      ordenarPor: 'data_inicio',
      ordem: 'DESC',
    })) as ResultadoPaginaViagens;

    return {
      pagina: this.converterInteiro(resultado.pagina) || pagina,
      limite: this.converterInteiro(resultado.limite) || limite,
      total: this.converterInteiro(resultado.total),
      totalPaginas: this.converterInteiro(resultado.paginas),
      viagens: Array.isArray(resultado.viagens) ? resultado.viagens : [],
    };
  }

  private async carregarPaginaDespesas(
    idEmpresa: number,
    periodo: PeriodoRelatorio,
    pagina: number,
    limite: number,
  ) {
    const resultado = (await this.despesasService.listarComFiltro(idEmpresa, {
      dataDe: periodo.inicioIso,
      dataAte: periodo.fimIso,
      situacao: 'ATIVO',
      pagina,
      limite,
      ordenarPor: 'data',
      ordem: 'DESC',
    })) as ResultadoPaginaDespesas;

    return {
      pagina: this.converterInteiro(resultado.paginaAtual) || pagina,
      limite: this.converterInteiro(resultado.limite) || limite,
      total: this.converterInteiro(resultado.total),
      totalPaginas: this.converterInteiro(resultado.totalPaginas),
      despesas: Array.isArray(resultado.despesas) ? resultado.despesas : [],
    };
  }

  private montarTabelaViagens(
    periodo: PeriodoRelatorio,
    pagina: {
      pagina: number;
      limite: number;
      total: number;
      totalPaginas: number;
      viagens: ViagemRelatorio[];
    },
  ): TabelaDetalhe {
    return {
      id: 'viagens',
      titulo: 'Viagens consideradas',
      descricao: `Viagens com data de inicio entre ${periodo.inicioData} e ${periodo.fimData}.`,
      paginacao: {
        pagina: pagina.pagina,
        limite: pagina.limite,
        totalRegistros: pagina.total,
        totalPaginas: pagina.totalPaginas,
      },
      colunas: [
        { chave: 'idViagem', label: 'Viagem', tipo: 'numero' },
        { chave: 'dataInicio', label: 'Data inicio', tipo: 'data' },
        { chave: 'dataFim', label: 'Data fim', tipo: 'data' },
        { chave: 'idVeiculo', label: 'Veiculo', tipo: 'numero' },
        { chave: 'idMotorista', label: 'Motorista', tipo: 'numero' },
        { chave: 'status', label: 'Status', tipo: 'texto' },
        { chave: 'valorFrete', label: 'Valor frete', tipo: 'moeda' },
      ],
      linhas: pagina.viagens.map((viagem) => ({
        idViagem: viagem.idViagem,
        dataInicio: this.formatarDataIso(viagem.dataInicio),
        dataFim: this.formatarDataIso(viagem.dataFim),
        idVeiculo: viagem.idVeiculo,
        idMotorista: viagem.idMotorista,
        status: viagem.status,
        valorFrete: this.arredondar(this.converterNumero(viagem.valorFrete)),
        observacao: viagem.observacao,
        totalDespesas: this.arredondar(this.converterNumero(viagem.totalDespesas)),
        totalLucro: this.arredondar(this.converterNumero(viagem.totalLucro)),
      })),
    };
  }

  private montarTabelaDespesas(
    periodo: PeriodoRelatorio,
    pagina: {
      pagina: number;
      limite: number;
      total: number;
      totalPaginas: number;
      despesas: DespesaRelatorio[];
    },
  ): TabelaDetalhe {
    return {
      id: 'despesas',
      titulo: 'Despesas consideradas',
      descricao: `Despesas ativas com data entre ${periodo.inicioData} e ${periodo.fimData}.`,
      paginacao: {
        pagina: pagina.pagina,
        limite: pagina.limite,
        totalRegistros: pagina.total,
        totalPaginas: pagina.totalPaginas,
      },
      colunas: [
        { chave: 'idDespesa', label: 'Despesa', tipo: 'numero' },
        { chave: 'data', label: 'Data', tipo: 'data' },
        { chave: 'tipoDescricao', label: 'Tipo', tipo: 'texto' },
        { chave: 'descricao', label: 'Descricao', tipo: 'texto' },
        { chave: 'idVeiculo', label: 'Veiculo', tipo: 'numero' },
        { chave: 'idMotorista', label: 'Motorista', tipo: 'numero' },
        { chave: 'idViagem', label: 'Viagem', tipo: 'numero' },
        { chave: 'valor', label: 'Valor', tipo: 'moeda' },
      ],
      linhas: pagina.despesas.map((despesa) => ({
        idDespesa: despesa.idDespesa,
        data: this.formatarDataIso(despesa.data),
        tipo: despesa.tipo,
        tipoDescricao: despesa.tipoDescricao,
        descricao: despesa.descricao,
        idVeiculo: despesa.idVeiculo,
        idMotorista: despesa.idMotorista,
        idViagem: despesa.idViagem,
        valor: this.arredondar(this.converterNumero(despesa.valor)),
      })),
    };
  }

  private resolverPeriodo(
    filtro: FiltroRelatorioFaturamentoDto,
  ): PeriodoRelatorio {
    const agora = new Date();
    const mesAtual = agora.getUTCMonth() + 1;
    const anoAtual = agora.getUTCFullYear();
    const mes = filtro.mes ?? mesAtual;
    const ano = filtro.ano ?? anoAtual;

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

  private montarPeriodoResposta(periodo: PeriodoRelatorio) {
    return {
      mes: periodo.mes,
      ano: periodo.ano,
      inicio: periodo.inicioData,
      fim: periodo.fimData,
      descricao: periodo.descricao,
    };
  }

  private formatarDataIso(valor: Date | string | null): string | null {
    if (!valor) {
      return null;
    }

    const data = valor instanceof Date ? valor : new Date(valor);
    if (Number.isNaN(data.getTime())) {
      return null;
    }

    return data.toISOString();
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
