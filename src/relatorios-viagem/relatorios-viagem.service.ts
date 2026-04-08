import { Injectable } from '@nestjs/common';
import { AbastecimentosService } from '../abastecimentos/abastecimentos.service';
import { DespesasService } from '../despesas/despesas.service';
import { MultasService } from '../multas/multas.service';
import { ViagensService } from '../viagens/viagens.service';
import { FiltroRelatorioViagemDto } from './dto/filtro-relatorio-viagem.dto';

type TipoColuna = 'moeda' | 'numero' | 'data' | 'texto';

type ColunaTabela = {
  chave: string;
  label: string;
  tipo?: TipoColuna;
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
  totalAbastecimentos: number | null;
  totalLucro: number | null;
};

type ResultadoBuscaViagem = {
  viagem?: ViagemRelatorio;
};

type AbastecimentoRelatorio = {
  idAbastecimento: number;
  idVeiculo: number;
  idFornecedor: number;
  dataAbastecimento: Date | string;
  litros: number;
  valorLitro: number;
  valorTotal: number;
  km: number;
};

type ResultadoPaginaAbastecimentos = {
  paginaAtual: number;
  limite: number;
  total: number;
  totalPaginas: number;
  abastecimentos: AbastecimentoRelatorio[];
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

type MultaRelatorio = {
  idMulta: number;
  idMotorista: number;
  idVeiculo: number;
  dataMulta: Date | string;
  dataVencimento: Date | string | null;
  descricao: string | null;
  valor: number;
  pontos: number | null;
  status: string | null;
  orgaoAutuador: string | null;
};

type ResultadoPaginaMultas = {
  paginaAtual: number;
  limite: number;
  total: number;
  totalPaginas: number;
  multas: MultaRelatorio[];
};

type BlocoTabela = {
  totalRegistros: number;
  colunas: ColunaTabela[];
  linhas: Array<Record<string, unknown>>;
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

const colunasAbastecimentos: ColunaTabela[] = [
  { chave: 'idAbastecimento', label: 'Abastecimento', tipo: 'numero' },
  { chave: 'dataAbastecimento', label: 'Data', tipo: 'data' },
  { chave: 'idVeiculo', label: 'Veiculo', tipo: 'numero' },
  { chave: 'idFornecedor', label: 'Fornecedor', tipo: 'numero' },
  { chave: 'litros', label: 'Litros', tipo: 'numero' },
  { chave: 'valorLitro', label: 'Valor/Litro', tipo: 'moeda' },
  { chave: 'valorTotal', label: 'Valor total', tipo: 'moeda' },
  { chave: 'km', label: 'KM', tipo: 'numero' },
];

const colunasDespesas: ColunaTabela[] = [
  { chave: 'idDespesa', label: 'Despesa', tipo: 'numero' },
  { chave: 'data', label: 'Data', tipo: 'data' },
  { chave: 'tipoDescricao', label: 'Tipo', tipo: 'texto' },
  { chave: 'descricao', label: 'Descricao', tipo: 'texto' },
  { chave: 'idVeiculo', label: 'Veiculo', tipo: 'numero' },
  { chave: 'idMotorista', label: 'Motorista', tipo: 'numero' },
  { chave: 'idViagem', label: 'Viagem', tipo: 'numero' },
  { chave: 'valor', label: 'Valor', tipo: 'moeda' },
];

const colunasMultas: ColunaTabela[] = [
  { chave: 'idMulta', label: 'Multa', tipo: 'numero' },
  { chave: 'dataMulta', label: 'Data', tipo: 'data' },
  { chave: 'idVeiculo', label: 'Veiculo', tipo: 'numero' },
  { chave: 'idMotorista', label: 'Motorista', tipo: 'numero' },
  { chave: 'status', label: 'Status', tipo: 'texto' },
  { chave: 'descricao', label: 'Descricao', tipo: 'texto' },
  { chave: 'orgaoAutuador', label: 'Orgao', tipo: 'texto' },
  { chave: 'pontos', label: 'Pontos', tipo: 'numero' },
  { chave: 'valor', label: 'Valor', tipo: 'moeda' },
];

@Injectable()
export class RelatoriosViagemService {
  private readonly limitePaginacao = 200;
  private readonly maxPaginasConsulta = 5000;

  constructor(
    private readonly viagensService: ViagensService,
    private readonly abastecimentosService: AbastecimentosService,
    private readonly despesasService: DespesasService,
    private readonly multasService: MultasService,
  ) {}

  async obterRelatorio(idEmpresa: number, filtro: FiltroRelatorioViagemDto) {
    const periodo = this.resolverPeriodo(filtro);
    const idViagem = this.converterInteiro(filtro.idViagem);

    if (idViagem <= 0) {
      return this.montarRespostaSemViagem(periodo);
    }

    const viagem = await this.carregarViagem(idEmpresa, idViagem);
    const [abastecimentosPeriodo, despesas, multasPeriodo] = await Promise.all([
      this.carregarAbastecimentosPeriodo(idEmpresa, periodo, viagem.idVeiculo),
      this.carregarDespesasViagemPeriodo(idEmpresa, periodo, idViagem),
      this.carregarMultasPeriodo(
        idEmpresa,
        periodo,
        viagem.idVeiculo,
        viagem.idMotorista,
      ),
    ]);

    const abastecimentos = this.filtrarAbastecimentosDaViagem(
      abastecimentosPeriodo,
      viagem,
    );
    const multas = this.filtrarMultasDaViagem(multasPeriodo, viagem);

    const totalFaturado = this.arredondar(this.converterNumero(viagem.valorFrete));
    const totalAbastecimentos = this.arredondar(
      abastecimentos.reduce((acumulado, item) => {
        return acumulado + this.converterNumero(item.valorTotal);
      }, 0),
    );
    const totalDespesas = this.arredondar(
      despesas.reduce((acumulado, item) => {
        return acumulado + this.converterNumero(item.valor);
      }, 0),
    );
    const totalMultas = this.arredondar(
      multas.reduce((acumulado, item) => {
        return acumulado + this.converterNumero(item.valor);
      }, 0),
    );
    const custoTotal = this.arredondar(
      totalAbastecimentos + totalDespesas + totalMultas,
    );
    const resultadoReal = this.arredondar(totalFaturado - custoTotal);

    return {
      sucesso: true,
      periodo: this.montarPeriodoResposta(periodo),
      viagem: {
        idViagem: viagem.idViagem,
        idVeiculo: viagem.idVeiculo,
        idMotorista: viagem.idMotorista,
        dataInicio: this.formatarDataIso(viagem.dataInicio),
        dataFim: this.formatarDataIso(viagem.dataFim),
        status: viagem.status,
        observacao: viagem.observacao,
        valorFrete: this.arredondar(this.converterNumero(viagem.valorFrete)),
        totalDespesas: this.arredondar(this.converterNumero(viagem.totalDespesas)),
        totalAbastecimentos: this.arredondar(
          this.converterNumero(viagem.totalAbastecimentos),
        ),
        totalLucro: this.arredondar(this.converterNumero(viagem.totalLucro)),
      },
      resumo: {
        totalFaturado,
        totalAbastecimentos,
        totalDespesas,
        totalMultas,
        custoTotal,
        resultadoReal,
        totalLancamentos:
          abastecimentos.length + despesas.length + multas.length,
      },
      lancamentos: {
        abastecimentos: this.montarTabelaAbastecimentos(abastecimentos),
        despesas: this.montarTabelaDespesas(despesas),
        multas: this.montarTabelaMultas(multas),
      },
    };
  }

  private montarRespostaSemViagem(periodo: PeriodoRelatorio) {
    return {
      sucesso: true,
      periodo: this.montarPeriodoResposta(periodo),
      viagem: null,
      resumo: {
        totalFaturado: 0,
        totalAbastecimentos: 0,
        totalDespesas: 0,
        totalMultas: 0,
        custoTotal: 0,
        resultadoReal: 0,
        totalLancamentos: 0,
      },
      lancamentos: {
        abastecimentos: this.montarTabelaAbastecimentos([]),
        despesas: this.montarTabelaDespesas([]),
        multas: this.montarTabelaMultas([]),
      },
      mensagem: 'Selecione uma viagem para visualizar os dados do periodo.',
    };
  }

  private async carregarViagem(idEmpresa: number, idViagem: number) {
    const resultado = (await this.viagensService.buscarPorId(
      idEmpresa,
      idViagem,
    )) as ResultadoBuscaViagem;

    return resultado.viagem as ViagemRelatorio;
  }

  private async carregarAbastecimentosPeriodo(
    idEmpresa: number,
    periodo: PeriodoRelatorio,
    idVeiculo: number,
  ) {
    const abastecimentos: AbastecimentoRelatorio[] = [];
    let pagina = 1;
    let totalPaginas = 1;

    while (pagina <= totalPaginas && pagina <= this.maxPaginasConsulta) {
      const resultado = (await this.abastecimentosService.listarComFiltro(
        idEmpresa,
        {
          idVeiculo,
          dataDe: periodo.inicioData,
          dataAte: periodo.fimData,
          pagina,
          limite: this.limitePaginacao,
          ordenarPor: 'data_abastecimento',
          ordem: 'DESC',
        },
      )) as ResultadoPaginaAbastecimentos;

      abastecimentos.push(
        ...(Array.isArray(resultado.abastecimentos)
          ? resultado.abastecimentos
          : []),
      );
      totalPaginas = Math.max(1, this.converterInteiro(resultado.totalPaginas));
      pagina += 1;
    }

    return abastecimentos;
  }

  private async carregarDespesasViagemPeriodo(
    idEmpresa: number,
    periodo: PeriodoRelatorio,
    idViagem: number,
  ) {
    const despesas: DespesaRelatorio[] = [];
    let pagina = 1;
    let totalPaginas = 1;

    while (pagina <= totalPaginas && pagina <= this.maxPaginasConsulta) {
      const resultado = (await this.despesasService.listarComFiltro(idEmpresa, {
        idViagem,
        dataDe: periodo.inicioIso,
        dataAte: periodo.fimIso,
        situacao: 'ATIVO',
        pagina,
        limite: this.limitePaginacao,
        ordenarPor: 'data',
        ordem: 'DESC',
      })) as ResultadoPaginaDespesas;

      despesas.push(...(Array.isArray(resultado.despesas) ? resultado.despesas : []));
      totalPaginas = Math.max(1, this.converterInteiro(resultado.totalPaginas));
      pagina += 1;
    }

    return despesas;
  }

  private async carregarMultasPeriodo(
    idEmpresa: number,
    periodo: PeriodoRelatorio,
    idVeiculo: number,
    idMotorista: number,
  ) {
    const multas: MultaRelatorio[] = [];
    let pagina = 1;
    let totalPaginas = 1;

    while (pagina <= totalPaginas && pagina <= this.maxPaginasConsulta) {
      const resultado = (await this.multasService.listarComFiltro(idEmpresa, {
        idVeiculo,
        idMotorista,
        dataDe: periodo.inicioIso,
        dataAte: periodo.fimIso,
        pagina,
        limite: this.limitePaginacao,
        ordenarPor: 'data_multa',
        ordem: 'DESC',
      })) as ResultadoPaginaMultas;

      multas.push(...(Array.isArray(resultado.multas) ? resultado.multas : []));
      totalPaginas = Math.max(1, this.converterInteiro(resultado.totalPaginas));
      pagina += 1;
    }

    return multas;
  }

  private filtrarAbastecimentosDaViagem(
    abastecimentos: AbastecimentoRelatorio[],
    viagem: ViagemRelatorio,
  ) {
    const inicioMs = this.converterDataParaMs(viagem.dataInicio);
    if (inicioMs === null) {
      return [];
    }

    const fimMs = this.converterDataParaMs(viagem.dataFim);
    return abastecimentos.filter((abastecimento) => {
      const dataMs = this.converterDataParaMs(abastecimento.dataAbastecimento);
      if (dataMs === null) {
        return false;
      }

      if (dataMs < inicioMs) {
        return false;
      }

      if (fimMs !== null && dataMs > fimMs) {
        return false;
      }

      return true;
    });
  }

  private filtrarMultasDaViagem(multas: MultaRelatorio[], viagem: ViagemRelatorio) {
    const inicioMs = this.converterDataParaMs(viagem.dataInicio);
    if (inicioMs === null) {
      return [];
    }

    const fimMs = this.converterDataParaMs(viagem.dataFim);
    return multas.filter((multa) => {
      const dataMs = this.converterDataParaMs(multa.dataMulta);
      if (dataMs === null) {
        return false;
      }

      if (dataMs < inicioMs) {
        return false;
      }

      if (fimMs !== null && dataMs > fimMs) {
        return false;
      }

      return true;
    });
  }

  private montarTabelaAbastecimentos(abastecimentos: AbastecimentoRelatorio[]): BlocoTabela {
    return {
      totalRegistros: abastecimentos.length,
      colunas: colunasAbastecimentos,
      linhas: abastecimentos.map((item) => ({
        idAbastecimento: this.converterInteiro(item.idAbastecimento),
        dataAbastecimento: this.formatarDataIso(item.dataAbastecimento),
        idVeiculo: this.converterInteiro(item.idVeiculo),
        idFornecedor: this.converterInteiro(item.idFornecedor),
        litros: this.arredondar(this.converterNumero(item.litros), 3),
        valorLitro: this.arredondar(this.converterNumero(item.valorLitro), 4),
        valorTotal: this.arredondar(this.converterNumero(item.valorTotal)),
        km: this.arredondar(this.converterNumero(item.km), 2),
      })),
    };
  }

  private montarTabelaDespesas(despesas: DespesaRelatorio[]): BlocoTabela {
    return {
      totalRegistros: despesas.length,
      colunas: colunasDespesas,
      linhas: despesas.map((item) => ({
        idDespesa: this.converterInteiro(item.idDespesa),
        data: this.formatarDataIso(item.data),
        tipo: item.tipo,
        tipoDescricao: item.tipoDescricao,
        descricao: item.descricao,
        idVeiculo: this.converterInteiroOuNulo(item.idVeiculo),
        idMotorista: this.converterInteiroOuNulo(item.idMotorista),
        idViagem: this.converterInteiroOuNulo(item.idViagem),
        valor: this.arredondar(this.converterNumero(item.valor)),
      })),
    };
  }

  private montarTabelaMultas(multas: MultaRelatorio[]): BlocoTabela {
    return {
      totalRegistros: multas.length,
      colunas: colunasMultas,
      linhas: multas.map((item) => ({
        idMulta: this.converterInteiro(item.idMulta),
        dataMulta: this.formatarDataIso(item.dataMulta),
        idVeiculo: this.converterInteiro(item.idVeiculo),
        idMotorista: this.converterInteiro(item.idMotorista),
        status: item.status,
        descricao: item.descricao,
        orgaoAutuador: item.orgaoAutuador,
        pontos: this.converterInteiroOuNulo(item.pontos),
        valor: this.arredondar(this.converterNumero(item.valor)),
      })),
    };
  }

  private resolverPeriodo(filtro: FiltroRelatorioViagemDto): PeriodoRelatorio {
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

  private montarPeriodoResposta(periodo: PeriodoRelatorio) {
    return {
      mes: periodo.mes,
      ano: periodo.ano,
      inicio: periodo.inicioData,
      fim: periodo.fimData,
      descricao: periodo.descricao,
    };
  }

  private formatarDataIso(valor: Date | string | null | undefined) {
    if (!valor) {
      return null;
    }

    const data = valor instanceof Date ? valor : new Date(valor);
    if (Number.isNaN(data.getTime())) {
      return null;
    }

    return data.toISOString();
  }

  private converterDataParaMs(valor: Date | string | null | undefined) {
    if (!valor) {
      return null;
    }

    const data = valor instanceof Date ? valor : new Date(valor);
    if (Number.isNaN(data.getTime())) {
      return null;
    }

    return data.getTime();
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
