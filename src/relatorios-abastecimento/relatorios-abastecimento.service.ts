import { Injectable } from '@nestjs/common';
import { AbastecimentosService } from '../abastecimentos/abastecimentos.service';
import { ViagensService } from '../viagens/viagens.service';
import { FiltroRelatorioAbastecimentoDto } from './dto/filtro-relatorio-abastecimento.dto';

type TipoColuna = 'moeda' | 'numero' | 'data' | 'texto';

type ColunaTabela = {
  chave: string;
  label: string;
  tipo?: TipoColuna;
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

type ViagemRelatorio = {
  idViagem: number;
  idVeiculo: number;
  idMotorista: number;
  dataInicio: Date | string;
  dataFim: Date | string | null;
  kmInicial: number;
  kmFinal: number | null;
  status: string;
};

type ResultadoPaginaViagens = {
  pagina: number;
  limite: number;
  total: number;
  paginas: number;
  viagens: ViagemRelatorio[];
};

type PeriodoRelatorio = {
  mes: number;
  ano: number;
  inicio: Date;
  fim: Date;
  inicioData: string;
  fimData: string;
  descricao: string;
};

type VinculoAbastecimento = {
  idViagem: number | null;
  idMotorista: number | null;
  kmViagem: number | null;
  dataInicioViagem: string | null;
  dataFimViagem: string | null;
};

type ViagemVinculoIntervalo = {
  idViagem: number;
  idMotorista: number | null;
  dataInicioMs: number;
  dataFimMs: number | null;
  kmViagem: number | null;
  dataInicioIso: string | null;
  dataFimIso: string | null;
};

type AcumuladorMediaMotorista = {
  idMotorista: number | null;
  totalAbastecimentos: number;
  litrosTotal: number;
  valorTotal: number;
  veiculos: Set<number>;
  viagens: Set<number>;
  viagensComKmSomadas: Set<number>;
  kmTotalViagens: number;
};

type AcumuladorMediaVeiculo = {
  idVeiculo: number;
  totalAbastecimentos: number;
  litrosTotal: number;
  valorTotal: number;
  motoristas: Set<number>;
  viagens: Set<number>;
  viagensComKmSomadas: Set<number>;
  kmTotalViagens: number;
};

type FiltrosRelacionados = {
  idVeiculo?: number;
  idMotorista?: number;
};

const colunasAbastecimentos: ColunaTabela[] = [
  { chave: 'idAbastecimento', label: 'Abastecimento', tipo: 'numero' },
  { chave: 'dataAbastecimento', label: 'Data', tipo: 'data' },
  { chave: 'idVeiculo', label: 'Veiculo', tipo: 'numero' },
  { chave: 'idMotorista', label: 'Motorista', tipo: 'numero' },
  { chave: 'idViagem', label: 'Viagem', tipo: 'numero' },
  { chave: 'idFornecedor', label: 'Fornecedor', tipo: 'numero' },
  { chave: 'litros', label: 'Litros', tipo: 'numero' },
  { chave: 'valorLitro', label: 'Valor/Litro', tipo: 'moeda' },
  { chave: 'valorTotal', label: 'Valor total', tipo: 'moeda' },
  { chave: 'km', label: 'KM', tipo: 'numero' },
];

const colunasMediaViagem: ColunaTabela[] = [
  { chave: 'idViagem', label: 'Viagem', tipo: 'numero' },
  { chave: 'idVeiculo', label: 'Veiculo', tipo: 'numero' },
  { chave: 'idMotorista', label: 'Motorista', tipo: 'numero' },
  { chave: 'totalAbastecimentos', label: 'Abastecimentos', tipo: 'numero' },
  { chave: 'litrosTotal', label: 'Litros total', tipo: 'numero' },
  { chave: 'valorTotal', label: 'Valor total', tipo: 'moeda' },
  { chave: 'mediaLitros', label: 'Media litros', tipo: 'numero' },
  { chave: 'ticketMedio', label: 'Ticket medio', tipo: 'moeda' },
  { chave: 'custoMedioLitro', label: 'Custo medio/L', tipo: 'moeda' },
  { chave: 'kmViagem', label: 'KM viagem', tipo: 'numero' },
  { chave: 'consumoKmLitro', label: 'Media KM/L', tipo: 'numero' },
];

const colunasMediaMotorista: ColunaTabela[] = [
  { chave: 'idMotorista', label: 'Motorista', tipo: 'numero' },
  { chave: 'totalAbastecimentos', label: 'Abastecimentos', tipo: 'numero' },
  { chave: 'viagensVinculadas', label: 'Viagens', tipo: 'numero' },
  { chave: 'veiculosUtilizados', label: 'Veiculos usados', tipo: 'numero' },
  { chave: 'veiculosIds', label: 'Veiculos do mes', tipo: 'texto' },
  { chave: 'litrosTotal', label: 'Litros total', tipo: 'numero' },
  { chave: 'valorTotal', label: 'Valor total', tipo: 'moeda' },
  { chave: 'mediaLitros', label: 'Media litros', tipo: 'numero' },
  { chave: 'ticketMedio', label: 'Ticket medio', tipo: 'moeda' },
  { chave: 'custoMedioLitro', label: 'Custo medio/L', tipo: 'moeda' },
  { chave: 'kmTotalViagens', label: 'KM total viagens', tipo: 'numero' },
  { chave: 'consumoKmLitro', label: 'Media KM/L', tipo: 'numero' },
];

const colunasMediaVeiculo: ColunaTabela[] = [
  { chave: 'idVeiculo', label: 'Veiculo', tipo: 'numero' },
  { chave: 'totalAbastecimentos', label: 'Abastecimentos', tipo: 'numero' },
  { chave: 'viagensVinculadas', label: 'Viagens', tipo: 'numero' },
  { chave: 'motoristasUtilizados', label: 'Motoristas', tipo: 'numero' },
  { chave: 'motoristasIds', label: 'Motoristas do mes', tipo: 'texto' },
  { chave: 'litrosTotal', label: 'Litros total', tipo: 'numero' },
  { chave: 'valorTotal', label: 'Valor total', tipo: 'moeda' },
  { chave: 'mediaLitros', label: 'Media litros', tipo: 'numero' },
  { chave: 'ticketMedio', label: 'Ticket medio', tipo: 'moeda' },
  { chave: 'custoMedioLitro', label: 'Custo medio/L', tipo: 'moeda' },
  { chave: 'kmTotalViagens', label: 'KM total viagens', tipo: 'numero' },
  { chave: 'consumoKmLitro', label: 'Media KM/L', tipo: 'numero' },
];

@Injectable()
export class RelatoriosAbastecimentoService {
  private readonly limitePaginacao = 100;
  private readonly maxPaginasConsulta = 500;
  private readonly janelaDiasViagens = 62;

  constructor(
    private readonly abastecimentosService: AbastecimentosService,
    private readonly viagensService: ViagensService,
  ) {}

  async obterRelatorio(
    idEmpresa: number,
    filtro: FiltroRelatorioAbastecimentoDto,
  ) {
    const periodo = this.resolverPeriodo(filtro);
    const filtrosRelacionados = this.extrairFiltrosRelacionados(filtro);
    const [abastecimentos, viagens] = await Promise.all([
      this.carregarAbastecimentosPeriodo(idEmpresa, periodo, filtrosRelacionados),
      this.carregarViagensParaVinculo(idEmpresa, periodo, filtrosRelacionados),
    ]);

    const vinculos = this.vincularAbastecimentosComViagens(
      abastecimentos,
      viagens,
    );
    const abastecimentosFiltrados = this.aplicarFiltroMotorista(
      abastecimentos,
      vinculos,
      filtrosRelacionados.idMotorista,
    );
    const resumo = this.calcularResumo(abastecimentosFiltrados);
    const tabelaAbastecimentos = this.montarTabelaAbastecimentos(
      abastecimentosFiltrados,
      vinculos,
    );
    const medias = this.montarTabelasMedias(abastecimentosFiltrados, vinculos);

    return {
      sucesso: true,
      periodo: {
        mes: periodo.mes,
        ano: periodo.ano,
        inicio: periodo.inicioData,
        fim: periodo.fimData,
        descricao: periodo.descricao,
      },
      resumo,
      abastecimentos: tabelaAbastecimentos,
      medias,
    };
  }

  private async carregarAbastecimentosPeriodo(
    idEmpresa: number,
    periodo: PeriodoRelatorio,
    filtrosRelacionados: FiltrosRelacionados,
  ): Promise<AbastecimentoRelatorio[]> {
    const dados: AbastecimentoRelatorio[] = [];
    let pagina = 1;
    let totalPaginas = 1;

    while (pagina <= totalPaginas && pagina <= this.maxPaginasConsulta) {
      const resultado = (await this.abastecimentosService.listarComFiltro(
        idEmpresa,
        {
          dataDe: periodo.inicioData,
          dataAte: periodo.fimData,
          idVeiculo: filtrosRelacionados.idVeiculo,
          pagina,
          limite: this.limitePaginacao,
          ordenarPor: 'data_abastecimento',
          ordem: 'DESC',
        },
      )) as ResultadoPaginaAbastecimentos;

      dados.push(...(resultado.abastecimentos ?? []));
      totalPaginas = Math.max(
        1,
        this.converterInteiro(resultado.totalPaginas),
      );
      pagina += 1;
    }

    return dados;
  }

  private async carregarViagensParaVinculo(
    idEmpresa: number,
    periodo: PeriodoRelatorio,
    filtrosRelacionados: FiltrosRelacionados,
  ): Promise<ViagemRelatorio[]> {
    const inicioJanela = new Date(periodo.inicio);
    inicioJanela.setUTCDate(inicioJanela.getUTCDate() - this.janelaDiasViagens);
    const inicioJanelaData = inicioJanela.toISOString().slice(0, 10);

    const [viagensFaixa, viagensAbertas] = await Promise.all([
      this.carregarViagensComFiltro(idEmpresa, {
        dataInicioDe: inicioJanelaData,
        dataInicioAte: periodo.fimData,
        idVeiculo: filtrosRelacionados.idVeiculo,
        idMotorista: filtrosRelacionados.idMotorista,
      }),
      this.carregarViagensComFiltro(idEmpresa, {
        dataInicioAte: periodo.fimData,
        apenasAbertas: true,
        idVeiculo: filtrosRelacionados.idVeiculo,
        idMotorista: filtrosRelacionados.idMotorista,
      }),
    ]);

    const mapa = new Map<number, ViagemRelatorio>();
    for (const viagem of viagensFaixa) {
      mapa.set(this.converterInteiro(viagem.idViagem), viagem);
    }
    for (const viagem of viagensAbertas) {
      mapa.set(this.converterInteiro(viagem.idViagem), viagem);
    }

    return Array.from(mapa.values());
  }

  private async carregarViagensComFiltro(
    idEmpresa: number,
    filtro: {
      dataInicioDe?: string;
      dataInicioAte?: string;
      apenasAbertas?: boolean;
      idVeiculo?: number;
      idMotorista?: number;
    },
  ): Promise<ViagemRelatorio[]> {
    const dados: ViagemRelatorio[] = [];
    let pagina = 1;
    let totalPaginas = 1;

    while (pagina <= totalPaginas && pagina <= this.maxPaginasConsulta) {
      const resultado = (await this.viagensService.listarComFiltro(idEmpresa, {
        dataInicioDe: filtro.dataInicioDe,
        dataInicioAte: filtro.dataInicioAte,
        apenasAbertas: filtro.apenasAbertas,
        idVeiculo: filtro.idVeiculo,
        idMotorista: filtro.idMotorista,
        pagina,
        limite: this.limitePaginacao,
        ordenarPor: 'data_inicio',
        ordem: 'DESC',
      })) as ResultadoPaginaViagens;

      dados.push(...(resultado.viagens ?? []));
      totalPaginas = Math.max(1, this.converterInteiro(resultado.paginas));
      pagina += 1;
    }

    return dados;
  }

  private vincularAbastecimentosComViagens(
    abastecimentos: AbastecimentoRelatorio[],
    viagens: ViagemRelatorio[],
  ): Map<number, VinculoAbastecimento> {
    const viagensPorVeiculo = new Map<number, ViagemVinculoIntervalo[]>();

    for (const viagem of viagens) {
      const idVeiculo = this.converterInteiro(viagem.idVeiculo);
      if (idVeiculo <= 0) {
        continue;
      }

      const dataInicioMs = this.converterDataParaMs(viagem.dataInicio);
      if (dataInicioMs === null) {
        continue;
      }

      const dataFimMs = this.converterDataParaMs(viagem.dataFim);
      const kmFinal = viagem.kmFinal !== null ? this.converterNumero(viagem.kmFinal) : null;
      const kmInicial = this.converterNumero(viagem.kmInicial);

      const item: ViagemVinculoIntervalo = {
        idViagem: this.converterInteiro(viagem.idViagem),
        idMotorista: this.converterInteiro(viagem.idMotorista) || null,
        dataInicioMs,
        dataFimMs,
        kmViagem:
          kmFinal !== null && kmFinal >= kmInicial
            ? this.arredondar(kmFinal - kmInicial, 2)
            : null,
        dataInicioIso: this.converterDataParaIso(viagem.dataInicio),
        dataFimIso: this.converterDataParaIso(viagem.dataFim),
      };

      const lista = viagensPorVeiculo.get(idVeiculo) ?? [];
      lista.push(item);
      viagensPorVeiculo.set(idVeiculo, lista);
    }

    for (const lista of viagensPorVeiculo.values()) {
      lista.sort((a, b) => a.dataInicioMs - b.dataInicioMs);
    }

    const vinculos = new Map<number, VinculoAbastecimento>();

    for (const abastecimento of abastecimentos) {
      const idAbastecimento = this.converterInteiro(abastecimento.idAbastecimento);
      if (idAbastecimento <= 0) {
        continue;
      }

      const idVeiculo = this.converterInteiro(abastecimento.idVeiculo);
      const dataAbastecimentoMs = this.converterDataParaMs(
        abastecimento.dataAbastecimento,
      );

      if (idVeiculo <= 0 || dataAbastecimentoMs === null) {
        vinculos.set(idAbastecimento, {
          idViagem: null,
          idMotorista: null,
          kmViagem: null,
          dataInicioViagem: null,
          dataFimViagem: null,
        });
        continue;
      }

      const viagensDoVeiculo = viagensPorVeiculo.get(idVeiculo) ?? [];
      let viagemSelecionada: ViagemVinculoIntervalo | null = null;

      for (let index = viagensDoVeiculo.length - 1; index >= 0; index -= 1) {
        const viagem = viagensDoVeiculo[index];
        if (dataAbastecimentoMs < viagem.dataInicioMs) {
          continue;
        }

        if (viagem.dataFimMs !== null && dataAbastecimentoMs > viagem.dataFimMs) {
          continue;
        }

        viagemSelecionada = viagem;
        break;
      }

      vinculos.set(idAbastecimento, {
        idViagem: viagemSelecionada?.idViagem ?? null,
        idMotorista: viagemSelecionada?.idMotorista ?? null,
        kmViagem: viagemSelecionada?.kmViagem ?? null,
        dataInicioViagem: viagemSelecionada?.dataInicioIso ?? null,
        dataFimViagem: viagemSelecionada?.dataFimIso ?? null,
      });
    }

    return vinculos;
  }

  private aplicarFiltroMotorista(
    abastecimentos: AbastecimentoRelatorio[],
    vinculos: Map<number, VinculoAbastecimento>,
    idMotorista?: number,
  ) {
    if (!idMotorista || idMotorista <= 0) {
      return abastecimentos;
    }

    return abastecimentos.filter((abastecimento) => {
      const idAbastecimento = this.converterInteiro(abastecimento.idAbastecimento);
      const vinculo = vinculos.get(idAbastecimento);
      return (vinculo?.idMotorista ?? null) === idMotorista;
    });
  }

  private calcularResumo(abastecimentos: AbastecimentoRelatorio[]) {
    const totalAbastecimentos = abastecimentos.length;
    const litrosTotal = abastecimentos.reduce(
      (acumulado, item) => acumulado + this.converterNumero(item.litros),
      0,
    );
    const valorTotal = abastecimentos.reduce(
      (acumulado, item) => acumulado + this.converterNumero(item.valorTotal),
      0,
    );

    return {
      totalAbastecimentos,
      litrosTotal: this.arredondar(litrosTotal, 3),
      valorTotal: this.arredondar(valorTotal),
      mediaLitrosPorAbastecimento:
        totalAbastecimentos > 0
          ? this.arredondar(litrosTotal / totalAbastecimentos, 3)
          : 0,
      ticketMedioAbastecimento:
        totalAbastecimentos > 0
          ? this.arredondar(valorTotal / totalAbastecimentos)
          : 0,
      custoMedioLitro:
        litrosTotal > 0 ? this.arredondar(valorTotal / litrosTotal, 4) : 0,
    };
  }

  private montarTabelaAbastecimentos(
    abastecimentos: AbastecimentoRelatorio[],
    vinculos: Map<number, VinculoAbastecimento>,
  ) {
    const linhas = abastecimentos.map((abastecimento) => {
      const idAbastecimento = this.converterInteiro(abastecimento.idAbastecimento);
      const vinculo = vinculos.get(idAbastecimento);

      return {
        idAbastecimento,
        dataAbastecimento: this.converterDataParaIso(abastecimento.dataAbastecimento),
        idVeiculo: this.converterInteiro(abastecimento.idVeiculo),
        idMotorista: vinculo?.idMotorista ?? null,
        idViagem: vinculo?.idViagem ?? null,
        idFornecedor: this.converterInteiro(abastecimento.idFornecedor),
        litros: this.arredondar(this.converterNumero(abastecimento.litros), 3),
        valorLitro: this.arredondar(
          this.converterNumero(abastecimento.valorLitro),
          4,
        ),
        valorTotal: this.arredondar(this.converterNumero(abastecimento.valorTotal)),
        km: this.arredondar(this.converterNumero(abastecimento.km), 2),
      };
    });

    return {
      totalRegistros: linhas.length,
      colunas: colunasAbastecimentos,
      linhas,
    };
  }

  private montarTabelasMedias(
    abastecimentos: AbastecimentoRelatorio[],
    vinculos: Map<number, VinculoAbastecimento>,
  ) {
    const porViagem = this.calcularMediaPorViagem(abastecimentos, vinculos);
    const porMotorista = this.calcularMediaPorMotorista(abastecimentos, vinculos);
    const porVeiculo = this.calcularMediaPorVeiculo(abastecimentos, vinculos);

    return {
      porViagem: {
        totalRegistros: porViagem.length,
        colunas: colunasMediaViagem,
        linhas: porViagem,
      },
      porMotorista: {
        totalRegistros: porMotorista.length,
        colunas: colunasMediaMotorista,
        linhas: porMotorista,
      },
      porVeiculo: {
        totalRegistros: porVeiculo.length,
        colunas: colunasMediaVeiculo,
        linhas: porVeiculo,
      },
    };
  }

  private calcularMediaPorViagem(
    abastecimentos: AbastecimentoRelatorio[],
    vinculos: Map<number, VinculoAbastecimento>,
  ) {
    const acumuladores = new Map<
      string,
      {
        idViagem: number | null;
        idVeiculo: number;
        idMotorista: number | null;
        totalAbastecimentos: number;
        litrosTotal: number;
        valorTotal: number;
        kmViagem: number;
      }
    >();

    for (const abastecimento of abastecimentos) {
      const idAbastecimento = this.converterInteiro(abastecimento.idAbastecimento);
      const idVeiculo = this.converterInteiro(abastecimento.idVeiculo);
      const vinculo = vinculos.get(idAbastecimento);

      const idViagem = vinculo?.idViagem ?? null;
      const chave = idViagem ? `V-${idViagem}` : `SV-${idVeiculo}`;

      const atual = acumuladores.get(chave) ?? {
        idViagem,
        idVeiculo,
        idMotorista: vinculo?.idMotorista ?? null,
        totalAbastecimentos: 0,
        litrosTotal: 0,
        valorTotal: 0,
        kmViagem: vinculo?.kmViagem ?? 0,
      };

      atual.totalAbastecimentos += 1;
      atual.litrosTotal += this.converterNumero(abastecimento.litros);
      atual.valorTotal += this.converterNumero(abastecimento.valorTotal);

      if (atual.idMotorista === null && vinculo?.idMotorista) {
        atual.idMotorista = vinculo.idMotorista;
      }

      if ((atual.kmViagem ?? 0) <= 0 && (vinculo?.kmViagem ?? 0) > 0) {
        atual.kmViagem = vinculo?.kmViagem ?? 0;
      }

      acumuladores.set(chave, atual);
    }

    return Array.from(acumuladores.values())
      .map((item) => {
        const mediaLitros =
          item.totalAbastecimentos > 0
            ? item.litrosTotal / item.totalAbastecimentos
            : 0;
        const ticketMedio =
          item.totalAbastecimentos > 0
            ? item.valorTotal / item.totalAbastecimentos
            : 0;
        const custoMedioLitro =
          item.litrosTotal > 0 ? item.valorTotal / item.litrosTotal : 0;
        const consumoKmLitro =
          item.kmViagem > 0 && item.litrosTotal > 0
            ? item.kmViagem / item.litrosTotal
            : null;

        return {
          idViagem: item.idViagem,
          idVeiculo: item.idVeiculo,
          idMotorista: item.idMotorista,
          totalAbastecimentos: item.totalAbastecimentos,
          litrosTotal: this.arredondar(item.litrosTotal, 3),
          valorTotal: this.arredondar(item.valorTotal),
          mediaLitros: this.arredondar(mediaLitros, 3),
          ticketMedio: this.arredondar(ticketMedio),
          custoMedioLitro: this.arredondar(custoMedioLitro, 4),
          kmViagem: this.arredondar(item.kmViagem, 2),
          consumoKmLitro:
            consumoKmLitro !== null
              ? this.arredondar(consumoKmLitro, 3)
              : null,
        };
      })
      .sort((a, b) => {
        if (a.idViagem === null && b.idViagem !== null) return 1;
        if (a.idViagem !== null && b.idViagem === null) return -1;
        return b.valorTotal - a.valorTotal;
      });
  }

  private calcularMediaPorMotorista(
    abastecimentos: AbastecimentoRelatorio[],
    vinculos: Map<number, VinculoAbastecimento>,
  ) {
    const acumuladores = new Map<string, AcumuladorMediaMotorista>();

    for (const abastecimento of abastecimentos) {
      const idAbastecimento = this.converterInteiro(abastecimento.idAbastecimento);
      const idVeiculo = this.converterInteiro(abastecimento.idVeiculo);
      const vinculo = vinculos.get(idAbastecimento);
      const idMotorista = vinculo?.idMotorista ?? null;
      const chave = idMotorista ? `M-${idMotorista}` : 'M-SEM';

      const atual = acumuladores.get(chave) ?? {
        idMotorista,
        totalAbastecimentos: 0,
        litrosTotal: 0,
        valorTotal: 0,
        veiculos: new Set<number>(),
        viagens: new Set<number>(),
        viagensComKmSomadas: new Set<number>(),
        kmTotalViagens: 0,
      };

      atual.totalAbastecimentos += 1;
      atual.litrosTotal += this.converterNumero(abastecimento.litros);
      atual.valorTotal += this.converterNumero(abastecimento.valorTotal);

      if (idVeiculo > 0) {
        atual.veiculos.add(idVeiculo);
      }

      const idViagem = vinculo?.idViagem ?? null;
      if (idViagem && idViagem > 0) {
        atual.viagens.add(idViagem);
        if (
          !atual.viagensComKmSomadas.has(idViagem) &&
          (vinculo?.kmViagem ?? 0) > 0
        ) {
          atual.viagensComKmSomadas.add(idViagem);
          atual.kmTotalViagens += vinculo?.kmViagem ?? 0;
        }
      }

      acumuladores.set(chave, atual);
    }

    return Array.from(acumuladores.values())
      .map((item) => {
        const mediaLitros =
          item.totalAbastecimentos > 0
            ? item.litrosTotal / item.totalAbastecimentos
            : 0;
        const ticketMedio =
          item.totalAbastecimentos > 0
            ? item.valorTotal / item.totalAbastecimentos
            : 0;
        const custoMedioLitro =
          item.litrosTotal > 0 ? item.valorTotal / item.litrosTotal : 0;
        const consumoKmLitro =
          item.kmTotalViagens > 0 && item.litrosTotal > 0
            ? item.kmTotalViagens / item.litrosTotal
            : null;

        return {
          idMotorista: item.idMotorista,
          totalAbastecimentos: item.totalAbastecimentos,
          viagensVinculadas: item.viagens.size,
          veiculosUtilizados: item.veiculos.size,
          veiculosIds: Array.from(item.veiculos.values()).sort((a, b) => a - b),
          litrosTotal: this.arredondar(item.litrosTotal, 3),
          valorTotal: this.arredondar(item.valorTotal),
          mediaLitros: this.arredondar(mediaLitros, 3),
          ticketMedio: this.arredondar(ticketMedio),
          custoMedioLitro: this.arredondar(custoMedioLitro, 4),
          kmTotalViagens: this.arredondar(item.kmTotalViagens, 2),
          consumoKmLitro:
            consumoKmLitro !== null
              ? this.arredondar(consumoKmLitro, 3)
              : null,
        };
      })
      .sort((a, b) => {
        if (a.idMotorista === null && b.idMotorista !== null) return 1;
        if (a.idMotorista !== null && b.idMotorista === null) return -1;
        return b.valorTotal - a.valorTotal;
      });
  }

  private calcularMediaPorVeiculo(
    abastecimentos: AbastecimentoRelatorio[],
    vinculos: Map<number, VinculoAbastecimento>,
  ) {
    const acumuladores = new Map<number, AcumuladorMediaVeiculo>();

    for (const abastecimento of abastecimentos) {
      const idAbastecimento = this.converterInteiro(abastecimento.idAbastecimento);
      const idVeiculo = this.converterInteiro(abastecimento.idVeiculo);
      if (idVeiculo <= 0) {
        continue;
      }

      const vinculo = vinculos.get(idAbastecimento);
      const atual = acumuladores.get(idVeiculo) ?? {
        idVeiculo,
        totalAbastecimentos: 0,
        litrosTotal: 0,
        valorTotal: 0,
        motoristas: new Set<number>(),
        viagens: new Set<number>(),
        viagensComKmSomadas: new Set<number>(),
        kmTotalViagens: 0,
      };

      atual.totalAbastecimentos += 1;
      atual.litrosTotal += this.converterNumero(abastecimento.litros);
      atual.valorTotal += this.converterNumero(abastecimento.valorTotal);

      if ((vinculo?.idMotorista ?? 0) > 0) {
        atual.motoristas.add(vinculo!.idMotorista!);
      }

      const idViagem = vinculo?.idViagem ?? null;
      if (idViagem && idViagem > 0) {
        atual.viagens.add(idViagem);
        if (
          !atual.viagensComKmSomadas.has(idViagem) &&
          (vinculo?.kmViagem ?? 0) > 0
        ) {
          atual.viagensComKmSomadas.add(idViagem);
          atual.kmTotalViagens += vinculo?.kmViagem ?? 0;
        }
      }

      acumuladores.set(idVeiculo, atual);
    }

    return Array.from(acumuladores.values())
      .map((item) => {
        const mediaLitros =
          item.totalAbastecimentos > 0
            ? item.litrosTotal / item.totalAbastecimentos
            : 0;
        const ticketMedio =
          item.totalAbastecimentos > 0
            ? item.valorTotal / item.totalAbastecimentos
            : 0;
        const custoMedioLitro =
          item.litrosTotal > 0 ? item.valorTotal / item.litrosTotal : 0;
        const consumoKmLitro =
          item.kmTotalViagens > 0 && item.litrosTotal > 0
            ? item.kmTotalViagens / item.litrosTotal
            : null;

        return {
          idVeiculo: item.idVeiculo,
          totalAbastecimentos: item.totalAbastecimentos,
          viagensVinculadas: item.viagens.size,
          motoristasUtilizados: item.motoristas.size,
          motoristasIds: Array.from(item.motoristas.values()).sort(
            (a, b) => a - b,
          ),
          litrosTotal: this.arredondar(item.litrosTotal, 3),
          valorTotal: this.arredondar(item.valorTotal),
          mediaLitros: this.arredondar(mediaLitros, 3),
          ticketMedio: this.arredondar(ticketMedio),
          custoMedioLitro: this.arredondar(custoMedioLitro, 4),
          kmTotalViagens: this.arredondar(item.kmTotalViagens, 2),
          consumoKmLitro:
            consumoKmLitro !== null
              ? this.arredondar(consumoKmLitro, 3)
              : null,
        };
      })
      .sort((a, b) => b.valorTotal - a.valorTotal);
  }

  private resolverPeriodo(
    filtro: FiltroRelatorioAbastecimentoDto,
  ): PeriodoRelatorio {
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

  private arredondar(valor: number, casas = 2) {
    const fator = 10 ** casas;
    return Math.round((valor + Number.EPSILON) * fator) / fator;
  }
}
