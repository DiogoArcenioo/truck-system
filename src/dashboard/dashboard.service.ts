import { BadRequestException, Injectable } from '@nestjs/common';
import { DataSource, EntityManager } from 'typeorm';
import { configurarContextoEmpresaRls } from '../common/database/rls.util';
import { FiltroDashboardDto } from './dto/filtro-dashboard.dto';

type RegistroBanco = Record<string, unknown>;

type MapaColunasVeiculoDashboard = {
  idVeiculo: string;
  idEmpresa: string | null;
  placa: string;
  idMotoristaAtual: string | null;
  vencimentoDocumento: string | null;
};

type MapaColunasAbastecimentoDashboard = {
  idAbastecimento: string;
  idEmpresa: string | null;
  idVeiculo: string;
  dataAbastecimento: string;
  litros: string;
  valorLitro: string;
  valorTotal: string | null;
  km: string;
};

type PeriodoConsultaDashboard = {
  inicio: Date;
  fimExclusivo: Date;
  comparativoInicio: Date;
  comparativoFimExclusivo: Date;
  mesesSerie: number;
  limiteRanking: number;
  diasAlertaDocumentos: number;
  diasOsAbertasCriticas: number;
};

type MetricasPeriodo = {
  faturamento: number;
  lucroReportadoViagens: number;
  totalViagens: number;
  viagensAbertas: number;
  viagensFechadas: number;
  viagensCanceladas: number;
  kmRodado: number;
  ticketMedio: number;
  duracaoMediaHoras: number;
  custoCombustivel: number;
  totalLitros: number;
  precoMedioLitro: number;
  totalAbastecimentos: number;
  custoManutencao: number;
  totalOs: number;
  osAbertas: number;
  osFechadas: number;
  osCanceladas: number;
  tempoMedioOsMin: number;
  totalRequisicoes: number;
  requisicoesAbertas: number;
  requisicoesFechadas: number;
  requisicoesCanceladas: number;
  valorTotalRequisitado: number;
};

type ResumoFrota = {
  totalVeiculos: number;
  veiculosSemMotorista: number;
  veiculosComViagemPeriodo: number;
  veiculosParadosPeriodo: number;
  percentualUtilizacao: number;
};

type ResumoMotoristas = {
  totalMotoristas: number;
  ativos: number;
  inativos: number;
  ferias: number;
};

type ResumoAlertas = {
  cnhVencida: number;
  cnhVencendo: number;
  documentosVeiculoVencidos: number;
  documentosVeiculoVencendo: number;
  osAbertasCriticas: number;
};

type IndicadorExplicacaoId =
  | 'faturamento'
  | 'custo_combustivel'
  | 'custo_manutencao'
  | 'lucro_operacional'
  | 'viagens_abertas'
  | 'alertas_criticos'
  | 'utilizacao_frota'
  | 'motoristas_ativos_percentual'
  | 'viagens_fechadas_percentual'
  | 'requisicoes_fechadas_percentual';

type IndicadorDefinicao = {
  id: IndicadorExplicacaoId;
  titulo: string;
  descricao: string;
  formula: string;
  considera: string[];
  unidade: 'moeda' | 'numero';
};

type DetalheIndicadorComposicao = {
  chave: string;
  label: string;
  valor: number | string;
  tipo?: 'moeda' | 'numero' | 'data' | 'texto';
};

type DetalheIndicadorTabela = {
  id: string;
  titulo: string;
  descricao?: string;
  colunas: Array<{
    chave: string;
    label: string;
    tipo?: 'moeda' | 'numero' | 'data' | 'texto';
  }>;
  linhas: Array<Record<string, unknown>>;
};

type DetalheIndicadorResultado = {
  valorIndicador: number;
  composicao: DetalheIndicadorComposicao[];
  tabelas: DetalheIndicadorTabela[];
};

const INDICADORES_DEFINICOES: Record<IndicadorExplicacaoId, IndicadorDefinicao> = {
  faturamento: {
    id: 'faturamento',
    titulo: 'Faturamento',
    descricao: 'Soma do valor de frete das viagens do periodo selecionado.',
    formula: 'SUM(viagens.valor_frete)',
    considera: [
      'Viagens da empresa com data_inicio dentro do periodo.',
      'Valores nulos de frete sao tratados como zero.',
      'Nao aplica deducao de custos neste indicador.',
    ],
    unidade: 'moeda',
  },
  custo_combustivel: {
    id: 'custo_combustivel',
    titulo: 'Custo combustível',
    descricao: 'Soma dos abastecimentos do período, com fallback para litros x valor/litro.',
    formula:
      'SUM(COALESCE(abastecimentos.valor_total, abastecimentos.litros * abastecimentos.valor_litro))',
    considera: [
      'Abastecimentos da empresa com data_abastecimento no período.',
      'Se valor_total não existir/for nulo, usa litros x valor_litro.',
      'Não inclui manutenção, requisições ou outras despesas.',
    ],
    unidade: 'moeda',
  },
  custo_manutencao: {
    id: 'custo_manutencao',
    titulo: 'Custo manutenção',
    descricao: 'Soma de valor_total das OS no período, exceto canceladas.',
    formula:
      "SUM(CASE WHEN ordem_servico.situacao_os = 'C' THEN 0 ELSE ordem_servico.valor_total END)",
    considera: [
      'Ordens de serviço com data_cadastro no período.',
      "OS com situação 'C' (cancelada) não entram no custo.",
      'Valor nulo de OS é tratado como zero.',
    ],
    unidade: 'moeda',
  },
  lucro_operacional: {
    id: 'lucro_operacional',
    titulo: 'Lucro operacional',
    descricao: 'Resultado estimado no período.',
    formula: 'faturamento - custo_combustivel - custo_manutencao',
    considera: [
      'Usa os três indicadores calculados no mesmo período.',
      'Não considera impostos, folha, pedágio, multas ou outros custos indiretos.',
      'Serve como visão gerencial rápida da operação.',
    ],
    unidade: 'moeda',
  },
  viagens_abertas: {
    id: 'viagens_abertas',
    titulo: 'Viagens abertas',
    descricao: 'Quantidade de viagens sem data_fim no período.',
    formula: 'COUNT(viagens) WHERE data_fim IS NULL',
    considera: [
      'Viagens da empresa com data_inicio no período.',
      'Considera aberta quando data_fim está nula.',
      'Status da viagem não altera o critério principal.',
    ],
    unidade: 'numero',
  },
  alertas_criticos: {
    id: 'alertas_criticos',
    titulo: 'Alertas críticos',
    descricao:
      'Soma de CNHs vencidas + documentos de veículo vencidos + OS abertas críticas.',
    formula:
      'cnh_vencida + documentos_veiculo_vencidos + os_abertas_criticas',
    considera: [
      'CNH vencida: validade_cnh menor que data atual.',
      'Documento vencido: data de vencimento do veículo menor que data atual.',
      'OS aberta crítica: OS em aberto acima da janela configurada de dias.',
    ],
    unidade: 'numero',
  },
  utilizacao_frota: {
    id: 'utilizacao_frota',
    titulo: 'Utilização da frota (%)',
    descricao:
      'Percentual de veículos que tiveram ao menos uma viagem iniciada no período.',
    formula: '(veiculos_com_viagem_periodo / total_veiculos) * 100',
    considera: [
      'Total de veículos cadastrados na empresa no momento da consulta.',
      'Veículo utilizado: possui ao menos uma viagem com data_inicio no período.',
      'Quando total_veiculos = 0, retorna 0%.',
    ],
    unidade: 'numero',
  },
  motoristas_ativos_percentual: {
    id: 'motoristas_ativos_percentual',
    titulo: 'Motoristas ativos (%)',
    descricao: 'Percentual de motoristas com status ativo no cadastro.',
    formula: '(motoristas_status_A / total_motoristas) * 100',
    considera: [
      "Status 'A' conta como ativo.",
      "Status 'I' e 'F' não contam como ativos.",
      'Quando total_motoristas = 0, retorna 0%.',
    ],
    unidade: 'numero',
  },
  viagens_fechadas_percentual: {
    id: 'viagens_fechadas_percentual',
    titulo: 'Viagens fechadas (%)',
    descricao: 'Percentual de viagens do periodo que possuem data_fim preenchida.',
    formula: '(viagens_com_data_fim / total_viagens_periodo) * 100',
    considera: [
      'Viagens da empresa com data_inicio no periodo.',
      'Viagem fechada: data_fim diferente de nulo.',
      'Quando total_viagens_periodo = 0, retorna 0%.',
    ],
    unidade: 'numero',
  },
  requisicoes_fechadas_percentual: {
    id: 'requisicoes_fechadas_percentual',
    titulo: 'Requisições fechadas (%)',
    descricao: "Percentual de requisições com situação 'F' no período.",
    formula: "(requisicoes_situacao_F / total_requisicoes_periodo) * 100",
    considera: [
      'Requisições da empresa com data_requisicao no período.',
      "Situação 'F' conta como fechada.",
      'Quando total_requisicoes_periodo = 0, retorna 0%.',
    ],
    unidade: 'numero',
  },
};

@Injectable()
export class DashboardService {
  constructor(private readonly dataSource: DataSource) {}

  listarIndicadores() {
    return {
      sucesso: true,
      indicadores: Object.values(INDICADORES_DEFINICOES),
    };
  }

  async obterDetalheIndicador(
    idEmpresa: number,
    indicadorIdBruto: string,
    filtro: FiltroDashboardDto,
  ) {
    const indicadorId = this.normalizarIndicadorId(indicadorIdBruto);
    const definicao = INDICADORES_DEFINICOES[indicadorId];
    const periodo = this.resolverPeriodo(filtro);

    return this.executarComRls(idEmpresa, async (manager) => {
      const colunasAbastecimento = await this.carregarMapaColunasAbastecimento(
        manager,
      );
      const colunasVeiculo = await this.carregarMapaColunasVeiculo(manager);

      const detalhes = await this.montarDetalheIndicador(
        manager,
        idEmpresa,
        indicadorId,
        periodo,
        colunasAbastecimento,
        colunasVeiculo,
      );

      return {
        sucesso: true,
        indicador: {
          id: definicao.id,
          titulo: definicao.titulo,
          descricao: definicao.descricao,
          formula: definicao.formula,
          considera: definicao.considera,
          unidade: definicao.unidade,
          periodo: {
            inicio: this.formatarDataIso(periodo.inicio),
            fim: this.formatarDataIso(
              this.adicionarDias(periodo.fimExclusivo, -1),
            ),
          },
          ...detalhes,
        },
      };
    });
  }

  async obterHome(idEmpresa: number, filtro: FiltroDashboardDto) {
    const periodo = this.resolverPeriodo(filtro);

    return this.executarComRls(idEmpresa, async (manager) => {
      const [colunasVeiculo, colunasAbastecimento] = await Promise.all([
        this.carregarMapaColunasVeiculo(manager),
        this.carregarMapaColunasAbastecimento(manager),
      ]);

      const [atual, comparativo] = await Promise.all([
        this.carregarMetricasPeriodo(
          manager,
          idEmpresa,
          periodo.inicio,
          periodo.fimExclusivo,
          colunasAbastecimento,
        ),
        this.carregarMetricasPeriodo(
          manager,
          idEmpresa,
          periodo.comparativoInicio,
          periodo.comparativoFimExclusivo,
          colunasAbastecimento,
        ),
      ]);

      const [
        frota,
        motoristas,
        alertas,
        serieResultadoMensal,
        rankingVeiculosCustoPorKm,
        rankingVeiculosManutencao,
        rankingMotoristas,
        rankingProdutos,
        custoManutencaoPorTipo,
        osAbertasAntigas,
        alertasCnhDetalhes,
        alertasDocumentoVeiculoDetalhes,
      ] = await Promise.all([
        this.carregarResumoFrota(
          manager,
          idEmpresa,
          periodo.inicio,
          periodo.fimExclusivo,
          colunasVeiculo,
        ),
        this.carregarResumoMotoristas(manager, idEmpresa),
        this.carregarResumoAlertas(
          manager,
          idEmpresa,
          periodo.diasAlertaDocumentos,
          periodo.diasOsAbertasCriticas,
          colunasVeiculo,
        ),
        this.carregarSerieResultadoMensal(
          manager,
          idEmpresa,
          periodo,
          colunasAbastecimento,
        ),
        this.carregarRankingVeiculosCustoPorKm(
          manager,
          idEmpresa,
          periodo.inicio,
          periodo.fimExclusivo,
          periodo.limiteRanking,
          colunasAbastecimento,
          colunasVeiculo,
        ),
        this.carregarRankingVeiculosManutencao(
          manager,
          idEmpresa,
          periodo.inicio,
          periodo.fimExclusivo,
          periodo.limiteRanking,
          colunasVeiculo,
        ),
        this.carregarRankingMotoristas(
          manager,
          idEmpresa,
          periodo.inicio,
          periodo.fimExclusivo,
          periodo.limiteRanking,
        ),
        this.carregarRankingProdutos(
          manager,
          idEmpresa,
          periodo.inicio,
          periodo.fimExclusivo,
          periodo.limiteRanking,
        ),
        this.carregarCustoManutencaoPorTipo(
          manager,
          idEmpresa,
          periodo.inicio,
          periodo.fimExclusivo,
        ),
        this.carregarOsAbertasAntigas(manager, idEmpresa, 10),
        this.carregarAlertasCnhDetalhes(
          manager,
          idEmpresa,
          periodo.diasAlertaDocumentos,
          10,
        ),
        this.carregarAlertasDocumentoVeiculoDetalhes(
          manager,
          idEmpresa,
          periodo.diasAlertaDocumentos,
          10,
          colunasVeiculo,
        ),
      ]);

      const lucroOperacionalAtual =
        atual.faturamento - atual.custoCombustivel - atual.custoManutencao;
      const lucroOperacionalComparativo =
        comparativo.faturamento -
        comparativo.custoCombustivel -
        comparativo.custoManutencao;

      const custoPorKm =
        atual.kmRodado > 0 ? atual.custoCombustivel / atual.kmRodado : 0;
      const consumoMedioKmLitro =
        atual.totalLitros > 0 ? atual.kmRodado / atual.totalLitros : 0;

      const alertasCriticos =
        alertas.cnhVencida +
        alertas.documentosVeiculoVencidos +
        alertas.osAbertasCriticas;

      return {
        sucesso: true,
        periodo: {
          inicio: this.formatarDataIso(periodo.inicio),
          fim: this.formatarDataIso(this.adicionarDias(periodo.fimExclusivo, -1)),
          comparativo: {
            inicio: this.formatarDataIso(periodo.comparativoInicio),
            fim: this.formatarDataIso(
              this.adicionarDias(periodo.comparativoFimExclusivo, -1),
            ),
          },
          parametros: {
            mesesSerie: periodo.mesesSerie,
            limiteRanking: periodo.limiteRanking,
            diasAlertaDocumentos: periodo.diasAlertaDocumentos,
            diasOsAbertasCriticas: periodo.diasOsAbertasCriticas,
          },
        },
        cards: {
          faturamento: this.arredondar(atual.faturamento),
          custoCombustivel: this.arredondar(atual.custoCombustivel),
          custoManutencao: this.arredondar(atual.custoManutencao),
          lucroOperacionalEstimado: this.arredondar(lucroOperacionalAtual),
          viagensAbertas: atual.viagensAbertas,
          alertasCriticos,
          variacoesPercentuais: {
            faturamento: this.calcularVariacaoPercentual(
              atual.faturamento,
              comparativo.faturamento,
            ),
            custoCombustivel: this.calcularVariacaoPercentual(
              atual.custoCombustivel,
              comparativo.custoCombustivel,
            ),
            custoManutencao: this.calcularVariacaoPercentual(
              atual.custoManutencao,
              comparativo.custoManutencao,
            ),
            lucroOperacionalEstimado: this.calcularVariacaoPercentual(
              lucroOperacionalAtual,
              lucroOperacionalComparativo,
            ),
          },
        },
        resumoOperacional: {
          viagens: {
            total: atual.totalViagens,
            abertas: atual.viagensAbertas,
            fechadas: atual.viagensFechadas,
            canceladas: atual.viagensCanceladas,
            kmRodado: this.arredondar(atual.kmRodado, 1),
            ticketMedio: this.arredondar(atual.ticketMedio),
            duracaoMediaHoras: this.arredondar(atual.duracaoMediaHoras, 1),
            lucroReportadoViagens: this.arredondar(atual.lucroReportadoViagens),
          },
          abastecimentos: {
            total: atual.totalAbastecimentos,
            totalLitros: this.arredondar(atual.totalLitros, 3),
            precoMedioLitro: this.arredondar(atual.precoMedioLitro, 4),
            custoPorKm: this.arredondar(custoPorKm, 4),
            consumoMedioKmLitro: this.arredondar(consumoMedioKmLitro, 4),
          },
          manutencao: {
            totalOs: atual.totalOs,
            abertas: atual.osAbertas,
            fechadas: atual.osFechadas,
            canceladas: atual.osCanceladas,
            custoTotal: this.arredondar(atual.custoManutencao),
            tempoMedioHoras: this.arredondar(atual.tempoMedioOsMin / 60, 2),
          },
          requisicoes: {
            total: atual.totalRequisicoes,
            abertas: atual.requisicoesAbertas,
            fechadas: atual.requisicoesFechadas,
            canceladas: atual.requisicoesCanceladas,
            valorTotal: this.arredondar(atual.valorTotalRequisitado),
          },
          frota: {
            totalVeiculos: frota.totalVeiculos,
            veiculosComViagemPeriodo: frota.veiculosComViagemPeriodo,
            veiculosParadosPeriodo: frota.veiculosParadosPeriodo,
            percentualUtilizacao: this.arredondar(frota.percentualUtilizacao, 2),
            veiculosSemMotorista: frota.veiculosSemMotorista,
          },
          motoristas: {
            total: motoristas.totalMotoristas,
            ativos: motoristas.ativos,
            inativos: motoristas.inativos,
            ferias: motoristas.ferias,
          },
        },
        series: {
          resultadoMensal: serieResultadoMensal,
          custoManutencaoPorTipo,
        },
        rankings: {
          veiculosCustoPorKm: rankingVeiculosCustoPorKm,
          veiculosMaiorCustoManutencao: rankingVeiculosManutencao,
          motoristasProdutividade: rankingMotoristas,
          produtosMaisConsumidos: rankingProdutos,
        },
        alertas: {
          resumo: {
            cnhVencida: alertas.cnhVencida,
            cnhVencendo: alertas.cnhVencendo,
            documentosVeiculoVencidos: alertas.documentosVeiculoVencidos,
            documentosVeiculoVencendo: alertas.documentosVeiculoVencendo,
            osAbertasCriticas: alertas.osAbertasCriticas,
          },
          listas: {
            cnh: alertasCnhDetalhes,
            documentoVeiculo: alertasDocumentoVeiculoDetalhes,
            osAbertasAntigas,
          },
        },
      };
    });
  }

  private normalizarIndicadorId(
    indicadorIdBruto: string,
  ): IndicadorExplicacaoId {
    const indicadorId = indicadorIdBruto.trim().toLowerCase();
    if (indicadorId in INDICADORES_DEFINICOES) {
      return indicadorId as IndicadorExplicacaoId;
    }

    throw new BadRequestException(
      `Indicador ${indicadorIdBruto} nao suportado para detalhamento.`,
    );
  }

  private async montarDetalheIndicador(
    manager: EntityManager,
    idEmpresa: number,
    indicadorId: IndicadorExplicacaoId,
    periodo: PeriodoConsultaDashboard,
    colunasAbastecimento: MapaColunasAbastecimentoDashboard,
    colunasVeiculo: MapaColunasVeiculoDashboard,
  ): Promise<DetalheIndicadorResultado> {
    if (indicadorId === 'faturamento') {
      return this.detalharFaturamento(
        manager,
        idEmpresa,
        periodo.inicio,
        periodo.fimExclusivo,
      );
    }

    if (indicadorId === 'custo_combustivel') {
      return this.detalharCustoCombustivel(
        manager,
        idEmpresa,
        periodo.inicio,
        periodo.fimExclusivo,
        colunasAbastecimento,
      );
    }

    if (indicadorId === 'custo_manutencao') {
      return this.detalharCustoManutencao(
        manager,
        idEmpresa,
        periodo.inicio,
        periodo.fimExclusivo,
      );
    }

    if (indicadorId === 'lucro_operacional') {
      return this.detalharLucroOperacional(
        manager,
        idEmpresa,
        periodo,
        colunasAbastecimento,
      );
    }

    if (indicadorId === 'viagens_abertas') {
      return this.detalharViagensAbertas(
        manager,
        idEmpresa,
        periodo.inicio,
        periodo.fimExclusivo,
      );
    }

    if (indicadorId === 'alertas_criticos') {
      return this.detalharAlertasCriticos(
        manager,
        idEmpresa,
        periodo,
        colunasVeiculo,
      );
    }

    if (indicadorId === 'utilizacao_frota') {
      return this.detalharUtilizacaoFrota(
        manager,
        idEmpresa,
        periodo.inicio,
        periodo.fimExclusivo,
        colunasVeiculo,
      );
    }

    if (indicadorId === 'motoristas_ativos_percentual') {
      return this.detalharMotoristasAtivosPercentual(manager, idEmpresa);
    }

    if (indicadorId === 'viagens_fechadas_percentual') {
      return this.detalharViagensFechadasPercentual(
        manager,
        idEmpresa,
        periodo.inicio,
        periodo.fimExclusivo,
      );
    }

    return this.detalharRequisicoesFechadasPercentual(
      manager,
      idEmpresa,
      periodo.inicio,
      periodo.fimExclusivo,
    );
  }

  private async detalharFaturamento(
    manager: EntityManager,
    idEmpresa: number,
    inicio: Date,
    fimExclusivo: Date,
  ): Promise<DetalheIndicadorResultado> {
    const resumoRows = (await manager.query(
      `
        SELECT
          COUNT(1)::int AS total_viagens,
          COALESCE(SUM(COALESCE(valor_frete, 0)::numeric), 0)::numeric AS valor_total,
          COALESCE(AVG(COALESCE(valor_frete, 0)::numeric), 0)::numeric AS ticket_medio
        FROM app.viagens
        WHERE id_empresa = $1
          AND data_inicio >= $2::timestamptz
          AND data_inicio < $3::timestamptz
      `,
      [String(idEmpresa), inicio.toISOString(), fimExclusivo.toISOString()],
    )) as RegistroBanco[];
    const linhasRows = (await manager.query(
      `
        SELECT
          id_viagem,
          id_veiculo,
          id_motorista,
          data_inicio,
          data_fim,
          status,
          COALESCE(valor_frete, 0)::numeric AS valor_frete
        FROM app.viagens
        WHERE id_empresa = $1
          AND data_inicio >= $2::timestamptz
          AND data_inicio < $3::timestamptz
        ORDER BY data_inicio DESC, id_viagem DESC
        LIMIT 120
      `,
      [String(idEmpresa), inicio.toISOString(), fimExclusivo.toISOString()],
    )) as RegistroBanco[];

    const resumo = resumoRows[0] ?? {};
    const valorTotal = this.arredondar(this.converterNumero(resumo.valor_total));

    return {
      valorIndicador: valorTotal,
      composicao: [
        {
          chave: 'total_viagens',
          label: 'Total de viagens consideradas',
          valor: this.converterInteiro(resumo.total_viagens),
          tipo: 'numero',
        },
        {
          chave: 'ticket_medio',
          label: 'Ticket medio por viagem',
          valor: this.arredondar(this.converterNumero(resumo.ticket_medio)),
          tipo: 'moeda',
        },
      ],
      tabelas: [
        {
          id: 'viagens_faturamento',
          titulo: 'Viagens usadas no calculo',
          descricao:
            'Registros da tabela app.viagens dentro do periodo (limitado para visualizacao).',
          colunas: [
            { chave: 'idViagem', label: 'Viagem', tipo: 'numero' },
            { chave: 'idVeiculo', label: 'Veiculo', tipo: 'numero' },
            { chave: 'idMotorista', label: 'Motorista', tipo: 'numero' },
            { chave: 'dataInicio', label: 'Data inicio', tipo: 'data' },
            { chave: 'dataFim', label: 'Data fim', tipo: 'data' },
            { chave: 'status', label: 'Status', tipo: 'texto' },
            { chave: 'valorFrete', label: 'Valor frete', tipo: 'moeda' },
          ],
          linhas: linhasRows.map((row) => ({
            idViagem: this.converterInteiro(row.id_viagem),
            idVeiculo: this.converterInteiro(row.id_veiculo),
            idMotorista: this.converterInteiro(row.id_motorista),
            dataInicio: this.converterData(row.data_inicio),
            dataFim: this.converterData(row.data_fim),
            status: this.converterTexto(row.status),
            valorFrete: this.arredondar(this.converterNumero(row.valor_frete)),
          })),
        },
      ],
    };
  }

  private async detalharCustoCombustivel(
    manager: EntityManager,
    idEmpresa: number,
    inicio: Date,
    fimExclusivo: Date,
    colunas: MapaColunasAbastecimentoDashboard,
  ): Promise<DetalheIndicadorResultado> {
    const dataColuna = this.colunaComAlias('ab', colunas.dataAbastecimento);
    const litrosColuna = this.colunaComAlias('ab', colunas.litros);
    const valorLitroColuna = this.colunaComAlias('ab', colunas.valorLitro);
    const kmColuna = this.colunaComAlias('ab', colunas.km);
    const idVeiculoColuna = this.colunaComAlias('ab', colunas.idVeiculo);
    const idAbastecimentoColuna = this.colunaComAlias('ab', colunas.idAbastecimento);
    const custoExpr = this.expressaoCustoAbastecimento(colunas, 'ab');
    const filtroEmpresa = colunas.idEmpresa
      ? `${this.colunaComAlias('ab', colunas.idEmpresa)} = $1`
      : '$1::text IS NOT NULL';

    const resumoRows = (await manager.query(
      `
        SELECT
          COUNT(1)::int AS total_registros,
          COALESCE(SUM(COALESCE(${litrosColuna}::numeric, 0)), 0)::numeric AS litros_total,
          COALESCE(AVG(NULLIF(COALESCE(${valorLitroColuna}::numeric, 0), 0)), 0)::numeric AS preco_medio,
          COALESCE(SUM(${custoExpr}), 0)::numeric AS custo_total
        FROM app.abastecimentos ab
        WHERE ${filtroEmpresa}
          AND ${dataColuna} >= $2::timestamptz
          AND ${dataColuna} < $3::timestamptz
      `,
      [String(idEmpresa), inicio.toISOString(), fimExclusivo.toISOString()],
    )) as RegistroBanco[];
    const linhasRows = (await manager.query(
      `
        SELECT
          ${idAbastecimentoColuna} AS id_abastecimento,
          ${idVeiculoColuna} AS id_veiculo,
          ${dataColuna} AS data_abastecimento,
          COALESCE(${litrosColuna}::numeric, 0)::numeric AS litros,
          COALESCE(${valorLitroColuna}::numeric, 0)::numeric AS valor_litro,
          COALESCE(${kmColuna}::numeric, 0)::numeric AS km,
          ${custoExpr}::numeric AS custo_calculado
        FROM app.abastecimentos ab
        WHERE ${filtroEmpresa}
          AND ${dataColuna} >= $2::timestamptz
          AND ${dataColuna} < $3::timestamptz
        ORDER BY ${dataColuna} DESC, ${idAbastecimentoColuna} DESC
        LIMIT 120
      `,
      [String(idEmpresa), inicio.toISOString(), fimExclusivo.toISOString()],
    )) as RegistroBanco[];

    const resumo = resumoRows[0] ?? {};
    const custoTotal = this.arredondar(this.converterNumero(resumo.custo_total));

    return {
      valorIndicador: custoTotal,
      composicao: [
        {
          chave: 'total_registros',
          label: 'Total de abastecimentos considerados',
          valor: this.converterInteiro(resumo.total_registros),
          tipo: 'numero',
        },
        {
          chave: 'litros_total',
          label: 'Total de litros',
          valor: this.arredondar(this.converterNumero(resumo.litros_total), 3),
          tipo: 'numero',
        },
        {
          chave: 'preco_medio',
          label: 'Preco medio por litro',
          valor: this.arredondar(this.converterNumero(resumo.preco_medio), 4),
          tipo: 'moeda',
        },
      ],
      tabelas: [
        {
          id: 'abastecimentos_custo',
          titulo: 'Abastecimentos usados no calculo',
          descricao:
            'Registros da tabela app.abastecimentos com custo calculado por linha (limitado para visualizacao).',
          colunas: [
            { chave: 'idAbastecimento', label: 'Abastecimento', tipo: 'numero' },
            { chave: 'idVeiculo', label: 'Veiculo', tipo: 'numero' },
            { chave: 'dataAbastecimento', label: 'Data', tipo: 'data' },
            { chave: 'litros', label: 'Litros', tipo: 'numero' },
            { chave: 'valorLitro', label: 'Valor/litro', tipo: 'moeda' },
            { chave: 'km', label: 'KM', tipo: 'numero' },
            { chave: 'custoCalculado', label: 'Custo calculado', tipo: 'moeda' },
          ],
          linhas: linhasRows.map((row) => ({
            idAbastecimento: this.converterInteiro(row.id_abastecimento),
            idVeiculo: this.converterInteiro(row.id_veiculo),
            dataAbastecimento: this.converterData(row.data_abastecimento),
            litros: this.arredondar(this.converterNumero(row.litros), 3),
            valorLitro: this.arredondar(this.converterNumero(row.valor_litro), 4),
            km: this.arredondar(this.converterNumero(row.km), 1),
            custoCalculado: this.arredondar(
              this.converterNumero(row.custo_calculado),
            ),
          })),
        },
      ],
    };
  }

  private async detalharCustoManutencao(
    manager: EntityManager,
    idEmpresa: number,
    inicio: Date,
    fimExclusivo: Date,
  ): Promise<DetalheIndicadorResultado> {
    const resumoRows = (await manager.query(
      `
        SELECT
          COUNT(1)::int AS total_os,
          COUNT(1) FILTER (WHERE situacao_os = 'A')::int AS os_abertas,
          COUNT(1) FILTER (WHERE situacao_os = 'F')::int AS os_fechadas,
          COUNT(1) FILTER (WHERE situacao_os = 'C')::int AS os_canceladas,
          COALESCE(
            SUM(
              CASE
                WHEN situacao_os = 'C' THEN 0
                ELSE COALESCE(valor_total, 0)::numeric
              END
            ),
            0
          )::numeric AS custo_total
        FROM app.ordem_servico
        WHERE id_empresa = $1
          AND data_cadastro >= $2::timestamptz
          AND data_cadastro < $3::timestamptz
      `,
      [String(idEmpresa), inicio.toISOString(), fimExclusivo.toISOString()],
    )) as RegistroBanco[];
    const linhasRows = (await manager.query(
      `
        SELECT
          id_os,
          id_veiculo,
          id_fornecedor,
          data_cadastro,
          data_fechamento,
          situacao_os,
          COALESCE(tipo_servico::text, 'N/D') AS tipo_servico,
          COALESCE(valor_total, 0)::numeric AS valor_total
        FROM app.ordem_servico
        WHERE id_empresa = $1
          AND data_cadastro >= $2::timestamptz
          AND data_cadastro < $3::timestamptz
        ORDER BY data_cadastro DESC, id_os DESC
        LIMIT 120
      `,
      [String(idEmpresa), inicio.toISOString(), fimExclusivo.toISOString()],
    )) as RegistroBanco[];

    const resumo = resumoRows[0] ?? {};
    const custoTotal = this.arredondar(this.converterNumero(resumo.custo_total));

    return {
      valorIndicador: custoTotal,
      composicao: [
        {
          chave: 'total_os',
          label: 'Total de OS no periodo',
          valor: this.converterInteiro(resumo.total_os),
          tipo: 'numero',
        },
        {
          chave: 'os_abertas',
          label: 'OS abertas',
          valor: this.converterInteiro(resumo.os_abertas),
          tipo: 'numero',
        },
        {
          chave: 'os_fechadas',
          label: 'OS fechadas',
          valor: this.converterInteiro(resumo.os_fechadas),
          tipo: 'numero',
        },
        {
          chave: 'os_canceladas',
          label: 'OS canceladas (nao entram no custo)',
          valor: this.converterInteiro(resumo.os_canceladas),
          tipo: 'numero',
        },
      ],
      tabelas: [
        {
          id: 'ordens_servico_custo',
          titulo: 'Ordens de servico usadas no calculo',
          descricao:
            'OS no periodo. Canceladas ficam visiveis para auditoria, mas entram com custo zero no indicador.',
          colunas: [
            { chave: 'idOs', label: 'OS', tipo: 'numero' },
            { chave: 'idVeiculo', label: 'Veiculo', tipo: 'numero' },
            { chave: 'idFornecedor', label: 'Fornecedor', tipo: 'numero' },
            { chave: 'dataCadastro', label: 'Data cadastro', tipo: 'data' },
            { chave: 'dataFechamento', label: 'Data fechamento', tipo: 'data' },
            { chave: 'situacao', label: 'Situacao', tipo: 'texto' },
            { chave: 'tipoServico', label: 'Tipo servico', tipo: 'texto' },
            { chave: 'valorTotal', label: 'Valor total', tipo: 'moeda' },
          ],
          linhas: linhasRows.map((row) => ({
            idOs: this.converterInteiro(row.id_os),
            idVeiculo: this.converterInteiro(row.id_veiculo),
            idFornecedor: this.converterInteiro(row.id_fornecedor),
            dataCadastro: this.converterData(row.data_cadastro),
            dataFechamento: this.converterData(row.data_fechamento),
            situacao: this.converterTexto(row.situacao_os),
            tipoServico: this.converterTexto(row.tipo_servico),
            valorTotal: this.arredondar(this.converterNumero(row.valor_total)),
          })),
        },
      ],
    };
  }

  private async detalharLucroOperacional(
    manager: EntityManager,
    idEmpresa: number,
    periodo: PeriodoConsultaDashboard,
    colunasAbastecimento: MapaColunasAbastecimentoDashboard,
  ): Promise<DetalheIndicadorResultado> {
    const [faturamento, combustivel, manutencao] = await Promise.all([
      this.detalharFaturamento(
        manager,
        idEmpresa,
        periodo.inicio,
        periodo.fimExclusivo,
      ),
      this.detalharCustoCombustivel(
        manager,
        idEmpresa,
        periodo.inicio,
        periodo.fimExclusivo,
        colunasAbastecimento,
      ),
      this.detalharCustoManutencao(
        manager,
        idEmpresa,
        periodo.inicio,
        periodo.fimExclusivo,
      ),
    ]);

    const valorIndicador = this.arredondar(
      faturamento.valorIndicador -
        combustivel.valorIndicador -
        manutencao.valorIndicador,
    );

    return {
      valorIndicador,
      composicao: [
        {
          chave: 'faturamento',
          label: 'Faturamento do periodo',
          valor: this.arredondar(faturamento.valorIndicador),
          tipo: 'moeda',
        },
        {
          chave: 'custo_combustivel',
          label: 'Custo combustivel do periodo',
          valor: this.arredondar(combustivel.valorIndicador),
          tipo: 'moeda',
        },
        {
          chave: 'custo_manutencao',
          label: 'Custo manutencao do periodo',
          valor: this.arredondar(manutencao.valorIndicador),
          tipo: 'moeda',
        },
      ],
      tabelas: [
        {
          id: 'lucro_componentes',
          titulo: 'Componentes do calculo',
          colunas: [
            { chave: 'componente', label: 'Componente', tipo: 'texto' },
            { chave: 'valor', label: 'Valor', tipo: 'moeda' },
          ],
          linhas: [
            {
              componente: 'Faturamento',
              valor: this.arredondar(faturamento.valorIndicador),
            },
            {
              componente: 'Custo combustivel',
              valor: this.arredondar(combustivel.valorIndicador),
            },
            {
              componente: 'Custo manutencao',
              valor: this.arredondar(manutencao.valorIndicador),
            },
            {
              componente: 'Lucro operacional estimado',
              valor: valorIndicador,
            },
          ],
        },
        ...faturamento.tabelas.slice(0, 1),
        ...combustivel.tabelas.slice(0, 1),
        ...manutencao.tabelas.slice(0, 1),
      ],
    };
  }

  private async detalharViagensAbertas(
    manager: EntityManager,
    idEmpresa: number,
    inicio: Date,
    fimExclusivo: Date,
  ): Promise<DetalheIndicadorResultado> {
    const rows = (await manager.query(
      `
        SELECT
          id_viagem,
          id_veiculo,
          id_motorista,
          data_inicio,
          km_inicial,
          status,
          COALESCE(valor_frete, 0)::numeric AS valor_frete
        FROM app.viagens
        WHERE id_empresa = $1
          AND data_inicio >= $2::timestamptz
          AND data_inicio < $3::timestamptz
          AND data_fim IS NULL
        ORDER BY data_inicio DESC, id_viagem DESC
      `,
      [String(idEmpresa), inicio.toISOString(), fimExclusivo.toISOString()],
    )) as RegistroBanco[];

    return {
      valorIndicador: rows.length,
      composicao: [
        {
          chave: 'total_viagens_abertas',
          label: 'Total de viagens abertas no periodo',
          valor: rows.length,
          tipo: 'numero',
        },
      ],
      tabelas: [
        {
          id: 'viagens_abertas',
          titulo: 'Viagens abertas consideradas',
          colunas: [
            { chave: 'idViagem', label: 'Viagem', tipo: 'numero' },
            { chave: 'idVeiculo', label: 'Veiculo', tipo: 'numero' },
            { chave: 'idMotorista', label: 'Motorista', tipo: 'numero' },
            { chave: 'dataInicio', label: 'Data inicio', tipo: 'data' },
            { chave: 'kmInicial', label: 'KM inicial', tipo: 'numero' },
            { chave: 'status', label: 'Status', tipo: 'texto' },
            { chave: 'valorFrete', label: 'Valor frete', tipo: 'moeda' },
          ],
          linhas: rows.map((row) => ({
            idViagem: this.converterInteiro(row.id_viagem),
            idVeiculo: this.converterInteiro(row.id_veiculo),
            idMotorista: this.converterInteiro(row.id_motorista),
            dataInicio: this.converterData(row.data_inicio),
            kmInicial: this.converterInteiro(row.km_inicial),
            status: this.converterTexto(row.status),
            valorFrete: this.arredondar(this.converterNumero(row.valor_frete)),
          })),
        },
      ],
    };
  }

  private async detalharAlertasCriticos(
    manager: EntityManager,
    idEmpresa: number,
    periodo: PeriodoConsultaDashboard,
    colunasVeiculo: MapaColunasVeiculoDashboard,
  ): Promise<DetalheIndicadorResultado> {
    const [resumo, cnh, documentoVeiculo, osAbertasAntigas] = await Promise.all([
      this.carregarResumoAlertas(
        manager,
        idEmpresa,
        periodo.diasAlertaDocumentos,
        periodo.diasOsAbertasCriticas,
        colunasVeiculo,
      ),
      this.carregarAlertasCnhDetalhes(
        manager,
        idEmpresa,
        periodo.diasAlertaDocumentos,
        50,
      ),
      this.carregarAlertasDocumentoVeiculoDetalhes(
        manager,
        idEmpresa,
        periodo.diasAlertaDocumentos,
        50,
        colunasVeiculo,
      ),
      this.carregarOsAbertasAntigas(manager, idEmpresa, 50),
    ]);

    const totalCriticos =
      resumo.cnhVencida +
      resumo.documentosVeiculoVencidos +
      resumo.osAbertasCriticas;

    return {
      valorIndicador: totalCriticos,
      composicao: [
        {
          chave: 'cnh_vencida',
          label: 'CNHs vencidas',
          valor: resumo.cnhVencida,
          tipo: 'numero',
        },
        {
          chave: 'documentos_vencidos',
          label: 'Documentos de veiculo vencidos',
          valor: resumo.documentosVeiculoVencidos,
          tipo: 'numero',
        },
        {
          chave: 'os_abertas_criticas',
          label: 'OS abertas criticas',
          valor: resumo.osAbertasCriticas,
          tipo: 'numero',
        },
      ],
      tabelas: [
        {
          id: 'alertas_cnh',
          titulo: 'Motoristas com alerta de CNH',
          colunas: [
            { chave: 'idMotorista', label: 'Motorista', tipo: 'numero' },
            { chave: 'nome', label: 'Nome', tipo: 'texto' },
            { chave: 'validadeCnh', label: 'Validade CNH', tipo: 'data' },
            { chave: 'status', label: 'Status', tipo: 'texto' },
            { chave: 'vencida', label: 'Vencida', tipo: 'texto' },
          ],
          linhas: cnh.map((item) => ({
            idMotorista: item.idMotorista,
            nome: item.nome,
            validadeCnh: item.validadeCnh,
            status: item.status,
            vencida: item.vencida ? 'SIM' : 'NAO',
          })),
        },
        {
          id: 'alertas_documento_veiculo',
          titulo: 'Veiculos com documento em alerta',
          colunas: [
            { chave: 'idVeiculo', label: 'Veiculo', tipo: 'numero' },
            { chave: 'placa', label: 'Placa', tipo: 'texto' },
            { chave: 'dataVencimento', label: 'Vencimento', tipo: 'data' },
            { chave: 'vencido', label: 'Vencido', tipo: 'texto' },
          ],
          linhas: documentoVeiculo.map((item) => ({
            idVeiculo: item.idVeiculo,
            placa: item.placa,
            dataVencimento: item.dataVencimento,
            vencido: item.vencido ? 'SIM' : 'NAO',
          })),
        },
        {
          id: 'alertas_os_abertas',
          titulo: 'Ordens de servico abertas antigas',
          colunas: [
            { chave: 'idOs', label: 'OS', tipo: 'numero' },
            { chave: 'idVeiculo', label: 'Veiculo', tipo: 'numero' },
            { chave: 'dataCadastro', label: 'Data cadastro', tipo: 'data' },
            { chave: 'diasEmAberto', label: 'Dias em aberto', tipo: 'numero' },
            { chave: 'valorTotal', label: 'Valor', tipo: 'moeda' },
          ],
          linhas: osAbertasAntigas.map((item) => ({
            idOs: item.idOs,
            idVeiculo: item.idVeiculo,
            dataCadastro: item.dataCadastro,
            diasEmAberto: item.diasEmAberto,
            valorTotal: item.valorTotal,
          })),
        },
      ],
    };
  }

  private async detalharUtilizacaoFrota(
    manager: EntityManager,
    idEmpresa: number,
    inicio: Date,
    fimExclusivo: Date,
    colunasVeiculo: MapaColunasVeiculoDashboard,
  ): Promise<DetalheIndicadorResultado> {
    const resumo = await this.carregarResumoFrota(
      manager,
      idEmpresa,
      inicio,
      fimExclusivo,
      colunasVeiculo,
    );

    const idVeiculoV = this.colunaComAlias('v', colunasVeiculo.idVeiculo);
    const placaV = this.colunaComAlias('v', colunasVeiculo.placa);
    const filtroEmpresaV = colunasVeiculo.idEmpresa
      ? `WHERE ${this.colunaComAlias('v', colunasVeiculo.idEmpresa)} = $1`
      : '';

    const linhasRows = (await manager.query(
      `
        WITH viagens_periodo AS (
          SELECT
            viagem.id_veiculo,
            COUNT(1)::int AS qtd_viagens
          FROM app.viagens viagem
          WHERE viagem.id_empresa = $1
            AND viagem.data_inicio >= $2::timestamptz
            AND viagem.data_inicio < $3::timestamptz
          GROUP BY viagem.id_veiculo
        )
        SELECT
          ${idVeiculoV} AS id_veiculo,
          COALESCE(${placaV}, 'SEM PLACA') AS placa,
          COALESCE(vp.qtd_viagens, 0)::int AS qtd_viagens,
          CASE
            WHEN COALESCE(vp.qtd_viagens, 0) > 0 THEN 'SIM'
            ELSE 'NAO'
          END AS utilizado
        FROM app.veiculo v
        LEFT JOIN viagens_periodo vp
          ON vp.id_veiculo = ${idVeiculoV}
        ${filtroEmpresaV}
        ORDER BY COALESCE(vp.qtd_viagens, 0) DESC, ${idVeiculoV} DESC
        LIMIT 200
      `,
      [String(idEmpresa), inicio.toISOString(), fimExclusivo.toISOString()],
    )) as RegistroBanco[];

    const percentual = this.arredondar(resumo.percentualUtilizacao, 2);

    return {
      valorIndicador: percentual,
      composicao: [
        {
          chave: 'total_veiculos',
          label: 'Total de veiculos',
          valor: resumo.totalVeiculos,
          tipo: 'numero',
        },
        {
          chave: 'veiculos_com_viagem_periodo',
          label: 'Veiculos com viagem no periodo',
          valor: resumo.veiculosComViagemPeriodo,
          tipo: 'numero',
        },
        {
          chave: 'veiculos_parados_periodo',
          label: 'Veiculos sem viagem no periodo',
          valor: resumo.veiculosParadosPeriodo,
          tipo: 'numero',
        },
        {
          chave: 'percentual_utilizacao',
          label: 'Percentual de utilizacao (%)',
          valor: percentual,
          tipo: 'numero',
        },
      ],
      tabelas: [
        {
          id: 'frota_utilizacao_base',
          titulo: 'Veiculos considerados no calculo',
          descricao:
            'Lista de veiculos com quantidade de viagens no periodo e classificacao de utilizacao.',
          colunas: [
            { chave: 'idVeiculo', label: 'Veiculo', tipo: 'numero' },
            { chave: 'placa', label: 'Placa', tipo: 'texto' },
            { chave: 'qtdViagens', label: 'Viagens no periodo', tipo: 'numero' },
            { chave: 'utilizado', label: 'Utilizado', tipo: 'texto' },
          ],
          linhas: linhasRows.map((row) => ({
            idVeiculo: this.converterInteiro(row.id_veiculo),
            placa: this.converterTexto(row.placa) ?? 'SEM PLACA',
            qtdViagens: this.converterInteiro(row.qtd_viagens),
            utilizado: this.converterTexto(row.utilizado) ?? 'NAO',
          })),
        },
      ],
    };
  }

  private async detalharMotoristasAtivosPercentual(
    manager: EntityManager,
    idEmpresa: number,
  ): Promise<DetalheIndicadorResultado> {
    const resumo = await this.carregarResumoMotoristas(manager, idEmpresa);
    const percentual = this.calcularPercentualSimples(
      resumo.ativos,
      resumo.totalMotoristas,
      2,
    );

    const rows = (await manager.query(
      `
        SELECT
          id_motorista,
          COALESCE(nome, 'SEM NOME') AS nome,
          COALESCE(status, '') AS status
        FROM app.motoristas
        WHERE id_empresa = $1
        ORDER BY nome ASC, id_motorista DESC
        LIMIT 200
      `,
      [String(idEmpresa)],
    )) as RegistroBanco[];

    return {
      valorIndicador: percentual,
      composicao: [
        {
          chave: 'total_motoristas',
          label: 'Total de motoristas',
          valor: resumo.totalMotoristas,
          tipo: 'numero',
        },
        {
          chave: 'motoristas_ativos',
          label: 'Motoristas ativos (status A)',
          valor: resumo.ativos,
          tipo: 'numero',
        },
        {
          chave: 'motoristas_inativos',
          label: 'Motoristas inativos (status I)',
          valor: resumo.inativos,
          tipo: 'numero',
        },
        {
          chave: 'motoristas_ferias',
          label: 'Motoristas em ferias (status F)',
          valor: resumo.ferias,
          tipo: 'numero',
        },
        {
          chave: 'percentual_ativos',
          label: 'Percentual de ativos (%)',
          valor: percentual,
          tipo: 'numero',
        },
      ],
      tabelas: [
        {
          id: 'motoristas_status_base',
          titulo: 'Motoristas considerados no calculo',
          descricao:
            "Todos os motoristas da empresa, com indicacao se entram como ativos (status = 'A').",
          colunas: [
            { chave: 'idMotorista', label: 'Motorista', tipo: 'numero' },
            { chave: 'nome', label: 'Nome', tipo: 'texto' },
            { chave: 'status', label: 'Status', tipo: 'texto' },
            { chave: 'consideradoAtivo', label: 'Conta como ativo', tipo: 'texto' },
          ],
          linhas: rows.map((row) => {
            const status = this.converterTexto(row.status)?.toUpperCase() ?? '';
            return {
              idMotorista: this.converterInteiro(row.id_motorista),
              nome: this.converterTexto(row.nome) ?? 'SEM NOME',
              status: status || 'N/D',
              consideradoAtivo: status === 'A' ? 'SIM' : 'NAO',
            };
          }),
        },
      ],
    };
  }

  private async detalharViagensFechadasPercentual(
    manager: EntityManager,
    idEmpresa: number,
    inicio: Date,
    fimExclusivo: Date,
  ): Promise<DetalheIndicadorResultado> {
    const resumoRows = (await manager.query(
      `
        SELECT
          COUNT(1)::int AS total_viagens,
          COUNT(1) FILTER (WHERE data_fim IS NOT NULL)::int AS viagens_fechadas,
          COUNT(1) FILTER (WHERE data_fim IS NULL)::int AS viagens_abertas,
          COUNT(1) FILTER (WHERE UPPER(COALESCE(status, '')) = 'C')::int AS viagens_canceladas
        FROM app.viagens
        WHERE id_empresa = $1
          AND data_inicio >= $2::timestamptz
          AND data_inicio < $3::timestamptz
      `,
      [String(idEmpresa), inicio.toISOString(), fimExclusivo.toISOString()],
    )) as RegistroBanco[];
    const linhasRows = (await manager.query(
      `
        SELECT
          id_viagem,
          id_veiculo,
          id_motorista,
          data_inicio,
          data_fim,
          status,
          COALESCE(valor_frete, 0)::numeric AS valor_frete
        FROM app.viagens
        WHERE id_empresa = $1
          AND data_inicio >= $2::timestamptz
          AND data_inicio < $3::timestamptz
        ORDER BY data_inicio DESC, id_viagem DESC
        LIMIT 160
      `,
      [String(idEmpresa), inicio.toISOString(), fimExclusivo.toISOString()],
    )) as RegistroBanco[];

    const resumo = resumoRows[0] ?? {};
    const totalViagens = this.converterInteiro(resumo.total_viagens);
    const viagensFechadas = this.converterInteiro(resumo.viagens_fechadas);
    const percentual = this.calcularPercentualSimples(viagensFechadas, totalViagens, 2);

    return {
      valorIndicador: percentual,
      composicao: [
        {
          chave: 'total_viagens',
          label: 'Total de viagens no periodo',
          valor: totalViagens,
          tipo: 'numero',
        },
        {
          chave: 'viagens_fechadas',
          label: 'Viagens fechadas (data_fim preenchida)',
          valor: viagensFechadas,
          tipo: 'numero',
        },
        {
          chave: 'viagens_abertas',
          label: 'Viagens abertas (data_fim nula)',
          valor: this.converterInteiro(resumo.viagens_abertas),
          tipo: 'numero',
        },
        {
          chave: 'viagens_canceladas',
          label: 'Viagens canceladas (status C)',
          valor: this.converterInteiro(resumo.viagens_canceladas),
          tipo: 'numero',
        },
        {
          chave: 'percentual_fechadas',
          label: 'Percentual de viagens fechadas (%)',
          valor: percentual,
          tipo: 'numero',
        },
      ],
      tabelas: [
        {
          id: 'viagens_fechadas_base',
          titulo: 'Viagens consideradas no calculo',
          descricao:
            'Viagens do periodo com marcacao de fechada (data_fim preenchida).',
          colunas: [
            { chave: 'idViagem', label: 'Viagem', tipo: 'numero' },
            { chave: 'idVeiculo', label: 'Veiculo', tipo: 'numero' },
            { chave: 'idMotorista', label: 'Motorista', tipo: 'numero' },
            { chave: 'dataInicio', label: 'Data inicio', tipo: 'data' },
            { chave: 'dataFim', label: 'Data fim', tipo: 'data' },
            { chave: 'status', label: 'Status', tipo: 'texto' },
            { chave: 'fechada', label: 'Fechada', tipo: 'texto' },
            { chave: 'valorFrete', label: 'Valor frete', tipo: 'moeda' },
          ],
          linhas: linhasRows.map((row) => ({
            idViagem: this.converterInteiro(row.id_viagem),
            idVeiculo: this.converterInteiro(row.id_veiculo),
            idMotorista: this.converterInteiro(row.id_motorista),
            dataInicio: this.converterData(row.data_inicio),
            dataFim: this.converterData(row.data_fim),
            status: this.converterTexto(row.status),
            fechada: row.data_fim ? 'SIM' : 'NAO',
            valorFrete: this.arredondar(this.converterNumero(row.valor_frete)),
          })),
        },
      ],
    };
  }

  private async detalharRequisicoesFechadasPercentual(
    manager: EntityManager,
    idEmpresa: number,
    inicio: Date,
    fimExclusivo: Date,
  ): Promise<DetalheIndicadorResultado> {
    const resumoRows = (await manager.query(
      `
        SELECT
          COUNT(1)::int AS total_requisicoes,
          COUNT(1) FILTER (WHERE situacao = 'F')::int AS requisicoes_fechadas,
          COUNT(1) FILTER (WHERE situacao = 'A')::int AS requisicoes_abertas,
          COUNT(1) FILTER (WHERE situacao = 'C')::int AS requisicoes_canceladas
        FROM app.requisicao
        WHERE id_empresa = $1
          AND data_requisicao >= $2::timestamptz
          AND data_requisicao < $3::timestamptz
      `,
      [String(idEmpresa), inicio.toISOString(), fimExclusivo.toISOString()],
    )) as RegistroBanco[];
    const linhasRows = (await manager.query(
      `
        SELECT
          req.id_requisicao,
          req.id_os,
          req.data_requisicao,
          req.situacao,
          COALESCE(
            SUM(
              COALESCE(itens.qtd_produto, 0)::numeric * COALESCE(itens.valor_un, 0)::numeric
            ),
            0
          )::numeric AS valor_total_calculado
        FROM app.requisicao req
        LEFT JOIN app.requisicao_itens itens
          ON itens.id_requisicao = req.id_requisicao
         AND itens.id_empresa = req.id_empresa
        WHERE req.id_empresa = $1
          AND req.data_requisicao >= $2::timestamptz
          AND req.data_requisicao < $3::timestamptz
        GROUP BY req.id_requisicao, req.id_os, req.data_requisicao, req.situacao
        ORDER BY req.data_requisicao DESC, req.id_requisicao DESC
        LIMIT 160
      `,
      [String(idEmpresa), inicio.toISOString(), fimExclusivo.toISOString()],
    )) as RegistroBanco[];

    const resumo = resumoRows[0] ?? {};
    const totalRequisicoes = this.converterInteiro(resumo.total_requisicoes);
    const requisicoesFechadas = this.converterInteiro(resumo.requisicoes_fechadas);
    const percentual = this.calcularPercentualSimples(
      requisicoesFechadas,
      totalRequisicoes,
      2,
    );

    return {
      valorIndicador: percentual,
      composicao: [
        {
          chave: 'total_requisicoes',
          label: 'Total de requisições no período',
          valor: totalRequisicoes,
          tipo: 'numero',
        },
        {
          chave: 'requisicoes_fechadas',
          label: "Requisições fechadas (situação = 'F')",
          valor: requisicoesFechadas,
          tipo: 'numero',
        },
        {
          chave: 'requisicoes_abertas',
          label: "Requisições abertas (situação = 'A')",
          valor: this.converterInteiro(resumo.requisicoes_abertas),
          tipo: 'numero',
        },
        {
          chave: 'requisicoes_canceladas',
          label: "Requisições canceladas (situação = 'C')",
          valor: this.converterInteiro(resumo.requisicoes_canceladas),
          tipo: 'numero',
        },
        {
          chave: 'percentual_fechadas',
          label: 'Percentual de requisições fechadas (%)',
          valor: percentual,
          tipo: 'numero',
        },
      ],
      tabelas: [
        {
          id: 'requisicoes_fechadas_base',
          titulo: 'Requisicoes consideradas no calculo',
          colunas: [
            { chave: 'idRequisicao', label: 'Requisicao', tipo: 'numero' },
            { chave: 'idOs', label: 'OS', tipo: 'numero' },
            { chave: 'dataRequisicao', label: 'Data requisicao', tipo: 'data' },
            { chave: 'situacao', label: 'Situacao', tipo: 'texto' },
            { chave: 'fechada', label: 'Fechada', tipo: 'texto' },
            { chave: 'valorTotalCalculado', label: 'Valor total', tipo: 'moeda' },
          ],
          linhas: linhasRows.map((row) => {
            const situacao = this.converterTexto(row.situacao)?.toUpperCase() ?? '';
            return {
              idRequisicao: this.converterInteiro(row.id_requisicao),
              idOs: this.converterInteiro(row.id_os),
              dataRequisicao: this.converterData(row.data_requisicao),
              situacao: situacao || 'N/D',
              fechada: situacao === 'F' ? 'SIM' : 'NAO',
              valorTotalCalculado: this.arredondar(
                this.converterNumero(row.valor_total_calculado),
              ),
            };
          }),
        },
      ],
    };
  }

  private async carregarMetricasPeriodo(
    manager: EntityManager,
    idEmpresa: number,
    inicio: Date,
    fimExclusivo: Date,
    colunasAbastecimento: MapaColunasAbastecimentoDashboard,
  ): Promise<MetricasPeriodo> {
    const [viagens, abastecimentos, ordemServico, requisicoes] = await Promise.all([
      this.carregarMetricasViagens(manager, idEmpresa, inicio, fimExclusivo),
      this.carregarMetricasAbastecimentos(
        manager,
        idEmpresa,
        inicio,
        fimExclusivo,
        colunasAbastecimento,
      ),
      this.carregarMetricasOrdemServico(manager, idEmpresa, inicio, fimExclusivo),
      this.carregarMetricasRequisicoes(manager, idEmpresa, inicio, fimExclusivo),
    ]);

    return {
      ...viagens,
      ...abastecimentos,
      ...ordemServico,
      ...requisicoes,
    };
  }

  private async carregarMetricasViagens(
    manager: EntityManager,
    idEmpresa: number,
    inicio: Date,
    fimExclusivo: Date,
  ) {
    const rows = (await manager.query(
      `
        SELECT
          COUNT(1)::int AS total_viagens,
          COUNT(1) FILTER (WHERE data_fim IS NULL)::int AS viagens_abertas,
          COUNT(1) FILTER (WHERE data_fim IS NOT NULL)::int AS viagens_fechadas,
          COUNT(1) FILTER (WHERE UPPER(COALESCE(status, '')) = 'C')::int AS viagens_canceladas,
          COALESCE(SUM(COALESCE(valor_frete, 0)::numeric), 0)::numeric AS faturamento,
          COALESCE(SUM(COALESCE(total_lucro, 0)::numeric), 0)::numeric AS lucro_reportado_viagens,
          COALESCE(
            SUM(
              COALESCE(
                total_km::numeric,
                GREATEST(COALESCE(km_final, km_inicial) - km_inicial, 0)::numeric
              )
            ),
            0
          )::numeric AS km_rodado,
          COALESCE(AVG(COALESCE(valor_frete, 0)::numeric), 0)::numeric AS ticket_medio,
          COALESCE(
            AVG(
              CASE
                WHEN data_fim IS NOT NULL THEN EXTRACT(EPOCH FROM (data_fim - data_inicio)) / 3600.0
                ELSE NULL
              END
            ),
            0
          )::numeric AS duracao_media_horas
        FROM app.viagens
        WHERE id_empresa = $1
          AND data_inicio >= $2::timestamptz
          AND data_inicio < $3::timestamptz
      `,
      [String(idEmpresa), inicio.toISOString(), fimExclusivo.toISOString()],
    )) as RegistroBanco[];

    const row = rows[0] ?? {};
    return {
      faturamento: this.converterNumero(row.faturamento),
      lucroReportadoViagens: this.converterNumero(row.lucro_reportado_viagens),
      totalViagens: this.converterInteiro(row.total_viagens),
      viagensAbertas: this.converterInteiro(row.viagens_abertas),
      viagensFechadas: this.converterInteiro(row.viagens_fechadas),
      viagensCanceladas: this.converterInteiro(row.viagens_canceladas),
      kmRodado: this.converterNumero(row.km_rodado),
      ticketMedio: this.converterNumero(row.ticket_medio),
      duracaoMediaHoras: this.converterNumero(row.duracao_media_horas),
    };
  }

  private async carregarMetricasAbastecimentos(
    manager: EntityManager,
    idEmpresa: number,
    inicio: Date,
    fimExclusivo: Date,
    colunas: MapaColunasAbastecimentoDashboard,
  ) {
    const dataColuna = this.colunaComAlias('ab', colunas.dataAbastecimento);
    const litrosColuna = this.colunaComAlias('ab', colunas.litros);
    const valorLitroColuna = this.colunaComAlias('ab', colunas.valorLitro);
    const custoExpr = this.expressaoCustoAbastecimento(colunas, 'ab');
    const filtroEmpresa = colunas.idEmpresa
      ? `${this.colunaComAlias('ab', colunas.idEmpresa)} = $1`
      : '$1::text IS NOT NULL';

    const rows = (await manager.query(
      `
        SELECT
          COUNT(1)::int AS total_abastecimentos,
          COALESCE(SUM(COALESCE(${litrosColuna}::numeric, 0)), 0)::numeric AS total_litros,
          COALESCE(AVG(NULLIF(COALESCE(${valorLitroColuna}::numeric, 0), 0)), 0)::numeric AS preco_medio_litro,
          COALESCE(SUM(${custoExpr}), 0)::numeric AS custo_combustivel
        FROM app.abastecimentos ab
        WHERE ${filtroEmpresa}
          AND ${dataColuna} >= $2::timestamptz
          AND ${dataColuna} < $3::timestamptz
      `,
      [String(idEmpresa), inicio.toISOString(), fimExclusivo.toISOString()],
    )) as RegistroBanco[];

    const row = rows[0] ?? {};
    return {
      custoCombustivel: this.converterNumero(row.custo_combustivel),
      totalLitros: this.converterNumero(row.total_litros),
      precoMedioLitro: this.converterNumero(row.preco_medio_litro),
      totalAbastecimentos: this.converterInteiro(row.total_abastecimentos),
    };
  }

  private async carregarMetricasOrdemServico(
    manager: EntityManager,
    idEmpresa: number,
    inicio: Date,
    fimExclusivo: Date,
  ) {
    const rows = (await manager.query(
      `
        SELECT
          COUNT(1)::int AS total_os,
          COUNT(1) FILTER (WHERE situacao_os = 'A')::int AS os_abertas,
          COUNT(1) FILTER (WHERE situacao_os = 'F')::int AS os_fechadas,
          COUNT(1) FILTER (WHERE situacao_os = 'C')::int AS os_canceladas,
          COALESCE(
            SUM(
              CASE
                WHEN situacao_os = 'C' THEN 0
                ELSE COALESCE(valor_total, 0)::numeric
              END
            ),
            0
          )::numeric AS custo_manutencao,
          COALESCE(AVG(NULLIF(tempo_os_min, 0)), 0)::numeric AS tempo_medio_os_min
        FROM app.ordem_servico
        WHERE id_empresa = $1
          AND data_cadastro >= $2::timestamptz
          AND data_cadastro < $3::timestamptz
      `,
      [String(idEmpresa), inicio.toISOString(), fimExclusivo.toISOString()],
    )) as RegistroBanco[];

    const row = rows[0] ?? {};
    return {
      custoManutencao: this.converterNumero(row.custo_manutencao),
      totalOs: this.converterInteiro(row.total_os),
      osAbertas: this.converterInteiro(row.os_abertas),
      osFechadas: this.converterInteiro(row.os_fechadas),
      osCanceladas: this.converterInteiro(row.os_canceladas),
      tempoMedioOsMin: this.converterNumero(row.tempo_medio_os_min),
    };
  }

  private async carregarMetricasRequisicoes(
    manager: EntityManager,
    idEmpresa: number,
    inicio: Date,
    fimExclusivo: Date,
  ) {
    const rows = (await manager.query(
      `
        SELECT
          COUNT(DISTINCT req.id_requisicao)::int AS total_requisicoes,
          COUNT(DISTINCT req.id_requisicao) FILTER (WHERE req.situacao = 'A')::int AS requisicoes_abertas,
          COUNT(DISTINCT req.id_requisicao) FILTER (WHERE req.situacao = 'F')::int AS requisicoes_fechadas,
          COUNT(DISTINCT req.id_requisicao) FILTER (WHERE req.situacao = 'C')::int AS requisicoes_canceladas,
          COALESCE(
            SUM(
              CASE
                WHEN req.situacao = 'C' THEN 0
                ELSE COALESCE(itens.qtd_produto, 0)::numeric * COALESCE(itens.valor_un, 0)::numeric
              END
            ),
            0
          )::numeric AS valor_total_requisitado
        FROM app.requisicao req
        LEFT JOIN app.requisicao_itens itens
          ON itens.id_requisicao = req.id_requisicao
         AND itens.id_empresa = req.id_empresa
        WHERE req.id_empresa = $1
          AND req.data_requisicao >= $2::timestamptz
          AND req.data_requisicao < $3::timestamptz
      `,
      [String(idEmpresa), inicio.toISOString(), fimExclusivo.toISOString()],
    )) as RegistroBanco[];

    const row = rows[0] ?? {};
    return {
      totalRequisicoes: this.converterInteiro(row.total_requisicoes),
      requisicoesAbertas: this.converterInteiro(row.requisicoes_abertas),
      requisicoesFechadas: this.converterInteiro(row.requisicoes_fechadas),
      requisicoesCanceladas: this.converterInteiro(row.requisicoes_canceladas),
      valorTotalRequisitado: this.converterNumero(row.valor_total_requisitado),
    };
  }

  private async carregarResumoFrota(
    manager: EntityManager,
    idEmpresa: number,
    inicio: Date,
    fimExclusivo: Date,
    colunasVeiculo: MapaColunasVeiculoDashboard,
  ): Promise<ResumoFrota> {
    const filtroEmpresa = colunasVeiculo.idEmpresa
      ? `${this.colunaComAlias('v', colunasVeiculo.idEmpresa)} = $1`
      : '$1::text IS NOT NULL';
    const exprSemMotorista = colunasVeiculo.idMotoristaAtual
      ? `COUNT(1) FILTER (WHERE ${this.colunaComAlias('v', colunasVeiculo.idMotoristaAtual)} IS NULL)::int`
      : '0::int';

    const frotaRows = (await manager.query(
      `
        SELECT
          COUNT(1)::int AS total_veiculos,
          ${exprSemMotorista} AS veiculos_sem_motorista
        FROM app.veiculo v
        WHERE ${filtroEmpresa}
      `,
      [String(idEmpresa)],
    )) as RegistroBanco[];
    const usoRows = (await manager.query(
      `
        SELECT
          COUNT(DISTINCT viagem.id_veiculo)::int AS veiculos_com_viagem
        FROM app.viagens viagem
        WHERE viagem.id_empresa = $1
          AND viagem.data_inicio >= $2::timestamptz
          AND viagem.data_inicio < $3::timestamptz
      `,
      [String(idEmpresa), inicio.toISOString(), fimExclusivo.toISOString()],
    )) as RegistroBanco[];

    const frota = frotaRows[0] ?? {};
    const uso = usoRows[0] ?? {};
    const totalVeiculos = this.converterInteiro(frota.total_veiculos);
    const veiculosComViagemPeriodo = this.converterInteiro(uso.veiculos_com_viagem);
    const veiculosParadosPeriodo = Math.max(
      totalVeiculos - veiculosComViagemPeriodo,
      0,
    );
    const percentualUtilizacao =
      totalVeiculos > 0 ? (veiculosComViagemPeriodo / totalVeiculos) * 100 : 0;

    return {
      totalVeiculos,
      veiculosSemMotorista: this.converterInteiro(frota.veiculos_sem_motorista),
      veiculosComViagemPeriodo,
      veiculosParadosPeriodo,
      percentualUtilizacao,
    };
  }

  private async carregarResumoMotoristas(
    manager: EntityManager,
    idEmpresa: number,
  ): Promise<ResumoMotoristas> {
    const rows = (await manager.query(
      `
        SELECT
          COUNT(1)::int AS total_motoristas,
          COUNT(1) FILTER (WHERE status = 'A')::int AS ativos,
          COUNT(1) FILTER (WHERE status = 'I')::int AS inativos,
          COUNT(1) FILTER (WHERE status = 'F')::int AS ferias
        FROM app.motoristas
        WHERE id_empresa = $1
      `,
      [String(idEmpresa)],
    )) as RegistroBanco[];

    const row = rows[0] ?? {};
    return {
      totalMotoristas: this.converterInteiro(row.total_motoristas),
      ativos: this.converterInteiro(row.ativos),
      inativos: this.converterInteiro(row.inativos),
      ferias: this.converterInteiro(row.ferias),
    };
  }

  private async carregarResumoAlertas(
    manager: EntityManager,
    idEmpresa: number,
    diasAlertaDocumentos: number,
    diasOsAbertasCriticas: number,
    colunasVeiculo: MapaColunasVeiculoDashboard,
  ): Promise<ResumoAlertas> {
    const cnhRows = (await manager.query(
      `
        SELECT
          COUNT(1) FILTER (WHERE validade_cnh < CURRENT_DATE)::int AS cnh_vencida,
          COUNT(1) FILTER (
            WHERE validade_cnh >= CURRENT_DATE
              AND validade_cnh <= CURRENT_DATE + $2::int
          )::int AS cnh_vencendo
        FROM app.motoristas
        WHERE id_empresa = $1
      `,
      [String(idEmpresa), diasAlertaDocumentos],
    )) as RegistroBanco[];
    const docRows = await this.carregarResumoAlertasDocumentoVeiculo(
      manager,
      idEmpresa,
      diasAlertaDocumentos,
      colunasVeiculo,
    );
    const osRows = (await manager.query(
      `
        SELECT
          COUNT(1)::int AS os_abertas_criticas
        FROM app.ordem_servico
        WHERE id_empresa = $1
          AND situacao_os = 'A'
          AND data_cadastro < NOW() - ($2::int * INTERVAL '1 day')
      `,
      [String(idEmpresa), diasOsAbertasCriticas],
    )) as RegistroBanco[];

    const cnh = cnhRows[0] ?? {};
    const doc = docRows[0] ?? {};
    const os = osRows[0] ?? {};

    return {
      cnhVencida: this.converterInteiro(cnh.cnh_vencida),
      cnhVencendo: this.converterInteiro(cnh.cnh_vencendo),
      documentosVeiculoVencidos: this.converterInteiro(doc.documentos_vencidos),
      documentosVeiculoVencendo: this.converterInteiro(doc.documentos_vencendo),
      osAbertasCriticas: this.converterInteiro(os.os_abertas_criticas),
    };
  }

  private async carregarResumoAlertasDocumentoVeiculo(
    manager: EntityManager,
    idEmpresa: number,
    diasAlertaDocumentos: number,
    colunasVeiculo: MapaColunasVeiculoDashboard,
  ): Promise<RegistroBanco[]> {
    if (!colunasVeiculo.vencimentoDocumento) {
      return [{ documentos_vencidos: 0, documentos_vencendo: 0 }];
    }

    const vencimentoColuna = this.colunaComAlias(
      'v',
      colunasVeiculo.vencimentoDocumento,
    );
    const filtroEmpresa = colunasVeiculo.idEmpresa
      ? `${this.colunaComAlias('v', colunasVeiculo.idEmpresa)} = $1 AND`
      : '$1::text IS NOT NULL AND';

    return manager.query(
      `
        SELECT
          COUNT(1) FILTER (
            WHERE ${vencimentoColuna} IS NOT NULL
              AND ${vencimentoColuna}::date < CURRENT_DATE
          )::int AS documentos_vencidos,
          COUNT(1) FILTER (
            WHERE ${vencimentoColuna} IS NOT NULL
              AND ${vencimentoColuna}::date >= CURRENT_DATE
              AND ${vencimentoColuna}::date <= CURRENT_DATE + $2::int
          )::int AS documentos_vencendo
        FROM app.veiculo v
        WHERE ${filtroEmpresa} 1 = 1
      `,
      [String(idEmpresa), diasAlertaDocumentos],
    );
  }

  private async carregarAlertasCnhDetalhes(
    manager: EntityManager,
    idEmpresa: number,
    diasAlertaDocumentos: number,
    limite: number,
  ) {
    const rows = (await manager.query(
      `
        SELECT
          id_motorista,
          nome,
          validade_cnh,
          status
        FROM app.motoristas
        WHERE id_empresa = $1
          AND validade_cnh IS NOT NULL
          AND validade_cnh <= CURRENT_DATE + $2::int
        ORDER BY validade_cnh ASC, id_motorista ASC
        LIMIT $3
      `,
      [String(idEmpresa), diasAlertaDocumentos, limite],
    )) as RegistroBanco[];

    return rows.map((row) => ({
      idMotorista: this.converterInteiro(row.id_motorista),
      nome: this.converterTexto(row.nome) ?? 'SEM NOME',
      validadeCnh: this.converterData(row.validade_cnh),
      status: this.converterTexto(row.status)?.toUpperCase() ?? null,
      vencida:
        row.validade_cnh !== null &&
        row.validade_cnh !== undefined &&
        new Date(String(row.validade_cnh)) < this.inicioDoDia(new Date()),
    }));
  }

  private async carregarAlertasDocumentoVeiculoDetalhes(
    manager: EntityManager,
    idEmpresa: number,
    diasAlertaDocumentos: number,
    limite: number,
    colunasVeiculo: MapaColunasVeiculoDashboard,
  ) {
    if (!colunasVeiculo.vencimentoDocumento) {
      return [];
    }

    const idVeiculoColuna = this.colunaComAlias('v', colunasVeiculo.idVeiculo);
    const placaColuna = this.colunaComAlias('v', colunasVeiculo.placa);
    const vencimentoColuna = this.colunaComAlias(
      'v',
      colunasVeiculo.vencimentoDocumento,
    );
    const filtroEmpresa = colunasVeiculo.idEmpresa
      ? `${this.colunaComAlias('v', colunasVeiculo.idEmpresa)} = $1 AND`
      : '';

    const rows = (await manager.query(
      `
        SELECT
          ${idVeiculoColuna} AS id_veiculo,
          ${placaColuna} AS placa,
          ${vencimentoColuna}::date AS data_vencimento
        FROM app.veiculo v
        WHERE ${filtroEmpresa} ${vencimentoColuna} IS NOT NULL
          AND ${vencimentoColuna}::date <= CURRENT_DATE + $2::int
        ORDER BY ${vencimentoColuna} ASC, ${idVeiculoColuna} ASC
        LIMIT $3
      `,
      [String(idEmpresa), diasAlertaDocumentos, limite],
    )) as RegistroBanco[];

    const hoje = this.inicioDoDia(new Date());

    return rows.map((row) => {
      const dataVencimento = this.converterData(row.data_vencimento);
      const dataVencimentoDate = dataVencimento ? new Date(dataVencimento) : null;
      return {
        idVeiculo: this.converterInteiro(row.id_veiculo),
        placa: this.converterTexto(row.placa) ?? 'SEM PLACA',
        dataVencimento,
        vencido:
          dataVencimentoDate !== null &&
          this.inicioDoDia(dataVencimentoDate) < hoje,
      };
    });
  }

  private async carregarOsAbertasAntigas(
    manager: EntityManager,
    idEmpresa: number,
    limite: number,
  ) {
    const rows = (await manager.query(
      `
        SELECT
          id_os,
          id_veiculo,
          data_cadastro,
          valor_total,
          ROUND((EXTRACT(EPOCH FROM (NOW() - data_cadastro)) / 86400.0)::numeric, 1) AS dias_em_aberto
        FROM app.ordem_servico
        WHERE id_empresa = $1
          AND situacao_os = 'A'
        ORDER BY data_cadastro ASC, id_os ASC
        LIMIT $2
      `,
      [String(idEmpresa), limite],
    )) as RegistroBanco[];

    return rows.map((row) => ({
      idOs: this.converterInteiro(row.id_os),
      idVeiculo: this.converterInteiro(row.id_veiculo),
      dataCadastro: this.converterData(row.data_cadastro),
      valorTotal: this.arredondar(this.converterNumero(row.valor_total)),
      diasEmAberto: this.arredondar(this.converterNumero(row.dias_em_aberto), 1),
    }));
  }

  private async carregarSerieResultadoMensal(
    manager: EntityManager,
    idEmpresa: number,
    periodo: PeriodoConsultaDashboard,
    colunasAbastecimento: MapaColunasAbastecimentoDashboard,
  ) {
    const referenciaMes = this.inicioDoMes(
      this.adicionarDias(periodo.fimExclusivo, -1),
    );
    const inicioSerie = this.adicionarMeses(
      referenciaMes,
      -(periodo.mesesSerie - 1),
    );
    const fimSerieExclusivo = this.adicionarMeses(referenciaMes, 1);

    const dataColunaAb = this.colunaComAlias(
      'ab',
      colunasAbastecimento.dataAbastecimento,
    );
    const custoExprAb = this.expressaoCustoAbastecimento(colunasAbastecimento, 'ab');
    const filtroEmpresaAb = colunasAbastecimento.idEmpresa
      ? `${this.colunaComAlias('ab', colunasAbastecimento.idEmpresa)} = $1`
      : '$1::text IS NOT NULL';

    const mesesRows = (await manager.query(
      `
        SELECT
          generate_series($1::date, $2::date, interval '1 month')::date AS inicio_mes
      `,
      [this.formatarDataIso(inicioSerie), this.formatarDataIso(referenciaMes)],
    )) as RegistroBanco[];
    const faturamentoRows = (await manager.query(
      `
        SELECT
          date_trunc('month', data_inicio)::date AS inicio_mes,
          COALESCE(SUM(COALESCE(valor_frete, 0)::numeric), 0)::numeric AS valor
        FROM app.viagens
        WHERE id_empresa = $1
          AND data_inicio >= $2::timestamptz
          AND data_inicio < $3::timestamptz
        GROUP BY 1
      `,
      [String(idEmpresa), inicioSerie.toISOString(), fimSerieExclusivo.toISOString()],
    )) as RegistroBanco[];
    const combustivelRows = (await manager.query(
      `
        SELECT
          date_trunc('month', ${dataColunaAb})::date AS inicio_mes,
          COALESCE(SUM(${custoExprAb}), 0)::numeric AS valor
        FROM app.abastecimentos ab
        WHERE ${filtroEmpresaAb}
          AND ${dataColunaAb} >= $2::timestamptz
          AND ${dataColunaAb} < $3::timestamptz
        GROUP BY 1
      `,
      [String(idEmpresa), inicioSerie.toISOString(), fimSerieExclusivo.toISOString()],
    )) as RegistroBanco[];
    const manutencaoRows = (await manager.query(
      `
        SELECT
          date_trunc('month', data_cadastro)::date AS inicio_mes,
          COALESCE(
            SUM(
              CASE
                WHEN situacao_os = 'C' THEN 0
                ELSE COALESCE(valor_total, 0)::numeric
              END
            ),
            0
          )::numeric AS valor
        FROM app.ordem_servico
        WHERE id_empresa = $1
          AND data_cadastro >= $2::timestamptz
          AND data_cadastro < $3::timestamptz
        GROUP BY 1
      `,
      [String(idEmpresa), inicioSerie.toISOString(), fimSerieExclusivo.toISOString()],
    )) as RegistroBanco[];

    const mapFaturamento = this.criarMapaMensalPorValor(faturamentoRows);
    const mapCombustivel = this.criarMapaMensalPorValor(combustivelRows);
    const mapManutencao = this.criarMapaMensalPorValor(manutencaoRows);

    return mesesRows.map((row) => {
      const chave = this.formatarMes(row.inicio_mes);
      const faturamento = mapFaturamento.get(chave) ?? 0;
      const combustivel = mapCombustivel.get(chave) ?? 0;
      const manutencao = mapManutencao.get(chave) ?? 0;

      return {
        mes: chave,
        faturamento: this.arredondar(faturamento),
        custoCombustivel: this.arredondar(combustivel),
        custoManutencao: this.arredondar(manutencao),
        resultado: this.arredondar(faturamento - combustivel - manutencao),
      };
    });
  }

  private async carregarRankingVeiculosCustoPorKm(
    manager: EntityManager,
    idEmpresa: number,
    inicio: Date,
    fimExclusivo: Date,
    limite: number,
    colunasAbastecimento: MapaColunasAbastecimentoDashboard,
    colunasVeiculo: MapaColunasVeiculoDashboard,
  ) {
    const idVeiculoAb = this.colunaComAlias('ab', colunasAbastecimento.idVeiculo);
    const dataAb = this.colunaComAlias('ab', colunasAbastecimento.dataAbastecimento);
    const kmAb = this.colunaComAlias('ab', colunasAbastecimento.km);
    const litrosAb = this.colunaComAlias('ab', colunasAbastecimento.litros);
    const custoExprAb = this.expressaoCustoAbastecimento(colunasAbastecimento, 'ab');
    const filtroEmpresaAb = colunasAbastecimento.idEmpresa
      ? `${this.colunaComAlias('ab', colunasAbastecimento.idEmpresa)} = $1`
      : '$1::text IS NOT NULL';

    const idVeiculoV = this.colunaComAlias('v', colunasVeiculo.idVeiculo);
    const placaV = this.colunaComAlias('v', colunasVeiculo.placa);
    const filtroEmpresaV = colunasVeiculo.idEmpresa
      ? `AND ${this.colunaComAlias('v', colunasVeiculo.idEmpresa)} = $1`
      : '';

    const rows = (await manager.query(
      `
        WITH base AS (
          SELECT
            ${idVeiculoAb} AS id_veiculo,
            COALESCE(SUM(${custoExprAb}), 0)::numeric AS custo_total,
            COALESCE(SUM(COALESCE(${litrosAb}::numeric, 0)), 0)::numeric AS litros_total,
            (MAX(COALESCE(${kmAb}::numeric, 0)) - MIN(COALESCE(${kmAb}::numeric, 0)))::numeric AS km_delta
          FROM app.abastecimentos ab
          WHERE ${filtroEmpresaAb}
            AND ${dataAb} >= $2::timestamptz
            AND ${dataAb} < $3::timestamptz
          GROUP BY ${idVeiculoAb}
        )
        SELECT
          base.id_veiculo,
          COALESCE(${placaV}, 'SEM PLACA') AS placa,
          base.custo_total,
          base.litros_total,
          GREATEST(COALESCE(base.km_delta, 0), 0)::numeric AS km_rodado,
          CASE
            WHEN GREATEST(COALESCE(base.km_delta, 0), 0) > 0
              THEN base.custo_total / GREATEST(COALESCE(base.km_delta, 0), 0)
            ELSE NULL
          END AS custo_por_km,
          CASE
            WHEN base.litros_total > 0
              THEN GREATEST(COALESCE(base.km_delta, 0), 0) / base.litros_total
            ELSE NULL
          END AS km_por_litro
        FROM base
        LEFT JOIN app.veiculo v
          ON ${idVeiculoV} = base.id_veiculo
         ${filtroEmpresaV}
        ORDER BY custo_por_km DESC NULLS LAST, base.custo_total DESC
        LIMIT $4
      `,
      [
        String(idEmpresa),
        inicio.toISOString(),
        fimExclusivo.toISOString(),
        limite,
      ],
    )) as RegistroBanco[];

    return rows.map((row) => ({
      idVeiculo: this.converterInteiro(row.id_veiculo),
      placa: this.converterTexto(row.placa) ?? 'SEM PLACA',
      custoTotal: this.arredondar(this.converterNumero(row.custo_total)),
      litrosTotal: this.arredondar(this.converterNumero(row.litros_total), 3),
      kmRodado: this.arredondar(this.converterNumero(row.km_rodado), 1),
      custoPorKm: this.arredondar(this.converterNumero(row.custo_por_km), 4),
      kmPorLitro: this.arredondar(this.converterNumero(row.km_por_litro), 4),
    }));
  }

  private async carregarRankingVeiculosManutencao(
    manager: EntityManager,
    idEmpresa: number,
    inicio: Date,
    fimExclusivo: Date,
    limite: number,
    colunasVeiculo: MapaColunasVeiculoDashboard,
  ) {
    const idVeiculoV = this.colunaComAlias('v', colunasVeiculo.idVeiculo);
    const placaV = this.colunaComAlias('v', colunasVeiculo.placa);
    const filtroEmpresaV = colunasVeiculo.idEmpresa
      ? `AND ${this.colunaComAlias('v', colunasVeiculo.idEmpresa)} = $1`
      : '';

    const rows = (await manager.query(
      `
        SELECT
          os.id_veiculo,
          COALESCE(${placaV}, 'SEM PLACA') AS placa,
          COUNT(1)::int AS qtd_os,
          COALESCE(SUM(COALESCE(os.valor_total, 0)::numeric), 0)::numeric AS valor_total
        FROM app.ordem_servico os
        LEFT JOIN app.veiculo v
          ON ${idVeiculoV} = os.id_veiculo
         ${filtroEmpresaV}
        WHERE os.id_empresa = $1
          AND os.data_cadastro >= $2::timestamptz
          AND os.data_cadastro < $3::timestamptz
          AND os.situacao_os <> 'C'
        GROUP BY os.id_veiculo, ${placaV}
        ORDER BY valor_total DESC, qtd_os DESC
        LIMIT $4
      `,
      [
        String(idEmpresa),
        inicio.toISOString(),
        fimExclusivo.toISOString(),
        limite,
      ],
    )) as RegistroBanco[];

    return rows.map((row) => ({
      idVeiculo: this.converterInteiro(row.id_veiculo),
      placa: this.converterTexto(row.placa) ?? 'SEM PLACA',
      qtdOs: this.converterInteiro(row.qtd_os),
      valorTotal: this.arredondar(this.converterNumero(row.valor_total)),
    }));
  }

  private async carregarRankingMotoristas(
    manager: EntityManager,
    idEmpresa: number,
    inicio: Date,
    fimExclusivo: Date,
    limite: number,
  ) {
    const rows = (await manager.query(
      `
        SELECT
          viagem.id_motorista,
          COALESCE(motorista.nome, 'SEM NOME') AS nome,
          COUNT(1)::int AS total_viagens,
          COALESCE(
            SUM(
              COALESCE(
                viagem.total_km::numeric,
                GREATEST(COALESCE(viagem.km_final, viagem.km_inicial) - viagem.km_inicial, 0)::numeric
              )
            ),
            0
          )::numeric AS km_rodado,
          COALESCE(SUM(COALESCE(viagem.valor_frete, 0)::numeric), 0)::numeric AS faturamento,
          COALESCE(SUM(COALESCE(viagem.total_lucro, 0)::numeric), 0)::numeric AS lucro
        FROM app.viagens viagem
        LEFT JOIN app.motoristas motorista
          ON motorista.id_motorista = viagem.id_motorista
         AND motorista.id_empresa = viagem.id_empresa
        WHERE viagem.id_empresa = $1
          AND viagem.data_inicio >= $2::timestamptz
          AND viagem.data_inicio < $3::timestamptz
        GROUP BY viagem.id_motorista, motorista.nome
        ORDER BY faturamento DESC, km_rodado DESC
        LIMIT $4
      `,
      [
        String(idEmpresa),
        inicio.toISOString(),
        fimExclusivo.toISOString(),
        limite,
      ],
    )) as RegistroBanco[];

    return rows.map((row) => ({
      idMotorista: this.converterInteiro(row.id_motorista),
      nome: this.converterTexto(row.nome) ?? 'SEM NOME',
      totalViagens: this.converterInteiro(row.total_viagens),
      kmRodado: this.arredondar(this.converterNumero(row.km_rodado), 1),
      faturamento: this.arredondar(this.converterNumero(row.faturamento)),
      lucro: this.arredondar(this.converterNumero(row.lucro)),
    }));
  }

  private async carregarRankingProdutos(
    manager: EntityManager,
    idEmpresa: number,
    inicio: Date,
    fimExclusivo: Date,
    limite: number,
  ) {
    const rows = (await manager.query(
      `
        SELECT
          itens.id_produto,
          COALESCE(produto.descricao_produto, 'SEM DESCRICAO') AS descricao_produto,
          COALESCE(SUM(COALESCE(itens.qtd_produto, 0)::numeric), 0)::numeric AS qtd_total,
          COALESCE(
            SUM(COALESCE(itens.qtd_produto, 0)::numeric * COALESCE(itens.valor_un, 0)::numeric),
            0
          )::numeric AS valor_total
        FROM app.requisicao_itens itens
        INNER JOIN app.requisicao req
          ON req.id_requisicao = itens.id_requisicao
         AND req.id_empresa = itens.id_empresa
        LEFT JOIN app.produto produto
          ON produto.id_produto = itens.id_produto
         AND produto.id_empresa = itens.id_empresa
        WHERE itens.id_empresa = $1
          AND req.data_requisicao >= $2::timestamptz
          AND req.data_requisicao < $3::timestamptz
          AND req.situacao <> 'C'
        GROUP BY itens.id_produto, produto.descricao_produto
        ORDER BY valor_total DESC, qtd_total DESC
        LIMIT $4
      `,
      [
        String(idEmpresa),
        inicio.toISOString(),
        fimExclusivo.toISOString(),
        limite,
      ],
    )) as RegistroBanco[];

    return rows.map((row) => ({
      idProduto: this.converterInteiro(row.id_produto),
      descricaoProduto: this.converterTexto(row.descricao_produto) ?? 'SEM DESCRICAO',
      qtdTotal: this.arredondar(this.converterNumero(row.qtd_total), 3),
      valorTotal: this.arredondar(this.converterNumero(row.valor_total)),
    }));
  }

  private async carregarCustoManutencaoPorTipo(
    manager: EntityManager,
    idEmpresa: number,
    inicio: Date,
    fimExclusivo: Date,
  ) {
    const rows = (await manager.query(
      `
        SELECT
          COALESCE(tipo_servico::text, 'N/D') AS tipo_servico,
          COUNT(1)::int AS qtd_os,
          COALESCE(SUM(COALESCE(valor_total, 0)::numeric), 0)::numeric AS valor_total
        FROM app.ordem_servico
        WHERE id_empresa = $1
          AND data_cadastro >= $2::timestamptz
          AND data_cadastro < $3::timestamptz
          AND situacao_os <> 'C'
        GROUP BY COALESCE(tipo_servico::text, 'N/D')
        ORDER BY valor_total DESC
      `,
      [String(idEmpresa), inicio.toISOString(), fimExclusivo.toISOString()],
    )) as RegistroBanco[];

    return rows.map((row) => ({
      tipoServico: this.converterTexto(row.tipo_servico) ?? 'N/D',
      qtdOs: this.converterInteiro(row.qtd_os),
      valorTotal: this.arredondar(this.converterNumero(row.valor_total)),
    }));
  }

  private criarMapaMensalPorValor(rows: RegistroBanco[]): Map<string, number> {
    const mapa = new Map<string, number>();
    for (const row of rows) {
      const chave = this.formatarMes(row.inicio_mes);
      mapa.set(chave, this.converterNumero(row.valor));
    }
    return mapa;
  }

  private resolverPeriodo(filtro: FiltroDashboardDto): PeriodoConsultaDashboard {
    const agora = new Date();
    const inicioDefault = this.inicioDoMes(agora);
    const inicio = filtro.dataInicio
      ? this.inicioDoDia(this.normalizarData(filtro.dataInicio, 'dataInicio'))
      : inicioDefault;

    const fimBase = filtro.dataFim
      ? this.inicioDoDia(this.normalizarData(filtro.dataFim, 'dataFim'))
      : this.inicioDoDia(agora);

    const fimExclusivo = this.adicionarDias(fimBase, 1);

    if (fimExclusivo <= inicio) {
      throw new BadRequestException(
        'Periodo invalido: dataFim deve ser maior ou igual a dataInicio.',
      );
    }

    const intervaloMs = fimExclusivo.getTime() - inicio.getTime();
    const comparativoFimExclusivo = new Date(inicio.getTime());
    const comparativoInicio = new Date(
      comparativoFimExclusivo.getTime() - intervaloMs,
    );

    return {
      inicio,
      fimExclusivo,
      comparativoInicio,
      comparativoFimExclusivo,
      mesesSerie: filtro.mesesSerie ?? 6,
      limiteRanking: filtro.limiteRanking ?? 5,
      diasAlertaDocumentos: filtro.diasAlertaDocumentos ?? 30,
      diasOsAbertasCriticas: filtro.diasOsAbertasCriticas ?? 7,
    };
  }

  private normalizarData(valor: string, campo: string): Date {
    const data = new Date(valor);
    if (Number.isNaN(data.getTime())) {
      throw new BadRequestException(`${campo} invalida.`);
    }
    return data;
  }

  private inicioDoDia(data: Date): Date {
    return new Date(data.getFullYear(), data.getMonth(), data.getDate());
  }

  private inicioDoMes(data: Date): Date {
    return new Date(data.getFullYear(), data.getMonth(), 1);
  }

  private adicionarDias(data: Date, dias: number): Date {
    const resultado = new Date(data.getTime());
    resultado.setDate(resultado.getDate() + dias);
    return resultado;
  }

  private adicionarMeses(data: Date, meses: number): Date {
    const resultado = new Date(data.getTime());
    resultado.setMonth(resultado.getMonth() + meses);
    return resultado;
  }

  private formatarMes(valor: unknown): string {
    const data = this.converterData(valor);
    if (!data) {
      return 'SEM-MES';
    }
    const date = new Date(data);
    const ano = date.getUTCFullYear();
    const mes = String(date.getUTCMonth() + 1).padStart(2, '0');
    return `${ano}-${mes}`;
  }

  private formatarDataIso(data: Date): string {
    return data.toISOString().slice(0, 10);
  }

  private arredondar(valor: number, casas = 2): number {
    if (!Number.isFinite(valor)) {
      return 0;
    }
    const fator = 10 ** casas;
    return Math.round(valor * fator) / fator;
  }

  private calcularVariacaoPercentual(atual: number, anterior: number): number | null {
    if (!Number.isFinite(atual) || !Number.isFinite(anterior)) {
      return null;
    }
    if (anterior === 0) {
      return atual === 0 ? 0 : null;
    }
    return this.arredondar(((atual - anterior) / Math.abs(anterior)) * 100, 2);
  }

  private calcularPercentualSimples(
    parte: number,
    total: number,
    casas = 2,
  ): number {
    if (!Number.isFinite(parte) || !Number.isFinite(total) || total <= 0) {
      return 0;
    }

    return this.arredondar((parte / total) * 100, casas);
  }

  private converterNumero(valor: unknown): number {
    if (valor === null || valor === undefined) {
      return 0;
    }
    const numero = Number(valor);
    return Number.isFinite(numero) ? numero : 0;
  }

  private converterInteiro(valor: unknown): number {
    return Math.trunc(this.converterNumero(valor));
  }

  private converterTexto(valor: unknown): string | null {
    if (typeof valor !== 'string') {
      return null;
    }
    const texto = valor.trim();
    return texto.length > 0 ? texto : null;
  }

  private converterData(valor: unknown): string | null {
    if (valor === null || valor === undefined) {
      return null;
    }
    const data = new Date(
      valor instanceof Date || typeof valor === 'string' || typeof valor === 'number'
        ? valor
        : '',
    );
    if (Number.isNaN(data.getTime())) {
      return null;
    }
    return data.toISOString().slice(0, 10);
  }

  private expressaoCustoAbastecimento(
    colunas: MapaColunasAbastecimentoDashboard,
    alias: string,
  ): string {
    const litrosColuna = this.colunaComAlias(alias, colunas.litros);
    const valorLitroColuna = this.colunaComAlias(alias, colunas.valorLitro);
    if (colunas.valorTotal) {
      const valorTotalColuna = this.colunaComAlias(alias, colunas.valorTotal);
      return `COALESCE(${valorTotalColuna}::numeric, COALESCE(${litrosColuna}::numeric, 0) * COALESCE(${valorLitroColuna}::numeric, 0))`;
    }

    return `COALESCE(${litrosColuna}::numeric, 0) * COALESCE(${valorLitroColuna}::numeric, 0)`;
  }

  private colunaComAlias(alias: string, coluna: string): string {
    return `${alias}.${this.quote(coluna)}`;
  }

  private async carregarMapaColunasVeiculo(
    manager: EntityManager,
  ): Promise<MapaColunasVeiculoDashboard> {
    const rows = (await manager.query(`
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = 'app'
        AND table_name = 'veiculo'
    `)) as Array<{ column_name?: string }>;

    if (rows.length === 0) {
      throw new BadRequestException('Tabela app.veiculo nao encontrada.');
    }

    const set = new Set(
      rows
        .map((row) => (typeof row.column_name === 'string' ? row.column_name : ''))
        .filter((value) => value.length > 0),
    );

    return {
      idVeiculo: this.encontrarColuna(set, ['id_veiculo', 'id'], 'id do veiculo')!,
      idEmpresa: this.encontrarColuna(set, ['id_empresa'], '', false),
      placa: this.encontrarColuna(set, ['placa'], 'placa do veiculo')!,
      idMotoristaAtual: this.encontrarColuna(set, ['id_motorista_atual'], '', false),
      vencimentoDocumento: this.encontrarColuna(
        set,
        ['vencimento_documento', 'data_vencimento', 'venc_documento'],
        '',
        false,
      ),
    };
  }

  private async carregarMapaColunasAbastecimento(
    manager: EntityManager,
  ): Promise<MapaColunasAbastecimentoDashboard> {
    const rows = (await manager.query(`
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = 'app'
        AND table_name = 'abastecimentos'
    `)) as Array<{ column_name?: string }>;

    if (rows.length === 0) {
      throw new BadRequestException('Tabela app.abastecimentos nao encontrada.');
    }

    const set = new Set(
      rows
        .map((row) => (typeof row.column_name === 'string' ? row.column_name : ''))
        .filter((value) => value.length > 0),
    );

    return {
      idAbastecimento: this.encontrarColuna(
        set,
        ['id_abastecimento', 'id'],
        'id do abastecimento',
      )!,
      idEmpresa: this.encontrarColuna(set, ['id_empresa'], '', false),
      idVeiculo: this.encontrarColuna(
        set,
        ['id_veiculo', 'veiculo_id'],
        'id do veiculo',
      )!,
      dataAbastecimento: this.encontrarColuna(
        set,
        ['data_abastecimento', 'data', 'data_lancamento', 'dt_abastecimento'],
        'data do abastecimento',
      )!,
      litros: this.encontrarColuna(
        set,
        ['litros', 'quantidade_litros', 'qtd_litros'],
        'litros',
      )!,
      valorLitro: this.encontrarColuna(
        set,
        ['valor_litro', 'preco_litro', 'vl_litro'],
        'valor por litro',
      )!,
      valorTotal: this.encontrarColuna(set, ['valor_total', 'total', 'valor'], '', false),
      km: this.encontrarColuna(
        set,
        ['km', 'km_abastecimento', 'km_veiculo', 'km_atual'],
        'km',
      )!,
    };
  }

  private encontrarColuna(
    set: Set<string>,
    candidatas: string[],
    descricao: string,
    obrigatoria = true,
  ): string | null {
    for (const candidata of candidatas) {
      if (set.has(candidata)) {
        return candidata;
      }
    }

    if (obrigatoria) {
      throw new BadRequestException(
        `Estrutura da tabela invalida: coluna de ${descricao} nao encontrada.`,
      );
    }

    return null;
  }

  private quote(coluna: string): string {
    if (!/^[a-z_][a-z0-9_]*$/.test(coluna)) {
      throw new BadRequestException(
        `Nome de coluna invalido detectado: ${coluna}`,
      );
    }
    return `"${coluna}"`;
  }

  private async executarComRls<T>(
    idEmpresa: number,
    callback: (manager: EntityManager) => Promise<T>,
  ): Promise<T> {
    return this.dataSource.manager.transaction(async (manager) => {
      await configurarContextoEmpresaRls(manager, idEmpresa);
      return callback(manager);
    });
  }
}
