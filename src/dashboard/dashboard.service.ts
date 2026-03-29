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

@Injectable()
export class DashboardService {
  constructor(private readonly dataSource: DataSource) {}

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

    const [frotaRows, usoRows] = await Promise.all([
      manager.query(
        `
          SELECT
            COUNT(1)::int AS total_veiculos,
            ${exprSemMotorista} AS veiculos_sem_motorista
          FROM app.veiculo v
          WHERE ${filtroEmpresa}
        `,
        [String(idEmpresa)],
      ) as Promise<RegistroBanco[]>,
      manager.query(
        `
          SELECT
            COUNT(DISTINCT viagem.id_veiculo)::int AS veiculos_com_viagem
          FROM app.viagens viagem
          WHERE viagem.id_empresa = $1
            AND viagem.data_inicio >= $2::timestamptz
            AND viagem.data_inicio < $3::timestamptz
        `,
        [String(idEmpresa), inicio.toISOString(), fimExclusivo.toISOString()],
      ) as Promise<RegistroBanco[]>,
    ]);

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
    const [cnhRows, docRows, osRows] = await Promise.all([
      manager.query(
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
      ) as Promise<RegistroBanco[]>,
      this.carregarResumoAlertasDocumentoVeiculo(
        manager,
        idEmpresa,
        diasAlertaDocumentos,
        colunasVeiculo,
      ),
      manager.query(
        `
          SELECT
            COUNT(1)::int AS os_abertas_criticas
          FROM app.ordem_servico
          WHERE id_empresa = $1
            AND situacao_os = 'A'
            AND data_cadastro < NOW() - ($2::int * INTERVAL '1 day')
        `,
        [String(idEmpresa), diasOsAbertasCriticas],
      ) as Promise<RegistroBanco[]>,
    ]);

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

    const [mesesRows, faturamentoRows, combustivelRows, manutencaoRows] =
      await Promise.all([
        manager.query(
          `
            SELECT
              generate_series($1::date, $2::date, interval '1 month')::date AS inicio_mes
          `,
          [this.formatarDataIso(inicioSerie), this.formatarDataIso(referenciaMes)],
        ) as Promise<RegistroBanco[]>,
        manager.query(
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
        ) as Promise<RegistroBanco[]>,
        manager.query(
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
        ) as Promise<RegistroBanco[]>,
        manager.query(
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
        ) as Promise<RegistroBanco[]>,
      ]);

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
