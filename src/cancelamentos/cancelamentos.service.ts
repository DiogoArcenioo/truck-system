import {
  BadRequestException,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { DataSource, EntityManager, QueryFailedError } from 'typeorm';
import { JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { configurarContextoEmpresaRls } from '../common/database/rls.util';
import {
  TIPOS_DOCUMENTO_CANCELAMENTO,
  TipoDocumentoCancelamento,
} from './cancelamentos.constants';
import { AtualizarMotivoCancelamentoDto } from './dto/atualizar-motivo-cancelamento.dto';
import { CriarCancelamentoDto } from './dto/criar-cancelamento.dto';
import { CriarMotivoCancelamentoDto } from './dto/criar-motivo-cancelamento.dto';
import { FiltroCancelamentosDto } from './dto/filtro-cancelamentos.dto';
import { FiltroMotivosCancelamentoDto } from './dto/filtro-motivos-cancelamento.dto';

type RegistroBanco = Record<string, unknown>;

type DocumentoConsultado = {
  tipoDocumento: TipoDocumentoCancelamento;
  tipoDocumentoLabel: string;
  idDocumento: number;
  statusAtual: string;
  podeReabrir: boolean;
  mensagemBloqueio: string | null;
  resumo: string;
  detalhes: Record<string, unknown>;
};

type ReaberturaResultado = {
  statusAnterior: string;
  statusNovo: string;
  resumoAcao: string;
};

type MotivoCancelamento = {
  idMotivo: number;
  codigo: string | null;
  descricao: string;
  ativo: boolean;
  usuarioAtualizacao: string | null;
  criadoEm: Date | null;
  atualizadoEm: Date | null;
};

type HistoricoCancelamento = {
  idCancelamento: number;
  tipoDocumento: TipoDocumentoCancelamento;
  tipoDocumentoLabel: string;
  idDocumento: number;
  idMotivo: number | null;
  motivoCodigo: string | null;
  motivoDescricao: string;
  usuarioCancelamento: string;
  usuarioSolicitante: string | null;
  observacao: string | null;
  statusAnterior: string | null;
  statusNovo: string | null;
  dataCancelamento: Date | null;
  referenciaDocumento: Record<string, unknown>;
  criadoEm: Date | null;
};

@Injectable()
export class CancelamentosService {
  private readonly logger = new Logger(CancelamentosService.name);
  private estruturaInicializada = false;
  private inicializacaoEmAndamento: Promise<void> | null = null;
  private estruturaDisponivel = true;

  constructor(private readonly dataSource: DataSource) {}

  async listarOpcoes(idEmpresa: number) {
    try {
      return this.executarComRls(idEmpresa, async (manager) => {
        const rows = (await manager.query(
          `
            SELECT *
            FROM app.cancelamento_motivos
            WHERE id_empresa = $1
              AND ativo = TRUE
            ORDER BY descricao ASC, id_motivo ASC
          `,
          [idEmpresa],
        )) as RegistroBanco[];

        return {
          sucesso: true,
          tiposDocumento: TIPOS_DOCUMENTO_CANCELAMENTO.map((tipo) => ({
            value: tipo.value,
            label: tipo.label,
            statusReaberto: tipo.statusReaberto,
          })),
          motivos: rows.map((row) => this.mapearMotivo(row)),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'listar opcoes de cancelamento');
    }
  }

  async listarMotivos(idEmpresa: number, filtro: FiltroMotivosCancelamentoDto) {
    try {
      return this.executarComRls(idEmpresa, async (manager) => {
        const situacao = (filtro.situacao ?? 'ATIVO').toUpperCase();
        const where: string[] = ['id_empresa = $1'];
        const valores: Array<number | boolean> = [idEmpresa];

        if (situacao === 'ATIVO') {
          where.push('ativo = TRUE');
        } else if (situacao === 'INATIVO') {
          where.push('ativo = FALSE');
        }

        const rows = (await manager.query(
          `
            SELECT *
            FROM app.cancelamento_motivos
            WHERE ${where.join(' AND ')}
            ORDER BY ativo DESC, descricao ASC, id_motivo ASC
          `,
          valores,
        )) as RegistroBanco[];

        return {
          sucesso: true,
          total: rows.length,
          motivos: rows.map((row) => this.mapearMotivo(row)),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'listar motivos de cancelamento');
    }
  }

  async criarMotivo(
    idEmpresa: number,
    dados: CriarMotivoCancelamentoDto,
    usuario: JwtUsuarioPayload,
  ) {
    try {
      return this.executarComRls(idEmpresa, async (manager) => {
        const descricao = this.normalizarTextoObrigatorio(
          dados.descricao,
          'descricao',
        );
        const codigo = this.normalizarCodigoOpcional(dados.codigo);
        const usuarioAtualizacao = this.normalizarTextoObrigatorio(
          dados.usuarioAtualizacao ?? this.obterUsuarioOperacao(usuario),
          'usuarioAtualizacao',
        );

        const rows = (await manager.query(
          `
            INSERT INTO app.cancelamento_motivos (
              id_empresa,
              codigo,
              descricao,
              ativo,
              usuario_atualizacao,
              criado_em,
              atualizado_em
            )
            VALUES ($1, $2, $3, TRUE, $4, NOW(), NOW())
            RETURNING *
          `,
          [idEmpresa, codigo, descricao, usuarioAtualizacao],
        )) as RegistroBanco[];

        const row = rows[0];
        if (!row) {
          throw new BadRequestException(
            'Nao foi possivel cadastrar o motivo de cancelamento.',
          );
        }

        return {
          sucesso: true,
          mensagem: 'Motivo cadastrado com sucesso.',
          motivo: this.mapearMotivo(row),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'cadastrar motivo de cancelamento');
    }
  }

  async atualizarMotivo(
    idEmpresa: number,
    idMotivo: number,
    dados: AtualizarMotivoCancelamentoDto,
    usuario: JwtUsuarioPayload,
  ) {
    try {
      return this.executarComRls(idEmpresa, async (manager) => {
        const atual = await this.buscarMotivoOuFalhar(manager, idEmpresa, idMotivo);
        const descricao =
          dados.descricao !== undefined
            ? this.normalizarTextoObrigatorio(dados.descricao, 'descricao')
            : atual.descricao;
        const codigo =
          dados.codigo !== undefined
            ? this.normalizarCodigoOpcional(dados.codigo)
            : atual.codigo;
        const ativo =
          dados.ativo !== undefined ? Boolean(dados.ativo) : Boolean(atual.ativo);
        const usuarioAtualizacao = this.normalizarTextoObrigatorio(
          dados.usuarioAtualizacao ?? this.obterUsuarioOperacao(usuario),
          'usuarioAtualizacao',
        );

        const rows = (await manager.query(
          `
            UPDATE app.cancelamento_motivos
            SET
              codigo = $3,
              descricao = $4,
              ativo = $5,
              usuario_atualizacao = $6,
              atualizado_em = NOW()
            WHERE id_empresa = $1
              AND id_motivo = $2
            RETURNING *
          `,
          [idEmpresa, idMotivo, codigo, descricao, ativo, usuarioAtualizacao],
        )) as RegistroBanco[];

        const row = rows[0];
        if (!row) {
          throw new NotFoundException('Motivo de cancelamento nao encontrado.');
        }

        return {
          sucesso: true,
          mensagem: 'Motivo atualizado com sucesso.',
          motivo: this.mapearMotivo(row),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'atualizar motivo de cancelamento');
    }
  }
  async removerMotivo(
    idEmpresa: number,
    idMotivo: number,
    usuario: JwtUsuarioPayload,
  ) {
    try {
      return this.executarComRls(idEmpresa, async (manager) => {
        const atual = await this.buscarMotivoOuFalhar(manager, idEmpresa, idMotivo);
        if (!atual.ativo) {
          return {
            sucesso: true,
            mensagem: 'Motivo ja estava inativo.',
            motivo: atual,
          };
        }

        const usuarioAtualizacao = this.obterUsuarioOperacao(usuario);
        const rows = (await manager.query(
          `
            UPDATE app.cancelamento_motivos
            SET
              ativo = FALSE,
              usuario_atualizacao = $3,
              atualizado_em = NOW()
            WHERE id_empresa = $1
              AND id_motivo = $2
            RETURNING *
          `,
          [idEmpresa, idMotivo, usuarioAtualizacao],
        )) as RegistroBanco[];

        const row = rows[0];
        if (!row) {
          throw new NotFoundException('Motivo de cancelamento nao encontrado.');
        }

        return {
          sucesso: true,
          mensagem: 'Motivo inativado com sucesso.',
          motivo: this.mapearMotivo(row),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'inativar motivo de cancelamento');
    }
  }

  async consultarDocumento(
    idEmpresa: number,
    tipoDocumento: TipoDocumentoCancelamento,
    idDocumento: number,
  ) {
    try {
      return this.executarComRls(idEmpresa, async (manager) => ({
        sucesso: true,
        documento: await this.consultarDocumentoInterno(
          manager,
          idEmpresa,
          tipoDocumento,
          idDocumento,
        ),
      }));
    } catch (error) {
      this.tratarErroPersistencia(error, 'consultar documento para cancelamento');
    }
  }

  async reabrirDocumento(
    idEmpresa: number,
    dados: CriarCancelamentoDto,
    usuario: JwtUsuarioPayload,
  ) {
    try {
      return this.executarComRls(idEmpresa, async (manager) => {
        const usuarioCancelamento = this.obterUsuarioOperacao(usuario);
        const usuarioSolicitante = this.normalizarTextoOpcional(
          dados.usuarioSolicitante,
        );
        const observacao = this.normalizarTextoOpcional(dados.observacao);
        const dataCancelamento = dados.dataCancelamento
          ? this.normalizarDataHora(dados.dataCancelamento, 'dataCancelamento')
          : new Date().toISOString();

        let motivoSelecionado: MotivoCancelamento | null = null;
        if (dados.idMotivo !== undefined) {
          motivoSelecionado = await this.buscarMotivoOuFalhar(
            manager,
            idEmpresa,
            dados.idMotivo,
          );
          if (!motivoSelecionado.ativo) {
            throw new BadRequestException(
              'O motivo selecionado esta inativo. Escolha um motivo ativo.',
            );
          }
        }

        const motivoDescricao = this.resolverDescricaoMotivo(
          dados.motivoDescricao,
          motivoSelecionado?.descricao ?? null,
        );

        if (!motivoDescricao) {
          throw new BadRequestException(
            'Informe um motivo para realizar a reabertura do documento.',
          );
        }

        const documento = await this.consultarDocumentoInterno(
          manager,
          idEmpresa,
          dados.tipoDocumento,
          dados.idDocumento,
        );

        if (!documento.podeReabrir) {
          throw new BadRequestException(
            documento.mensagemBloqueio ??
              'Este documento nao esta em situacao de baixado/finalizado.',
          );
        }

        const reabertura = await this.reabrirDocumentoInterno(
          manager,
          idEmpresa,
          documento,
          usuarioCancelamento,
        );

        const cancelamento = await this.registrarHistorico(
          manager,
          idEmpresa,
          documento,
          {
            idMotivo: motivoSelecionado?.idMotivo ?? null,
            motivoDescricao,
            usuarioCancelamento,
            usuarioSolicitante,
            observacao,
            statusAnterior: reabertura.statusAnterior,
            statusNovo: reabertura.statusNovo,
            dataCancelamento,
          },
        );

        const documentoAtualizado = await this.consultarDocumentoInterno(
          manager,
          idEmpresa,
          dados.tipoDocumento,
          dados.idDocumento,
        );

        return {
          sucesso: true,
          mensagem: 'Documento reaberto com sucesso.',
          reabertura,
          documento: documentoAtualizado,
          cancelamento,
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'reabrir documento');
    }
  }

  async listarHistorico(idEmpresa: number, filtro: FiltroCancelamentosDto) {
    try {
      return this.executarComRls(idEmpresa, async (manager) => {
        const where: string[] = ['h.id_empresa = $1'];
        const valores: Array<string | number> = [String(idEmpresa)];

        if (filtro.tipoDocumento) {
          valores.push(filtro.tipoDocumento);
          where.push(`h.tipo_documento = $${valores.length}`);
        }

        if (filtro.idDocumento !== undefined) {
          valores.push(filtro.idDocumento);
          where.push(`h.id_documento = $${valores.length}`);
        }

        if (filtro.idMotivo !== undefined) {
          valores.push(filtro.idMotivo);
          where.push(`h.id_motivo = $${valores.length}`);
        }

        if (filtro.usuario?.trim()) {
          const texto = `%${filtro.usuario.trim()}%`;
          valores.push(texto);
          where.push(
            `(COALESCE(h.usuario_cancelamento, '') ILIKE $${valores.length} OR COALESCE(h.usuario_solicitante, '') ILIKE $${valores.length})`,
          );
        }

        const dataDe = filtro.dataDe
          ? this.normalizarDataHora(filtro.dataDe, 'dataDe')
          : null;
        const dataAte = filtro.dataAte
          ? this.normalizarDataHora(filtro.dataAte, 'dataAte')
          : null;

        if (dataDe && dataAte && new Date(dataAte).getTime() < new Date(dataDe).getTime()) {
          throw new BadRequestException(
            'dataAte deve ser maior ou igual a dataDe.',
          );
        }

        if (dataDe) {
          valores.push(dataDe);
          where.push(`h.data_cancelamento >= $${valores.length}::timestamptz`);
        }

        if (dataAte) {
          valores.push(dataAte);
          where.push(`h.data_cancelamento <= $${valores.length}::timestamptz`);
        }

        const limite = Math.max(1, Math.min(200, filtro.limite ?? 50));
        valores.push(limite);

        const rows = (await manager.query(
          `
            SELECT
              h.*,
              m.codigo AS motivo_codigo
            FROM app.cancelamento_documentos h
            LEFT JOIN app.cancelamento_motivos m
              ON m.id_empresa = h.id_empresa
             AND m.id_motivo = h.id_motivo
            WHERE ${where.join(' AND ')}
            ORDER BY h.data_cancelamento DESC, h.id_cancelamento DESC
            LIMIT $${valores.length}
          `,
          valores,
        )) as RegistroBanco[];

        return {
          sucesso: true,
          total: rows.length,
          historico: rows.map((row) => this.mapearHistorico(row)),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'listar historico de cancelamentos');
    }
  }
  private async buscarMotivoOuFalhar(
    manager: EntityManager,
    idEmpresa: number,
    idMotivo: number,
  ): Promise<MotivoCancelamento> {
    const rows = (await manager.query(
      `
        SELECT *
        FROM app.cancelamento_motivos
        WHERE id_empresa = $1
          AND id_motivo = $2
        LIMIT 1
      `,
      [idEmpresa, idMotivo],
    )) as RegistroBanco[];

    const row = rows[0];
    if (!row) {
      throw new NotFoundException('Motivo de cancelamento nao encontrado.');
    }

    return this.mapearMotivo(row);
  }

  private async consultarDocumentoInterno(
    manager: EntityManager,
    idEmpresa: number,
    tipoDocumento: TipoDocumentoCancelamento,
    idDocumento: number,
  ): Promise<DocumentoConsultado> {
    if (tipoDocumento === 'VIAGEM') {
      return this.consultarViagem(manager, idEmpresa, idDocumento);
    }

    if (tipoDocumento === 'ORDEM_SERVICO') {
      return this.consultarOrdemServico(manager, idEmpresa, idDocumento);
    }

    if (tipoDocumento === 'REQUISICAO') {
      return this.consultarRequisicao(manager, idEmpresa, idDocumento);
    }

    throw new BadRequestException('Tipo de documento nao suportado.');
  }

  private async consultarViagem(
    manager: EntityManager,
    idEmpresa: number,
    idDocumento: number,
  ): Promise<DocumentoConsultado> {
    const rows = (await manager.query(
      `
        SELECT
          v.id_viagem,
          v.id_veiculo,
          v.id_motorista,
          v.data_inicio,
          v.data_fim,
          v.km_inicial,
          v.km_final,
          v.status,
          veic.placa,
          mot.nome AS motorista_nome
        FROM app.viagens v
        LEFT JOIN app.veiculo veic
          ON CAST(veic.id_empresa AS TEXT) = CAST(v.id_empresa AS TEXT)
         AND veic.id_veiculo = v.id_veiculo
        LEFT JOIN app.motoristas mot
          ON CAST(mot.id_empresa AS TEXT) = CAST(v.id_empresa AS TEXT)
         AND mot.id_motorista = v.id_motorista
        WHERE v.id_empresa = $1
          AND v.id_viagem = $2
        LIMIT 1
      `,
      [String(idEmpresa), idDocumento],
    )) as RegistroBanco[];

    const row = rows[0];
    if (!row) {
      throw new NotFoundException('Viagem nao encontrada para a empresa logada.');
    }

    const status = (this.toText(row.status) ?? 'A').toUpperCase();
    const dataFim = this.toDate(row.data_fim);
    const kmFinal = this.toNumber(row.km_final);
    const finalizada = status === 'F' || dataFim !== null || kmFinal !== null;
    const inativa = status === 'I';
    const podeReabrir = finalizada && !inativa;
    const placa = this.toText(row.placa) ?? `#${this.toNumber(row.id_veiculo) ?? 0}`;

    return {
      tipoDocumento: 'VIAGEM',
      tipoDocumentoLabel: this.resolverLabelTipoDocumento('VIAGEM'),
      idDocumento: this.toNumber(row.id_viagem) ?? idDocumento,
      statusAtual: status,
      podeReabrir,
      mensagemBloqueio: inativa
        ? 'Viagem inativa nao pode ser reaberta por este modulo.'
        : finalizada
          ? null
          : 'Viagem ainda nao foi finalizada.',
      resumo: `Viagem #${this.toNumber(row.id_viagem) ?? idDocumento} - Veiculo ${placa}`,
      detalhes: {
        placa,
        motorista: this.toText(row.motorista_nome),
        dataInicio: this.toDate(row.data_inicio),
        dataFim,
        kmInicial: this.toNumber(row.km_inicial),
        kmFinal,
      },
    };
  }

  private async consultarOrdemServico(
    manager: EntityManager,
    idEmpresa: number,
    idDocumento: number,
  ): Promise<DocumentoConsultado> {
    const rows = (await manager.query(
      `
        SELECT
          os.id_os,
          os.id_veiculo,
          os.situacao_os,
          os.data_cadastro,
          os.data_fechamento,
          os.tempo_os_min,
          os.valor_total,
          veic.placa
        FROM app.ordem_servico os
        LEFT JOIN app.veiculo veic
          ON CAST(veic.id_empresa AS TEXT) = CAST(os.id_empresa AS TEXT)
         AND veic.id_veiculo = os.id_veiculo
        WHERE os.id_empresa = $1
          AND os.id_os = $2
        LIMIT 1
      `,
      [String(idEmpresa), idDocumento],
    )) as RegistroBanco[];

    const row = rows[0];
    if (!row) {
      throw new NotFoundException(
        'Ordem de servico nao encontrada para a empresa logada.',
      );
    }

    const situacao = (this.toText(row.situacao_os) ?? 'A').toUpperCase();
    const podeReabrir = situacao === 'F';
    const placa = this.toText(row.placa) ?? `#${this.toNumber(row.id_veiculo) ?? 0}`;

    return {
      tipoDocumento: 'ORDEM_SERVICO',
      tipoDocumentoLabel: this.resolverLabelTipoDocumento('ORDEM_SERVICO'),
      idDocumento: this.toNumber(row.id_os) ?? idDocumento,
      statusAtual: situacao,
      podeReabrir,
      mensagemBloqueio:
        situacao === 'C'
          ? 'Ordem de servico cancelada nao pode ser reaberta por este modulo.'
          : situacao === 'A'
            ? 'Ordem de servico ja esta aberta.'
            : podeReabrir
              ? null
              : 'Ordem de servico nao esta em situacao finalizada.',
      resumo: `OS #${this.toNumber(row.id_os) ?? idDocumento} - Veiculo ${placa}`,
      detalhes: {
        placa,
        dataCadastro: this.toDate(row.data_cadastro),
        dataFechamento: this.toDate(row.data_fechamento),
        tempoOsMin: this.toNumber(row.tempo_os_min),
        valorTotal: this.toNumber(row.valor_total),
      },
    };
  }

  private async consultarRequisicao(
    manager: EntityManager,
    idEmpresa: number,
    idDocumento: number,
  ): Promise<DocumentoConsultado> {
    const rows = (await manager.query(
      `
        SELECT
          req.id_requisicao,
          req.id_os,
          req.situacao,
          req.data_requisicao,
          req.valor_total,
          os.situacao_os
        FROM app.requisicao req
        LEFT JOIN app.ordem_servico os
          ON os.id_empresa = req.id_empresa
         AND os.id_os = req.id_os
        WHERE req.id_empresa = $1
          AND req.id_requisicao = $2
        LIMIT 1
      `,
      [String(idEmpresa), idDocumento],
    )) as RegistroBanco[];

    const row = rows[0];
    if (!row) {
      throw new NotFoundException('Requisicao nao encontrada para a empresa logada.');
    }

    const situacao = (this.toText(row.situacao) ?? 'A').toUpperCase();
    const idOs = this.toNumber(row.id_os);
    const situacaoOs = this.toText(row.situacao_os)?.toUpperCase() ?? null;
    const osBloqueada = Boolean(idOs && situacaoOs && situacaoOs !== 'A');
    const podeReabrir = situacao === 'F' && !osBloqueada;

    return {
      tipoDocumento: 'REQUISICAO',
      tipoDocumentoLabel: this.resolverLabelTipoDocumento('REQUISICAO'),
      idDocumento: this.toNumber(row.id_requisicao) ?? idDocumento,
      statusAtual: situacao,
      podeReabrir,
      mensagemBloqueio:
        situacao === 'A'
          ? 'Requisicao ja esta aberta.'
          : situacao === 'C'
            ? 'Requisicao cancelada nao pode ser reaberta por este modulo.'
            : osBloqueada
              ? 'A requisicao pertence a uma OS fechada/cancelada. Reabra a OS antes.'
              : podeReabrir
                ? null
                : 'Requisicao nao esta em situacao finalizada.',
      resumo: `Requisicao #${this.toNumber(row.id_requisicao) ?? idDocumento}${idOs ? ` - OS #${idOs}` : ''}`,
      detalhes: {
        idOs,
        situacaoOs,
        dataRequisicao: this.toDate(row.data_requisicao),
        valorTotal: this.toNumber(row.valor_total),
      },
    };
  }
  private async reabrirDocumentoInterno(
    manager: EntityManager,
    idEmpresa: number,
    documento: DocumentoConsultado,
    usuarioCancelamento: string,
  ): Promise<ReaberturaResultado> {
    if (documento.tipoDocumento === 'VIAGEM') {
      return this.reabrirViagem(
        manager,
        idEmpresa,
        documento,
        usuarioCancelamento,
      );
    }

    if (documento.tipoDocumento === 'ORDEM_SERVICO') {
      return this.reabrirOrdemServico(
        manager,
        idEmpresa,
        documento,
        usuarioCancelamento,
      );
    }

    if (documento.tipoDocumento === 'REQUISICAO') {
      return this.reabrirRequisicao(
        manager,
        idEmpresa,
        documento,
        usuarioCancelamento,
      );
    }

    throw new BadRequestException('Tipo de documento nao suportado para reabertura.');
  }

  private async reabrirViagem(
    manager: EntityManager,
    idEmpresa: number,
    documento: DocumentoConsultado,
    usuarioCancelamento: string,
  ): Promise<ReaberturaResultado> {
    const rows = (await manager.query(
      `
        UPDATE app.viagens
        SET
          status = 'A',
          data_fim = NULL,
          km_final = NULL,
          usuario_atualizacao = $3,
          atualizado_em = NOW()
        WHERE id_empresa = $1
          AND id_viagem = $2
        RETURNING id_viagem
      `,
      [String(idEmpresa), documento.idDocumento, usuarioCancelamento],
    )) as RegistroBanco[];

    if (!rows[0]) {
      throw new NotFoundException('Viagem nao encontrada para a empresa logada.');
    }

    return {
      statusAnterior: documento.statusAtual,
      statusNovo: 'A',
      resumoAcao: 'Viagem reaberta para edicao.',
    };
  }

  private async reabrirOrdemServico(
    manager: EntityManager,
    idEmpresa: number,
    documento: DocumentoConsultado,
    usuarioCancelamento: string,
  ): Promise<ReaberturaResultado> {
    const rows = (await manager.query(
      `
        UPDATE app.ordem_servico
        SET
          situacao_os = 'A',
          data_fechamento = NULL,
          tempo_os_min = NULL,
          usuario_atualizacao = $3,
          data_atualizacao = NOW(),
          atualizado_em = NOW()
        WHERE id_empresa = $1
          AND id_os = $2
        RETURNING id_os
      `,
      [String(idEmpresa), documento.idDocumento, usuarioCancelamento],
    )) as RegistroBanco[];

    if (!rows[0]) {
      throw new NotFoundException(
        'Ordem de servico nao encontrada para a empresa logada.',
      );
    }

    const requisicoesReabertas = (await manager.query(
      `
        UPDATE app.requisicao
        SET
          situacao = 'A',
          usuario_atualizacao = $3,
          atualizado_em = NOW()
        WHERE id_empresa = $1
          AND id_os = $2
          AND situacao = 'F'
        RETURNING id_requisicao
      `,
      [String(idEmpresa), documento.idDocumento, usuarioCancelamento],
    )) as RegistroBanco[];

    return {
      statusAnterior: documento.statusAtual,
      statusNovo: 'A',
      resumoAcao: `OS reaberta. Requisicoes reabertas: ${requisicoesReabertas.length}.`,
    };
  }

  private async reabrirRequisicao(
    manager: EntityManager,
    idEmpresa: number,
    documento: DocumentoConsultado,
    usuarioCancelamento: string,
  ): Promise<ReaberturaResultado> {
    const rows = (await manager.query(
      `
        UPDATE app.requisicao
        SET
          situacao = 'A',
          usuario_atualizacao = $3,
          atualizado_em = NOW()
        WHERE id_empresa = $1
          AND id_requisicao = $2
        RETURNING id_requisicao
      `,
      [String(idEmpresa), documento.idDocumento, usuarioCancelamento],
    )) as RegistroBanco[];

    if (!rows[0]) {
      throw new NotFoundException('Requisicao nao encontrada para a empresa logada.');
    }

    return {
      statusAnterior: documento.statusAtual,
      statusNovo: 'A',
      resumoAcao: 'Requisicao reaberta para edicao.',
    };
  }

  private async registrarHistorico(
    manager: EntityManager,
    idEmpresa: number,
    documento: DocumentoConsultado,
    dados: {
      idMotivo: number | null;
      motivoDescricao: string;
      usuarioCancelamento: string;
      usuarioSolicitante: string | null;
      observacao: string | null;
      statusAnterior: string;
      statusNovo: string;
      dataCancelamento: string;
    },
  ): Promise<HistoricoCancelamento> {
    const referenciaDocumento = {
      resumo: documento.resumo,
      detalhes: documento.detalhes,
    };

    const rows = (await manager.query(
      `
        INSERT INTO app.cancelamento_documentos (
          id_empresa,
          tipo_documento,
          id_documento,
          id_motivo,
          motivo_descricao,
          usuario_cancelamento,
          usuario_solicitante,
          observacao,
          status_anterior,
          status_novo,
          data_cancelamento,
          referencia_documento_json,
          criado_em
        )
        VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::timestamptz, $12::jsonb, NOW()
        )
        RETURNING *
      `,
      [
        String(idEmpresa),
        documento.tipoDocumento,
        documento.idDocumento,
        dados.idMotivo,
        dados.motivoDescricao,
        dados.usuarioCancelamento,
        dados.usuarioSolicitante,
        dados.observacao,
        dados.statusAnterior,
        dados.statusNovo,
        dados.dataCancelamento,
        JSON.stringify(referenciaDocumento),
      ],
    )) as RegistroBanco[];

    if (!rows[0]) {
      throw new BadRequestException('Nao foi possivel registrar o cancelamento.');
    }

    return this.mapearHistorico(rows[0]);
  }

  private mapearMotivo(row: RegistroBanco): MotivoCancelamento {
    return {
      idMotivo: this.toNumber(row.id_motivo) ?? 0,
      codigo: this.toText(row.codigo),
      descricao: this.toText(row.descricao) ?? '',
      ativo: this.toBoolean(row.ativo) ?? true,
      usuarioAtualizacao: this.toText(row.usuario_atualizacao),
      criadoEm: this.toDate(row.criado_em),
      atualizadoEm: this.toDate(row.atualizado_em),
    };
  }

  private mapearHistorico(row: RegistroBanco): HistoricoCancelamento {
    const tipoDocumento =
      (this.toText(row.tipo_documento)?.toUpperCase() as TipoDocumentoCancelamento) ??
      'VIAGEM';
    const referencia = this.parseJsonTalvez(row.referencia_documento_json);
    const referenciaObjeto =
      referencia && typeof referencia === 'object' && !Array.isArray(referencia)
        ? (referencia as Record<string, unknown>)
        : {};

    return {
      idCancelamento: this.toNumber(row.id_cancelamento) ?? 0,
      tipoDocumento,
      tipoDocumentoLabel: this.resolverLabelTipoDocumento(tipoDocumento),
      idDocumento: this.toNumber(row.id_documento) ?? 0,
      idMotivo: this.toNumber(row.id_motivo),
      motivoCodigo: this.toText(row.motivo_codigo),
      motivoDescricao: this.toText(row.motivo_descricao) ?? '',
      usuarioCancelamento: this.toText(row.usuario_cancelamento) ?? '',
      usuarioSolicitante: this.toText(row.usuario_solicitante),
      observacao: this.toText(row.observacao),
      statusAnterior: this.toText(row.status_anterior),
      statusNovo: this.toText(row.status_novo),
      dataCancelamento: this.toDate(row.data_cancelamento),
      referenciaDocumento: referenciaObjeto,
      criadoEm: this.toDate(row.criado_em),
    };
  }

  private resolverDescricaoMotivo(
    motivoLivre: string | undefined,
    motivoPadrao: string | null,
  ) {
    const livre = this.normalizarTextoOpcional(motivoLivre);
    if (livre) {
      return livre;
    }
    return this.normalizarTextoOpcional(motivoPadrao);
  }

  private resolverLabelTipoDocumento(tipoDocumento: TipoDocumentoCancelamento): string {
    const tipo = TIPOS_DOCUMENTO_CANCELAMENTO.find((item) => item.value === tipoDocumento);
    return tipo?.label ?? tipoDocumento;
  }
  private async executarComRls<T>(
    idEmpresa: number,
    callback: (manager: EntityManager) => Promise<T>,
  ): Promise<T> {
    await this.garantirEstrutura();
    this.exigirEstruturaDisponivel();

    return this.dataSource.transaction(async (manager) => {
      await configurarContextoEmpresaRls(manager, idEmpresa);
      return callback(manager);
    });
  }

  private obterUsuarioOperacao(usuario: JwtUsuarioPayload) {
    return (
      this.normalizarTextoOpcional(usuario.nomeUsuario) ??
      this.normalizarTextoOpcional(usuario.email) ??
      `USUARIO_${usuario.sub}`
    );
  }

  private normalizarTextoObrigatorio(valor: string | undefined, campo: string) {
    const texto = (valor ?? '').trim();
    if (!texto) {
      throw new BadRequestException(`${campo} e obrigatorio.`);
    }
    return texto;
  }

  private normalizarTextoOpcional(valor: string | null | undefined) {
    if (typeof valor !== 'string') {
      return null;
    }
    const texto = valor.trim();
    return texto ? texto : null;
  }

  private normalizarCodigoOpcional(valor: string | undefined) {
    if (!valor?.trim()) {
      return null;
    }

    const codigo = valor
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .trim()
      .toUpperCase()
      .replace(/\s+/g, '_')
      .replace(/[^A-Z0-9_-]/g, '_')
      .replace(/_+/g, '_')
      .replace(/^_+|_+$/g, '')
      .slice(0, 30);

    return codigo || null;
  }

  private normalizarDataHora(valor: string, campo: string) {
    const data = new Date(valor);
    if (Number.isNaN(data.getTime())) {
      throw new BadRequestException(`${campo} invalido.`);
    }
    return data.toISOString();
  }

  private toNumber(valor: unknown): number | null {
    if (typeof valor === 'number' && Number.isFinite(valor)) {
      return Math.trunc(valor);
    }

    if (typeof valor === 'string' && valor.trim()) {
      const numero = Number(valor);
      if (Number.isFinite(numero)) {
        return Math.trunc(numero);
      }
    }

    return null;
  }

  private toText(valor: unknown): string | null {
    if (typeof valor !== 'string') {
      return null;
    }
    const texto = valor.trim();
    return texto || null;
  }

  private toBoolean(valor: unknown): boolean | null {
    if (typeof valor === 'boolean') return valor;
    if (typeof valor === 'number') return valor !== 0;
    if (typeof valor === 'string') {
      const texto = valor.trim().toLowerCase();
      if (texto === 'true' || texto === 't' || texto === '1') return true;
      if (texto === 'false' || texto === 'f' || texto === '0') return false;
    }
    return null;
  }

  private toDate(valor: unknown): Date | null {
    if (!valor) return null;
    if (valor instanceof Date) {
      return Number.isNaN(valor.getTime()) ? null : valor;
    }
    if (typeof valor === 'string' || typeof valor === 'number') {
      const data = new Date(valor);
      return Number.isNaN(data.getTime()) ? null : data;
    }
    return null;
  }

  private parseJsonTalvez(valor: unknown) {
    if (valor === null || valor === undefined) {
      return {};
    }

    if (typeof valor === 'object') {
      return valor;
    }

    if (typeof valor !== 'string') {
      return {};
    }

    try {
      return JSON.parse(valor);
    } catch {
      return {};
    }
  }

  private async garantirEstrutura() {
    if (this.estruturaInicializada) return;

    if (this.inicializacaoEmAndamento) {
      await this.inicializacaoEmAndamento;
      return;
    }

    this.inicializacaoEmAndamento = (async () => {
      try {
        const existe = await this.verificarEstruturaCancelamentosExiste();
        if (existe) {
          this.estruturaDisponivel = true;
          this.estruturaInicializada = true;
          return;
        }

        await this.criarEstruturaCancelamentos();

        this.estruturaDisponivel = await this.verificarEstruturaCancelamentosExiste();
        this.estruturaInicializada = true;
      } catch (error) {
        this.logger.error(
          `Falha ao garantir estrutura de cancelamentos. message=${error instanceof Error ? error.message : 'Erro desconhecido'}`,
        );
        this.estruturaDisponivel = false;
        this.estruturaInicializada = true;
      } finally {
        this.inicializacaoEmAndamento = null;
      }
    })();

    await this.inicializacaoEmAndamento;
  }

  private async verificarEstruturaCancelamentosExiste(): Promise<boolean> {
    const rows = (await this.dataSource.query(
      `
        SELECT
          EXISTS (
            SELECT 1
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'app'
              AND c.relname = 'cancelamento_motivos'
              AND c.relkind IN ('r', 'p')
          ) AS motivos,
          EXISTS (
            SELECT 1
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'app'
              AND c.relname = 'cancelamento_documentos'
              AND c.relkind IN ('r', 'p')
          ) AS historico
      `,
    )) as RegistroBanco[];

    const row = rows[0] ?? {};
    const possuiMotivos = this.toBoolean(row.motivos) ?? false;
    const possuiHistorico = this.toBoolean(row.historico) ?? false;
    return possuiMotivos && possuiHistorico;
  }

  private async criarEstruturaCancelamentos() {
    await this.dataSource.query(`
      CREATE TABLE IF NOT EXISTS app.cancelamento_motivos (
        id_motivo BIGSERIAL PRIMARY KEY,
        id_empresa BIGINT NOT NULL,
        codigo VARCHAR(30),
        descricao VARCHAR(180) NOT NULL,
        ativo BOOLEAN NOT NULL DEFAULT TRUE,
        usuario_atualizacao TEXT NOT NULL DEFAULT 'SISTEMA',
        criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);

    await this.dataSource.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS ux_cancelamento_motivos_empresa_codigo
      ON app.cancelamento_motivos (id_empresa, UPPER(codigo))
      WHERE codigo IS NOT NULL AND BTRIM(codigo) <> ''
    `);

    await this.dataSource.query(`
      CREATE INDEX IF NOT EXISTS ix_cancelamento_motivos_empresa_ativo
      ON app.cancelamento_motivos (id_empresa, ativo, descricao)
    `);

    await this.dataSource.query(`
      CREATE TABLE IF NOT EXISTS app.cancelamento_documentos (
        id_cancelamento BIGSERIAL PRIMARY KEY,
        id_empresa BIGINT NOT NULL,
        tipo_documento VARCHAR(40) NOT NULL,
        id_documento BIGINT NOT NULL,
        id_motivo BIGINT,
        motivo_descricao TEXT NOT NULL DEFAULT '',
        usuario_cancelamento TEXT NOT NULL DEFAULT 'SISTEMA',
        usuario_solicitante TEXT,
        observacao TEXT,
        status_anterior VARCHAR(40),
        status_novo VARCHAR(40),
        data_cancelamento TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        referencia_documento_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);

    await this.dataSource.query(`
      CREATE INDEX IF NOT EXISTS ix_cancelamento_documentos_empresa_data
      ON app.cancelamento_documentos (id_empresa, data_cancelamento DESC, id_cancelamento DESC)
    `);

    await this.dataSource.query(`
      CREATE INDEX IF NOT EXISTS ix_cancelamento_documentos_empresa_documento
      ON app.cancelamento_documentos (id_empresa, tipo_documento, id_documento)
    `);

    await this.dataSource.query(`
      CREATE INDEX IF NOT EXISTS ix_cancelamento_documentos_empresa_motivo
      ON app.cancelamento_documentos (id_empresa, id_motivo)
    `);
  }

  private exigirEstruturaDisponivel() {
    if (this.estruturaDisponivel) return;
    throw new BadRequestException(
      'Estrutura de cancelamentos indisponivel no banco. Contate o suporte para habilitar app.cancelamento_motivos e app.cancelamento_documentos.',
    );
  }

  private tratarErroPersistencia(error: unknown, acao: string): never {
    if (error instanceof BadRequestException || error instanceof NotFoundException) {
      throw error;
    }

    if (error instanceof QueryFailedError) {
      const erroPg = error.driverError as { code?: string; message?: string };
      this.logger.error(
        `Falha ao ${acao}. code=${erroPg.code ?? 'N/A'} message=${erroPg.message ?? 'Erro desconhecido'}`,
      );

      if (erroPg.code === '23505') {
        throw new BadRequestException(
          'Ja existe um motivo com o mesmo codigo para esta empresa.',
        );
      }
      if (erroPg.code === '23514') {
        throw new BadRequestException(
          'Dados informados violam as regras de cadastro de cancelamento.',
        );
      }
      if (erroPg.code === '42501') {
        throw new BadRequestException(
          'Usuario do banco sem permissao para gravar tabelas de cancelamento.',
        );
      }
      if (erroPg.code === '42P01') {
        throw new BadRequestException(
          'Estrutura de cancelamentos nao encontrada no banco.',
        );
      }
    }

    this.logger.error(
      `Falha ao ${acao} sem codigo SQL mapeado. message=${error instanceof Error ? error.message : 'Erro desconhecido'}`,
    );
    throw new BadRequestException(
      `Nao foi possivel ${acao} neste momento. Tente novamente.`,
    );
  }
}
