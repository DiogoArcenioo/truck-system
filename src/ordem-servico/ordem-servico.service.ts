import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { DataSource, EntityManager, QueryFailedError } from 'typeorm';
import { JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { configurarContextoEmpresaRls } from '../common/database/rls.util';
import { AtualizarOrdemServicoDto } from './dto/atualizar-ordem-servico.dto';
import { CriarOrdemServicoDto } from './dto/criar-ordem-servico.dto';
import { FiltroOrdemServicoDto } from './dto/filtro-ordem-servico.dto';
import { ListarOrdemServicoDto } from './dto/listar-ordem-servico.dto';
import { SITUACAO_OS_OPCOES, TIPO_SERVICO_OPCOES } from './ordem-servico.constants';

type RegistroBanco = Record<string, unknown>;

type PayloadCriacaoNormalizado = {
  idVeiculo: number;
  idFornecedor: number | null;
  dataCadastro: string;
  dataFechamento: string | null;
  tempoOsMin: number | null;
  situacaoOs: string;
  observacao: string | null;
  valorTotal: number;
  kmVeiculo: number | null;
  chaveNfe: string | null;
  tipoServico: string;
  usuarioAtualizacao: string;
};

type PayloadAtualizacaoNormalizado = {
  idVeiculo?: number;
  idFornecedor?: number | null;
  dataCadastro?: string;
  dataFechamento?: string | null;
  tempoOsMin?: number | null;
  situacaoOs?: string;
  observacao?: string | null;
  valorTotal?: number;
  kmVeiculo?: number | null;
  chaveNfe?: string | null;
  tipoServico?: string;
  usuarioAtualizacao: string;
};

@Injectable()
export class OrdemServicoService {
  constructor(private readonly dataSource: DataSource) {}

  listarOpcoes() {
    return {
      sucesso: true,
      situacaoOs: SITUACAO_OS_OPCOES,
      tipoServico: TIPO_SERVICO_OPCOES,
    };
  }

  async listarTodos(idEmpresa: number) {
    return this.executarComRls(idEmpresa, async (manager) => {
      const rows = (await manager.query(
        `
          SELECT
            os.*,
            (
              SELECT COUNT(1)::int
              FROM app.requisicao req
              WHERE req.id_empresa = os.id_empresa
                AND req.id_os = os.id_os
            ) AS qtd_requisicoes
          FROM app.ordem_servico os
          WHERE os.id_empresa = $1
          ORDER BY os.data_cadastro DESC, os.id_os DESC
        `,
        [String(idEmpresa)],
      )) as RegistroBanco[];

      const ordensServico = rows.map((row) => this.mapearOrdemServico(row));

      return {
        sucesso: true,
        total: ordensServico.length,
        ordensServico,
      };
    });
  }

  async listarComFiltro(idEmpresa: number, filtro: FiltroOrdemServicoDto) {
    this.validarIntervalosDoFiltro(filtro);

    return this.executarComRls(idEmpresa, async (manager) => {
      const filtros: string[] = ['os.id_empresa = $1'];
      const valores: Array<string | number> = [String(idEmpresa)];

      if (filtro.idOs !== undefined) {
        valores.push(filtro.idOs);
        filtros.push(`os.id_os = $${valores.length}`);
      }

      if (filtro.idVeiculo !== undefined) {
        valores.push(filtro.idVeiculo);
        filtros.push(`os.id_veiculo = $${valores.length}`);
      }

      if (filtro.idFornecedor !== undefined) {
        valores.push(filtro.idFornecedor);
        filtros.push(`os.id_fornecedor = $${valores.length}`);
      }

      if (filtro.situacaoOs) {
        valores.push(filtro.situacaoOs);
        filtros.push(`os.situacao_os = $${valores.length}`);
      }

      if (filtro.tipoServico) {
        valores.push(filtro.tipoServico);
        filtros.push(`os.tipo_servico = $${valores.length}::app.tipo_servico_enum`);
      }

      if (filtro.dataCadastroDe) {
        valores.push(this.normalizarDataHora(filtro.dataCadastroDe, 'dataCadastroDe'));
        filtros.push(`os.data_cadastro >= $${valores.length}`);
      }

      if (filtro.dataCadastroAte) {
        valores.push(this.normalizarDataHora(filtro.dataCadastroAte, 'dataCadastroAte'));
        filtros.push(`os.data_cadastro <= $${valores.length}`);
      }

      if (filtro.dataFechamentoDe) {
        valores.push(
          this.normalizarDataHora(filtro.dataFechamentoDe, 'dataFechamentoDe'),
        );
        filtros.push(`os.data_fechamento >= $${valores.length}`);
      }

      if (filtro.dataFechamentoAte) {
        valores.push(
          this.normalizarDataHora(filtro.dataFechamentoAte, 'dataFechamentoAte'),
        );
        filtros.push(`os.data_fechamento <= $${valores.length}`);
      }

      if (filtro.texto?.trim()) {
        valores.push(`%${filtro.texto.trim().toUpperCase()}%`);
        filtros.push(
          `(CAST(os.id_os AS TEXT) LIKE $${valores.length} OR UPPER(COALESCE(os.observacao, '')) LIKE $${valores.length} OR UPPER(COALESCE(os.chave_nfe, '')) LIKE $${valores.length} OR UPPER(COALESCE(os.usuario_atualizacao, '')) LIKE $${valores.length})`,
        );
      }

      const pagina = filtro.pagina ?? 1;
      const limite = filtro.limite ?? 20;
      const offset = (pagina - 1) * limite;
      const ordem = filtro.ordem ?? 'DESC';
      const colunaOrdenacao = this.resolverColunaOrdenacao(filtro.ordenarPor);
      const whereSql = filtros.length > 0 ? `WHERE ${filtros.join(' AND ')}` : '';

      const sqlCount = `
        SELECT COUNT(1)::int AS total
        FROM app.ordem_servico os
        ${whereSql}
      `;

      const sqlDados = `
        SELECT
          os.*,
          (
            SELECT COUNT(1)::int
            FROM app.requisicao req
            WHERE req.id_empresa = os.id_empresa
              AND req.id_os = os.id_os
          ) AS qtd_requisicoes
        FROM app.ordem_servico os
        ${whereSql}
        ORDER BY os.${colunaOrdenacao} ${ordem}, os.id_os DESC
        LIMIT $${valores.length + 1}
        OFFSET $${valores.length + 2}
      `;

      const [countRows, rows] = await Promise.all([
        manager.query(sqlCount, valores) as Promise<Array<{ total: number }>>,
        manager.query(sqlDados, [...valores, limite, offset]) as Promise<
          RegistroBanco[]
        >,
      ]);

      const total = Number(countRows[0]?.total ?? 0);
      const ordensServico = rows.map((row) => this.mapearOrdemServico(row));

      return {
        sucesso: true,
        paginaAtual: pagina,
        limite,
        total,
        totalPaginas: total > 0 ? Math.ceil(total / limite) : 0,
        ordensServico,
      };
    });
  }

  async buscarPorId(idEmpresa: number, idOs: number) {
    return this.executarComRls(idEmpresa, async (manager) => {
      const ordemServico = await this.buscarPorIdInterno(manager, idEmpresa, idOs);
      return {
        sucesso: true,
        ordemServico,
      };
    });
  }

  async cadastrar(
    idEmpresa: number,
    dados: CriarOrdemServicoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    try {
      return this.executarComRls(idEmpresa, async (manager) => {
        const payload = this.normalizarCriacao(dados, usuarioJwt);
        const idOs = await this.obterProximoId(manager, 'app.ordem_servico_id_os_seq');

        const rows = (await manager.query(
          `
            INSERT INTO app.ordem_servico (
              id_os,
              id_veiculo,
              data_cadastro,
              data_fechamento,
              tempo_os_min,
              id_fornecedor,
              situacao_os,
              data_atualizacao,
              observacao,
              valor_total,
              km_veiculo,
              chave_nfe,
              usuario_atualizacao,
              tipo_servico,
              id_empresa
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,NOW(),$8,$9,$10,$11,$12,$13,$14)
            RETURNING *
          `,
          [
            idOs,
            payload.idVeiculo,
            payload.dataCadastro,
            payload.dataFechamento,
            payload.tempoOsMin,
            payload.idFornecedor,
            payload.situacaoOs,
            payload.observacao,
            payload.valorTotal,
            payload.kmVeiculo,
            payload.chaveNfe,
            payload.usuarioAtualizacao,
            payload.tipoServico,
            String(idEmpresa),
          ],
        )) as RegistroBanco[];

        const row = rows[0];
        if (!row) {
          throw new BadRequestException('Falha ao cadastrar ordem de servico.');
        }

        return {
          sucesso: true,
          mensagem: 'Ordem de servico cadastrada com sucesso.',
          ordemServico: this.mapearOrdemServico(row),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'cadastrar');
    }
  }

  async atualizar(
    idEmpresa: number,
    idOs: number,
    dados: AtualizarOrdemServicoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    try {
      return this.executarComRls(idEmpresa, async (manager) => {
        const atual = await this.buscarRegistroPorIdOuFalhar(manager, idEmpresa, idOs);
        const payload = this.normalizarAtualizacao(dados, usuarioJwt);

        const dataCadastro =
          payload.dataCadastro ??
          this.converterDataIso(atual.data_cadastro) ??
          new Date().toISOString();
        const dataFechamento =
          payload.dataFechamento !== undefined
            ? payload.dataFechamento
            : this.converterDataIso(atual.data_fechamento);
        const tempoOsMin =
          payload.tempoOsMin !== undefined
            ? payload.tempoOsMin
            : this.converterNumero(atual.tempo_os_min) ??
              this.calcularTempoMinutos(dataCadastro, dataFechamento);

        const rows = (await manager.query(
          `
            UPDATE app.ordem_servico
            SET
              id_veiculo = $1,
              id_fornecedor = $2,
              data_cadastro = $3,
              data_fechamento = $4,
              tempo_os_min = $5,
              situacao_os = $6,
              observacao = $7,
              valor_total = $8,
              km_veiculo = $9,
              chave_nfe = $10,
              usuario_atualizacao = $11,
              tipo_servico = $12::app.tipo_servico_enum,
              data_atualizacao = NOW(),
              atualizado_em = NOW()
            WHERE id_empresa = $13
              AND id_os = $14
            RETURNING *
          `,
          [
            payload.idVeiculo ?? this.converterNumero(atual.id_veiculo) ?? 0,
            payload.idFornecedor !== undefined
              ? payload.idFornecedor
              : this.converterNumero(atual.id_fornecedor),
            dataCadastro,
            dataFechamento,
            tempoOsMin,
            payload.situacaoOs ??
              this.converterTexto(atual.situacao_os)?.toUpperCase() ??
              'A',
            payload.observacao !== undefined
              ? payload.observacao
              : this.converterTexto(atual.observacao),
            payload.valorTotal !== undefined
              ? payload.valorTotal
              : this.converterNumero(atual.valor_total) ?? 0,
            payload.kmVeiculo !== undefined
              ? payload.kmVeiculo
              : this.converterNumero(atual.km_veiculo),
            payload.chaveNfe !== undefined
              ? payload.chaveNfe
              : this.converterTexto(atual.chave_nfe),
            payload.usuarioAtualizacao,
            payload.tipoServico ??
              this.converterTexto(atual.tipo_servico)?.toUpperCase() ??
              'C',
            String(idEmpresa),
            idOs,
          ],
        )) as RegistroBanco[];

        const row = rows[0];
        if (!row) {
          throw new NotFoundException('Ordem de servico nao encontrada para a empresa logada.');
        }

        return {
          sucesso: true,
          mensagem: 'Ordem de servico atualizada com sucesso.',
          ordemServico: this.mapearOrdemServico(row),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'atualizar');
    }
  }

  private async buscarPorIdInterno(
    manager: EntityManager,
    idEmpresa: number,
    idOs: number,
  ): Promise<ListarOrdemServicoDto> {
    const rows = (await manager.query(
      `
        SELECT
          os.*,
          (
            SELECT COUNT(1)::int
            FROM app.requisicao req
            WHERE req.id_empresa = os.id_empresa
              AND req.id_os = os.id_os
          ) AS qtd_requisicoes
        FROM app.ordem_servico os
        WHERE os.id_empresa = $1
          AND os.id_os = $2
        LIMIT 1
      `,
      [String(idEmpresa), idOs],
    )) as RegistroBanco[];

    const row = rows[0];
    if (!row) {
      throw new NotFoundException('Ordem de servico nao encontrada para a empresa logada.');
    }

    return this.mapearOrdemServico(row);
  }

  private async buscarRegistroPorIdOuFalhar(
    manager: EntityManager,
    idEmpresa: number,
    idOs: number,
  ): Promise<RegistroBanco> {
    const rows = (await manager.query(
      `
        SELECT *
        FROM app.ordem_servico
        WHERE id_empresa = $1
          AND id_os = $2
        LIMIT 1
      `,
      [String(idEmpresa), idOs],
    )) as RegistroBanco[];

    const row = rows[0];
    if (!row) {
      throw new NotFoundException('Ordem de servico nao encontrada para a empresa logada.');
    }

    return row;
  }

  private mapearOrdemServico(row: RegistroBanco): ListarOrdemServicoDto {
    return {
      idOs: this.converterNumero(row.id_os) ?? 0,
      idVeiculo: this.converterNumero(row.id_veiculo) ?? 0,
      idFornecedor: this.converterNumero(row.id_fornecedor),
      dataCadastro: this.converterDataIso(row.data_cadastro) ?? '',
      dataFechamento: this.converterDataIso(row.data_fechamento),
      tempoOsMin:
        this.converterNumero(row.tempo_os_min) ??
        this.calcularTempoMinutos(
          this.converterDataIso(row.data_cadastro),
          this.converterDataIso(row.data_fechamento),
        ),
      situacaoOs: this.converterTexto(row.situacao_os)?.toUpperCase() ?? 'A',
      observacao: this.converterTexto(row.observacao),
      valorTotal: this.converterNumero(row.valor_total) ?? 0,
      kmVeiculo: this.converterNumero(row.km_veiculo),
      chaveNfe: this.converterTexto(row.chave_nfe),
      usuarioAtualizacao: this.converterTexto(row.usuario_atualizacao),
      tipoServico: this.converterTexto(row.tipo_servico)?.toUpperCase() ?? null,
      dataAtualizacao: this.converterDataIso(row.data_atualizacao) ?? '',
      atualizadoEm: this.converterDataIso(row.atualizado_em),
      qtdRequisicoes: this.converterNumero(row.qtd_requisicoes) ?? 0,
    };
  }

  private normalizarCriacao(
    dados: CriarOrdemServicoDto,
    usuarioJwt: JwtUsuarioPayload,
  ): PayloadCriacaoNormalizado {
    const usuarioAtualizacao = this.normalizarTexto(
      dados.usuarioAtualizacao ?? usuarioJwt.email,
      'usuarioAtualizacao',
    );

    const dataCadastro = dados.dataCadastro
      ? this.normalizarDataHora(dados.dataCadastro, 'dataCadastro')
      : new Date().toISOString();
    const dataFechamento = dados.dataFechamento
      ? this.normalizarDataHora(dados.dataFechamento, 'dataFechamento')
      : null;

    return {
      idVeiculo: dados.idVeiculo,
      idFornecedor: dados.idFornecedor ?? null,
      dataCadastro,
      dataFechamento,
      tempoOsMin:
        dados.tempoOsMin ?? this.calcularTempoMinutos(dataCadastro, dataFechamento),
      situacaoOs: dados.situacaoOs ?? 'A',
      observacao: this.normalizarTextoOpcional(dados.observacao),
      valorTotal: this.normalizarValor(dados.valorTotal ?? 0, 'valorTotal'),
      kmVeiculo: dados.kmVeiculo ?? null,
      chaveNfe: this.normalizarTextoOpcional(dados.chaveNfe),
      tipoServico: dados.tipoServico ?? 'C',
      usuarioAtualizacao,
    };
  }

  private normalizarAtualizacao(
    dados: AtualizarOrdemServicoDto,
    usuarioJwt: JwtUsuarioPayload,
  ): PayloadAtualizacaoNormalizado {
    const usuarioAtualizacao = this.normalizarTexto(
      dados.usuarioAtualizacao ?? usuarioJwt.email,
      'usuarioAtualizacao',
    );

    return {
      idVeiculo: dados.idVeiculo,
      idFornecedor: dados.idFornecedor,
      dataCadastro:
        dados.dataCadastro !== undefined
          ? this.normalizarDataHora(dados.dataCadastro, 'dataCadastro')
          : undefined,
      dataFechamento:
        dados.dataFechamento !== undefined
          ? dados.dataFechamento
            ? this.normalizarDataHora(dados.dataFechamento, 'dataFechamento')
            : null
          : undefined,
      tempoOsMin: dados.tempoOsMin,
      situacaoOs: dados.situacaoOs,
      observacao:
        dados.observacao !== undefined
          ? this.normalizarTextoOpcional(dados.observacao)
          : undefined,
      valorTotal:
        dados.valorTotal !== undefined
          ? this.normalizarValor(dados.valorTotal, 'valorTotal')
          : undefined,
      kmVeiculo: dados.kmVeiculo,
      chaveNfe:
        dados.chaveNfe !== undefined
          ? this.normalizarTextoOpcional(dados.chaveNfe)
          : undefined,
      tipoServico: dados.tipoServico,
      usuarioAtualizacao,
    };
  }

  private validarIntervalosDoFiltro(filtro: FiltroOrdemServicoDto) {
    this.validarIntervaloDatas(
      filtro.dataCadastroDe,
      filtro.dataCadastroAte,
      'dataCadastroDe',
      'dataCadastroAte',
    );
    this.validarIntervaloDatas(
      filtro.dataFechamentoDe,
      filtro.dataFechamentoAte,
      'dataFechamentoDe',
      'dataFechamentoAte',
    );
  }

  private validarIntervaloDatas(
    dataInicio: string | undefined,
    dataFim: string | undefined,
    campoInicio: string,
    campoFim: string,
  ) {
    if (!dataInicio || !dataFim) {
      return;
    }

    const inicio = new Date(dataInicio);
    const fim = new Date(dataFim);

    if (Number.isNaN(inicio.getTime()) || Number.isNaN(fim.getTime())) {
      throw new BadRequestException(
        `Intervalo invalido: ${campoInicio} ou ${campoFim} contem data invalida.`,
      );
    }

    if (fim < inicio) {
      throw new BadRequestException(
        `Intervalo invalido: ${campoFim} deve ser maior ou igual a ${campoInicio}.`,
      );
    }
  }

  private resolverColunaOrdenacao(
    ordenarPor: FiltroOrdemServicoDto['ordenarPor'],
  ): string {
    if (ordenarPor === 'id_os') return 'id_os';
    if (ordenarPor === 'data_fechamento') return 'data_fechamento';
    if (ordenarPor === 'valor_total') return 'valor_total';
    if (ordenarPor === 'situacao_os') return 'situacao_os';
    if (ordenarPor === 'tipo_servico') return 'tipo_servico';
    if (ordenarPor === 'km_veiculo') return 'km_veiculo';
    if (ordenarPor === 'atualizado_em') return 'atualizado_em';
    return 'data_cadastro';
  }

  private calcularTempoMinutos(
    dataCadastro: string | null | undefined,
    dataFechamento: string | null | undefined,
  ): number | null {
    if (!dataCadastro || !dataFechamento) {
      return null;
    }

    const inicio = new Date(dataCadastro);
    const fim = new Date(dataFechamento);
    if (Number.isNaN(inicio.getTime()) || Number.isNaN(fim.getTime())) {
      return null;
    }

    const diferencaMs = fim.getTime() - inicio.getTime();
    if (!Number.isFinite(diferencaMs) || diferencaMs < 0) {
      return 0;
    }

    return Math.round(diferencaMs / 60000);
  }

  private normalizarValor(valor: number, campo: string): number {
    const numero = Number(valor);
    if (!Number.isFinite(numero) || numero < 0) {
      throw new BadRequestException(`${campo} invalido.`);
    }

    return Number(numero.toFixed(2));
  }

  private normalizarTexto(valor: string, campo: string): string {
    const texto = valor.trim().toUpperCase();
    if (!texto) {
      throw new BadRequestException(`${campo} invalido.`);
    }
    return texto;
  }

  private normalizarTextoOpcional(valor: string | null | undefined): string | null {
    if (valor === null || valor === undefined) {
      return null;
    }

    const texto = valor.trim();
    return texto ? texto.toUpperCase() : null;
  }

  private normalizarDataHora(valor: string, campo: string): string {
    const data = new Date(valor);
    if (Number.isNaN(data.getTime())) {
      throw new BadRequestException(`${campo} invalido.`);
    }

    return data.toISOString();
  }

  private converterNumero(valor: unknown): number | null {
    if (valor === null || valor === undefined) {
      return null;
    }

    const numero = Number(valor);
    return Number.isFinite(numero) ? numero : null;
  }

  private converterTexto(valor: unknown): string | null {
    if (typeof valor !== 'string') {
      return null;
    }

    const texto = valor.trim();
    return texto ? texto : null;
  }

  private converterDataIso(valor: unknown): string | null {
    if (valor === null || valor === undefined) {
      return null;
    }

    const data = new Date(
      valor instanceof Date || typeof valor === 'string' || typeof valor === 'number'
        ? valor
        : '',
    );

    return Number.isNaN(data.getTime()) ? null : data.toISOString();
  }

  private async obterProximoId(
    manager: EntityManager,
    sequence: string,
  ): Promise<number> {
    const rows = (await manager.query(
      `SELECT nextval('${sequence}')::bigint AS id`,
    )) as Array<{ id?: string | number }>;

    const id = Number(rows[0]?.id ?? 0);
    if (!Number.isFinite(id) || id <= 0) {
      throw new BadRequestException(`Nao foi possivel gerar id para ${sequence}.`);
    }

    return id;
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

  private tratarErroPersistencia(error: unknown, acao: string): never {
    if (error instanceof BadRequestException || error instanceof NotFoundException) {
      throw error;
    }

    if (error instanceof QueryFailedError) {
      const erroPg = error.driverError as { code?: string };

      if (erroPg.code === '23503') {
        throw new BadRequestException(
          'Veiculo ou fornecedor informado nao existe para a empresa logada.',
        );
      }

      if (erroPg.code === '23514') {
        throw new BadRequestException(
          'Dados invalidos para situacao, tipo de servico, valor ou quilometragem.',
        );
      }

      if (erroPg.code === '22P02' || erroPg.code === '22007') {
        throw new BadRequestException('Formato de numero ou data invalido.');
      }
    }

    throw new BadRequestException(
      `Nao foi possivel ${acao} ordem de servico neste momento.`,
    );
  }
}
