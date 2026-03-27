import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { DataSource, EntityManager, QueryFailedError } from 'typeorm';
import { JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { configurarContextoEmpresaRls } from '../common/database/rls.util';
import { AtualizarOrdemServicoDto } from './dto/atualizar-ordem-servico.dto';
import { CriarOrdemServicoDto } from './dto/criar-ordem-servico.dto';
import { FiltroOrdemServicoDto } from './dto/filtro-ordem-servico.dto';
import {
  ListarOrdemServicoDto,
  ListarOrdemServicoItemDto,
  ListarOrdemServicoRequisicaoDto,
} from './dto/listar-ordem-servico.dto';
import { ItemRequisicaoDto } from './dto/item-requisicao.dto';
import { RequisicaoOrdemServicoDto } from './dto/requisicao-ordem-servico.dto';
import { SITUACAO_OS_OPCOES, TIPO_SERVICO_OPCOES } from './ordem-servico.constants';

type RegistroBanco = Record<string, unknown>;

type ItemNormalizado = {
  idProduto: number;
  qtdProduto: number;
  valorUn: number;
  observacao: string | null;
  usuarioAtualizacao: string;
  valorTotalItem: number;
};

type RequisicaoNormalizada = {
  dataRequisicao: string;
  situacao: string;
  observacao: string | null;
  usuarioAtualizacao: string;
};

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
  requisicao?: RequisicaoNormalizada;
  itens: ItemNormalizado[];
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
  requisicao?: RequisicaoNormalizada;
  itens?: ItemNormalizado[];
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
          SELECT *
          FROM app.ordem_servico
          WHERE id_empresa = $1
          ORDER BY data_cadastro DESC, id_os DESC
        `,
        [String(idEmpresa)],
      )) as RegistroBanco[];

      const ordensServico = await this.mapearOrdensComRequisicoes(
        manager,
        idEmpresa,
        rows,
      );

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
      const filtros: string[] = ['id_empresa = $1'];
      const valores: Array<string | number> = [String(idEmpresa)];

      if (filtro.idOs !== undefined) {
        valores.push(filtro.idOs);
        filtros.push(`id_os = $${valores.length}`);
      }

      if (filtro.idVeiculo !== undefined) {
        valores.push(filtro.idVeiculo);
        filtros.push(`id_veiculo = $${valores.length}`);
      }

      if (filtro.idFornecedor !== undefined) {
        valores.push(filtro.idFornecedor);
        filtros.push(`id_fornecedor = $${valores.length}`);
      }

      if (filtro.situacaoOs) {
        valores.push(filtro.situacaoOs);
        filtros.push(`situacao_os = $${valores.length}`);
      }

      if (filtro.tipoServico) {
        valores.push(filtro.tipoServico);
        filtros.push(`tipo_servico = $${valores.length}::app.tipo_servico_enum`);
      }

      if (filtro.dataCadastroDe) {
        valores.push(this.normalizarDataHora(filtro.dataCadastroDe, 'dataCadastroDe'));
        filtros.push(`data_cadastro >= $${valores.length}`);
      }

      if (filtro.dataCadastroAte) {
        valores.push(this.normalizarDataHora(filtro.dataCadastroAte, 'dataCadastroAte'));
        filtros.push(`data_cadastro <= $${valores.length}`);
      }

      if (filtro.dataFechamentoDe) {
        valores.push(
          this.normalizarDataHora(filtro.dataFechamentoDe, 'dataFechamentoDe'),
        );
        filtros.push(`data_fechamento >= $${valores.length}`);
      }

      if (filtro.dataFechamentoAte) {
        valores.push(
          this.normalizarDataHora(filtro.dataFechamentoAte, 'dataFechamentoAte'),
        );
        filtros.push(`data_fechamento <= $${valores.length}`);
      }

      if (filtro.texto?.trim()) {
        valores.push(`%${filtro.texto.trim().toUpperCase()}%`);
        filtros.push(
          `(CAST(id_os AS TEXT) LIKE $${valores.length} OR UPPER(COALESCE(observacao, '')) LIKE $${valores.length} OR UPPER(COALESCE(chave_nfe, '')) LIKE $${valores.length} OR UPPER(COALESCE(usuario_atualizacao, '')) LIKE $${valores.length})`,
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
        FROM app.ordem_servico
        ${whereSql}
      `;

      const sqlDados = `
        SELECT *
        FROM app.ordem_servico
        ${whereSql}
        ORDER BY ${colunaOrdenacao} ${ordem}, id_os DESC
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
      const ordensServico = await this.mapearOrdensComRequisicoes(
        manager,
        idEmpresa,
        rows,
      );

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

        if (payload.itens.length > 0) {
          await this.validarProdutosInformados(manager, idEmpresa, payload.itens);
        }

        const idOs = await this.obterProximoId(manager, 'app.ordem_servico_id_os_seq');

        await manager.query(
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
        );

        await this.persistirRequisicaoEItens(
          manager,
          idEmpresa,
          idOs,
          payload.requisicao,
          payload.itens,
          payload.usuarioAtualizacao,
        );

        const ordemServico = await this.buscarPorIdInterno(manager, idEmpresa, idOs);

        return {
          sucesso: true,
          mensagem: 'Ordem de servico cadastrada com sucesso.',
          ordemServico,
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
        const atual = await this.buscarRegistroOrdemOuFalhar(manager, idEmpresa, idOs);
        const payload = this.normalizarAtualizacao(dados, usuarioJwt);

        if (payload.itens !== undefined && payload.itens.length > 0) {
          await this.validarProdutosInformados(manager, idEmpresa, payload.itens);
        }

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
            : this.converterNumero(atual.tempo_os_min);
        const situacaoOs =
          payload.situacaoOs ??
          this.converterTexto(atual.situacao_os)?.trim().toUpperCase() ??
          'A';
        const observacao =
          payload.observacao !== undefined
            ? payload.observacao
            : this.converterTexto(atual.observacao);
        const valorTotalAtual = this.converterNumero(atual.valor_total) ?? 0;
        const valorTotal =
          payload.valorTotal !== undefined
            ? payload.valorTotal
            : payload.itens !== undefined
              ? this.calcularValorTotalItens(payload.itens)
              : valorTotalAtual;
        const kmVeiculo =
          payload.kmVeiculo !== undefined
            ? payload.kmVeiculo
            : this.converterNumero(atual.km_veiculo);
        const idVeiculo = payload.idVeiculo ?? this.converterNumero(atual.id_veiculo) ?? 0;
        const idFornecedor =
          payload.idFornecedor !== undefined
            ? payload.idFornecedor
            : this.converterNumero(atual.id_fornecedor);
        const chaveNfe =
          payload.chaveNfe !== undefined
            ? payload.chaveNfe
            : this.converterTexto(atual.chave_nfe);
        const tipoServico =
          payload.tipoServico ??
          this.converterTexto(atual.tipo_servico)?.trim().toUpperCase() ??
          'C';

        const tempoFinal =
          tempoOsMin ?? this.calcularTempoMinutos(dataCadastro, dataFechamento);

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
            idVeiculo,
            idFornecedor,
            dataCadastro,
            dataFechamento,
            tempoFinal,
            situacaoOs,
            observacao,
            valorTotal,
            kmVeiculo,
            chaveNfe,
            payload.usuarioAtualizacao,
            tipoServico,
            String(idEmpresa),
            idOs,
          ],
        )) as RegistroBanco[];

        const row = rows[0];
        if (!row) {
          throw new NotFoundException('Ordem de servico nao encontrada para a empresa logada.');
        }

        if (payload.requisicao !== undefined || payload.itens !== undefined) {
          await this.persistirRequisicaoEItens(
            manager,
            idEmpresa,
            idOs,
            payload.requisicao,
            payload.itens,
            payload.usuarioAtualizacao,
          );
        }

        const ordemServico = await this.buscarPorIdInterno(manager, idEmpresa, idOs);

        return {
          sucesso: true,
          mensagem: 'Ordem de servico atualizada com sucesso.',
          ordemServico,
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'atualizar');
    }
  }

  private async persistirRequisicaoEItens(
    manager: EntityManager,
    idEmpresa: number,
    idOs: number,
    requisicao: RequisicaoNormalizada | undefined,
    itens: ItemNormalizado[] | undefined,
    usuarioPadrao: string,
  ) {
    const existeEntrada = requisicao !== undefined || itens !== undefined;
    if (!existeEntrada) {
      return;
    }

    const requisicaoAtual = await this.buscarRequisicaoMaisRecente(manager, idEmpresa, idOs);
    let idRequisicao = this.converterNumero(requisicaoAtual?.id_requisicao);

    if (idRequisicao === null) {
      idRequisicao = await this.obterProximoId(
        manager,
        'app.requisicao_id_requisicao_seq',
      );

      const base = requisicao ?? {
        dataRequisicao: new Date().toISOString(),
        situacao: 'A',
        observacao: null,
        usuarioAtualizacao: usuarioPadrao,
      };

      await manager.query(
        `
          INSERT INTO app.requisicao (
            id_requisicao,
            id_os,
            data_requisicao,
            situacao,
            observacao,
            usuario_atualizacao,
            id_empresa
          )
          VALUES ($1,$2,$3,$4,$5,$6,$7)
        `,
        [
          idRequisicao,
          idOs,
          base.dataRequisicao,
          base.situacao,
          base.observacao,
          base.usuarioAtualizacao,
          String(idEmpresa),
        ],
      );
    } else if (requisicao !== undefined) {
      await manager.query(
        `
          UPDATE app.requisicao
          SET
            data_requisicao = $1,
            situacao = $2,
            observacao = $3,
            usuario_atualizacao = $4,
            atualizado_em = NOW()
          WHERE id_empresa = $5
            AND id_requisicao = $6
        `,
        [
          requisicao.dataRequisicao,
          requisicao.situacao,
          requisicao.observacao,
          requisicao.usuarioAtualizacao,
          String(idEmpresa),
          idRequisicao,
        ],
      );
    }

    if (itens !== undefined) {
      await manager.query(
        `
          DELETE FROM app.requisicao_itens
          WHERE id_empresa = $1
            AND id_requisicao = $2
        `,
        [String(idEmpresa), idRequisicao],
      );

      for (const item of itens) {
        const idItem = await this.obterProximoId(
          manager,
          'app.requisicao_itens_id_item_seq',
        );

        await manager.query(
          `
            INSERT INTO app.requisicao_itens (
              id_item,
              id_requisicao,
              id_produto,
              qtd_produto,
              valor_un,
              observacao,
              usuario_atualizacao,
              id_empresa
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
          `,
          [
            idItem,
            idRequisicao,
            item.idProduto,
            item.qtdProduto,
            item.valorUn,
            item.observacao,
            item.usuarioAtualizacao,
            String(idEmpresa),
          ],
        );
      }
    }
  }

  private async buscarPorIdInterno(
    manager: EntityManager,
    idEmpresa: number,
    idOs: number,
  ): Promise<ListarOrdemServicoDto> {
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

    const [ordemServico] = await this.mapearOrdensComRequisicoes(
      manager,
      idEmpresa,
      [row],
    );

    if (!ordemServico) {
      throw new NotFoundException('Ordem de servico nao encontrada para a empresa logada.');
    }

    return ordemServico;
  }

  private async buscarRegistroOrdemOuFalhar(
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

  private async buscarRequisicaoMaisRecente(
    manager: EntityManager,
    idEmpresa: number,
    idOs: number,
  ): Promise<RegistroBanco | null> {
    const rows = (await manager.query(
      `
        SELECT *
        FROM app.requisicao
        WHERE id_empresa = $1
          AND id_os = $2
        ORDER BY id_requisicao DESC
        LIMIT 1
      `,
      [String(idEmpresa), idOs],
    )) as RegistroBanco[];

    return rows[0] ?? null;
  }

  private async mapearOrdensComRequisicoes(
    manager: EntityManager,
    idEmpresa: number,
    rows: RegistroBanco[],
  ): Promise<ListarOrdemServicoDto[]> {
    if (rows.length === 0) {
      return [];
    }

    const idsOs = rows
      .map((row) => this.converterNumero(row.id_os))
      .filter((id): id is number => id !== null);

    if (idsOs.length === 0) {
      return rows.map((row) => this.mapearOrdemServico(row, null));
    }

    const requisicaoRows = (await manager.query(
      `
        SELECT *
        FROM app.requisicao
        WHERE id_empresa = $1
          AND id_os = ANY($2::bigint[])
        ORDER BY id_os ASC, id_requisicao DESC
      `,
      [String(idEmpresa), idsOs],
    )) as RegistroBanco[];

    const requisicaoPorOs = new Map<number, RegistroBanco>();
    for (const requisicao of requisicaoRows) {
      const idOs = this.converterNumero(requisicao.id_os);
      if (idOs === null) {
        continue;
      }
      if (!requisicaoPorOs.has(idOs)) {
        requisicaoPorOs.set(idOs, requisicao);
      }
    }

    const idsRequisicao = Array.from(requisicaoPorOs.values())
      .map((item) => this.converterNumero(item.id_requisicao))
      .filter((id): id is number => id !== null);

    const itensPorRequisicao = await this.carregarItensPorRequisicao(
      manager,
      idEmpresa,
      idsRequisicao,
    );

    return rows.map((row) => {
      const idOs = this.converterNumero(row.id_os);
      const requisicaoRow = idOs !== null ? requisicaoPorOs.get(idOs) ?? null : null;
      return this.mapearOrdemServico(row, requisicaoRow, itensPorRequisicao);
    });
  }

  private async carregarItensPorRequisicao(
    manager: EntityManager,
    idEmpresa: number,
    idsRequisicao: number[],
  ): Promise<Map<number, ListarOrdemServicoItemDto[]>> {
    const itensPorRequisicao = new Map<number, ListarOrdemServicoItemDto[]>();

    if (idsRequisicao.length === 0) {
      return itensPorRequisicao;
    }

    const rows = (await manager.query(
      `
        SELECT
          itens.*,
          produto.descricao_produto
        FROM app.requisicao_itens itens
        LEFT JOIN app.produto produto
          ON produto.id_produto = itens.id_produto
         AND produto.id_empresa = itens.id_empresa
        WHERE itens.id_empresa = $1
          AND itens.id_requisicao = ANY($2::bigint[])
        ORDER BY itens.id_requisicao ASC, itens.id_item ASC
      `,
      [String(idEmpresa), idsRequisicao],
    )) as RegistroBanco[];

    for (const row of rows) {
      const idRequisicao = this.converterNumero(row.id_requisicao);
      if (idRequisicao === null) {
        continue;
      }

      const item = this.mapearItemRequisicao(row);
      const lista = itensPorRequisicao.get(idRequisicao) ?? [];
      lista.push(item);
      itensPorRequisicao.set(idRequisicao, lista);
    }

    return itensPorRequisicao;
  }

  private mapearItemRequisicao(registro: RegistroBanco): ListarOrdemServicoItemDto {
    const qtdProduto = this.converterNumero(registro.qtd_produto) ?? 0;
    const valorUn = this.converterNumero(registro.valor_un) ?? 0;

    return {
      idItem: this.converterNumero(registro.id_item) ?? 0,
      idRequisicao: this.converterNumero(registro.id_requisicao) ?? 0,
      idProduto: this.converterNumero(registro.id_produto) ?? 0,
      descricaoProduto: this.converterTexto(registro.descricao_produto),
      qtdProduto,
      valorUn,
      valorTotalItem: Number((qtdProduto * valorUn).toFixed(2)),
      observacao: this.converterTexto(registro.observacao),
      usuarioAtualizacao: this.converterTexto(registro.usuario_atualizacao),
      criadoEm: this.converterDataIso(registro.criado_em) ?? '',
      atualizadoEm: this.converterDataIso(registro.atualizado_em) ?? '',
    };
  }

  private mapearRequisicao(
    registro: RegistroBanco,
    itens: ListarOrdemServicoItemDto[],
  ): ListarOrdemServicoRequisicaoDto {
    return {
      idRequisicao: this.converterNumero(registro.id_requisicao) ?? 0,
      idOs: this.converterNumero(registro.id_os) ?? 0,
      dataRequisicao: this.converterDataIso(registro.data_requisicao) ?? '',
      situacao:
        this.converterTexto(registro.situacao)?.trim().toUpperCase() ?? 'A',
      observacao: this.converterTexto(registro.observacao),
      usuarioAtualizacao: this.converterTexto(registro.usuario_atualizacao),
      criadoEm: this.converterDataIso(registro.criado_em) ?? '',
      atualizadoEm: this.converterDataIso(registro.atualizado_em) ?? '',
      itens,
    };
  }

  private mapearOrdemServico(
    registro: RegistroBanco,
    requisicaoRegistro: RegistroBanco | null,
    itensPorRequisicao: Map<number, ListarOrdemServicoItemDto[]> = new Map(),
  ): ListarOrdemServicoDto {
    const idRequisicao = requisicaoRegistro
      ? this.converterNumero(requisicaoRegistro.id_requisicao)
      : null;
    const itens =
      idRequisicao !== null ? itensPorRequisicao.get(idRequisicao) ?? [] : [];
    const requisicao =
      requisicaoRegistro !== null
        ? this.mapearRequisicao(requisicaoRegistro, itens)
        : null;

    return {
      idOs: this.converterNumero(registro.id_os) ?? 0,
      idVeiculo: this.converterNumero(registro.id_veiculo) ?? 0,
      idFornecedor: this.converterNumero(registro.id_fornecedor),
      dataCadastro: this.converterDataIso(registro.data_cadastro) ?? '',
      dataFechamento: this.converterDataIso(registro.data_fechamento),
      tempoOsMin:
        this.converterNumero(registro.tempo_os_min) ??
        this.calcularTempoMinutos(
          this.converterDataIso(registro.data_cadastro),
          this.converterDataIso(registro.data_fechamento),
        ),
      situacaoOs: this.converterTexto(registro.situacao_os)?.toUpperCase() ?? 'A',
      observacao: this.converterTexto(registro.observacao),
      valorTotal: this.converterNumero(registro.valor_total) ?? 0,
      kmVeiculo: this.converterNumero(registro.km_veiculo),
      chaveNfe: this.converterTexto(registro.chave_nfe),
      usuarioAtualizacao: this.converterTexto(registro.usuario_atualizacao),
      tipoServico: this.converterTexto(registro.tipo_servico)?.toUpperCase() ?? null,
      dataAtualizacao: this.converterDataIso(registro.data_atualizacao) ?? '',
      atualizadoEm: this.converterDataIso(registro.atualizado_em),
      requisicao,
      itens,
    };
  }

  private async validarProdutosInformados(
    manager: EntityManager,
    idEmpresa: number,
    itens: ItemNormalizado[],
  ) {
    if (itens.length === 0) {
      return;
    }

    const idsProduto = Array.from(new Set(itens.map((item) => item.idProduto)));
    const rows = (await manager.query(
      `
        SELECT id_produto
        FROM app.produto
        WHERE id_empresa = $1
          AND id_produto = ANY($2::bigint[])
      `,
      [String(idEmpresa), idsProduto],
    )) as Array<{ id_produto?: string | number }>;

    const encontrados = new Set(
      rows
        .map((item) => Number(item.id_produto))
        .filter((id) => Number.isFinite(id)),
    );

    const faltantes = idsProduto.filter((id) => !encontrados.has(id));
    if (faltantes.length > 0) {
      throw new BadRequestException(
        `Produto(s) nao encontrado(s) para a empresa logada: ${faltantes.join(', ')}.`,
      );
    }
  }

  private normalizarCriacao(
    dados: CriarOrdemServicoDto,
    usuarioJwt: JwtUsuarioPayload,
  ): PayloadCriacaoNormalizado {
    const usuarioAtualizacao = this.normalizarTexto(
      dados.usuarioAtualizacao ?? usuarioJwt.email,
      'usuarioAtualizacao',
    );

    const itens = this.normalizarItens(dados.itens, usuarioAtualizacao);
    const valorTotal =
      dados.valorTotal !== undefined
        ? this.normalizarValor(dados.valorTotal, 'valorTotal')
        : this.calcularValorTotalItens(itens);
    const dataCadastro = dados.dataCadastro
      ? this.normalizarDataHora(dados.dataCadastro, 'dataCadastro')
      : new Date().toISOString();
    const dataFechamento = dados.dataFechamento
      ? this.normalizarDataHora(dados.dataFechamento, 'dataFechamento')
      : null;
    const tempoOsMin =
      dados.tempoOsMin ?? this.calcularTempoMinutos(dataCadastro, dataFechamento);

    return {
      idVeiculo: dados.idVeiculo,
      idFornecedor: dados.idFornecedor ?? null,
      dataCadastro,
      dataFechamento,
      tempoOsMin,
      situacaoOs: dados.situacaoOs ?? 'A',
      observacao: this.normalizarTextoOpcional(dados.observacao),
      valorTotal,
      kmVeiculo: dados.kmVeiculo ?? null,
      chaveNfe: this.normalizarTextoOpcional(dados.chaveNfe),
      tipoServico: dados.tipoServico ?? 'C',
      usuarioAtualizacao,
      requisicao: dados.requisicao
        ? this.normalizarRequisicao(dados.requisicao, usuarioAtualizacao, dataCadastro)
        : itens.length > 0
          ? {
              dataRequisicao: dataCadastro,
              situacao: dados.situacaoOs ?? 'A',
              observacao: null,
              usuarioAtualizacao,
            }
          : undefined,
      itens,
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
      requisicao:
        dados.requisicao !== undefined
          ? this.normalizarRequisicao(
              dados.requisicao,
              usuarioAtualizacao,
              new Date().toISOString(),
            )
          : undefined,
      itens:
        dados.itens !== undefined
          ? this.normalizarItens(dados.itens, usuarioAtualizacao)
          : undefined,
    };
  }

  private normalizarRequisicao(
    requisicao: RequisicaoOrdemServicoDto,
    usuarioPadrao: string,
    dataPadrao: string,
  ): RequisicaoNormalizada {
    return {
      dataRequisicao: requisicao.dataRequisicao
        ? this.normalizarDataHora(requisicao.dataRequisicao, 'requisicao.dataRequisicao')
        : dataPadrao,
      situacao: requisicao.situacao ?? 'A',
      observacao: this.normalizarTextoOpcional(requisicao.observacao),
      usuarioAtualizacao: requisicao.usuarioAtualizacao
        ? this.normalizarTexto(requisicao.usuarioAtualizacao, 'requisicao.usuarioAtualizacao')
        : usuarioPadrao,
    };
  }

  private normalizarItens(
    itens: ItemRequisicaoDto[] | undefined,
    usuarioPadrao: string,
  ): ItemNormalizado[] {
    if (!itens || itens.length === 0) {
      return [];
    }

    return itens.map((item, index) => {
      const idProduto = Number(item.idProduto);
      const qtdProduto = Number(item.qtdProduto);
      const valorUn = Number(item.valorUn);

      if (!Number.isFinite(idProduto) || idProduto <= 0) {
        throw new BadRequestException(`Item ${index + 1}: idProduto invalido.`);
      }

      if (!Number.isFinite(qtdProduto) || qtdProduto <= 0) {
        throw new BadRequestException(`Item ${index + 1}: qtdProduto deve ser maior que zero.`);
      }

      if (!Number.isFinite(valorUn) || valorUn < 0) {
        throw new BadRequestException(`Item ${index + 1}: valorUn invalido.`);
      }

      const quantidadeNormalizada = Number(qtdProduto.toFixed(3));
      const valorUnNormalizado = Number(valorUn.toFixed(4));

      return {
        idProduto,
        qtdProduto: quantidadeNormalizada,
        valorUn: valorUnNormalizado,
        observacao: this.normalizarTextoOpcional(item.observacao),
        usuarioAtualizacao: item.usuarioAtualizacao
          ? this.normalizarTexto(item.usuarioAtualizacao, `item[${index}].usuarioAtualizacao`)
          : usuarioPadrao,
        valorTotalItem: Number((quantidadeNormalizada * valorUnNormalizado).toFixed(2)),
      };
    });
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
    if (ordenarPor === 'id_os') {
      return 'id_os';
    }
    if (ordenarPor === 'data_fechamento') {
      return 'data_fechamento';
    }
    if (ordenarPor === 'valor_total') {
      return 'valor_total';
    }
    if (ordenarPor === 'situacao_os') {
      return 'situacao_os';
    }
    if (ordenarPor === 'tipo_servico') {
      return 'tipo_servico';
    }
    if (ordenarPor === 'km_veiculo') {
      return 'km_veiculo';
    }
    if (ordenarPor === 'atualizado_em') {
      return 'atualizado_em';
    }

    return 'data_cadastro';
  }

  private calcularValorTotalItens(itens: ItemNormalizado[]): number {
    if (itens.length === 0) {
      return 0;
    }

    const total = itens.reduce((acc, item) => acc + item.valorTotalItem, 0);
    return Number(total.toFixed(2));
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
          'Veiculo, fornecedor ou produto informado nao existe para a empresa logada.',
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
