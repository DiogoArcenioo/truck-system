import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { DataSource, EntityManager, QueryFailedError } from 'typeorm';
import { JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { configurarContextoEmpresaRls } from '../common/database/rls.util';
import { AtualizarRequisicaoDto } from './dto/atualizar-requisicao.dto';
import { CriarRequisicaoDto } from './dto/criar-requisicao.dto';
import { FiltroRequisicaoDto } from './dto/filtro-requisicao.dto';
import { ItemRequisicaoDto } from './dto/item-requisicao.dto';
import { ListarRequisicaoDto, ListarRequisicaoItemDto } from './dto/listar-requisicao.dto';
import { SITUACAO_REQUISICAO_OPCOES } from './requisicao.constants';

type RegistroBanco = Record<string, unknown>;

type ItemNormalizado = {
  idProduto: number;
  qtdProduto: number;
  valorUn: number;
  observacao: string;
  usuarioAtualizacao: string;
};

type PayloadCriacaoNormalizado = {
  idOs: number;
  dataRequisicao: string;
  situacao: string;
  observacao: string;
  usuarioAtualizacao: string;
  itens: ItemNormalizado[];
};

type PayloadAtualizacaoNormalizado = {
  idOs?: number;
  dataRequisicao?: string;
  situacao?: string;
  observacao?: string;
  usuarioAtualizacao: string;
  itens?: ItemNormalizado[];
};

@Injectable()
export class RequisicaoService {
  constructor(private readonly dataSource: DataSource) {}

  listarOpcoes() {
    return {
      sucesso: true,
      situacao: SITUACAO_REQUISICAO_OPCOES,
    };
  }

  async listarTodos(idEmpresa: number) {
    return this.executarComRls(idEmpresa, async (manager) => {
      const rows = (await manager.query(
        `
          SELECT *
          FROM app.requisicao
          WHERE id_empresa = $1
          ORDER BY data_requisicao DESC, id_requisicao DESC
        `,
        [String(idEmpresa)],
      )) as RegistroBanco[];

      const requisicoes = await this.mapearRequisicoesComItens(
        manager,
        idEmpresa,
        rows,
      );

      return {
        sucesso: true,
        total: requisicoes.length,
        requisicoes,
      };
    });
  }

  async listarComFiltro(idEmpresa: number, filtro: FiltroRequisicaoDto) {
    this.validarIntervaloDatas(filtro.dataDe, filtro.dataAte, 'dataDe', 'dataAte');

    return this.executarComRls(idEmpresa, async (manager) => {
      const filtros: string[] = ['id_empresa = $1'];
      const valores: Array<string | number> = [String(idEmpresa)];

      if (filtro.idRequisicao !== undefined) {
        valores.push(filtro.idRequisicao);
        filtros.push(`id_requisicao = $${valores.length}`);
      }

      if (filtro.idOs !== undefined) {
        valores.push(filtro.idOs);
        filtros.push(`id_os = $${valores.length}`);
      }

      if (filtro.situacao) {
        valores.push(filtro.situacao);
        filtros.push(`situacao = $${valores.length}`);
      }

      if (filtro.dataDe) {
        valores.push(this.normalizarDataHora(filtro.dataDe, 'dataDe'));
        filtros.push(`data_requisicao >= $${valores.length}`);
      }

      if (filtro.dataAte) {
        valores.push(this.normalizarDataHora(filtro.dataAte, 'dataAte'));
        filtros.push(`data_requisicao <= $${valores.length}`);
      }

      if (filtro.texto?.trim()) {
        valores.push(`%${filtro.texto.trim().toUpperCase()}%`);
        filtros.push(
          `(CAST(id_requisicao AS TEXT) LIKE $${valores.length} OR CAST(id_os AS TEXT) LIKE $${valores.length} OR UPPER(COALESCE(observacao, '')) LIKE $${valores.length} OR UPPER(COALESCE(usuario_atualizacao, '')) LIKE $${valores.length})`,
        );
      }

      const pagina = filtro.pagina ?? 1;
      const limite = filtro.limite ?? 20;
      const offset = (pagina - 1) * limite;
      const ordem = filtro.ordem ?? 'DESC';
      const colunaOrdenacao = this.resolverColunaOrdenacao(filtro.ordenarPor);
      const whereSql = `WHERE ${filtros.join(' AND ')}`;

      const sqlCount = `
        SELECT COUNT(1)::int AS total
        FROM app.requisicao
        ${whereSql}
      `;

      const sqlDados = `
        SELECT *
        FROM app.requisicao
        ${whereSql}
        ORDER BY ${colunaOrdenacao} ${ordem}, id_requisicao DESC
        LIMIT $${valores.length + 1}
        OFFSET $${valores.length + 2}
      `;

      const [countRows, rows] = await Promise.all([
        manager.query(sqlCount, valores) as Promise<Array<{ total: number }>>,
        manager.query(sqlDados, [...valores, limite, offset]) as Promise<RegistroBanco[]>,
      ]);

      const total = Number(countRows[0]?.total ?? 0);
      const requisicoes = await this.mapearRequisicoesComItens(
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
        requisicoes,
      };
    });
  }

  async buscarPorId(idEmpresa: number, idRequisicao: number) {
    return this.executarComRls(idEmpresa, async (manager) => {
      const requisicao = await this.buscarPorIdInterno(manager, idEmpresa, idRequisicao);
      return {
        sucesso: true,
        requisicao,
      };
    });
  }

  async cadastrar(
    idEmpresa: number,
    dados: CriarRequisicaoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    try {
      return this.executarComRls(idEmpresa, async (manager) => {
        const payload = this.normalizarCriacao(dados, usuarioJwt);

        await this.validarOrdemServicoExiste(manager, idEmpresa, payload.idOs);
        await this.validarProdutosInformados(manager, idEmpresa, payload.itens);

        const idRequisicao = await this.inserirCabecalhoRequisicao(
          manager,
          idEmpresa,
          payload,
        );

        await this.substituirItensDaRequisicao(
          manager,
          idEmpresa,
          idRequisicao,
          payload.itens,
        );

        const requisicao = await this.buscarPorIdInterno(
          manager,
          idEmpresa,
          idRequisicao,
        );

        return {
          sucesso: true,
          mensagem: 'Requisicao cadastrada com sucesso.',
          requisicao,
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'cadastrar');
    }
  }

  async atualizar(
    idEmpresa: number,
    idRequisicao: number,
    dados: AtualizarRequisicaoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    try {
      return this.executarComRls(idEmpresa, async (manager) => {
        const atual = await this.buscarRegistroPorIdOuFalhar(
          manager,
          idEmpresa,
          idRequisicao,
        );
        const payload = this.normalizarAtualizacao(dados, usuarioJwt);

        const idOs = payload.idOs ?? this.converterNumero(atual.id_os) ?? 0;
        if (idOs <= 0) {
          throw new BadRequestException('idOs invalido para requisicao.');
        }

        await this.validarOrdemServicoExiste(manager, idEmpresa, idOs);

        if (payload.itens !== undefined) {
          await this.validarProdutosInformados(manager, idEmpresa, payload.itens);
        }

        const rows = (await manager.query(
          `
            UPDATE app.requisicao
            SET
              id_os = $1,
              data_requisicao = $2,
              situacao = $3,
              observacao = $4,
              usuario_atualizacao = $5,
              atualizado_em = NOW()
            WHERE id_empresa = $6
              AND id_requisicao = $7
            RETURNING *
          `,
          [
            idOs,
            payload.dataRequisicao ?? this.converterDataIso(atual.data_requisicao) ?? new Date().toISOString(),
            payload.situacao ?? this.converterTexto(atual.situacao)?.toUpperCase() ?? 'A',
            payload.observacao !== undefined
              ? payload.observacao
              : this.converterTexto(atual.observacao),
            payload.usuarioAtualizacao,
            String(idEmpresa),
            idRequisicao,
          ],
        )) as RegistroBanco[];

        if (!rows[0]) {
          throw new NotFoundException('Requisicao nao encontrada para a empresa logada.');
        }

        if (payload.itens !== undefined) {
          await this.substituirItensDaRequisicao(
            manager,
            idEmpresa,
            idRequisicao,
            payload.itens,
          );
        }

        const requisicao = await this.buscarPorIdInterno(
          manager,
          idEmpresa,
          idRequisicao,
        );

        return {
          sucesso: true,
          mensagem: 'Requisicao atualizada com sucesso.',
          requisicao,
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'atualizar');
    }
  }

  private async substituirItensDaRequisicao(
    manager: EntityManager,
    idEmpresa: number,
    idRequisicao: number,
    itens: ItemNormalizado[],
  ) {
    await manager.query(
      `
        DELETE FROM app.requisicao_itens
        WHERE id_empresa = $1
          AND id_requisicao = $2
      `,
      [String(idEmpresa), idRequisicao],
    );

    for (const item of itens) {
      await this.inserirItemRequisicao(
        manager,
        idEmpresa,
        idRequisicao,
        item,
      );
    }
  }

  private async inserirCabecalhoRequisicao(
    manager: EntityManager,
    idEmpresa: number,
    payload: PayloadCriacaoNormalizado,
  ): Promise<number> {
    try {
      const idRequisicao = await this.obterProximoId(
        manager,
        'app.requisicao_id_requisicao_seq',
        'app.requisicao',
        'id_requisicao',
        idEmpresa,
      );

      const rows = (await manager.query(
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
          RETURNING id_requisicao
        `,
        [
          idRequisicao,
          payload.idOs,
          payload.dataRequisicao,
          payload.situacao,
          payload.observacao,
          payload.usuarioAtualizacao,
          String(idEmpresa),
        ],
      )) as Array<{ id_requisicao?: string | number }>;

      const idGerado = this.converterNumero(rows[0]?.id_requisicao);
      if (!idGerado || idGerado <= 0) {
        throw new BadRequestException('Nao foi possivel obter o id da requisicao cadastrada.');
      }

      return idGerado;
    } catch (error) {
      if (!this.ehErroIdentityAlways(error)) {
        throw error;
      }
    }

    const rows = (await manager.query(
      `
        INSERT INTO app.requisicao (
          id_os,
          data_requisicao,
          situacao,
          observacao,
          usuario_atualizacao,
          id_empresa
        )
        VALUES ($1,$2,$3,$4,$5,$6)
        RETURNING id_requisicao
      `,
      [
        payload.idOs,
        payload.dataRequisicao,
        payload.situacao,
        payload.observacao,
        payload.usuarioAtualizacao,
        String(idEmpresa),
      ],
    )) as Array<{ id_requisicao?: string | number }>;

    const idGerado = this.converterNumero(rows[0]?.id_requisicao);
    if (!idGerado || idGerado <= 0) {
      throw new BadRequestException('Nao foi possivel obter o id da requisicao cadastrada.');
    }

    return idGerado;
  }

  private async inserirItemRequisicao(
    manager: EntityManager,
    idEmpresa: number,
    idRequisicao: number,
    item: ItemNormalizado,
  ) {
    try {
      const idItem = await this.obterProximoId(
        manager,
        'app.requisicao_itens_id_item_seq',
        'app.requisicao_itens',
        'id_item',
        idEmpresa,
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
      return;
    } catch (error) {
      if (!this.ehErroIdentityAlways(error)) {
        throw error;
      }
    }

    await manager.query(
      `
        INSERT INTO app.requisicao_itens (
          id_requisicao,
          id_produto,
          qtd_produto,
          valor_un,
          observacao,
          usuario_atualizacao,
          id_empresa
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7)
      `,
      [
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

  private async buscarPorIdInterno(
    manager: EntityManager,
    idEmpresa: number,
    idRequisicao: number,
  ): Promise<ListarRequisicaoDto> {
    const rows = (await manager.query(
      `
        SELECT *
        FROM app.requisicao
        WHERE id_empresa = $1
          AND id_requisicao = $2
        LIMIT 1
      `,
      [String(idEmpresa), idRequisicao],
    )) as RegistroBanco[];

    const row = rows[0];
    if (!row) {
      throw new NotFoundException('Requisicao nao encontrada para a empresa logada.');
    }

    const [requisicao] = await this.mapearRequisicoesComItens(
      manager,
      idEmpresa,
      [row],
    );

    if (!requisicao) {
      throw new NotFoundException('Requisicao nao encontrada para a empresa logada.');
    }

    return requisicao;
  }

  private async buscarRegistroPorIdOuFalhar(
    manager: EntityManager,
    idEmpresa: number,
    idRequisicao: number,
  ): Promise<RegistroBanco> {
    const rows = (await manager.query(
      `
        SELECT *
        FROM app.requisicao
        WHERE id_empresa = $1
          AND id_requisicao = $2
        LIMIT 1
      `,
      [String(idEmpresa), idRequisicao],
    )) as RegistroBanco[];

    const row = rows[0];
    if (!row) {
      throw new NotFoundException('Requisicao nao encontrada para a empresa logada.');
    }

    return row;
  }

  private async mapearRequisicoesComItens(
    manager: EntityManager,
    idEmpresa: number,
    requisicoesRows: RegistroBanco[],
  ): Promise<ListarRequisicaoDto[]> {
    if (requisicoesRows.length === 0) {
      return [];
    }

    const idsRequisicao = requisicoesRows
      .map((row) => this.converterNumero(row.id_requisicao))
      .filter((id): id is number => id !== null);

    const itensPorRequisicao = await this.carregarItensPorRequisicao(
      manager,
      idEmpresa,
      idsRequisicao,
    );

    return requisicoesRows.map((row) => {
      const idRequisicao = this.converterNumero(row.id_requisicao) ?? 0;
      const itens = itensPorRequisicao.get(idRequisicao) ?? [];
      const valorTotal = Number(
        itens.reduce((acc, item) => acc + item.valorTotalItem, 0).toFixed(2),
      );

      return {
        idRequisicao,
        idOs: this.converterNumero(row.id_os) ?? 0,
        dataRequisicao: this.converterDataIso(row.data_requisicao) ?? '',
        situacao: this.converterTexto(row.situacao)?.toUpperCase() ?? 'A',
        observacao: this.converterTexto(row.observacao),
        usuarioAtualizacao: this.converterTexto(row.usuario_atualizacao),
        criadoEm: this.converterDataIso(row.criado_em) ?? '',
        atualizadoEm: this.converterDataIso(row.atualizado_em) ?? '',
        valorTotal,
        itens,
      };
    });
  }

  private async carregarItensPorRequisicao(
    manager: EntityManager,
    idEmpresa: number,
    idsRequisicao: number[],
  ): Promise<Map<number, ListarRequisicaoItemDto[]>> {
    const resultado = new Map<number, ListarRequisicaoItemDto[]>();

    if (idsRequisicao.length === 0) {
      return resultado;
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

      const item = this.mapearItem(row);
      const lista = resultado.get(idRequisicao) ?? [];
      lista.push(item);
      resultado.set(idRequisicao, lista);
    }

    return resultado;
  }

  private mapearItem(row: RegistroBanco): ListarRequisicaoItemDto {
    const qtdProduto = this.converterNumero(row.qtd_produto) ?? 0;
    const valorUn = this.converterNumero(row.valor_un) ?? 0;

    return {
      idItem: this.converterNumero(row.id_item) ?? 0,
      idRequisicao: this.converterNumero(row.id_requisicao) ?? 0,
      idProduto: this.converterNumero(row.id_produto) ?? 0,
      descricaoProduto: this.converterTexto(row.descricao_produto),
      qtdProduto,
      valorUn,
      valorTotalItem: Number((qtdProduto * valorUn).toFixed(2)),
      observacao: this.converterTexto(row.observacao),
      usuarioAtualizacao: this.converterTexto(row.usuario_atualizacao),
      criadoEm: this.converterDataIso(row.criado_em) ?? '',
      atualizadoEm: this.converterDataIso(row.atualizado_em) ?? '',
    };
  }

  private async validarOrdemServicoExiste(
    manager: EntityManager,
    idEmpresa: number,
    idOs: number,
  ) {
    const rows = (await manager.query(
      `
        SELECT id_os
        FROM app.ordem_servico
        WHERE id_empresa = $1
          AND id_os = $2
        LIMIT 1
      `,
      [String(idEmpresa), idOs],
    )) as Array<{ id_os?: number | string }>;

    if (!rows[0]) {
      throw new BadRequestException('Ordem de servico nao encontrada para vincular a requisicao.');
    }
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
    dados: CriarRequisicaoDto,
    usuarioJwt: JwtUsuarioPayload,
  ): PayloadCriacaoNormalizado {
    const usuarioAtualizacao = this.normalizarTexto(
      dados.usuarioAtualizacao ?? usuarioJwt.email,
      'usuarioAtualizacao',
    );

    return {
      idOs: dados.idOs,
      dataRequisicao: dados.dataRequisicao
        ? this.normalizarDataHora(dados.dataRequisicao, 'dataRequisicao')
        : new Date().toISOString(),
      situacao: dados.situacao ?? 'A',
      observacao: this.normalizarTextoOpcional(dados.observacao) ?? '',
      usuarioAtualizacao,
      itens: this.normalizarItens(dados.itens, usuarioAtualizacao),
    };
  }

  private normalizarAtualizacao(
    dados: AtualizarRequisicaoDto,
    usuarioJwt: JwtUsuarioPayload,
  ): PayloadAtualizacaoNormalizado {
    const usuarioAtualizacao = this.normalizarTexto(
      dados.usuarioAtualizacao ?? usuarioJwt.email,
      'usuarioAtualizacao',
    );

    return {
      idOs: dados.idOs,
      dataRequisicao:
        dados.dataRequisicao !== undefined
          ? this.normalizarDataHora(dados.dataRequisicao, 'dataRequisicao')
          : undefined,
      situacao: dados.situacao,
      observacao:
        dados.observacao !== undefined
          ? (this.normalizarTextoOpcional(dados.observacao) ?? '')
          : undefined,
      usuarioAtualizacao,
      itens:
        dados.itens !== undefined
          ? this.normalizarItens(dados.itens, usuarioAtualizacao)
          : undefined,
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

      const observacaoNormalizada = this.normalizarTextoOpcional(item.observacao);

      return {
        idProduto,
        qtdProduto: Number(qtdProduto.toFixed(3)),
        valorUn: Number(valorUn.toFixed(4)),
        observacao: observacaoNormalizada ?? '',
        usuarioAtualizacao: item.usuarioAtualizacao
          ? this.normalizarTexto(item.usuarioAtualizacao, `item[${index}].usuarioAtualizacao`)
          : usuarioPadrao,
      };
    });
  }

  private resolverColunaOrdenacao(
    ordenarPor: FiltroRequisicaoDto['ordenarPor'],
  ): string {
    if (ordenarPor === 'id_requisicao') return 'id_requisicao';
    if (ordenarPor === 'id_os') return 'id_os';
    if (ordenarPor === 'situacao') return 'situacao';
    if (ordenarPor === 'atualizado_em') return 'atualizado_em';
    return 'data_requisicao';
  }

  private validarIntervaloDatas(
    dataDe: string | undefined,
    dataAte: string | undefined,
    campoDe: string,
    campoAte: string,
  ) {
    if (!dataDe || !dataAte) {
      return;
    }

    const inicio = new Date(dataDe);
    const fim = new Date(dataAte);
    if (Number.isNaN(inicio.getTime()) || Number.isNaN(fim.getTime())) {
      throw new BadRequestException(
        `Intervalo invalido: ${campoDe} ou ${campoAte} contem data invalida.`,
      );
    }

    if (fim < inicio) {
      throw new BadRequestException(
        `Intervalo invalido: ${campoAte} deve ser maior ou igual a ${campoDe}.`,
      );
    }
  }

  private normalizarDataHora(valor: string, campo: string): string {
    const data = new Date(valor);
    if (Number.isNaN(data.getTime())) {
      throw new BadRequestException(`${campo} invalido.`);
    }

    return data.toISOString();
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
    tabela: string,
    coluna: string,
    idEmpresa: number,
  ): Promise<number> {
    let rows: Array<{ id?: string | number }> = [];

    try {
      rows = (await manager.query(
        `SELECT nextval('${sequence}')::bigint AS id`,
      )) as Array<{ id?: string | number }>;
    } catch (error) {
      const erroPg =
        error instanceof QueryFailedError
          ? (error.driverError as { code?: string })
          : undefined;

      if (erroPg?.code !== '42P01' && erroPg?.code !== '42501') {
        throw error;
      }

      rows = (await manager.query(
        `
          SELECT COALESCE(MAX(${coluna}), 0)::bigint + 1 AS id
          FROM ${tabela}
          WHERE id_empresa = $1
        `,
        [String(idEmpresa)],
      )) as Array<{ id?: string | number }>;
    }

    const id = Number(rows[0]?.id ?? 0);
    if (!Number.isFinite(id) || id <= 0) {
      throw new BadRequestException(`Nao foi possivel gerar id para ${sequence}.`);
    }

    return id;
  }

  private ehErroIdentityAlways(error: unknown): boolean {
    if (!(error instanceof QueryFailedError)) {
      return false;
    }

    const erroPg = error.driverError as { code?: string };
    return erroPg.code === '428C9';
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
          'Ordem de servico ou produto informado nao existe para a empresa logada.',
        );
      }

      if (erroPg.code === '23514') {
        throw new BadRequestException(
          'Dados invalidos para situacao, quantidade ou valor unitario.',
        );
      }

      if (erroPg.code === '22P02' || erroPg.code === '22007') {
        throw new BadRequestException('Formato de numero ou data invalido.');
      }

      if (erroPg.code === '23502') {
        throw new BadRequestException(
          'Campos obrigatorios nao foram informados para cadastrar/atualizar a requisicao.',
        );
      }

      if (erroPg.code === '42501') {
        throw new BadRequestException(
          'Permissao insuficiente no banco (RLS/sequence). Verifique policy da empresa e grants.',
        );
      }

      if (erroPg.code === '428C9') {
        throw new BadRequestException(
          'Coluna identity GENERATED ALWAYS exige insercao sem id manual.',
        );
      }
    }

    throw new BadRequestException(
      `Nao foi possivel ${acao} requisicao neste momento.`,
    );
  }
}
