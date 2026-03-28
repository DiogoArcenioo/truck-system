import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { DataSource, EntityManager, QueryFailedError } from 'typeorm';
import { JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { configurarContextoEmpresaRls } from '../common/database/rls.util';
import { AtualizarProdutoDto } from './dto/atualizar-produto.dto';
import { CriarProdutoDto } from './dto/criar-produto.dto';
import { FiltroProdutosDto } from './dto/filtro-produtos.dto';
import { ListarProdutoDto } from './dto/listar-produto.dto';
import { SITUACAO_PRODUTO_OPCOES, TIPO_PRODUTO_OPCOES } from './produto.constants';

type RegistroBanco = Record<string, unknown>;

@Injectable()
export class ProdutoService {
  constructor(private readonly dataSource: DataSource) {}

  listarOpcoes() {
    return {
      sucesso: true,
      situacao: SITUACAO_PRODUTO_OPCOES,
      tipoProduto: TIPO_PRODUTO_OPCOES,
    };
  }

  async listarTodos(idEmpresa: number) {
    return this.executarComRls(idEmpresa, async (manager) => {
      const rows = (await manager.query(
        `
          SELECT *
          FROM app.produto
          WHERE id_empresa = $1
          ORDER BY descricao_produto ASC, id_produto ASC
        `,
        [String(idEmpresa)],
      )) as RegistroBanco[];

      const produtos = rows.map((row) => this.mapearProduto(row));

      return {
        sucesso: true,
        total: produtos.length,
        produtos,
      };
    });
  }

  async listarComFiltro(idEmpresa: number, filtro: FiltroProdutosDto) {
    return this.executarComRls(idEmpresa, async (manager) => {
      const filtros: string[] = ['id_empresa = $1'];
      const valores: Array<string | number> = [String(idEmpresa)];

      if (filtro.idProduto !== undefined) {
        valores.push(filtro.idProduto);
        filtros.push(`id_produto = $${valores.length}`);
      }

      if (filtro.descricaoProduto) {
        valores.push(`%${filtro.descricaoProduto}%`);
        filtros.push(`descricao_produto ILIKE $${valores.length}`);
      }

      if (filtro.situacao) {
        valores.push(filtro.situacao);
        filtros.push(`situacao = $${valores.length}`);
      }

      if (filtro.tipoProduto) {
        valores.push(filtro.tipoProduto);
        filtros.push(`tipo_produto = $${valores.length}`);
      }

      if (filtro.texto?.trim()) {
        valores.push(`%${filtro.texto.trim().toUpperCase()}%`);
        filtros.push(
          `(UPPER(descricao_produto) LIKE $${valores.length} OR UPPER(COALESCE(observacao, '')) LIKE $${valores.length})`,
        );
      }

      const pagina = filtro.pagina ?? 1;
      const limite = filtro.limite ?? 20;
      const offset = (pagina - 1) * limite;
      const ordem = filtro.ordem ?? 'ASC';
      const colunaOrdenacao = this.resolverColunaOrdenacao(filtro.ordenarPor);
      const whereSql = filtros.length > 0 ? `WHERE ${filtros.join(' AND ')}` : '';

      const sqlCount = `
        SELECT COUNT(1)::int AS total
        FROM app.produto
        ${whereSql}
      `;

      const sqlDados = `
        SELECT *
        FROM app.produto
        ${whereSql}
        ORDER BY ${colunaOrdenacao} ${ordem}, id_produto ASC
        LIMIT $${valores.length + 1}
        OFFSET $${valores.length + 2}
      `;

      const [countRows, rows] = await Promise.all([
        manager.query(sqlCount, valores) as Promise<Array<{ total: number }>>,
        manager.query(sqlDados, [...valores, limite, offset]) as Promise<RegistroBanco[]>,
      ]);

      const total = Number(countRows[0]?.total ?? 0);
      const produtos = rows.map((row) => this.mapearProduto(row));

      return {
        sucesso: true,
        paginaAtual: pagina,
        limite,
        total,
        totalPaginas: total > 0 ? Math.ceil(total / limite) : 0,
        produtos,
      };
    });
  }

  async buscarPorId(idEmpresa: number, idProduto: number) {
    return this.executarComRls(idEmpresa, async (manager) => {
      const rows = (await manager.query(
        `
          SELECT *
          FROM app.produto
          WHERE id_empresa = $1
            AND id_produto = $2
          LIMIT 1
        `,
        [String(idEmpresa), idProduto],
      )) as RegistroBanco[];

      const row = rows[0];
      if (!row) {
        throw new NotFoundException('Produto nao encontrado para a empresa logada.');
      }

      return {
        sucesso: true,
        produto: this.mapearProduto(row),
      };
    });
  }

  async cadastrar(
    idEmpresa: number,
    dados: CriarProdutoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    try {
      return await this.executarComRls(idEmpresa, async (manager) => {
        const payload = this.normalizarCriacao(dados, usuarioJwt);

        const rows = (await manager.query(
          `
            INSERT INTO app.produto (
              descricao_produto,
              id_grupo_produto,
              id_subgrupo,
              observacao,
              situacao,
              id_un,
              id_marca,
              usuario_atualizacao,
              tipo_produto,
              id_empresa
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            RETURNING *
          `,
          [
            payload.descricaoProduto,
            payload.idGrupoProduto,
            payload.idSubgrupo,
            payload.observacao,
            payload.situacao,
            payload.idUn,
            payload.idMarca,
            payload.usuarioAtualizacao,
            payload.tipoProduto,
            String(idEmpresa),
          ],
        )) as RegistroBanco[];

        const row = rows[0];
        if (!row) {
          throw new BadRequestException('Falha ao cadastrar produto.');
        }

        return {
          sucesso: true,
          mensagem: 'Produto cadastrado com sucesso.',
          produto: this.mapearProduto(row),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'cadastrar');
    }
  }

  async atualizar(
    idEmpresa: number,
    idProduto: number,
    dados: AtualizarProdutoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    try {
      return await this.executarComRls(idEmpresa, async (manager) => {
        const atual = await this.buscarRegistroPorIdOuFalhar(
          manager,
          idEmpresa,
          idProduto,
        );

        const payload = this.normalizarAtualizacao(dados, usuarioJwt);
        const descricaoProduto =
          payload.descricaoProduto ?? this.converterTexto(atual.descricao_produto) ?? '';
        const idGrupoProduto =
          payload.idGrupoProduto ?? this.converterNumero(atual.id_grupo_produto);
        const idSubgrupo = payload.idSubgrupo ?? this.converterNumero(atual.id_subgrupo);
        const observacao =
          payload.observacao !== undefined
            ? payload.observacao
            : this.converterTexto(atual.observacao);
        const situacao =
          payload.situacao ??
          (this.converterTexto(atual.situacao)?.trim().toUpperCase() || 'A');
        const idUn = payload.idUn ?? this.converterNumero(atual.id_un);
        const idMarca = payload.idMarca ?? this.converterNumero(atual.id_marca);
        const usuarioAtualizacao =
          payload.usuarioAtualizacao ??
          this.normalizarTexto(usuarioJwt.email, 'usuarioAtualizacao');
        const tipoProduto =
          payload.tipoProduto ??
          this.converterTexto(atual.tipo_produto)?.trim().toUpperCase() ??
          'P';

        const rows = (await manager.query(
          `
            UPDATE app.produto
            SET
              descricao_produto = $1,
              id_grupo_produto = $2,
              id_subgrupo = $3,
              observacao = $4,
              situacao = $5,
              id_un = $6,
              id_marca = $7,
              usuario_atualizacao = $8,
              tipo_produto = $9,
              atualizado_em = NOW()
            WHERE id_empresa = $10
              AND id_produto = $11
            RETURNING *
          `,
          [
            descricaoProduto,
            idGrupoProduto,
            idSubgrupo,
            observacao,
            situacao,
            idUn,
            idMarca,
            usuarioAtualizacao,
            tipoProduto,
            String(idEmpresa),
            idProduto,
          ],
        )) as RegistroBanco[];

        const row = rows[0];
        if (!row) {
          throw new NotFoundException('Produto nao encontrado para a empresa logada.');
        }

        return {
          sucesso: true,
          mensagem: 'Produto atualizado com sucesso.',
          produto: this.mapearProduto(row),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'atualizar');
    }
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

  private async buscarRegistroPorIdOuFalhar(
    manager: EntityManager,
    idEmpresa: number,
    idProduto: number,
  ): Promise<RegistroBanco> {
    const rows = (await manager.query(
      `
        SELECT *
        FROM app.produto
        WHERE id_empresa = $1
          AND id_produto = $2
        LIMIT 1
      `,
      [String(idEmpresa), idProduto],
    )) as RegistroBanco[];

    const row = rows[0];
    if (!row) {
      throw new NotFoundException('Produto nao encontrado para a empresa logada.');
    }

    return row;
  }

  private normalizarCriacao(
    dados: CriarProdutoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    return {
      descricaoProduto: this.normalizarTexto(dados.descricaoProduto, 'descricaoProduto'),
      idGrupoProduto: dados.idGrupoProduto ?? null,
      idSubgrupo: dados.idSubgrupo ?? null,
      observacao: dados.observacao?.trim() ? dados.observacao.trim().toUpperCase() : null,
      situacao: dados.situacao ?? 'A',
      idUn: dados.idUn ?? null,
      idMarca: dados.idMarca ?? null,
      usuarioAtualizacao: dados.usuarioAtualizacao?.trim()
        ? this.normalizarTexto(dados.usuarioAtualizacao, 'usuarioAtualizacao')
        : this.normalizarTexto(usuarioJwt.email, 'usuarioAtualizacao'),
      tipoProduto: dados.tipoProduto ?? 'P',
    };
  }

  private normalizarAtualizacao(
    dados: AtualizarProdutoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    return {
      descricaoProduto:
        dados.descricaoProduto !== undefined
          ? this.normalizarTexto(dados.descricaoProduto, 'descricaoProduto')
          : undefined,
      idGrupoProduto: dados.idGrupoProduto,
      idSubgrupo: dados.idSubgrupo,
      observacao:
        dados.observacao !== undefined
          ? dados.observacao.trim()
            ? dados.observacao.trim().toUpperCase()
            : null
          : undefined,
      situacao: dados.situacao,
      idUn: dados.idUn,
      idMarca: dados.idMarca,
      usuarioAtualizacao:
        dados.usuarioAtualizacao !== undefined
          ? this.normalizarTexto(dados.usuarioAtualizacao, 'usuarioAtualizacao')
          : this.normalizarTexto(usuarioJwt.email, 'usuarioAtualizacao'),
      tipoProduto: dados.tipoProduto,
    };
  }

  private resolverColunaOrdenacao(
    ordenarPor: FiltroProdutosDto['ordenarPor'],
  ): string {
    if (ordenarPor === 'id_produto') {
      return 'id_produto';
    }
    if (ordenarPor === 'data_cadastro') {
      return 'data_cadastro';
    }
    if (ordenarPor === 'atualizado_em') {
      return 'atualizado_em';
    }
    if (ordenarPor === 'situacao') {
      return 'situacao';
    }
    if (ordenarPor === 'tipo_produto') {
      return 'tipo_produto';
    }

    return 'descricao_produto';
  }

  private mapearProduto(registro: RegistroBanco): ListarProdutoDto {
    return {
      idProduto: this.converterNumero(registro.id_produto) ?? 0,
      descricaoProduto: this.converterTexto(registro.descricao_produto) ?? 'SEM DESCRICAO',
      dataCadastro: this.converterDataIso(registro.data_cadastro) ?? '',
      idGrupoProduto: this.converterNumero(registro.id_grupo_produto),
      idSubgrupo: this.converterNumero(registro.id_subgrupo),
      observacao: this.converterTexto(registro.observacao),
      situacao: (this.converterTexto(registro.situacao) ?? 'A').toUpperCase(),
      idUn: this.converterNumero(registro.id_un),
      idMarca: this.converterNumero(registro.id_marca),
      usuarioAtualizacao: this.converterTexto(registro.usuario_atualizacao),
      tipoProduto: this.converterTexto(registro.tipo_produto)?.toUpperCase() ?? null,
      atualizadoEm: this.converterDataIso(registro.atualizado_em) ?? '',
    };
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

  private normalizarTexto(valor: string, campo: string): string {
    const texto = valor.trim().toUpperCase();
    if (!texto) {
      throw new BadRequestException(`${campo} invalido.`);
    }
    return texto;
  }

  private tratarErroPersistencia(error: unknown, acao: string): never {
    if (error instanceof BadRequestException || error instanceof NotFoundException) {
      throw error;
    }

    if (error instanceof QueryFailedError) {
      const erroPg = error.driverError as {
        code?: string;
        detail?: string;
        message?: string;
      };

      if (erroPg.code === '23505') {
        throw new BadRequestException(
          'Ja existe produto com essa descricao para a empresa.',
        );
      }

      if (erroPg.code === '23503') {
        throw new BadRequestException(
          'Grupo, subgrupo, marca ou unidade informada nao existe.',
        );
      }

      if (erroPg.code === '23514') {
        throw new BadRequestException('Dados invalidos para tipo/situacao do produto.');
      }

      if (erroPg.code === '23502') {
        throw new BadRequestException(
          'Campos obrigatorios nao foram informados para cadastrar/atualizar o produto.',
        );
      }

      if (erroPg.code === '42501') {
        throw new BadRequestException(
          'Permissao insuficiente no banco (RLS/sequence). Verifique policy da empresa e grants.',
        );
      }

      if (erroPg.code === '428C9') {
        throw new BadRequestException(
          'A API tentou inserir valor em coluna identity GENERATED ALWAYS. IDs auto incremento nao devem ser enviados no INSERT.',
        );
      }

      if (erroPg.code === '42703') {
        throw new BadRequestException(
          'Erro de estrutura no banco (coluna inexistente em SQL/trigger/function).',
        );
      }

      if (erroPg.code === '25P02') {
        throw new BadRequestException(
          'Transacao abortada no banco por erro SQL anterior. Verifique triggers/funcoes e o primeiro erro no log do banco.',
        );
      }

      const detalhe = [erroPg.code, erroPg.detail ?? erroPg.message]
        .filter((parte): parte is string => Boolean(parte))
        .join(' - ');

      if (detalhe) {
        throw new BadRequestException(
          `Falha ao ${acao} produto no banco: ${detalhe}.`,
        );
      }
    }

    throw new BadRequestException(`Nao foi possivel ${acao} produto neste momento.`);
  }
}
