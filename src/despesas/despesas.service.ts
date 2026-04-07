import {
  BadRequestException,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { DataSource, EntityManager, QueryFailedError } from 'typeorm';
import { JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { configurarContextoEmpresaRls } from '../common/database/rls.util';
import { AtualizarDespesaDto } from './dto/atualizar-despesa.dto';
import { CriarDespesaDto } from './dto/criar-despesa.dto';
import { FiltroDespesasDto } from './dto/filtro-despesas.dto';

type RegistroBanco = Record<string, unknown>;

type MapaColunasDespesa = {
  idDespesa: string;
  idEmpresa: string | null;
  idVeiculo: string;
  idMotorista: string | null;
  idViagem: string | null;
  ativo: string | null;
  situacao: string | null;
  data: string;
  tipo: string;
  descricao: string | null;
  valor: string;
  kmRegistro: string | null;
  criadoEm: string | null;
  atualizadoEm: string | null;
  usuarioAtualizacao: string | null;
};

type DespesaNormalizada = {
  idDespesa: number;
  idEmpresa: number | null;
  idVeiculo: number | null;
  idMotorista: number | null;
  idViagem: number | null;
  ativo: boolean;
  data: Date;
  tipo: string;
  tipoDescricao: string;
  descricao: string | null;
  valor: number;
  kmRegistro: number | null;
  criadoEm: Date | null;
  atualizadoEm: Date | null;
  usuarioAtualizacao: string | null;
};

type DespesaPersistencia = {
  idVeiculo: number | null;
  idMotorista: number | null;
  idViagem: number | null;
  data: Date;
  tipo: string;
  descricao: string | null;
  valor: number;
  usuarioAtualizacao: string;
};

type ViagemVinculoResumo = {
  idVeiculo: number;
  idMotorista: number | null;
};

type TipoDespesaConfig = {
  codigo: string;
  descricao: string;
  aliases: string[];
};

type MapaColunasTipoDespesa = {
  codigo: string;
  descricao: string | null;
  aliases: string | null;
  ativo: string | null;
  ordem: string | null;
};

const TIPOS_DESPESA_PADRAO: TipoDespesaConfig[] = [
  {
    codigo: 'P',
    descricao: 'PEDAGIO',
    aliases: ['P', 'PEDAGIO', 'PEDAGIOS', 'M'],
  },
  {
    codigo: 'A',
    descricao: 'ALIMENTACAO',
    aliases: ['A', 'ALIMENTACAO', 'ALIMENTACAO_MOTORISTA', 'ALIMENTOS'],
  },
  {
    codigo: 'E',
    descricao: 'ESTADIA',
    aliases: ['E', 'ESTADIA', 'HOSPEDAGEM', 'H'],
  },
  {
    codigo: 'L',
    descricao: 'LAVACAO',
    aliases: ['L', 'LAVACAO', 'LAVAGEM'],
  },
  {
    codigo: 'O',
    descricao: 'OUTROS',
    aliases: ['O', 'OUTRO', 'OUTROS'],
  },
];

@Injectable()
export class DespesasService {
  private readonly logger = new Logger(DespesasService.name);

  constructor(private readonly dataSource: DataSource) {}

  async listarTodas(idEmpresa: number) {
    return this.executarComRls(idEmpresa, async (manager, colunas) => {
      const tiposDespesa = await this.carregarTiposDespesa(manager);
      const filtros: string[] = [];
      const valores: Array<string | number | boolean> = [];

      if (colunas.idEmpresa) {
        valores.push(String(idEmpresa));
        filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
      }

      if (colunas.ativo) {
        valores.push(true);
        filtros.push(`${this.quote(colunas.ativo)} = $${valores.length}`);
      } else if (colunas.situacao) {
        filtros.push(
          `UPPER(COALESCE(${this.quote(colunas.situacao)}::text, 'A')) NOT IN ('I', 'INATIVO', 'INATIVA', 'FALSE', 'F', '0')`,
        );
      }

      const sql = [
        'SELECT * FROM app.despesas',
        filtros.length > 0 ? `WHERE ${filtros.join(' AND ')}` : '',
        `ORDER BY ${this.quote(colunas.data)} DESC, ${this.quote(colunas.idDespesa)} DESC`,
      ]
        .filter(Boolean)
        .join('\n');

      const registros = (await manager.query(sql, valores)) as RegistroBanco[];
      const dados = registros.map((registro) =>
        this.mapearRegistro(registro, colunas, tiposDespesa),
      );

      return {
        sucesso: true,
        total: dados.length,
        despesas: dados,
      };
    });
  }

  async listarComFiltro(idEmpresa: number, filtro: FiltroDespesasDto) {
    this.validarIntervalosDoFiltro(filtro);

    return this.executarComRls(idEmpresa, async (manager, colunas) => {
      const tiposDespesa = await this.carregarTiposDespesa(manager);
      const filtros: string[] = [];
      const valores: Array<string | number | boolean> = [];

      if (colunas.idEmpresa) {
        valores.push(String(idEmpresa));
        filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
      }

      if (filtro.idDespesa !== undefined) {
        valores.push(filtro.idDespesa);
        filtros.push(`${this.quote(colunas.idDespesa)} = $${valores.length}`);
      }

      const situacaoFiltro = this.resolverSituacaoFiltro(filtro.situacao);
      if (situacaoFiltro !== 'TODOS') {
        if (colunas.ativo) {
          valores.push(situacaoFiltro === 'ATIVO');
          filtros.push(`${this.quote(colunas.ativo)} = $${valores.length}`);
        } else if (colunas.situacao) {
          const colunaSituacao = this.quote(colunas.situacao);
          if (situacaoFiltro === 'ATIVO') {
            filtros.push(
              `UPPER(COALESCE(${colunaSituacao}::text, 'A')) NOT IN ('I', 'INATIVO', 'INATIVA', 'FALSE', 'F', '0')`,
            );
          } else {
            filtros.push(
              `UPPER(COALESCE(${colunaSituacao}::text, 'A')) IN ('I', 'INATIVO', 'INATIVA', 'FALSE', 'F', '0')`,
            );
          }
        }
      }

      if (filtro.idVeiculo !== undefined) {
        valores.push(filtro.idVeiculo);
        filtros.push(`${this.quote(colunas.idVeiculo)} = $${valores.length}`);
      }

      if (filtro.idMotorista !== undefined && colunas.idMotorista) {
        valores.push(filtro.idMotorista);
        filtros.push(`${this.quote(colunas.idMotorista)} = $${valores.length}`);
      }

      if (filtro.idViagem !== undefined && colunas.idViagem) {
        valores.push(filtro.idViagem);
        filtros.push(`${this.quote(colunas.idViagem)} = $${valores.length}`);
      }

      if (filtro.tipo) {
        const tiposPesquisa = this.resolverValoresBuscaTipo(
          filtro.tipo,
          tiposDespesa,
        );
        if (tiposPesquisa.length > 0) {
          const filtrosTipo: string[] = [];
          for (const tipo of tiposPesquisa) {
            valores.push(tipo);
            filtrosTipo.push(
              `UPPER(COALESCE(${this.quote(colunas.tipo)}::text, '')) = UPPER($${valores.length})`,
            );
          }
          filtros.push(`(${filtrosTipo.join(' OR ')})`);
        }
      }

      if (filtro.texto) {
        valores.push(`%${filtro.texto}%`);
        const colunaTexto = this.quote(colunas.descricao ?? colunas.tipo);
        filtros.push(
          `COALESCE(${colunaTexto}::text, '') ILIKE $${valores.length}`,
        );
      }

      if (filtro.dataDe) {
        valores.push(filtro.dataDe);
        filtros.push(`${this.quote(colunas.data)} >= $${valores.length}`);
      }

      if (filtro.dataAte) {
        valores.push(filtro.dataAte);
        filtros.push(`${this.quote(colunas.data)} <= $${valores.length}`);
      }

      if (filtro.valorMin !== undefined) {
        valores.push(filtro.valorMin);
        filtros.push(`${this.quote(colunas.valor)} >= $${valores.length}`);
      }

      if (filtro.valorMax !== undefined) {
        valores.push(filtro.valorMax);
        filtros.push(`${this.quote(colunas.valor)} <= $${valores.length}`);
      }

      if (filtro.kmMin !== undefined && colunas.kmRegistro) {
        valores.push(filtro.kmMin);
        filtros.push(`${this.quote(colunas.kmRegistro)} >= $${valores.length}`);
      }

      if (filtro.kmMax !== undefined && colunas.kmRegistro) {
        valores.push(filtro.kmMax);
        filtros.push(`${this.quote(colunas.kmRegistro)} <= $${valores.length}`);
      }

      const whereSql =
        filtros.length > 0 ? `WHERE ${filtros.join(' AND ')}` : '';

      const pagina = filtro.pagina ?? 1;
      const limite = filtro.limite ?? 20;
      const offset = (pagina - 1) * limite;
      const ordem = filtro.ordem ?? 'DESC';
      const colunaOrdenacao = this.resolverColunaOrdenacao(
        filtro.ordenarPor,
        colunas,
      );

      const sqlCount = `
        SELECT COUNT(1)::int AS total
        FROM app.despesas
        ${whereSql}
      `;

      const sqlDados = `
        SELECT *
        FROM app.despesas
        ${whereSql}
        ORDER BY ${this.quote(colunaOrdenacao)} ${ordem}, ${this.quote(colunas.idDespesa)} DESC
        LIMIT $${valores.length + 1}
        OFFSET $${valores.length + 2}
      `;

      const countRows = (await manager.query(sqlCount, valores)) as Array<{
        total: number;
      }>;
      const registros = (await manager.query(sqlDados, [
        ...valores,
        limite,
        offset,
      ])) as RegistroBanco[];

      const total = Number(countRows[0]?.total ?? 0);
      const dados = registros.map((registro) =>
        this.mapearRegistro(registro, colunas, tiposDespesa),
      );

      return {
        sucesso: true,
        paginaAtual: pagina,
        limite,
        total,
        totalPaginas: total > 0 ? Math.ceil(total / limite) : 0,
        despesas: dados,
      };
    });
  }

  async listarTipos(idEmpresa: number) {
    return this.executarComRls(idEmpresa, async (manager) => {
      const tipos = await this.carregarTiposDespesa(manager);
      return {
        sucesso: true,
        atalhoCadastroDespesa: true,
        total: tipos.length,
        tipos: tipos.map((tipo) => ({
          codigo: tipo.codigo,
          descricao: tipo.descricao,
        })),
      };
    });
  }

  async buscarPorId(idEmpresa: number, idDespesa: number) {
    return this.executarComRls(idEmpresa, async (manager, colunas) => {
      const tiposDespesa = await this.carregarTiposDespesa(manager);
      const registro = await this.buscarRegistroPorIdOuFalhar(
        manager,
        colunas,
        idEmpresa,
        idDespesa,
      );

      return {
        sucesso: true,
        despesa: this.mapearRegistro(registro, colunas, tiposDespesa),
      };
    });
  }

  async cadastrar(
    idEmpresa: number,
    dados: CriarDespesaDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    try {
      return this.executarComRls(idEmpresa, async (manager, colunas) => {
        const tiposDespesa = await this.carregarTiposDespesa(manager);
        const codigosTipoPermitidos = await this.carregarCodigosTipoPermitidos(
          manager,
          colunas.tipo,
        );
        const payloadBase = this.normalizarCriacao(
          dados,
          usuarioJwt,
          tiposDespesa,
          codigosTipoPermitidos,
        );
        const payload = await this.resolverVinculosDespesa(
          manager,
          colunas,
          idEmpresa,
          payloadBase,
        );
        this.validarConsistencia(payload);

        const campos = [colunas.idVeiculo, colunas.data, colunas.tipo, colunas.valor];
        const valores: Array<string | number | Date | null> = [
          payload.idVeiculo,
          payload.data,
          payload.tipo,
          payload.valor,
        ];

        if (colunas.idMotorista) {
          campos.push(colunas.idMotorista);
          valores.push(payload.idMotorista);
        }

        if (colunas.idViagem) {
          campos.push(colunas.idViagem);
          valores.push(payload.idViagem);
        }

        if (colunas.descricao) {
          campos.push(colunas.descricao);
          valores.push(payload.descricao);
        }

        if (colunas.usuarioAtualizacao) {
          campos.push(colunas.usuarioAtualizacao);
          valores.push(payload.usuarioAtualizacao);
        }

        if (colunas.idEmpresa) {
          campos.push(colunas.idEmpresa);
          valores.push(String(idEmpresa));
        }

        if (colunas.situacao) {
          campos.push(colunas.situacao);
          valores.push('A');
        }

        const sql = `
          INSERT INTO app.despesas (${campos.map((campo) => this.quote(campo)).join(', ')})
          VALUES (${valores.map((_, index) => `$${index + 1}`).join(', ')})
          RETURNING *
        `;

        const rows = (await manager.query(sql, valores)) as RegistroBanco[];
        const registro = rows[0];
        if (!registro) {
          throw new BadRequestException(
            'Falha ao cadastrar despesa (retorno vazio da base).',
          );
        }

        const despesa = this.mapearRegistro(registro, colunas, tiposDespesa);
        if (despesa.idViagem !== null) {
          await this.recalcularTotaisViagem(
            manager,
            idEmpresa,
            despesa.idViagem,
            colunas,
          );
        }

        return {
          sucesso: true,
          mensagem: 'Despesa cadastrada com sucesso.',
          despesa,
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'cadastrar');
    }
  }

  async atualizar(
    idEmpresa: number,
    idDespesa: number,
    dados: AtualizarDespesaDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    try {
      return this.executarComRls(idEmpresa, async (manager, colunas) => {
        const tiposDespesa = await this.carregarTiposDespesa(manager);
        const codigosTipoPermitidos = await this.carregarCodigosTipoPermitidos(
          manager,
          colunas.tipo,
        );
        const payload = this.normalizarAtualizacao(
          dados,
          usuarioJwt,
          tiposDespesa,
          codigosTipoPermitidos,
        );
        const registroAtual = await this.buscarRegistroPorIdOuFalhar(
          manager,
          colunas,
          idEmpresa,
          idDespesa,
        );
        const atual = this.mapearRegistro(registroAtual, colunas, tiposDespesa);

        const payloadResolvido = await this.resolverVinculosDespesa(
          manager,
          colunas,
          idEmpresa,
          {
            idVeiculo:
              payload.idVeiculo !== undefined ? payload.idVeiculo : atual.idVeiculo,
            idMotorista:
              payload.idMotorista !== undefined
                ? payload.idMotorista
                : atual.idMotorista,
            idViagem:
              payload.idViagem !== undefined ? payload.idViagem : atual.idViagem,
            data: payload.data ?? atual.data,
            tipo: payload.tipo ?? atual.tipo,
            descricao:
              this.normalizarDescricaoPersistencia(
                payload.descricao !== undefined
                  ? payload.descricao
                  : atual.descricao,
              ) ?? null,
            valor: payload.valor ?? atual.valor,
            usuarioAtualizacao:
              payload.usuarioAtualizacao ?? this.normalizarUsuario(usuarioJwt.email),
          },
        );

        const idVeiculo = payloadResolvido.idVeiculo;
        const idMotorista = payloadResolvido.idMotorista;
        const idViagem = payloadResolvido.idViagem;
        const data = payload.data ?? atual.data;
        const tipo = payload.tipo ?? atual.tipo;
        const descricao =
          this.normalizarDescricaoPersistencia(
            payload.descricao !== undefined
              ? payload.descricao
              : atual.descricao,
          ) ?? null;
        const valor = payload.valor ?? atual.valor;
        const usuarioAtualizacao =
          payload.usuarioAtualizacao ?? this.normalizarUsuario(usuarioJwt.email);

        this.validarConsistencia({
          idVeiculo,
          idMotorista,
          idViagem,
          data,
          tipo,
          descricao,
          valor,
          usuarioAtualizacao,
        });

        const sets: string[] = [];
        const valores: Array<string | number | Date | null> = [];

        const adicionarSet = (
          coluna: string,
          valorSet: string | number | Date | null,
        ) => {
          valores.push(valorSet);
          sets.push(`${this.quote(coluna)} = $${valores.length}`);
        };

        adicionarSet(colunas.idVeiculo, idVeiculo);
        adicionarSet(colunas.data, data);
        adicionarSet(colunas.tipo, tipo);
        adicionarSet(colunas.valor, valor);

        if (colunas.idMotorista) {
          adicionarSet(colunas.idMotorista, idMotorista);
        }

        if (colunas.idViagem) {
          adicionarSet(colunas.idViagem, idViagem);
        }

        if (colunas.descricao) {
          adicionarSet(colunas.descricao, descricao);
        }

        if (colunas.usuarioAtualizacao) {
          adicionarSet(colunas.usuarioAtualizacao, usuarioAtualizacao);
        }

        valores.push(idDespesa);
        const filtros: string[] = [
          `${this.quote(colunas.idDespesa)} = $${valores.length}`,
        ];

        if (colunas.idEmpresa) {
          valores.push(String(idEmpresa));
          filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
        }

        const sql = `
          UPDATE app.despesas
          SET ${sets.join(', ')}
          WHERE ${filtros.join(' AND ')}
          RETURNING *
        `;

        const rows = (await manager.query(sql, valores)) as RegistroBanco[];
        const registroAtualizado = rows[0];
        if (!registroAtualizado) {
          throw new NotFoundException(
            'Despesa nao encontrada para a empresa logada.',
          );
        }

        const despesaAtualizada = this.mapearRegistro(
          registroAtualizado,
          colunas,
          tiposDespesa,
        );
        await this.recalcularViagensImpactadas(
          manager,
          idEmpresa,
          atual.idViagem,
          despesaAtualizada.idViagem,
          colunas,
        );

        return {
          sucesso: true,
          mensagem: 'Despesa atualizada com sucesso.',
          despesa: despesaAtualizada,
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'atualizar');
    }
  }

  async remover(idEmpresa: number, idDespesa: number) {
    return this.executarComRls(idEmpresa, async (manager, colunas) => {
      const tiposDespesa = await this.carregarTiposDespesa(manager);
      const filtros: string[] = [];
      const valores: Array<string | number> = [];

      valores.push(idDespesa);
      filtros.push(`${this.quote(colunas.idDespesa)} = $${valores.length}`);

      if (colunas.idEmpresa) {
        valores.push(String(idEmpresa));
        filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
      }

      const sql = colunas.ativo
        ? `
        UPDATE app.despesas
        SET ${this.quote(colunas.ativo)} = false
        WHERE ${filtros.join(' AND ')}
          AND ${this.quote(colunas.ativo)} = true
        RETURNING *
      `
        : colunas.situacao
          ? `
        UPDATE app.despesas
        SET ${this.quote(colunas.situacao)} = 'I'
        WHERE ${filtros.join(' AND ')}
          AND UPPER(COALESCE(${this.quote(colunas.situacao)}::text, 'A')) NOT IN ('I', 'INATIVO', 'INATIVA', 'FALSE', 'F', '0')
        RETURNING *
      `
        : `
        DELETE FROM app.despesas
        WHERE ${filtros.join(' AND ')}
        RETURNING *
      `;

      const removidos = (await manager.query(sql, valores)) as RegistroBanco[];
      const removido = removidos[0];
      if (!removido) {
        throw new NotFoundException(
          'Despesa nao encontrada para a empresa logada.',
        );
      }

      const despesaRemovida = this.mapearRegistro(
        removido,
        colunas,
        tiposDespesa,
      );
      if (despesaRemovida.idViagem !== null) {
        await this.recalcularTotaisViagem(
          manager,
          idEmpresa,
          despesaRemovida.idViagem,
          colunas,
        );
      }

      return {
        sucesso: true,
        mensagem: colunas.ativo || colunas.situacao
          ? 'Despesa inativada com sucesso.'
          : 'Despesa removida com sucesso.',
        idDespesa,
      };
    });
  }

  private async executarComRls<T>(
    idEmpresa: number,
    callback: (
      manager: EntityManager,
      colunas: MapaColunasDespesa,
    ) => Promise<T>,
  ): Promise<T> {
    return this.dataSource.manager.transaction(async (manager) => {
      await configurarContextoEmpresaRls(manager, idEmpresa);
      const colunas = await this.carregarMapaColunas(manager);
      return callback(manager, colunas);
    });
  }

  private async carregarMapaColunas(
    manager: EntityManager,
  ): Promise<MapaColunasDespesa> {
    const rows = (await manager.query(`
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = 'app'
        AND table_name = 'despesas'
    `)) as Array<{ column_name?: string }>;

    if (rows.length === 0) {
      throw new BadRequestException('Tabela app.despesas nao encontrada.');
    }

    const set = new Set(
      rows
        .map((row) => (typeof row.column_name === 'string' ? row.column_name : ''))
        .filter((value) => value.length > 0),
    );

    return {
      idDespesa: this.encontrarColuna(
        set,
        ['id_despesa', 'id'],
        'id da despesa',
      )!,
      idEmpresa: this.encontrarColuna(set, ['id_empresa'], 'id da empresa', false),
      idVeiculo: this.encontrarColuna(
        set,
        ['id_veiculo', 'veiculo_id'],
        'id do veiculo',
      )!,
      idMotorista: this.encontrarColuna(
        set,
        ['id_motorista', 'motorista_id'],
        'id do motorista',
        false,
      ),
      idViagem: this.encontrarColuna(
        set,
        ['id_viagem', 'viagem_id'],
        'id da viagem',
        false,
      ),
      ativo: this.encontrarColuna(
        set,
        ['ativo'],
        'situacao ativa',
        false,
      ),
      situacao: this.encontrarColuna(
        set,
        ['situacao', 'status'],
        'situacao',
        false,
      ),
      data: this.encontrarColuna(
        set,
        ['data', 'data_despesa', 'data_lancamento'],
        'data',
      )!,
      tipo: this.encontrarColuna(set, ['tipo', 'tipo_despesa'], 'tipo')!,
      descricao: this.encontrarColuna(
        set,
        ['descricao', 'observacao', 'obs'],
        'descricao',
        false,
      ),
      valor: this.encontrarColuna(
        set,
        ['valor', 'valor_total', 'total'],
        'valor',
      )!,
      kmRegistro: this.encontrarColuna(
        set,
        ['km_registro', 'km', 'km_atual'],
        'km de registro',
        false,
      ),
      criadoEm: this.encontrarColuna(set, ['criado_em', 'created_at'], '', false),
      atualizadoEm: this.encontrarColuna(
        set,
        ['atualizado_em', 'updated_at'],
        '',
        false,
      ),
      usuarioAtualizacao: this.encontrarColuna(
        set,
        ['usuario_atualizacao', 'usuario_update'],
        '',
        false,
      ),
    };
  }

  private async carregarTiposDespesa(
    manager: EntityManager,
  ): Promise<TipoDespesaConfig[]> {
    const colunas = await this.carregarMapaColunasTipoDespesa(manager);
    let rows: Array<{ codigo?: unknown; descricao?: unknown; aliases?: unknown }> = [];

    try {
      const whereAtivo = colunas.ativo
        ? `WHERE COALESCE(${this.quote(colunas.ativo)}, true) = true`
        : '';
      const orderSql = colunas.ordem
        ? `ORDER BY ${this.quote(colunas.ordem)} ASC, ${this.quote(colunas.codigo)} ASC`
        : `ORDER BY ${this.quote(colunas.codigo)} ASC`;
      const descricaoSql = colunas.descricao
        ? this.quote(colunas.descricao)
        : `${this.quote(colunas.codigo)}::text`;
      const aliasesSql = colunas.aliases
        ? this.quote(colunas.aliases)
        : 'NULL::text[]';

      rows = (await manager.query(
        `
        SELECT
          ${this.quote(colunas.codigo)} AS codigo,
          ${descricaoSql} AS descricao,
          ${aliasesSql} AS aliases
        FROM app.tipo_despesas
        ${whereAtivo}
        ${orderSql}
      `,
      )) as Array<{ codigo?: unknown; descricao?: unknown; aliases?: unknown }>;
    } catch (error) {
      if (error instanceof QueryFailedError) {
        const erroPg = error.driverError as { code?: string };
        if (erroPg.code === '42P01') {
          this.logger.warn(
            'Tabela app.tipo_despesas nao encontrada. Aplicando fallback de tipos padrao para despesas.',
          );
          rows = [];
        } else {
          throw error;
        }
      } else {
        throw error;
      }
    }

    const tiposCarregados: TipoDespesaConfig[] = [];
    for (const row of rows) {
      const codigoRaw = this.converterTexto(row.codigo);
      if (!codigoRaw) {
        continue;
      }

      const codigo = this.normalizarTextoBusca(codigoRaw);
      if (!codigo) {
        continue;
      }

      const descricao = this.converterTexto(row.descricao) ?? codigo;
      const aliases = this.normalizarAliasesTipo(row.aliases, codigo, descricao);
      tiposCarregados.push({ codigo, descricao, aliases });
    }

    const tipos = this.mesclarTiposDespesaComPadrao(tiposCarregados);
    if (tipos.length > 0) {
      return tipos;
    }

    throw new BadRequestException(
      'Tabela app.tipo_despesas sem tipos de despesa ativos.',
    );
  }

  private async carregarMapaColunasTipoDespesa(
    manager: EntityManager,
  ): Promise<MapaColunasTipoDespesa> {
    const rows = (await manager.query(`
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = 'app'
        AND table_name = 'tipo_despesas'
    `)) as Array<{ column_name?: string }>;

    if (rows.length === 0) {
      return {
        codigo: 'codigo',
        descricao: 'descricao',
        aliases: 'aliases',
        ativo: 'ativo',
        ordem: 'ordem',
      };
    }

    const set = new Set(
      rows
        .map((row) => (typeof row.column_name === 'string' ? row.column_name : ''))
        .filter((value) => value.length > 0),
    );

    return {
      codigo: this.encontrarColuna(set, ['codigo'], 'codigo do tipo')!,
      descricao: this.encontrarColuna(set, ['descricao'], 'descricao', false),
      aliases: this.encontrarColuna(set, ['aliases'], 'aliases', false),
      ativo: this.encontrarColuna(set, ['ativo'], 'ativo', false),
      ordem: this.encontrarColuna(set, ['ordem'], 'ordem', false),
    };
  }

  private async carregarCodigosTipoPermitidos(
    manager: EntityManager,
    colunaTipo: string,
  ): Promise<Set<string> | null> {
    const dominioRows = (await manager.query(
      `
      SELECT domain_name
      FROM information_schema.columns
      WHERE table_schema = 'app'
        AND table_name = 'despesas'
        AND column_name = $1
      LIMIT 1
    `,
      [colunaTipo],
    )) as Array<{ domain_name?: unknown }>;

    const nomeDominio = this.converterTexto(dominioRows[0]?.domain_name);
    if (!nomeDominio) {
      return null;
    }

    const restricoes = (await manager.query(
      `
      SELECT pg_get_constraintdef(c.oid) AS definicao
      FROM pg_constraint c
      JOIN pg_type t ON t.oid = c.contypid
      JOIN pg_namespace n ON n.oid = t.typnamespace
      WHERE n.nspname = 'app'
        AND t.typname = $1
        AND c.contype = 'c'
    `,
      [nomeDominio],
    )) as Array<{ definicao?: unknown }>;

    if (restricoes.length === 0) {
      return null;
    }

    const codigos = new Set<string>();
    for (const item of restricoes) {
      const definicao = this.converterTexto(item.definicao);
      if (!definicao) {
        continue;
      }

      const matches = definicao.matchAll(/'([^']+)'::/g);
      for (const match of matches) {
        const codigo = this.normalizarTextoBusca(match[1]);
        if (codigo.length > 0) {
          codigos.add(codigo);
        }
      }
    }

    return codigos.size > 0 ? codigos : null;
  }

  private normalizarAliasesTipo(
    aliasesOriginais: unknown,
    codigo: string,
    descricao: string,
  ) {
    const aliases: string[] = [codigo, descricao];

    if (Array.isArray(aliasesOriginais)) {
      for (const item of aliasesOriginais) {
        if (typeof item === 'string') {
          aliases.push(item);
        }
      }
    } else if (typeof aliasesOriginais === 'string' && aliasesOriginais.trim()) {
      aliases.push(aliasesOriginais);
    }

    return Array.from(
      new Set(
        aliases
          .map((item) => this.normalizarTextoBusca(item))
          .filter((item) => item.length > 0),
      ),
    );
  }

  private mesclarTiposDespesaComPadrao(tiposCarregados: TipoDespesaConfig[]) {
    const mapaOrdemPadrao = new Map<string, number>();
    TIPOS_DESPESA_PADRAO.forEach((tipo, index) => {
      mapaOrdemPadrao.set(this.normalizarTextoBusca(tipo.codigo), index);
    });

    const mapaTipos = new Map<string, TipoDespesaConfig>();
    const upsert = (tipo: TipoDespesaConfig, priorizarDescricaoNova: boolean) => {
      const codigo = this.normalizarTextoBusca(tipo.codigo);
      if (!codigo) {
        return;
      }

      const descricaoNova = this.converterTexto(tipo.descricao) ?? codigo;
      const aliasesNovos = this.normalizarAliasesTipo(
        tipo.aliases,
        codigo,
        descricaoNova,
      );

      const atual = mapaTipos.get(codigo);
      if (!atual) {
        mapaTipos.set(codigo, {
          codigo,
          descricao: descricaoNova,
          aliases: aliasesNovos,
        });
        return;
      }

      const descricaoFinal = priorizarDescricaoNova ? descricaoNova : atual.descricao;
      const aliasesFinal = this.normalizarAliasesTipo(
        [...atual.aliases, ...aliasesNovos],
        codigo,
        descricaoFinal,
      );
      mapaTipos.set(codigo, {
        codigo,
        descricao: descricaoFinal,
        aliases: aliasesFinal,
      });
    };

    for (const tipoPadrao of TIPOS_DESPESA_PADRAO) {
      upsert(tipoPadrao, false);
    }

    for (const tipoCarregado of tiposCarregados) {
      upsert(tipoCarregado, true);
    }

    return Array.from(mapaTipos.values()).sort((a, b) => {
      const ordemA = mapaOrdemPadrao.get(a.codigo) ?? Number.MAX_SAFE_INTEGER;
      const ordemB = mapaOrdemPadrao.get(b.codigo) ?? Number.MAX_SAFE_INTEGER;
      if (ordemA !== ordemB) {
        return ordemA - ordemB;
      }
      return a.codigo.localeCompare(b.codigo);
    });
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
        `Estrutura da tabela app.despesas invalida: coluna de ${descricao} nao encontrada.`,
      );
    }

    return null;
  }

  private async buscarRegistroPorIdOuFalhar(
    manager: EntityManager,
    colunas: MapaColunasDespesa,
    idEmpresa: number,
    idDespesa: number,
  ): Promise<RegistroBanco> {
    const filtros: string[] = [];
    const valores: Array<string | number> = [];

    valores.push(idDespesa);
    filtros.push(`${this.quote(colunas.idDespesa)} = $${valores.length}`);

    if (colunas.idEmpresa) {
      valores.push(String(idEmpresa));
      filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
    }

    const sql = `
      SELECT *
      FROM app.despesas
      WHERE ${filtros.join(' AND ')}
      LIMIT 1
    `;

    const rows = (await manager.query(sql, valores)) as RegistroBanco[];
    const registro = rows[0];
    if (!registro) {
      throw new NotFoundException('Despesa nao encontrada para a empresa logada.');
    }

    return registro;
  }

  private normalizarCriacao(
    dados: CriarDespesaDto,
    usuarioJwt: JwtUsuarioPayload,
    tiposDespesa: TipoDespesaConfig[],
    codigosTipoPermitidos: Set<string> | null,
  ): DespesaPersistencia {
    return {
      idVeiculo: dados.idVeiculo ?? null,
      idMotorista: dados.idMotorista ?? null,
      idViagem: dados.idViagem ?? null,
      data: new Date(dados.data),
      tipo: this.normalizarTipoPersistencia(
        dados.tipo,
        tiposDespesa,
        codigosTipoPermitidos,
      ),
      descricao: this.normalizarDescricaoPersistencia(dados.descricao) ?? null,
      valor: dados.valor,
      usuarioAtualizacao: dados.usuarioAtualizacao?.trim()
        ? this.normalizarUsuario(dados.usuarioAtualizacao)
        : this.normalizarUsuario(usuarioJwt.email),
    };
  }

  private normalizarAtualizacao(
    dados: AtualizarDespesaDto,
    usuarioJwt: JwtUsuarioPayload,
    tiposDespesa: TipoDespesaConfig[],
    codigosTipoPermitidos: Set<string> | null,
  ) {
    const idViagemNormalizada = dados.idViagem === null ? null : dados.idViagem;
    const idVeiculoNormalizado = dados.idVeiculo === null ? null : dados.idVeiculo;
    const idMotoristaNormalizado =
      dados.idMotorista === null ? null : dados.idMotorista;
    return {
      idVeiculo:
        dados.idVeiculo !== undefined ? idVeiculoNormalizado : undefined,
      idMotorista:
        dados.idMotorista !== undefined ? idMotoristaNormalizado : undefined,
      idViagem:
        dados.idViagem !== undefined ? idViagemNormalizada : undefined,
      data: dados.data !== undefined ? new Date(dados.data) : undefined,
      tipo:
        dados.tipo !== undefined
          ? this.normalizarTipoPersistencia(
              dados.tipo,
              tiposDespesa,
              codigosTipoPermitidos,
            )
          : undefined,
      descricao: this.normalizarDescricaoPersistencia(dados.descricao),
      valor: dados.valor,
      usuarioAtualizacao:
        dados.usuarioAtualizacao !== undefined
          ? this.normalizarUsuario(dados.usuarioAtualizacao)
          : this.normalizarUsuario(usuarioJwt.email),
    };
  }

  private validarConsistencia(payload: {
    idVeiculo: number | null;
    idMotorista: number | null;
    idViagem: number | null;
    data: Date;
    tipo: string;
    descricao: string | null;
    valor: number;
    usuarioAtualizacao: string;
  }) {
    if (
      payload.idVeiculo !== null &&
      (!Number.isFinite(payload.idVeiculo) || payload.idVeiculo <= 0)
    ) {
      throw new BadRequestException('idVeiculo invalido.');
    }

    if (
      payload.idMotorista !== null &&
      (!Number.isFinite(payload.idMotorista) || payload.idMotorista <= 0)
    ) {
      throw new BadRequestException('idMotorista invalido.');
    }

    if (payload.idVeiculo === null && payload.idMotorista === null) {
      throw new BadRequestException(
        'Informe pelo menos um vinculo: veiculo, motorista ou viagem.',
      );
    }

    if (
      payload.idViagem !== null &&
      (!Number.isFinite(payload.idViagem) || payload.idViagem <= 0)
    ) {
      throw new BadRequestException('idViagem invalido.');
    }

    if (!(payload.data instanceof Date) || Number.isNaN(payload.data.getTime())) {
      throw new BadRequestException('data invalida.');
    }

    if (!payload.tipo || payload.tipo.length === 0) {
      throw new BadRequestException('tipo da despesa invalido.');
    }

    if (!Number.isFinite(payload.valor) || payload.valor < 0) {
      throw new BadRequestException('valor da despesa invalido.');
    }

    if (!payload.usuarioAtualizacao || payload.usuarioAtualizacao.length < 2) {
      throw new BadRequestException('usuarioAtualizacao invalido.');
    }
  }

  private validarIntervalosDoFiltro(filtro: FiltroDespesasDto) {
    if (
      filtro.dataDe &&
      filtro.dataAte &&
      new Date(filtro.dataAte) < new Date(filtro.dataDe)
    ) {
      throw new BadRequestException(
        'Filtro invalido: dataAte deve ser maior ou igual a dataDe.',
      );
    }

    if (
      filtro.valorMin !== undefined &&
      filtro.valorMax !== undefined &&
      filtro.valorMax < filtro.valorMin
    ) {
      throw new BadRequestException(
        'Filtro invalido: valorMax deve ser maior ou igual a valorMin.',
      );
    }

    if (
      filtro.kmMin !== undefined &&
      filtro.kmMax !== undefined &&
      filtro.kmMax < filtro.kmMin
    ) {
      throw new BadRequestException(
        'Filtro invalido: kmMax deve ser maior ou igual a kmMin.',
      );
    }
  }

  private resolverSituacaoFiltro(
    situacao?: FiltroDespesasDto['situacao'],
  ): 'ATIVO' | 'INATIVO' | 'TODOS' {
    if (situacao === 'ATIVO' || situacao === 'INATIVO' || situacao === 'TODOS') {
      return situacao;
    }
    return 'ATIVO';
  }

  private resolverColunaOrdenacao(
    ordenarPor: FiltroDespesasDto['ordenarPor'],
    colunas: MapaColunasDespesa,
  ) {
    if (ordenarPor === 'id_despesa') return colunas.idDespesa;
    if (ordenarPor === 'id_veiculo') return colunas.idVeiculo;
    if (ordenarPor === 'id_motorista' && colunas.idMotorista) return colunas.idMotorista;
    if (ordenarPor === 'id_viagem' && colunas.idViagem) return colunas.idViagem;
    if (ordenarPor === 'tipo') return colunas.tipo;
    if (ordenarPor === 'valor') return colunas.valor;
    if (ordenarPor === 'km_registro' && colunas.kmRegistro) return colunas.kmRegistro;
    if (ordenarPor === 'criado_em' && colunas.criadoEm) return colunas.criadoEm;
    if (ordenarPor === 'atualizado_em' && colunas.atualizadoEm) {
      return colunas.atualizadoEm;
    }
    return colunas.data;
  }

  private mapearRegistro(
    registro: RegistroBanco,
    colunas: MapaColunasDespesa,
    tiposDespesa: TipoDespesaConfig[],
  ): DespesaNormalizada {
    const tipoRaw = this.converterTexto(registro[colunas.tipo]) ?? 'O';
    const tipo = this.normalizarTipoRetorno(tipoRaw, tiposDespesa);
    return {
      idDespesa: this.converterNumero(registro[colunas.idDespesa]) ?? 0,
      idEmpresa:
        colunas.idEmpresa !== null
          ? this.converterNumero(registro[colunas.idEmpresa])
          : null,
      idVeiculo: this.converterNumero(registro[colunas.idVeiculo]),
      idMotorista:
        colunas.idMotorista !== null
          ? this.converterNumero(registro[colunas.idMotorista])
          : null,
      idViagem:
        colunas.idViagem !== null
          ? this.converterNumero(registro[colunas.idViagem])
          : null,
      ativo:
        colunas.ativo !== null
          ? this.converterBooleano(registro[colunas.ativo]) ?? true
          : colunas.situacao !== null
            ? !this.situacaoTextoEhInativa(
                this.converterTexto(registro[colunas.situacao]),
              )
            : true,
      data: this.converterData(registro[colunas.data]) ?? new Date(0),
      tipo,
      tipoDescricao: this.descreverTipo(tipo, tipoRaw, tiposDespesa),
      descricao:
        colunas.descricao !== null
          ? this.converterTexto(registro[colunas.descricao])
          : null,
      valor: this.converterNumero(registro[colunas.valor]) ?? 0,
      kmRegistro:
        colunas.kmRegistro !== null
          ? this.converterNumero(registro[colunas.kmRegistro])
          : null,
      criadoEm:
        colunas.criadoEm !== null
          ? this.converterData(registro[colunas.criadoEm])
          : null,
      atualizadoEm:
        colunas.atualizadoEm !== null
          ? this.converterData(registro[colunas.atualizadoEm])
          : null,
      usuarioAtualizacao:
        colunas.usuarioAtualizacao !== null
          ? this.converterTexto(registro[colunas.usuarioAtualizacao])
          : null,
    };
  }

  private resolverValoresBuscaTipo(
    valor: string,
    tiposDespesa: TipoDespesaConfig[],
  ) {
    const chave = this.normalizarTextoBusca(valor);
    if (!chave) {
      return [];
    }

    const tipo = this.buscarTipoPorValor(chave, tiposDespesa);
    if (tipo) {
      const termos = new Set<string>();
      termos.add(tipo.codigo);
      for (const alias of tipo.aliases) {
        termos.add(this.normalizarTextoBusca(alias));
      }
      return Array.from(termos);
    }

    if (chave.length === 1) {
      return [chave];
    }

    return [valor.trim()];
  }

  private normalizarDescricaoPersistencia(valor?: string | null) {
    if (valor === undefined) {
      return undefined;
    }

    if (valor === null) {
      return null;
    }

    const texto = valor.trim();
    if (!texto) {
      return null;
    }

    return texto.toUpperCase();
  }

  private normalizarTipoPersistencia(
    valor: string,
    tiposDespesa: TipoDespesaConfig[],
    codigosTipoPermitidos: Set<string> | null,
  ) {
    const tipo = this.buscarTipoPorValor(valor, tiposDespesa);
    if (tipo) {
      return this.resolverCodigoTipoPersistencia(
        tipo.codigo,
        tipo,
        codigosTipoPermitidos,
      );
    }

    const chave = this.normalizarTextoBusca(valor);
    if (!chave) {
      const tipoOutros = tiposDespesa.find((item) => item.codigo === 'O');
      return this.resolverCodigoTipoPersistencia(
        tipoOutros?.codigo ?? 'O',
        tipoOutros ?? null,
        codigosTipoPermitidos,
      );
    }

    const tipoPorInicial = tiposDespesa.find(
      (item) => item.codigo === chave.slice(0, 1),
    );
    if (tipoPorInicial) {
      return this.resolverCodigoTipoPersistencia(
        tipoPorInicial.codigo,
        tipoPorInicial,
        codigosTipoPermitidos,
      );
    }

    throw new BadRequestException('tipo da despesa invalido.');
  }

  private resolverCodigoTipoPersistencia(
    codigoPrincipal: string,
    tipo: TipoDespesaConfig | null,
    codigosTipoPermitidos: Set<string> | null,
  ) {
    const codigo = this.normalizarTextoBusca(codigoPrincipal);
    if (!codigosTipoPermitidos || codigosTipoPermitidos.size === 0) {
      return codigo;
    }

    if (codigosTipoPermitidos.has(codigo)) {
      return codigo;
    }

    if (tipo) {
      for (const alias of tipo.aliases) {
        const candidato = this.normalizarTextoBusca(alias);
        if (codigosTipoPermitidos.has(candidato)) {
          return candidato;
        }
      }
    }

    throw new BadRequestException(
      'tipo da despesa invalido para as regras atuais da base.',
    );
  }

  private normalizarTipoRetorno(
    valor: string,
    tiposDespesa: TipoDespesaConfig[],
  ) {
    const tipo = this.buscarTipoPorValor(valor, tiposDespesa);
    if (tipo) {
      return tipo.codigo;
    }

    const chave = this.normalizarTextoBusca(valor);
    if (!chave) {
      const tipoOutros = tiposDespesa.find((item) => item.codigo === 'O');
      return tipoOutros?.codigo ?? 'O';
    }

    return chave.slice(0, 1);
  }

  private descreverTipo(
    codigo: string,
    valorOriginal: string | null | undefined,
    tiposDespesa: TipoDespesaConfig[],
  ) {
    const chaveCodigo = this.normalizarTextoBusca(codigo);
    const tipoPorCodigo = tiposDespesa.find((item) => item.codigo === chaveCodigo);
    if (tipoPorCodigo) {
      return tipoPorCodigo.descricao;
    }

    const tipoPorValorOriginal = this.buscarTipoPorValor(
      valorOriginal ?? '',
      tiposDespesa,
    );
    if (tipoPorValorOriginal) {
      return tipoPorValorOriginal.descricao;
    }

    const texto = (valorOriginal ?? '').trim();
    return texto || 'Outros';
  }

  private buscarTipoPorValor(
    valor: string,
    tiposDespesa: TipoDespesaConfig[],
  ): TipoDespesaConfig | null {
    const chave = this.normalizarTextoBusca(valor);
    if (!chave) {
      return null;
    }

    for (const tipo of tiposDespesa) {
      if (tipo.codigo === chave) {
        return tipo;
      }

      if (tipo.aliases.some((alias) => this.normalizarTextoBusca(alias) === chave)) {
        return tipo;
      }
    }

    return null;
  }

  private normalizarTextoBusca(valor: string) {
    return valor
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .trim()
      .toUpperCase();
  }

  private situacaoTextoEhInativa(valor: string | null) {
    const texto = (valor ?? '').trim().toUpperCase();
    return (
      texto === 'I' ||
      texto === 'INATIVO' ||
      texto === 'INATIVA' ||
      texto === 'FALSE' ||
      texto === 'F' ||
      texto === '0'
    );
  }

  private async resolverVinculosDespesa(
    manager: EntityManager,
    colunas: MapaColunasDespesa,
    idEmpresa: number,
    payload: DespesaPersistencia,
  ): Promise<DespesaPersistencia> {
    let idVeiculo = payload.idVeiculo;
    let idMotorista = payload.idMotorista;
    const idViagem = payload.idViagem;
    const motoristaInformadoDireto = payload.idMotorista !== null && payload.idViagem === null;

    if (idViagem !== null) {
      const vinculoViagem = await this.buscarVinculoDaViagem(
        manager,
        idEmpresa,
        idViagem,
      );
      idVeiculo = vinculoViagem.idVeiculo;
      idMotorista = vinculoViagem.idMotorista;
    }

    if (colunas.idMotorista === null) {
      if (motoristaInformadoDireto) {
        throw new BadRequestException(
          'Este ambiente nao possui campo de motorista em despesas. Use vinculo por veiculo ou viagem.',
        );
      }
      idMotorista = null;
    }

    return {
      ...payload,
      idVeiculo,
      idMotorista,
      idViagem,
    };
  }

  private async buscarVinculoDaViagem(
    manager: EntityManager,
    idEmpresa: number,
    idViagem: number,
  ): Promise<ViagemVinculoResumo> {
    const valores: Array<string | number> = [idViagem];
    const filtros: string[] = ['id_viagem = $1'];

    valores.push(String(idEmpresa));
    filtros.push(`id_empresa = $${valores.length}`);

    const sql = `
      SELECT id_veiculo, id_motorista, status, data_fim
      FROM app.viagens
      WHERE ${filtros.join(' AND ')}
      LIMIT 1
    `;

    const rows = (await manager.query(sql, valores)) as RegistroBanco[];
    const registro = rows[0];
    if (!registro) {
      throw new BadRequestException(
        `Viagem #${idViagem} nao encontrada para a empresa logada.`,
      );
    }

    const statusViagem =
      this.converterTexto(registro.status)?.toUpperCase() ?? '';
    const dataFimViagem = this.converterData(registro.data_fim);
    if (statusViagem !== 'A' || dataFimViagem !== null) {
      throw new BadRequestException(
        `Viagem #${idViagem} encerrada. Vinculo de despesa permitido apenas para viagem aberta (status='A' e dataFim nula).`,
      );
    }

    const idVeiculo = this.converterNumero(registro.id_veiculo);
    if (!idVeiculo || idVeiculo <= 0) {
      throw new BadRequestException(
        `Viagem #${idViagem} sem veiculo valido para vinculo da despesa.`,
      );
    }

    const idMotorista = this.converterNumero(registro.id_motorista);

    return {
      idVeiculo,
      idMotorista: idMotorista && idMotorista > 0 ? idMotorista : null,
    };
  }

  private async recalcularViagensImpactadas(
    manager: EntityManager,
    idEmpresa: number,
    idViagemAnterior: number | null,
    idViagemNova: number | null,
    colunas: MapaColunasDespesa,
  ) {
    const viagens = new Set<number>();
    if (idViagemAnterior && idViagemAnterior > 0) {
      viagens.add(idViagemAnterior);
    }
    if (idViagemNova && idViagemNova > 0) {
      viagens.add(idViagemNova);
    }

    for (const idViagem of viagens) {
      await this.recalcularTotaisViagem(
        manager,
        idEmpresa,
        idViagem,
        colunas,
      );
    }
  }

  private async recalcularTotaisViagem(
    manager: EntityManager,
    idEmpresa: number,
    idViagem: number,
    colunas: MapaColunasDespesa,
  ) {
    const filtroAtivo = this.resolverFiltroAtivoRecalculo(colunas);
    const totalRows = (await manager.query(
      `
      SELECT COALESCE(SUM(COALESCE(valor, 0)), 0)::numeric AS total_despesas
      FROM app.despesas
      WHERE id_viagem = $1
        AND id_empresa = $2
        ${filtroAtivo}
      `,
      [idViagem, String(idEmpresa)],
    )) as Array<{ total_despesas?: unknown }>;

    const totalDespesas = this.converterNumero(totalRows[0]?.total_despesas) ?? 0;

    await manager.query(
      `
      UPDATE app.viagens
      SET
        total_despesas = $1,
        total_lucro = COALESCE(valor_frete, 0) - COALESCE(total_abastecimentos, 0) - $1,
        atualizado_em = NOW()
      WHERE id_viagem = $2
        AND id_empresa = $3
      `,
      [totalDespesas, idViagem, String(idEmpresa)],
    );
  }

  private resolverFiltroAtivoRecalculo(colunas: MapaColunasDespesa) {
    if (colunas.ativo) {
      return `AND COALESCE(${this.quote(colunas.ativo)}, true) = true`;
    }

    if (colunas.situacao) {
      return `AND UPPER(COALESCE(${this.quote(colunas.situacao)}::text, 'A')) NOT IN ('I', 'INATIVO', 'INATIVA', 'FALSE', 'F', '0')`;
    }

    return '';
  }

  private quote(coluna: string): string {
    if (!/^[a-z_][a-z0-9_]*$/.test(coluna)) {
      throw new BadRequestException(
        `Nome de coluna invalido detectado: ${coluna}`,
      );
    }
    return `"${coluna}"`;
  }

  private converterNumero(valor: unknown): number | null {
    if (valor === null || valor === undefined) {
      return null;
    }
    const numero = Number(valor);
    return Number.isFinite(numero) ? numero : null;
  }

  private converterBooleano(valor: unknown): boolean | null {
    if (valor === null || valor === undefined) {
      return null;
    }

    if (typeof valor === 'boolean') {
      return valor;
    }

    if (typeof valor === 'number') {
      if (valor === 1) return true;
      if (valor === 0) return false;
      return null;
    }

    if (typeof valor === 'string') {
      const normalizado = valor.trim().toLowerCase();
      if (normalizado === 'true' || normalizado === 't' || normalizado === '1') {
        return true;
      }
      if (normalizado === 'false' || normalizado === 'f' || normalizado === '0') {
        return false;
      }
    }

    return null;
  }

  private converterData(valor: unknown): Date | null {
    if (valor === null || valor === undefined) {
      return null;
    }

    const data = new Date(
      valor instanceof Date || typeof valor === 'string' || typeof valor === 'number'
        ? valor
        : '',
    );
    return Number.isNaN(data.getTime()) ? null : data;
  }

  private converterTexto(valor: unknown): string | null {
    if (typeof valor !== 'string') {
      return null;
    }
    const texto = valor.trim();
    return texto || null;
  }

  private normalizarUsuario(valor: string): string {
    return valor.trim().toUpperCase();
  }

  private tratarErroPersistencia(error: unknown, acao: string): never {
    if (error instanceof BadRequestException || error instanceof NotFoundException) {
      throw error;
    }

    if (error instanceof QueryFailedError) {
      const erroPg = error.driverError as { code?: string; message?: string };

      this.logger.error(
        `Falha ao ${acao} despesa. code=${erroPg.code ?? 'N/A'} message=${erroPg.message ?? 'Erro desconhecido'}`,
      );

      if (erroPg.code === '23503') {
        throw new BadRequestException(
          'Veiculo, motorista ou viagem informada nao existe para a empresa.',
        );
      }

      if (erroPg.code === '23502') {
        throw new BadRequestException(
          'A estrutura atual da tabela exige campos obrigatorios nao preenchidos para esta despesa.',
        );
      }

      if (erroPg.code === '23514') {
        throw new BadRequestException(
          'Dados da despesa invalidos para as regras da base.',
        );
      }

      if (erroPg.code === '42501') {
        throw new BadRequestException(
          'Usuario do banco sem permissao para gravar em app.despesas.',
        );
      }

      if (erroPg.code === '42P01') {
        throw new BadRequestException('Tabela app.despesas nao encontrada.');
      }

      if (erroPg.code === '42703') {
        throw new BadRequestException(
          'Estrutura da tabela app.despesas esta diferente do esperado.',
        );
      }
    }

    this.logger.error(
      `Falha ao ${acao} despesa sem codigo SQL mapeado. message=${error instanceof Error ? error.message : 'Erro desconhecido'}`,
    );
    throw new BadRequestException(
      `Nao foi possivel ${acao} a despesa neste momento.`,
    );
  }
}
