import {
  BadRequestException,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { DataSource, EntityManager, QueryFailedError } from 'typeorm';
import { JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { configurarContextoEmpresaRls } from '../common/database/rls.util';
import { AtualizarAbastecimentoDto } from './dto/atualizar-abastecimento.dto';
import { CriarAbastecimentoDto } from './dto/criar-abastecimento.dto';
import { FiltroAbastecimentosDto } from './dto/filtro-abastecimentos.dto';

type RegistroBanco = Record<string, unknown>;

type MapaColunasAbastecimento = {
  idAbastecimento: string;
  idEmpresa: string | null;
  idVeiculo: string;
  idFornecedor: string;
  idViagem: string | null;
  idNotaFiscal: string | null;
  origemLancamento: string | null;
  dataAbastecimento: string;
  litros: string;
  valorLitro: string;
  valorTotal: string | null;
  km: string;
  observacao: string | null;
  criadoEm: string | null;
  atualizadoEm: string | null;
  usuarioAtualizacao: string | null;
};

type AbastecimentoNormalizado = {
  idAbastecimento: number;
  idEmpresa: number | null;
  idVeiculo: number;
  idFornecedor: number;
  idViagem: number | null;
  idNotaFiscal: number | null;
  origemLancamento: string | null;
  dataAbastecimento: Date;
  litros: number;
  valorLitro: number;
  valorTotal: number;
  km: number;
  observacao: string | null;
  criadoEm: Date | null;
  atualizadoEm: Date | null;
  usuarioAtualizacao: string | null;
};

type AbastecimentoPersistencia = {
  idVeiculo: number;
  idFornecedor: number;
  idViagem: number | null;
  dataAbastecimento: Date;
  litros: number;
  valorLitro: number;
  valorTotal: number | null;
  km: number;
  observacao: string | null;
  usuarioAtualizacao: string;
};

type AbastecimentoAtualizacao = {
  idVeiculo?: number;
  idFornecedor?: number;
  idViagem?: number | null;
  dataAbastecimento?: Date;
  litros?: number;
  valorLitro?: number;
  valorTotal?: number;
  km?: number;
  observacao?: string | null;
  usuarioAtualizacao?: string;
};

@Injectable()
export class AbastecimentosService {
  private readonly logger = new Logger(AbastecimentosService.name);

  constructor(private readonly dataSource: DataSource) {}

  async listarTodos(idEmpresa: number) {
    return this.executarComRls(idEmpresa, async (manager, colunas) => {
      const filtros: string[] = [];
      const valores: Array<string | number> = [];

      if (colunas.idEmpresa) {
        valores.push(String(idEmpresa));
        filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
      }

      const sql = [
        'SELECT * FROM app.abastecimentos',
        filtros.length > 0 ? `WHERE ${filtros.join(' AND ')}` : '',
        `ORDER BY ${this.quote(colunas.dataAbastecimento)} DESC, ${this.quote(colunas.idAbastecimento)} DESC`,
      ]
        .filter(Boolean)
        .join('\n');

      const registros = (await manager.query(sql, valores)) as RegistroBanco[];
      const dados = registros.map((registro) =>
        this.mapearRegistro(registro, colunas),
      );

      return {
        sucesso: true,
        total: dados.length,
        abastecimentos: dados,
      };
    });
  }

  async listarComFiltro(idEmpresa: number, filtro: FiltroAbastecimentosDto) {
    this.validarIntervalosDoFiltro(filtro);

    return this.executarComRls(idEmpresa, async (manager, colunas) => {
      const pagina = filtro.pagina ?? 1;
      const limite = filtro.limite ?? 20;

      if (filtro.idViagem !== undefined && !colunas.idViagem) {
        this.garantirColunaIdViagemDisponivel(colunas.idViagem);
      }

      const filtros: string[] = [];
      const valores: Array<string | number> = [];

      if (colunas.idEmpresa) {
        valores.push(String(idEmpresa));
        filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
      }

      if (filtro.idAbastecimento !== undefined) {
        valores.push(filtro.idAbastecimento);
        filtros.push(
          `${this.quote(colunas.idAbastecimento)} = $${valores.length}`,
        );
      }

      if (filtro.idVeiculo !== undefined) {
        valores.push(filtro.idVeiculo);
        filtros.push(`${this.quote(colunas.idVeiculo)} = $${valores.length}`);
      }

      if (filtro.idFornecedor !== undefined) {
        valores.push(filtro.idFornecedor);
        filtros.push(
          `${this.quote(colunas.idFornecedor)} = $${valores.length}`,
        );
      }

      if (filtro.idViagem !== undefined && colunas.idViagem) {
        valores.push(filtro.idViagem);
        filtros.push(`${this.quote(colunas.idViagem)} = $${valores.length}`);
      }

      if (filtro.texto && colunas.observacao) {
        valores.push(`%${filtro.texto}%`);
        filtros.push(
          `COALESCE(${this.quote(colunas.observacao)}, '') ILIKE $${valores.length}`,
        );
      }

      if (filtro.dataDe) {
        valores.push(filtro.dataDe);
        filtros.push(
          `${this.quote(colunas.dataAbastecimento)} >= $${valores.length}`,
        );
      }

      if (filtro.dataAte) {
        valores.push(filtro.dataAte);
        filtros.push(
          `${this.quote(colunas.dataAbastecimento)} <= $${valores.length}`,
        );
      }

      if (filtro.litrosMin !== undefined) {
        valores.push(filtro.litrosMin);
        filtros.push(`${this.quote(colunas.litros)} >= $${valores.length}`);
      }

      if (filtro.litrosMax !== undefined) {
        valores.push(filtro.litrosMax);
        filtros.push(`${this.quote(colunas.litros)} <= $${valores.length}`);
      }

      if (filtro.valorLitroMin !== undefined) {
        valores.push(filtro.valorLitroMin);
        filtros.push(
          `${this.quote(colunas.valorLitro)} >= $${valores.length}`,
        );
      }

      if (filtro.valorLitroMax !== undefined) {
        valores.push(filtro.valorLitroMax);
        filtros.push(
          `${this.quote(colunas.valorLitro)} <= $${valores.length}`,
        );
      }

      if (filtro.valorTotalMin !== undefined && colunas.valorTotal) {
        valores.push(filtro.valorTotalMin);
        filtros.push(
          `${this.quote(colunas.valorTotal)} >= $${valores.length}`,
        );
      }

      if (filtro.valorTotalMax !== undefined && colunas.valorTotal) {
        valores.push(filtro.valorTotalMax);
        filtros.push(
          `${this.quote(colunas.valorTotal)} <= $${valores.length}`,
        );
      }

      if (filtro.kmMin !== undefined) {
        valores.push(filtro.kmMin);
        filtros.push(`${this.quote(colunas.km)} >= $${valores.length}`);
      }

      if (filtro.kmMax !== undefined) {
        valores.push(filtro.kmMax);
        filtros.push(`${this.quote(colunas.km)} <= $${valores.length}`);
      }

      const whereSql =
        filtros.length > 0 ? `WHERE ${filtros.join(' AND ')}` : '';

      const offset = (pagina - 1) * limite;
      const ordem = filtro.ordem ?? 'DESC';
      const colunaOrdenacao = this.resolverColunaOrdenacao(
        filtro.ordenarPor,
        colunas,
      );

      const sqlCount = `
        SELECT COUNT(1)::int AS total
        FROM app.abastecimentos
        ${whereSql}
      `;

      const sqlDados = `
        SELECT *
        FROM app.abastecimentos
        ${whereSql}
        ORDER BY ${this.quote(colunaOrdenacao)} ${ordem}, ${this.quote(colunas.idAbastecimento)} DESC
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
        this.mapearRegistro(registro, colunas),
      );

      return {
        sucesso: true,
        paginaAtual: pagina,
        limite,
        total,
        totalPaginas: total > 0 ? Math.ceil(total / limite) : 0,
        abastecimentos: dados,
      };
    });
  }

  async buscarPorId(idEmpresa: number, idAbastecimento: number) {
    return this.executarComRls(idEmpresa, async (manager, colunas) => {
      const registro = await this.buscarRegistroPorIdOuFalhar(
        manager,
        colunas,
        idEmpresa,
        idAbastecimento,
      );

      return {
        sucesso: true,
        abastecimento: this.mapearRegistro(registro, colunas),
      };
    });
  }

  async cadastrar(
    idEmpresa: number,
    dados: CriarAbastecimentoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    const payload = this.normalizarCriacao(dados, usuarioJwt);
    this.validarConsistencia(payload);

    try {
      return this.executarComRls(idEmpresa, async (manager, colunas) => {
        if (payload.idViagem !== null) {
          this.garantirColunaIdViagemDisponivel(colunas.idViagem);
        }

        const idViagem = colunas.idViagem
          ? await this.resolverIdViagemParaAbastecimento(
              manager,
              idEmpresa,
              payload.idVeiculo,
              payload.idViagem,
            )
          : null;
        const campos = [
          colunas.idVeiculo,
          colunas.idFornecedor,
          colunas.dataAbastecimento,
          colunas.litros,
          colunas.valorLitro,
          colunas.km,
        ];

        const valores: Array<string | number | Date | null> = [
          payload.idVeiculo,
          payload.idFornecedor,
          payload.dataAbastecimento,
          payload.litros,
          payload.valorLitro,
          payload.km,
        ];

        if (colunas.idViagem) {
          campos.push(colunas.idViagem);
          valores.push(idViagem);
        }

        if (colunas.valorTotal) {
          campos.push(colunas.valorTotal);
          valores.push(payload.valorTotal);
        }

        if (colunas.observacao) {
          campos.push(colunas.observacao);
          valores.push(payload.observacao);
        }

        if (colunas.usuarioAtualizacao) {
          campos.push(colunas.usuarioAtualizacao);
          valores.push(payload.usuarioAtualizacao);
        }

        if (colunas.idEmpresa) {
          campos.push(colunas.idEmpresa);
          valores.push(String(idEmpresa));
        }

        const placeholders = valores.map((_, index) => `$${index + 1}`).join(', ');
        const sql = `
          INSERT INTO app.abastecimentos (${campos.map((campo) => this.quote(campo)).join(', ')})
          VALUES (${placeholders})
          RETURNING *
        `;

        const rows = (await manager.query(sql, valores)) as RegistroBanco[];
        const registro = rows[0];
        if (!registro) {
          throw new BadRequestException(
            'Falha ao cadastrar abastecimento (retorno vazio da base).',
          );
        }

        return {
          sucesso: true,
          mensagem: 'Abastecimento cadastrado com sucesso.',
          abastecimento: this.mapearRegistro(registro, colunas),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'cadastrar');
    }
  }

  async atualizar(
    idEmpresa: number,
    idAbastecimento: number,
    dados: AtualizarAbastecimentoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    const payload = this.normalizarAtualizacao(dados, usuarioJwt);

    try {
      return this.executarComRls(idEmpresa, async (manager, colunas) => {
        if (payload.idViagem !== undefined) {
          this.garantirColunaIdViagemDisponivel(colunas.idViagem);
        }

        const registroAtual = await this.buscarRegistroPorIdOuFalhar(
          manager,
          colunas,
          idEmpresa,
          idAbastecimento,
        );
        const atual = this.mapearRegistro(registroAtual, colunas);

        const idVeiculo = payload.idVeiculo ?? atual.idVeiculo;
        const idFornecedor = payload.idFornecedor ?? atual.idFornecedor;
        const idViagem = colunas.idViagem
          ? payload.idViagem !== undefined
            ? await this.resolverIdViagemParaAbastecimento(
                manager,
                idEmpresa,
                idVeiculo,
                payload.idViagem,
                false,
              )
            : atual.idViagem
          : null;
        const dataAbastecimento =
          payload.dataAbastecimento ?? atual.dataAbastecimento;
        const litros = payload.litros ?? atual.litros;
        const valorLitro = payload.valorLitro ?? atual.valorLitro;
        const km = payload.km ?? atual.km;
        const valorTotal =
          payload.valorTotal ?? Number((litros * valorLitro).toFixed(2));
        const observacao =
          payload.observacao !== undefined ? payload.observacao : atual.observacao;
        const usuarioAtualizacao =
          payload.usuarioAtualizacao ?? this.normalizarUsuario(usuarioJwt.email);

        this.validarConsistencia({
          idVeiculo,
          idFornecedor,
          dataAbastecimento,
          litros,
          valorLitro,
          valorTotal,
          km,
        });

        const sets: string[] = [];
        const valores: Array<string | number | Date | null> = [];

        const adicionarSet = (coluna: string, valor: string | number | Date | null) => {
          valores.push(valor);
          sets.push(`${this.quote(coluna)} = $${valores.length}`);
        };

        adicionarSet(colunas.idVeiculo, idVeiculo);
        adicionarSet(colunas.idFornecedor, idFornecedor);
        adicionarSet(colunas.dataAbastecimento, dataAbastecimento);
        adicionarSet(colunas.litros, litros);
        adicionarSet(colunas.valorLitro, valorLitro);
        adicionarSet(colunas.km, km);

        if (colunas.idViagem) {
          adicionarSet(colunas.idViagem, idViagem);
        }

        if (colunas.valorTotal) {
          adicionarSet(colunas.valorTotal, valorTotal);
        }

        if (colunas.observacao) {
          adicionarSet(colunas.observacao, observacao);
        }

        if (colunas.usuarioAtualizacao) {
          adicionarSet(colunas.usuarioAtualizacao, usuarioAtualizacao);
        }

        const filtros: string[] = [];

        valores.push(idAbastecimento);
        filtros.push(
          `${this.quote(colunas.idAbastecimento)} = $${valores.length}`,
        );

        if (colunas.idEmpresa) {
          valores.push(String(idEmpresa));
          filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
        }

        const sql = `
          UPDATE app.abastecimentos
          SET ${sets.join(', ')}
          WHERE ${filtros.join(' AND ')}
          RETURNING *
        `;

        const rows = (await manager.query(sql, valores)) as RegistroBanco[];
        const atualizado = rows[0];
        if (!atualizado) {
          throw new NotFoundException(
            'Abastecimento nao encontrado para a empresa logada.',
          );
        }

        return {
          sucesso: true,
          mensagem: 'Abastecimento atualizado com sucesso.',
          abastecimento: this.mapearRegistro(atualizado, colunas),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'atualizar');
    }
  }

  async remover(idEmpresa: number, idAbastecimento: number) {
    return this.executarComRls(idEmpresa, async (manager, colunas) => {
      const filtros: string[] = [];
      const valores: Array<string | number> = [];

      valores.push(idAbastecimento);
      filtros.push(`${this.quote(colunas.idAbastecimento)} = $${valores.length}`);

      if (colunas.idEmpresa) {
        valores.push(String(idEmpresa));
        filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
      }

      const sql = `
        DELETE FROM app.abastecimentos
        WHERE ${filtros.join(' AND ')}
        RETURNING ${this.quote(colunas.idAbastecimento)}
      `;

      const removidos = (await manager.query(sql, valores)) as RegistroBanco[];

      if (removidos.length === 0) {
        throw new NotFoundException(
          'Abastecimento nao encontrado para a empresa logada.',
        );
      }

      return {
        sucesso: true,
        mensagem: 'Abastecimento removido com sucesso.',
        idAbastecimento,
      };
    });
  }

  private async executarComRls<T>(
    idEmpresa: number,
    callback: (
      manager: EntityManager,
      colunas: MapaColunasAbastecimento,
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
  ): Promise<MapaColunasAbastecimento> {
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
      idEmpresa: this.encontrarColuna(
        set,
        ['id_empresa'],
        'id da empresa',
        false,
      ),
      idVeiculo: this.encontrarColuna(
        set,
        ['id_veiculo', 'veiculo_id'],
        'id do veiculo',
      )!,
      idFornecedor: this.encontrarColuna(
        set,
        ['id_fornecedor', 'fornecedor_id'],
        'id do fornecedor',
      )!,
      idViagem: this.encontrarColuna(
        set,
        ['id_viagem', 'viagem_id'],
        '',
        false,
      ),
      idNotaFiscal: this.encontrarColuna(
        set,
        ['id_nota_fiscal'],
        '',
        false,
      ),
      origemLancamento: this.encontrarColuna(
        set,
        ['origem_lancamento'],
        '',
        false,
      ),
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
      valorTotal: this.encontrarColuna(
        set,
        ['valor_total', 'total', 'valor'],
        'valor total',
        false,
      ),
      km: this.encontrarColuna(
        set,
        ['km', 'km_abastecimento', 'km_veiculo', 'km_atual'],
        'km',
      )!,
      observacao: this.encontrarColuna(
        set,
        ['observacao', 'observacoes', 'obs'],
        '',
        false,
      ),
      criadoEm: this.encontrarColuna(
        set,
        ['criado_em', 'created_at'],
        '',
        false,
      ),
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
        `Estrutura da tabela app.abastecimentos invalida: coluna de ${descricao} nao encontrada.`,
      );
    }

    return null;
  }

  private garantirColunaIdViagemDisponivel(colunaIdViagem?: string | null): void {
    if (colunaIdViagem) {
      return;
    }

    throw new BadRequestException(
      'Campo de vinculo da viagem nao encontrado em app.abastecimentos (id_viagem/viagem_id). Execute o script sql/abastecimentos_add_id_viagem.sql e reinicie o backend.',
    );
  }

  private async buscarRegistroPorIdOuFalhar(
    manager: EntityManager,
    colunas: MapaColunasAbastecimento,
    idEmpresa: number,
    idAbastecimento: number,
  ): Promise<RegistroBanco> {
    const filtros: string[] = [];
    const valores: Array<string | number> = [];

    valores.push(idAbastecimento);
    filtros.push(`${this.quote(colunas.idAbastecimento)} = $${valores.length}`);

    if (colunas.idEmpresa) {
      valores.push(String(idEmpresa));
      filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
    }

    const sql = `
      SELECT *
      FROM app.abastecimentos
      WHERE ${filtros.join(' AND ')}
      LIMIT 1
    `;

    const rows = (await manager.query(sql, valores)) as RegistroBanco[];
    const registro = rows[0];

    if (!registro) {
      throw new NotFoundException(
        'Abastecimento nao encontrado para a empresa logada.',
      );
    }

    return registro;
  }

  private normalizarCriacao(
    dados: CriarAbastecimentoDto,
    usuarioJwt: JwtUsuarioPayload,
  ): AbastecimentoPersistencia {
    const litros = dados.litros;
    const valorLitro = dados.valorLitro;

    return {
      idVeiculo: dados.idVeiculo,
      idFornecedor: dados.idFornecedor,
      idViagem: dados.idViagem ?? null,
      dataAbastecimento: new Date(dados.dataAbastecimento),
      litros,
      valorLitro,
      valorTotal:
        dados.valorTotal !== undefined
          ? dados.valorTotal
          : Number((litros * valorLitro).toFixed(2)),
      km: dados.km,
      observacao: dados.observacao?.trim()
        ? dados.observacao.trim().toUpperCase()
        : null,
      usuarioAtualizacao: dados.usuarioAtualizacao?.trim()
        ? this.normalizarUsuario(dados.usuarioAtualizacao)
        : this.normalizarUsuario(usuarioJwt.email),
    };
  }

  private normalizarAtualizacao(
    dados: AtualizarAbastecimentoDto,
    usuarioJwt: JwtUsuarioPayload,
  ): AbastecimentoAtualizacao {
    return {
      idVeiculo: dados.idVeiculo,
      idFornecedor: dados.idFornecedor,
      idViagem: dados.idViagem,
      dataAbastecimento:
        dados.dataAbastecimento !== undefined
          ? new Date(dados.dataAbastecimento)
          : undefined,
      litros: dados.litros,
      valorLitro: dados.valorLitro,
      valorTotal: dados.valorTotal,
      km: dados.km,
      observacao:
        dados.observacao !== undefined
          ? dados.observacao.trim()
            ? dados.observacao.trim().toUpperCase()
            : null
          : undefined,
      usuarioAtualizacao:
        dados.usuarioAtualizacao !== undefined
          ? this.normalizarUsuario(dados.usuarioAtualizacao)
          : this.normalizarUsuario(usuarioJwt.email),
    };
  }

  private validarConsistencia(payload: {
    idVeiculo: number;
    idFornecedor: number;
    dataAbastecimento: Date;
    litros: number;
    valorLitro: number;
    valorTotal: number | null;
    km: number;
  }) {
    if (!Number.isFinite(payload.idVeiculo) || payload.idVeiculo <= 0) {
      throw new BadRequestException('idVeiculo invalido.');
    }

    if (!Number.isFinite(payload.idFornecedor) || payload.idFornecedor <= 0) {
      throw new BadRequestException('idFornecedor invalido.');
    }

    if (!(payload.dataAbastecimento instanceof Date) ||
        Number.isNaN(payload.dataAbastecimento.getTime())) {
      throw new BadRequestException('dataAbastecimento invalida.');
    }

    if (!Number.isFinite(payload.litros) || payload.litros <= 0) {
      throw new BadRequestException('litros deve ser maior que zero.');
    }

    if (!Number.isFinite(payload.valorLitro) || payload.valorLitro < 0) {
      throw new BadRequestException('valorLitro invalido.');
    }

    if (
      payload.valorTotal !== null &&
      (!Number.isFinite(payload.valorTotal) || payload.valorTotal < 0)
    ) {
      throw new BadRequestException('valorTotal invalido.');
    }

    if (!Number.isFinite(payload.km) || payload.km < 0) {
      throw new BadRequestException('km invalido.');
    }
  }

  private async resolverIdViagemParaAbastecimento(
    manager: EntityManager,
    idEmpresa: number,
    idVeiculo: number,
    idViagemInformada: number | null,
    buscarAbertaQuandoNulo = true,
  ): Promise<number | null> {
    if (idViagemInformada !== null) {
      if (!Number.isFinite(idViagemInformada) || idViagemInformada <= 0) {
        throw new BadRequestException('idViagem invalido.');
      }

      const viagem = await this.buscarViagemAbertaPorId(
        manager,
        idEmpresa,
        idViagemInformada,
      );
      if (viagem.idVeiculo !== idVeiculo) {
        throw new BadRequestException(
          `Viagem #${idViagemInformada} nao pertence ao veiculo informado.`,
        );
      }

      return viagem.idViagem;
    }

    if (!buscarAbertaQuandoNulo) {
      return null;
    }

    return this.buscarViagemAbertaMaisRecentePorVeiculo(
      manager,
      idEmpresa,
      idVeiculo,
    );
  }

  private async buscarViagemAbertaPorId(
    manager: EntityManager,
    idEmpresa: number,
    idViagem: number,
  ): Promise<{ idViagem: number; idVeiculo: number }> {
    const rows = (await manager.query(
      `
      SELECT id_viagem, id_veiculo, status, data_fim
      FROM app.viagens
      WHERE id_viagem = $1
        AND id_empresa = $2
      LIMIT 1
      `,
      [idViagem, String(idEmpresa)],
    )) as RegistroBanco[];

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
        `Viagem #${idViagem} encerrada. Vinculo de abastecimento permitido apenas para viagem aberta (status='A' e dataFim nula).`,
      );
    }

    const idVeiculo = this.converterNumero(registro.id_veiculo);
    if (!idVeiculo || idVeiculo <= 0) {
      throw new BadRequestException(
        `Viagem #${idViagem} sem veiculo valido para vinculo do abastecimento.`,
      );
    }

    return { idViagem, idVeiculo };
  }

  private async buscarViagemAbertaMaisRecentePorVeiculo(
    manager: EntityManager,
    idEmpresa: number,
    idVeiculo: number,
  ): Promise<number | null> {
    const rows = (await manager.query(
      `
      SELECT id_viagem
      FROM app.viagens
      WHERE id_veiculo = $1
        AND id_empresa = $2
        AND status = 'A'
        AND data_fim IS NULL
      ORDER BY data_inicio DESC, id_viagem DESC
      LIMIT 1
      `,
      [idVeiculo, String(idEmpresa)],
    )) as RegistroBanco[];

    return this.converterNumero(rows[0]?.id_viagem);
  }

  private validarIntervalosDoFiltro(filtro: FiltroAbastecimentosDto) {
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
      filtro.litrosMin !== undefined &&
      filtro.litrosMax !== undefined &&
      filtro.litrosMax < filtro.litrosMin
    ) {
      throw new BadRequestException(
        'Filtro invalido: litrosMax deve ser maior ou igual a litrosMin.',
      );
    }

    if (
      filtro.valorLitroMin !== undefined &&
      filtro.valorLitroMax !== undefined &&
      filtro.valorLitroMax < filtro.valorLitroMin
    ) {
      throw new BadRequestException(
        'Filtro invalido: valorLitroMax deve ser maior ou igual a valorLitroMin.',
      );
    }

    if (
      filtro.valorTotalMin !== undefined &&
      filtro.valorTotalMax !== undefined &&
      filtro.valorTotalMax < filtro.valorTotalMin
    ) {
      throw new BadRequestException(
        'Filtro invalido: valorTotalMax deve ser maior ou igual a valorTotalMin.',
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

  private resolverColunaOrdenacao(
    ordenarPor: FiltroAbastecimentosDto['ordenarPor'],
    colunas: MapaColunasAbastecimento,
  ): string {
    if (ordenarPor === 'id_abastecimento') {
      return colunas.idAbastecimento;
    }
    if (ordenarPor === 'litros') {
      return colunas.litros;
    }
    if (ordenarPor === 'valor_litro') {
      return colunas.valorLitro;
    }
    if (ordenarPor === 'valor_total' && colunas.valorTotal) {
      return colunas.valorTotal;
    }
    if (ordenarPor === 'km') {
      return colunas.km;
    }
    if (ordenarPor === 'criado_em' && colunas.criadoEm) {
      return colunas.criadoEm;
    }
    if (ordenarPor === 'atualizado_em' && colunas.atualizadoEm) {
      return colunas.atualizadoEm;
    }
    return colunas.dataAbastecimento;
  }

  private mapearRegistro(
    registro: RegistroBanco,
    colunas: MapaColunasAbastecimento,
  ): AbastecimentoNormalizado {
    const litros = this.converterNumero(registro[colunas.litros]) ?? 0;
    const valorLitro = this.converterNumero(registro[colunas.valorLitro]) ?? 0;
    const valorTotalDaTabela = colunas.valorTotal
      ? this.converterNumero(registro[colunas.valorTotal])
      : null;

    return {
      idAbastecimento:
        this.converterNumero(registro[colunas.idAbastecimento]) ?? 0,
      idEmpresa:
        colunas.idEmpresa !== null
          ? this.converterNumero(registro[colunas.idEmpresa])
          : null,
      idVeiculo: this.converterNumero(registro[colunas.idVeiculo]) ?? 0,
      idFornecedor: this.converterNumero(registro[colunas.idFornecedor]) ?? 0,
      idViagem:
        colunas.idViagem !== null
          ? this.converterNumero(registro[colunas.idViagem])
          : null,
      idNotaFiscal:
        colunas.idNotaFiscal !== null
          ? this.converterNumero(registro[colunas.idNotaFiscal])
          : null,
      origemLancamento:
        colunas.origemLancamento !== null
          ? this.converterTexto(registro[colunas.origemLancamento])
          : null,
      dataAbastecimento:
        this.converterData(registro[colunas.dataAbastecimento]) ?? new Date(0),
      litros,
      valorLitro,
      valorTotal:
        valorTotalDaTabela ?? Number((litros * valorLitro).toFixed(2)),
      km: this.converterNumero(registro[colunas.km]) ?? 0,
      observacao:
        colunas.observacao !== null
          ? this.converterTexto(registro[colunas.observacao])
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
    return texto ? texto : null;
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
        `Falha ao ${acao} abastecimento. code=${erroPg.code ?? 'N/A'} message=${erroPg.message ?? 'Erro desconhecido'}`,
      );

      if (erroPg.code === '23503') {
        throw new BadRequestException(
          'Veiculo, fornecedor ou viagem informada nao existe para a empresa.',
        );
      }

      if (erroPg.code === '42501') {
        throw new BadRequestException(
          'Usuario do banco sem permissao para gravar em app.abastecimentos.',
        );
      }

      if (erroPg.code === '42P01') {
        throw new BadRequestException(
          'Tabela app.abastecimentos nao encontrada.',
        );
      }

      if (erroPg.code === '42703') {
        throw new BadRequestException(
          'Estrutura da tabela app.abastecimentos esta diferente do esperado.',
        );
      }
    }

    this.logger.error(
      `Falha ao ${acao} abastecimento sem codigo SQL mapeado. message=${error instanceof Error ? error.message : 'Erro desconhecido'}`,
    );
    throw new BadRequestException(
      `Nao foi possivel ${acao} o abastecimento neste momento.`,
    );
  }
}
