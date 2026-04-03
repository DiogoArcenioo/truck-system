import {
  BadRequestException,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { DataSource, EntityManager, QueryFailedError } from 'typeorm';
import { JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { configurarContextoEmpresaRls } from '../common/database/rls.util';
import { AtualizarEngateDesengateDto } from './dto/atualizar-engate-desengate.dto';
import { CriarEngateDesengateDto } from './dto/criar-engate-desengate.dto';
import { FiltroEngateDesengateDto } from './dto/filtro-engate-desengate.dto';

type RegistroBanco = Record<string, unknown>;

type MapaColunasEngate = {
  idEngate: string;
  idEmpresa: string | null;
  idVeiculo: string;
  idMotorista: string;
  dataInclusao: string;
  dataMovi: string;
  situacao: string | null;
  tipoEngate: string;
  usuarioAtualizacao: string | null;
  placa2: string | null;
  placa3: string | null;
  placa4: string | null;
  criadoEm: string | null;
  atualizadoEm: string | null;
};

type EngateNormalizado = {
  idEngate: number;
  idEmpresa: number | null;
  idVeiculo: number;
  idMotorista: number;
  dataInclusao: Date;
  dataMovi: Date;
  situacao: string;
  tipoEngate: string;
  usuarioAtualizacao: string | null;
  placa2: string | null;
  placa3: string | null;
  placa4: string | null;
  criadoEm: Date | null;
  atualizadoEm: Date | null;
};

type EngatePersistencia = {
  idVeiculo: number;
  idMotorista: number;
  dataInclusao: Date;
  dataMovi: Date;
  situacao: string;
  tipoEngate: string;
  usuarioAtualizacao: string;
  placa2: string | null;
  placa3: string | null;
  placa4: string | null;
};

@Injectable()
export class EngateDesengateService {
  private readonly logger = new Logger(EngateDesengateService.name);
  private readonly tabelasCandidatas = [
    'engate_veiculo',
    'engate',
    'engates',
    'engate_desengate',
    'engate_desengates',
    'movimentacao_engate',
  ];

  constructor(private readonly dataSource: DataSource) {}

  async listarTodos(idEmpresa: number) {
    return this.executarComRls(idEmpresa, async (manager, tabela, colunas) => {
      const { filtros, valores } = this.criarFiltrosEmpresa(idEmpresa, colunas);
      const sql = [
        `SELECT * FROM app.${this.quoteIdentifier(tabela)}`,
        filtros.length > 0 ? `WHERE ${filtros.join(' AND ')}` : '',
        `ORDER BY ${this.quote(colunas.dataMovi)} DESC, ${this.quote(colunas.idEngate)} DESC`,
      ]
        .filter(Boolean)
        .join('\n');

      const registros = (await manager.query(sql, valores)) as RegistroBanco[];
      const movimentos = registros.map((registro) =>
        this.mapearRegistro(registro, colunas),
      );

      return {
        sucesso: true,
        total: movimentos.length,
        movimentos,
      };
    });
  }

  async listarComFiltro(idEmpresa: number, filtro: FiltroEngateDesengateDto) {
    return this.executarComRls(idEmpresa, async (manager, tabela, colunas) => {
      const { filtros, valores } = this.criarFiltrosEmpresa(idEmpresa, colunas);

      if (filtro.idEngate !== undefined) {
        valores.push(filtro.idEngate);
        filtros.push(`${this.quote(colunas.idEngate)} = $${valores.length}`);
      }

      if (filtro.idVeiculo !== undefined) {
        valores.push(filtro.idVeiculo);
        filtros.push(`${this.quote(colunas.idVeiculo)} = $${valores.length}`);
      }

      if (filtro.idMotorista !== undefined) {
        valores.push(filtro.idMotorista);
        filtros.push(`${this.quote(colunas.idMotorista)} = $${valores.length}`);
      }

      if (filtro.tipoEngate) {
        valores.push(filtro.tipoEngate);
        filtros.push(`${this.quote(colunas.tipoEngate)} = $${valores.length}`);
      }

      if (filtro.situacao && colunas.situacao) {
        valores.push(filtro.situacao);
        filtros.push(`${this.quote(colunas.situacao)} = $${valores.length}`);
      }

      if (filtro.dataDe) {
        valores.push(filtro.dataDe);
        filtros.push(`${this.quote(colunas.dataMovi)} >= $${valores.length}`);
      }

      if (filtro.dataAte) {
        valores.push(filtro.dataAte);
        filtros.push(`${this.quote(colunas.dataMovi)} <= $${valores.length}`);
      }

      if (filtro.texto?.trim()) {
        valores.push(`%${filtro.texto.trim()}%`);
        const referencia = `$${valores.length}`;
        const camposTexto = [
          colunas.usuarioAtualizacao,
          colunas.placa2,
          colunas.placa3,
          colunas.placa4,
        ]
          .filter((item): item is string => item !== null)
          .map(
            (coluna) =>
              `COALESCE(${this.quote(coluna)}, '') ILIKE ${referencia}`,
          );

        if (camposTexto.length > 0) {
          filtros.push(`(${camposTexto.join(' OR ')})`);
        }
      }

      const pagina = filtro.pagina ?? 1;
      const limite = filtro.limite ?? 50;
      const offset = (pagina - 1) * limite;
      const ordem = filtro.ordem ?? 'DESC';
      const colunaOrdenacao = this.resolverColunaOrdenacao(
        filtro.ordenarPor,
        colunas,
      );
      const whereSql =
        filtros.length > 0 ? `WHERE ${filtros.join(' AND ')}` : '';

      const sqlCount = `
        SELECT COUNT(1)::int AS total
        FROM app.${this.quoteIdentifier(tabela)}
        ${whereSql}
      `;

      const sqlDados = `
        SELECT *
        FROM app.${this.quoteIdentifier(tabela)}
        ${whereSql}
        ORDER BY ${this.quote(colunaOrdenacao)} ${ordem}, ${this.quote(colunas.idEngate)} DESC
        LIMIT $${valores.length + 1}
        OFFSET $${valores.length + 2}
      `;

      const [countRows, registros] = await Promise.all([
        manager.query(sqlCount, valores) as Promise<Array<{ total: number }>>,
        manager.query(sqlDados, [...valores, limite, offset]) as Promise<
          RegistroBanco[]
        >,
      ]);

      const movimentos = registros.map((registro) =>
        this.mapearRegistro(registro, colunas),
      );
      const total = Number(countRows[0]?.total ?? 0);

      return {
        sucesso: true,
        paginaAtual: pagina,
        limite,
        total,
        totalPaginas: total > 0 ? Math.ceil(total / limite) : 0,
        movimentos,
      };
    });
  }

  async buscarPorId(idEmpresa: number, idEngate: number) {
    return this.executarComRls(idEmpresa, async (manager, tabela, colunas) => {
      const registro = await this.buscarRegistroPorIdOuFalhar(
        manager,
        tabela,
        colunas,
        idEmpresa,
        idEngate,
      );

      return {
        sucesso: true,
        movimento: this.mapearRegistro(registro, colunas),
      };
    });
  }

  async cadastrar(
    idEmpresa: number,
    dados: CriarEngateDesengateDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    const payload = this.normalizarCriacao(dados, usuarioJwt);

    try {
      return this.executarComRls(idEmpresa, async (manager, tabela, colunas) => {
        await this.validarRecursosParaEngate(
          manager,
          tabela,
          colunas,
          idEmpresa,
          payload,
        );

        const campos: string[] = [];
        const valores: Array<string | number | Date | null> = [];

        const adicionarCampo = (
          coluna: string | null,
          valor: string | number | Date | null | undefined,
        ) => {
          if (!coluna || valor === undefined) {
            return;
          }

          campos.push(coluna);
          valores.push(valor);
        };

        adicionarCampo(colunas.idVeiculo, payload.idVeiculo);
        adicionarCampo(colunas.idMotorista, payload.idMotorista);
        adicionarCampo(colunas.dataInclusao, payload.dataInclusao);
        adicionarCampo(colunas.dataMovi, payload.dataMovi);
        adicionarCampo(colunas.tipoEngate, payload.tipoEngate);
        adicionarCampo(colunas.situacao, payload.situacao);
        adicionarCampo(colunas.usuarioAtualizacao, payload.usuarioAtualizacao);
        adicionarCampo(colunas.placa2, payload.placa2);
        adicionarCampo(colunas.placa3, payload.placa3);
        adicionarCampo(colunas.placa4, payload.placa4);
        adicionarCampo(colunas.criadoEm, new Date());
        adicionarCampo(colunas.atualizadoEm, new Date());
        adicionarCampo(colunas.idEmpresa, String(idEmpresa));

        const placeholders = valores
          .map((_, index) => `$${index + 1}`)
          .join(', ');
        const sql = `
          INSERT INTO app.${this.quoteIdentifier(tabela)} (${campos.map((campo) => this.quote(campo)).join(', ')})
          VALUES (${placeholders})
          RETURNING *
        `;

        const rows = (await manager.query(sql, valores)) as RegistroBanco[];
        const registro = rows[0];

        if (!registro) {
          throw new BadRequestException(
            'Falha ao cadastrar movimentacao de engate/desengate.',
          );
        }

        return {
          sucesso: true,
          mensagem: 'Movimentacao cadastrada com sucesso.',
          movimento: this.mapearRegistro(registro, colunas),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'cadastrar');
    }
  }

  async atualizar(
    idEmpresa: number,
    idEngate: number,
    dados: AtualizarEngateDesengateDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    const payload = this.normalizarAtualizacao(dados, usuarioJwt);

    try {
      return this.executarComRls(idEmpresa, async (manager, tabela, colunas) => {
        const atual = await this.buscarRegistroPorIdOuFalhar(
          manager,
          tabela,
          colunas,
          idEmpresa,
          idEngate,
        );

        const movimentoAtual = this.mapearRegistro(atual, colunas);
        const estadoFinal = this.montarEstadoFinalAtualizacao(
          movimentoAtual,
          payload,
        );
        await this.validarRecursosParaEngate(
          manager,
          tabela,
          colunas,
          idEmpresa,
          estadoFinal,
          idEngate,
        );

        const sets: string[] = [];
        const valores: Array<string | number | Date | null> = [];

        const adicionarSet = (
          coluna: string | null,
          valor: string | number | Date | null | undefined,
        ) => {
          if (!coluna || typeof valor === 'undefined') {
            return;
          }

          valores.push(valor);
          sets.push(`${this.quote(coluna)} = $${valores.length}`);
        };

        adicionarSet(colunas.idVeiculo, payload.idVeiculo);
        adicionarSet(colunas.idMotorista, payload.idMotorista);
        adicionarSet(colunas.dataInclusao, payload.dataInclusao);
        adicionarSet(colunas.dataMovi, payload.dataMovi);
        adicionarSet(colunas.tipoEngate, payload.tipoEngate);
        adicionarSet(colunas.situacao, payload.situacao);
        adicionarSet(colunas.usuarioAtualizacao, payload.usuarioAtualizacao);
        adicionarSet(colunas.placa2, payload.placa2);
        adicionarSet(colunas.placa3, payload.placa3);
        adicionarSet(colunas.placa4, payload.placa4);
        adicionarSet(colunas.atualizadoEm, new Date());

        if (sets.length === 0) {
          throw new BadRequestException(
            'Nenhum campo valido foi informado para atualizar a movimentacao.',
          );
        }

        valores.push(idEngate);
        const filtros = [
          `${this.quote(colunas.idEngate)} = $${valores.length}`,
        ];

        if (colunas.idEmpresa) {
          valores.push(String(idEmpresa));
          filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
        }

        const sql = `
          UPDATE app.${this.quoteIdentifier(tabela)}
          SET ${sets.join(', ')}
          WHERE ${filtros.join(' AND ')}
          RETURNING *
        `;

        const rows = (await manager.query(sql, valores)) as RegistroBanco[];
        const atualizado = rows[0];

        if (!atualizado) {
          throw new NotFoundException(
            'Movimentacao nao encontrada para a empresa logada.',
          );
        }

        return {
          sucesso: true,
          mensagem: 'Movimentacao atualizada com sucesso.',
          movimento: this.mapearRegistro(atualizado, colunas),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'atualizar');
    }
  }

  async remover(idEmpresa: number, idEngate: number) {
    return this.executarComRls(idEmpresa, async (manager, tabela, colunas) => {
      await this.buscarRegistroPorIdOuFalhar(
        manager,
        tabela,
        colunas,
        idEmpresa,
        idEngate,
      );

      const valores: Array<string | number> = [idEngate];
      const filtros = [`${this.quote(colunas.idEngate)} = $1`];

      if (colunas.idEmpresa) {
        valores.push(String(idEmpresa));
        filtros.push(`${this.quote(colunas.idEmpresa)} = $2`);
      }

      const sql = `
        DELETE FROM app.${this.quoteIdentifier(tabela)}
        WHERE ${filtros.join(' AND ')}
      `;

      await manager.query(sql, valores);

      return {
        sucesso: true,
        mensagem: 'Movimentacao removida com sucesso.',
      };
    });
  }

  private async executarComRls<T>(
    idEmpresa: number,
    callback: (
      manager: EntityManager,
      tabela: string,
      colunas: MapaColunasEngate,
    ) => Promise<T>,
  ): Promise<T> {
    return this.dataSource.manager.transaction(async (manager) => {
      await configurarContextoEmpresaRls(manager, idEmpresa);
      const tabela = await this.resolverTabela(manager);
      const colunas = await this.carregarMapaColunas(manager, tabela);
      return callback(manager, tabela, colunas);
    });
  }

  private async resolverTabela(manager: EntityManager) {
    const rows = (await manager.query(
      `
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'app'
          AND table_name = ANY($1::text[])
        ORDER BY array_position($1::text[], table_name)
        LIMIT 1
      `,
      [this.tabelasCandidatas],
    )) as Array<{ table_name?: unknown }>;

    const tabela =
      typeof rows[0]?.table_name === 'string' ? rows[0].table_name : '';
    if (!tabela) {
      throw new BadRequestException(
        `Nenhuma tabela de engate/desengate encontrada no schema app. Candidatas: ${this.tabelasCandidatas.join(', ')}.`,
      );
    }

    return tabela;
  }

  private async carregarMapaColunas(
    manager: EntityManager,
    tabela: string,
  ): Promise<MapaColunasEngate> {
    const rows = (await manager.query(
      `
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'app'
          AND table_name = $1
      `,
      [tabela],
    )) as Array<{ column_name?: unknown }>;

    const set = new Set<string>(
      rows
        .map((row) =>
          typeof row.column_name === 'string' ? row.column_name : '',
        )
        .filter(Boolean),
    );

    if (set.size === 0) {
      throw new BadRequestException(`Tabela app.${tabela} nao encontrada.`);
    }

    return {
      idEngate: this.encontrarColuna(set, ['id_engate', 'id'])!,
      idEmpresa: this.encontrarColuna(set, ['id_empresa'], false),
      idVeiculo: this.encontrarColuna(set, ['id_veiculo'])!,
      idMotorista: this.encontrarColuna(set, ['id_motorista'])!,
      dataInclusao: this.encontrarColuna(set, ['data_inclusao'])!,
      dataMovi: this.encontrarColuna(set, ['data_movi'])!,
      situacao: this.encontrarColuna(set, ['situacao', 'status'], false),
      tipoEngate: this.encontrarColuna(set, ['tipo_engate'])!,
      usuarioAtualizacao: this.encontrarColuna(
        set,
        ['usuario_atualizacao', 'usuario_update'],
        false,
      ),
      placa2: this.encontrarColuna(set, ['placa2', 'placa_2'], false),
      placa3: this.encontrarColuna(set, ['placa3', 'placa_3'], false),
      placa4: this.encontrarColuna(set, ['placa4', 'placa_4'], false),
      criadoEm: this.encontrarColuna(set, ['criado_em', 'created_at'], false),
      atualizadoEm: this.encontrarColuna(
        set,
        ['atualizado_em', 'updated_at'],
        false,
      ),
    };
  }

  private encontrarColuna(
    set: Set<string>,
    candidatas: string[],
    obrigatoria = true,
  ) {
    for (const candidata of candidatas) {
      if (set.has(candidata)) {
        return candidata;
      }
    }

    if (obrigatoria) {
      throw new BadRequestException(
        `Estrutura da tabela de engate/desengate invalida: coluna ${candidatas.join('/')} nao encontrada.`,
      );
    }

    return null;
  }

  private criarFiltrosEmpresa(idEmpresa: number, colunas: MapaColunasEngate) {
    const filtros: string[] = [];
    const valores: Array<string | number> = [];

    if (colunas.idEmpresa) {
      valores.push(String(idEmpresa));
      filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
    }

    return { filtros, valores };
  }

  private async buscarRegistroPorIdOuFalhar(
    manager: EntityManager,
    tabela: string,
    colunas: MapaColunasEngate,
    idEmpresa: number,
    idEngate: number,
  ) {
    const valores: Array<string | number> = [idEngate];
    const filtros = [`${this.quote(colunas.idEngate)} = $1`];

    if (colunas.idEmpresa) {
      valores.push(String(idEmpresa));
      filtros.push(`${this.quote(colunas.idEmpresa)} = $2`);
    }

    const sql = `
      SELECT *
      FROM app.${this.quoteIdentifier(tabela)}
      WHERE ${filtros.join(' AND ')}
      LIMIT 1
    `;

    const rows = (await manager.query(sql, valores)) as RegistroBanco[];
    const registro = rows[0];

    if (!registro) {
      throw new NotFoundException(
        'Movimentacao nao encontrada para a empresa logada.',
      );
    }

    return registro;
  }

  private resolverColunaOrdenacao(
    ordenarPor: FiltroEngateDesengateDto['ordenarPor'] | undefined,
    colunas: MapaColunasEngate,
  ) {
    if (ordenarPor === 'id_engate') return colunas.idEngate;
    if (ordenarPor === 'data_inclusao') return colunas.dataInclusao;
    if (ordenarPor === 'tipo_engate') return colunas.tipoEngate;
    if (ordenarPor === 'situacao' && colunas.situacao) return colunas.situacao;
    if (ordenarPor === 'criado_em' && colunas.criadoEm) return colunas.criadoEm;
    if (ordenarPor === 'atualizado_em' && colunas.atualizadoEm) {
      return colunas.atualizadoEm;
    }
    return colunas.dataMovi;
  }

  private mapearRegistro(
    registro: RegistroBanco,
    colunas: MapaColunasEngate,
  ): EngateNormalizado {
    return {
      idEngate: this.converterInteiro(registro[colunas.idEngate]) ?? 0,
      idEmpresa: colunas.idEmpresa
        ? this.converterInteiro(registro[colunas.idEmpresa])
        : null,
      idVeiculo: this.converterInteiro(registro[colunas.idVeiculo]) ?? 0,
      idMotorista: this.converterInteiro(registro[colunas.idMotorista]) ?? 0,
      dataInclusao:
        this.converterData(registro[colunas.dataInclusao]) ?? new Date(0),
      dataMovi: this.converterData(registro[colunas.dataMovi]) ?? new Date(0),
      situacao: colunas.situacao
        ? this.converterTexto(registro[colunas.situacao])?.toUpperCase() ?? 'A'
        : 'A',
      tipoEngate:
        this.converterTexto(registro[colunas.tipoEngate])?.toUpperCase() ??
        'E',
      usuarioAtualizacao: colunas.usuarioAtualizacao
        ? this.converterTexto(registro[colunas.usuarioAtualizacao])
        : null,
      placa2: colunas.placa2
        ? this.converterTexto(registro[colunas.placa2])?.toUpperCase() ?? null
        : null,
      placa3: colunas.placa3
        ? this.converterTexto(registro[colunas.placa3])?.toUpperCase() ?? null
        : null,
      placa4: colunas.placa4
        ? this.converterTexto(registro[colunas.placa4])?.toUpperCase() ?? null
        : null,
      criadoEm: colunas.criadoEm
        ? this.converterData(registro[colunas.criadoEm])
        : null,
      atualizadoEm: colunas.atualizadoEm
        ? this.converterData(registro[colunas.atualizadoEm])
        : null,
    };
  }

  private montarEstadoFinalAtualizacao(
    atual: EngateNormalizado,
    payload: ReturnType<EngateDesengateService['normalizarAtualizacao']>,
  ): EngatePersistencia {
    return {
      idVeiculo: payload.idVeiculo ?? atual.idVeiculo,
      idMotorista: payload.idMotorista ?? atual.idMotorista,
      dataInclusao: payload.dataInclusao ?? atual.dataInclusao,
      dataMovi: payload.dataMovi ?? atual.dataMovi,
      situacao: payload.situacao ?? atual.situacao,
      tipoEngate: payload.tipoEngate ?? atual.tipoEngate,
      usuarioAtualizacao:
        payload.usuarioAtualizacao ??
        this.normalizarUsuario(atual.usuarioAtualizacao ?? 'APP_WEB'),
      placa2:
        typeof payload.placa2 === 'undefined' ? atual.placa2 : payload.placa2,
      placa3:
        typeof payload.placa3 === 'undefined' ? atual.placa3 : payload.placa3,
      placa4:
        typeof payload.placa4 === 'undefined' ? atual.placa4 : payload.placa4,
    };
  }

  private async validarRecursosParaEngate(
    manager: EntityManager,
    tabela: string,
    colunas: MapaColunasEngate,
    idEmpresa: number,
    payload: EngatePersistencia,
    ignorarIdEngate?: number,
  ) {
    this.validarDuplicidadeInterna(payload);

    if (payload.tipoEngate !== 'E' || payload.situacao === 'I') {
      return;
    }

    const { placaPorVeiculoId, motoristaOcupado, placaOcupada } =
      await this.carregarEstadoAtualRecursos(
        manager,
        tabela,
        colunas,
        idEmpresa,
        ignorarIdEngate,
      );

    const placasSelecionadas = [payload.placa2, payload.placa3, payload.placa4]
      .filter((placa): placa is string => Boolean(placa))
      .map((placa) => placa.trim().toUpperCase());

    if (motoristaOcupado.has(payload.idMotorista)) {
      throw new BadRequestException(
        'Este motorista ja esta engatado em outro veiculo. Faca o desengate antes de reutiliza-lo.',
      );
    }

    for (const placa of placasSelecionadas) {
      const veiculoRelacionado = placaPorVeiculoId.get(placa) ?? null;
      if (placaOcupada.has(placa) || (veiculoRelacionado !== null && placaOcupada.has(`ID:${veiculoRelacionado}`))) {
        throw new BadRequestException(
          `A placa ${placa} ja esta engatada em outro veiculo. Faca o desengate antes de reutiliza-la.`,
        );
      }
    }
  }

  private validarDuplicidadeInterna(payload: Pick<EngatePersistencia, 'placa2' | 'placa3' | 'placa4'>) {
    const placas = [payload.placa2, payload.placa3, payload.placa4]
      .filter((placa): placa is string => Boolean(placa))
      .map((placa) => placa.trim().toUpperCase());

    if (new Set(placas).size !== placas.length) {
      throw new BadRequestException(
        'As placas 2, 3 e 4 nao podem se repetir na mesma movimentacao.',
      );
    }
  }

  private async carregarEstadoAtualRecursos(
    manager: EntityManager,
    tabela: string,
    colunas: MapaColunasEngate,
    idEmpresa: number,
    ignorarIdEngate?: number,
  ) {
    const filtros: string[] = [];
    const valores: Array<string | number> = [];

    if (colunas.idEmpresa) {
      valores.push(String(idEmpresa));
      filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
    }

    if (ignorarIdEngate !== undefined) {
      valores.push(ignorarIdEngate);
      filtros.push(`${this.quote(colunas.idEngate)} <> $${valores.length}`);
    }

    const sqlHistorico = `
      SELECT *
      FROM app.${this.quoteIdentifier(tabela)}
      ${filtros.length > 0 ? `WHERE ${filtros.join(' AND ')}` : ''}
      ORDER BY ${this.quote(colunas.dataMovi)} DESC, ${this.quote(colunas.idEngate)} DESC
    `;

    const sqlVeiculos = `
      SELECT id_veiculo, placa
      FROM app.veiculo
      ${colunas.idEmpresa ? 'WHERE id_empresa = $1' : ''}
    `;

    const [rowsHistorico, rowsVeiculos] = await Promise.all([
      manager.query(sqlHistorico, valores) as Promise<RegistroBanco[]>,
      manager.query(sqlVeiculos, colunas.idEmpresa ? [String(idEmpresa)] : []) as Promise<
        Array<{ id_veiculo?: unknown; placa?: unknown }>
      >,
    ]);

    const placaPrincipalPorId = new Map<number, string>();
    const placaPorVeiculoId = new Map<string, number>();

    for (const row of rowsVeiculos) {
      const idVeiculo = this.converterInteiro(row.id_veiculo);
      const placa = this.converterTexto(row.placa)?.toUpperCase() ?? '';
      if (!idVeiculo || !placa) {
        continue;
      }

      placaPrincipalPorId.set(idVeiculo, placa);
      placaPorVeiculoId.set(placa, idVeiculo);
    }

    const motoristaOcupado = new Set<number>();
    const placaOcupada = new Set<string>();
    const ultimoMovimentoPorVeiculo = new Map<number, RegistroBanco>();
    const ultimoMovimentoPorMotorista = new Map<number, RegistroBanco>();

    for (const row of rowsHistorico) {
      const idMotorista = this.converterInteiro(row[colunas.idMotorista]);
      const idVeiculo = this.converterInteiro(row[colunas.idVeiculo]);

      if (idVeiculo && !ultimoMovimentoPorVeiculo.has(idVeiculo)) {
        ultimoMovimentoPorVeiculo.set(idVeiculo, row);
      }

      if (idMotorista && !ultimoMovimentoPorMotorista.has(idMotorista)) {
        ultimoMovimentoPorMotorista.set(idMotorista, row);
      }
    }

    for (const row of ultimoMovimentoPorMotorista.values()) {
      const idMotorista = this.converterInteiro(row[colunas.idMotorista]);
      const tipoEngate =
        this.converterTexto(row[colunas.tipoEngate])?.toUpperCase() ?? 'E';
      const situacao = colunas.situacao
        ? this.converterTexto(row[colunas.situacao])?.toUpperCase() ?? 'A'
        : 'A';

      if (idMotorista && tipoEngate === 'E' && situacao !== 'I') {
        motoristaOcupado.add(idMotorista);
      }
    }

    for (const row of ultimoMovimentoPorVeiculo.values()) {
      const tipoEngate =
        this.converterTexto(row[colunas.tipoEngate])?.toUpperCase() ?? 'E';
      const situacao = colunas.situacao
        ? this.converterTexto(row[colunas.situacao])?.toUpperCase() ?? 'A'
        : 'A';

      if (tipoEngate !== 'E' || situacao === 'I') {
        continue;
      }

      const idVeiculo = this.converterInteiro(row[colunas.idVeiculo]);
      const recursosPlaca = [
        colunas.placa2 ? this.converterTexto(row[colunas.placa2])?.toUpperCase() ?? '' : '',
        colunas.placa3 ? this.converterTexto(row[colunas.placa3])?.toUpperCase() ?? '' : '',
        colunas.placa4 ? this.converterTexto(row[colunas.placa4])?.toUpperCase() ?? '' : '',
      ].filter(Boolean);

      for (const placa of recursosPlaca) {
        placaOcupada.add(placa);
      }

      if (idVeiculo) {
        placaOcupada.add(`ID:${idVeiculo}`);
      }
    }

    return { placaPorVeiculoId, motoristaOcupado, placaOcupada };
  }

  private normalizarCriacao(
    dados: CriarEngateDesengateDto,
    usuarioJwt: JwtUsuarioPayload,
  ): EngatePersistencia {
    return {
      idVeiculo: dados.idVeiculo,
      idMotorista: dados.idMotorista,
      dataInclusao: new Date(dados.dataInclusao),
      dataMovi: new Date(dados.dataMovi),
      situacao: (dados.situacao ?? 'A').trim().toUpperCase(),
      tipoEngate: dados.tipoEngate.trim().toUpperCase(),
      usuarioAtualizacao: this.normalizarUsuario(
        dados.usuarioAtualizacao ?? usuarioJwt.email ?? 'APP_WEB',
      ),
      placa2: this.normalizarTextoOpcional(dados.placa2),
      placa3: this.normalizarTextoOpcional(dados.placa3),
      placa4: this.normalizarTextoOpcional(dados.placa4),
    };
  }

  private normalizarAtualizacao(
    dados: AtualizarEngateDesengateDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    return {
      idVeiculo: dados.idVeiculo,
      idMotorista: dados.idMotorista,
      dataInclusao: dados.dataInclusao ? new Date(dados.dataInclusao) : undefined,
      dataMovi: dados.dataMovi ? new Date(dados.dataMovi) : undefined,
      situacao: dados.situacao?.trim().toUpperCase(),
      tipoEngate: dados.tipoEngate?.trim().toUpperCase(),
      usuarioAtualizacao:
        dados.usuarioAtualizacao !== undefined
          ? this.normalizarUsuario(dados.usuarioAtualizacao)
          : this.normalizarUsuario(usuarioJwt.email ?? 'APP_WEB'),
      placa2:
        dados.placa2 !== undefined
          ? this.normalizarTextoOpcional(dados.placa2)
          : undefined,
      placa3:
        dados.placa3 !== undefined
          ? this.normalizarTextoOpcional(dados.placa3)
          : undefined,
      placa4:
        dados.placa4 !== undefined
          ? this.normalizarTextoOpcional(dados.placa4)
          : undefined,
    };
  }

  private normalizarTextoOpcional(valor: string | undefined) {
    const texto = valor?.trim().toUpperCase();
    return texto ? texto : null;
  }

  private normalizarUsuario(valor: string) {
    const texto = valor.trim();
    return texto ? texto.slice(0, 120) : 'APP_WEB';
  }

  private converterTexto(valor: unknown) {
    return typeof valor === 'string' ? valor.trim() : null;
  }

  private converterInteiro(valor: unknown) {
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

  private converterData(valor: unknown) {
    if (!valor) {
      return null;
    }

    const data = valor instanceof Date ? valor : new Date(String(valor));
    return Number.isNaN(data.getTime()) ? null : data;
  }

  private quote(coluna: string) {
    return `"${coluna.replace(/"/g, '""')}"`;
  }

  private quoteIdentifier(valor: string) {
    return valor.replace(/"/g, '""');
  }

  private tratarErroPersistencia(
    error: unknown,
    acao: 'cadastrar' | 'atualizar',
  ): never {
    if (
      error instanceof BadRequestException ||
      error instanceof NotFoundException
    ) {
      throw error;
    }

    if (error instanceof QueryFailedError) {
      const erroPg = error as QueryFailedError & {
        code?: string;
        message?: string;
      };

      this.logger.error(
        `Falha ao ${acao} movimentacao de engate/desengate. code=${erroPg.code ?? 'N/A'} message=${erroPg.message ?? 'Erro desconhecido'}`,
      );

      if (erroPg.code === '42P01') {
        throw new BadRequestException(
          'Tabela de engate/desengate nao encontrada.',
        );
      }

      throw new BadRequestException(
        `Nao foi possivel ${acao} a movimentacao neste momento.`,
      );
    }

    throw error;
  }
}
