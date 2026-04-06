import {
  BadRequestException,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { DataSource, EntityManager, QueryFailedError } from 'typeorm';
import { JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { configurarContextoEmpresaRls } from '../common/database/rls.util';
import { AtualizarMultaDto } from './dto/atualizar-multa.dto';
import { CriarMultaDto } from './dto/criar-multa.dto';
import { FiltroMultasDto } from './dto/filtro-multas.dto';

type RegistroBanco = Record<string, unknown>;

type MapaColunasMulta = {
  idMulta: string;
  idEmpresa: string | null;
  usuarioAtualizacao: string | null;
  idMotorista: string;
  idVeiculo: string;
  dataMulta: string;
  dataVencimento: string | null;
  horaMulta: string | null;
  local: string | null;
  logradouro: string | null;
  cidade: string | null;
  estado: string | null;
  cep: string | null;
  rodovia: string | null;
  kmRodovia: string | null;
  descricao: string | null;
  valor: string;
  pontos: string | null;
  status: string | null;
  orgaoAutuador: string | null;
  numeroAuto: string | null;
  dataPagamento: string | null;
  desconto: string | null;
  juros: string | null;
  valorPago: string | null;
  criadoEm: string | null;
  atualizadoEm: string | null;
};

type MultaNormalizada = {
  idMulta: number;
  idEmpresa: number | null;
  usuarioAtualizacao: string | null;
  idMotorista: number;
  idVeiculo: number;
  dataMulta: Date;
  dataVencimento: Date | null;
  horaMulta: string | null;
  local: string | null;
  logradouro: string | null;
  cidade: string | null;
  estado: string | null;
  cep: string | null;
  rodovia: string | null;
  kmRodovia: string | null;
  descricao: string | null;
  valor: number;
  pontos: number | null;
  status: string;
  orgaoAutuador: string | null;
  numeroAuto: string | null;
  dataPagamento: Date | null;
  desconto: number | null;
  juros: number | null;
  valorPago: number | null;
  criadoEm: Date | null;
  atualizadoEm: Date | null;
};

type MultaPersistencia = {
  usuarioAtualizacao: string;
  idMotorista: number;
  idVeiculo: number;
  dataMulta: Date;
  dataVencimento: Date | null;
  horaMulta: string | null;
  local: string | null;
  logradouro: string | null;
  cidade: string | null;
  estado: string | null;
  cep: string | null;
  rodovia: string | null;
  kmRodovia: string | null;
  descricao: string | null;
  valor: number;
  pontos: number | null;
  status: string;
  orgaoAutuador: string | null;
  numeroAuto: string | null;
  dataPagamento: Date | null;
  desconto: number | null;
  juros: number | null;
  valorPago: number | null;
};

@Injectable()
export class MultasService {
  private readonly logger = new Logger(MultasService.name);

  constructor(private readonly dataSource: DataSource) {}

  async listarTodas(idEmpresa: number) {
    try {
      return await this.executarComRls(idEmpresa, async (manager, colunas) => {
        const where = this.montarFiltroEmpresa(colunas, idEmpresa);

        const sql = [
          'SELECT * FROM app.multas',
          where.clausula,
          `ORDER BY ${this.quote(colunas.dataMulta)} DESC, ${this.quote(colunas.idMulta)} DESC`,
        ]
          .filter(Boolean)
          .join('\n');

        const registros = (await manager.query(sql, where.valores)) as RegistroBanco[];
        const multas = registros.map((registro) => this.mapearRegistro(registro, colunas));

        return {
          sucesso: true,
          total: multas.length,
          multas,
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'listar');
    }
  }

  async listarComFiltro(idEmpresa: number, filtro: FiltroMultasDto) {
    this.validarIntervalosDoFiltro(filtro);

    try {
      return await this.executarComRls(idEmpresa, async (manager, colunas) => {
        const filtros: string[] = [];
        const valores: Array<string | number> = [];

        this.adicionarFiltroEmpresa(colunas, idEmpresa, filtros, valores);

        if (filtro.idMulta !== undefined) {
          valores.push(filtro.idMulta);
          filtros.push(`${this.quote(colunas.idMulta)} = $${valores.length}`);
        }

        if (filtro.idMotorista !== undefined) {
          valores.push(filtro.idMotorista);
          filtros.push(`${this.quote(colunas.idMotorista)} = $${valores.length}`);
        }

        if (filtro.idVeiculo !== undefined) {
          valores.push(filtro.idVeiculo);
          filtros.push(`${this.quote(colunas.idVeiculo)} = $${valores.length}`);
        }

        if (filtro.status && colunas.status) {
          valores.push(filtro.status);
          filtros.push(
            `UPPER(COALESCE(${this.quote(colunas.status)}::text, '')) = UPPER($${valores.length})`,
          );
        }

        if (filtro.texto) {
          valores.push(`%${filtro.texto}%`);
          const camposTexto = [
            colunas.local,
            colunas.logradouro,
            colunas.cidade,
            colunas.descricao,
            colunas.orgaoAutuador,
            colunas.numeroAuto,
            colunas.rodovia,
          ].filter((campo): campo is string => Boolean(campo));

          if (camposTexto.length > 0) {
            filtros.push(
              `(${camposTexto
                .map(
                  (campo) =>
                    `COALESCE(${this.quote(campo)}::text, '') ILIKE $${valores.length}`,
                )
                .join(' OR ')})`,
            );
          }
        }

        if (filtro.dataDe) {
          valores.push(filtro.dataDe);
          filtros.push(`${this.quote(colunas.dataMulta)} >= $${valores.length}`);
        }

        if (filtro.dataAte) {
          valores.push(filtro.dataAte);
          filtros.push(`${this.quote(colunas.dataMulta)} <= $${valores.length}`);
        }

        if (filtro.valorMin !== undefined) {
          valores.push(filtro.valorMin);
          filtros.push(`${this.quote(colunas.valor)} >= $${valores.length}`);
        }

        if (filtro.valorMax !== undefined) {
          valores.push(filtro.valorMax);
          filtros.push(`${this.quote(colunas.valor)} <= $${valores.length}`);
        }

        if (filtro.pontosMin !== undefined && colunas.pontos) {
          valores.push(filtro.pontosMin);
          filtros.push(`${this.quote(colunas.pontos)} >= $${valores.length}`);
        }

        if (filtro.pontosMax !== undefined && colunas.pontos) {
          valores.push(filtro.pontosMax);
          filtros.push(`${this.quote(colunas.pontos)} <= $${valores.length}`);
        }

        const whereSql = filtros.length > 0 ? `WHERE ${filtros.join(' AND ')}` : '';
        const pagina = filtro.pagina ?? 1;
        const limite = filtro.limite ?? 50;
        const offset = (pagina - 1) * limite;
        const ordem = filtro.ordem ?? 'DESC';
        const colunaOrdenacao = this.resolverColunaOrdenacao(filtro.ordenarPor, colunas);

        const sqlCount = `
          SELECT COUNT(1)::int AS total
          FROM app.multas
          ${whereSql}
        `;

        const sqlDados = `
          SELECT *
          FROM app.multas
          ${whereSql}
          ORDER BY ${this.quote(colunaOrdenacao)} ${ordem}, ${this.quote(colunas.idMulta)} DESC
          LIMIT $${valores.length + 1}
          OFFSET $${valores.length + 2}
        `;

        const countRows = (await manager.query(sqlCount, valores)) as Array<{ total: number }>;
        const registros = (await manager.query(sqlDados, [
          ...valores,
          limite,
          offset,
        ])) as RegistroBanco[];

        const total = Number(countRows[0]?.total ?? 0);
        const multas = registros.map((registro) => this.mapearRegistro(registro, colunas));

        return {
          sucesso: true,
          paginaAtual: pagina,
          limite,
          total,
          totalPaginas: total > 0 ? Math.ceil(total / limite) : 0,
          multas,
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'listar');
    }
  }

  async buscarPorId(idEmpresa: number, idMulta: number) {
    try {
      return await this.executarComRls(idEmpresa, async (manager, colunas) => {
        const registro = await this.buscarRegistroPorIdOuFalhar(
          manager,
          colunas,
          idEmpresa,
          idMulta,
        );

        return {
          sucesso: true,
          multa: this.mapearRegistro(registro, colunas),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'consultar');
    }
  }

  async cadastrar(
    idEmpresa: number,
    dados: CriarMultaDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    try {
      return this.executarComRls(idEmpresa, async (manager, colunas) => {
        const payload = this.normalizarCriacao(dados, usuarioJwt);
        this.validarConsistencia(payload);
        await this.validarRelacionamentos(manager, idEmpresa, payload);

        const campos = [colunas.idMotorista, colunas.idVeiculo, colunas.dataMulta, colunas.valor];
        const valores: Array<string | number | Date | null> = [
          payload.idMotorista,
          payload.idVeiculo,
          payload.dataMulta,
          payload.valor,
        ];

        this.adicionarCampoOpcional(
          campos,
          valores,
          colunas.dataVencimento,
          payload.dataVencimento,
        );
        this.adicionarCampoOpcional(campos, valores, colunas.horaMulta, payload.horaMulta);
        this.adicionarCampoOpcional(campos, valores, colunas.local, payload.local);
        this.adicionarCampoOpcional(campos, valores, colunas.logradouro, payload.logradouro);
        this.adicionarCampoOpcional(campos, valores, colunas.cidade, payload.cidade);
        this.adicionarCampoOpcional(campos, valores, colunas.estado, payload.estado);
        this.adicionarCampoOpcional(campos, valores, colunas.cep, payload.cep);
        this.adicionarCampoOpcional(campos, valores, colunas.rodovia, payload.rodovia);
        this.adicionarCampoOpcional(campos, valores, colunas.kmRodovia, payload.kmRodovia);
        this.adicionarCampoOpcional(campos, valores, colunas.descricao, payload.descricao);
        this.adicionarCampoOpcional(campos, valores, colunas.pontos, payload.pontos);
        this.adicionarCampoOpcional(campos, valores, colunas.status, payload.status);
        this.adicionarCampoOpcional(campos, valores, colunas.orgaoAutuador, payload.orgaoAutuador);
        this.adicionarCampoOpcional(campos, valores, colunas.numeroAuto, payload.numeroAuto);
        this.adicionarCampoOpcional(campos, valores, colunas.dataPagamento, payload.dataPagamento);
        this.adicionarCampoOpcional(campos, valores, colunas.desconto, payload.desconto);
        this.adicionarCampoOpcional(campos, valores, colunas.juros, payload.juros);
        this.adicionarCampoOpcional(campos, valores, colunas.valorPago, payload.valorPago);
        this.adicionarCampoOpcional(
          campos,
          valores,
          colunas.usuarioAtualizacao,
          payload.usuarioAtualizacao,
        );

        if (colunas.idEmpresa) {
          campos.push(colunas.idEmpresa);
          valores.push(String(idEmpresa));
        }

        const sql = `
          INSERT INTO app.multas (${campos.map((campo) => this.quote(campo)).join(', ')})
          VALUES (${valores.map((_, index) => `$${index + 1}`).join(', ')})
          RETURNING *
        `;

        const rows = (await manager.query(sql, valores)) as RegistroBanco[];
        const registro = rows[0];
        if (!registro) {
          throw new BadRequestException('Falha ao cadastrar multa (retorno vazio da base).');
        }

        return {
          sucesso: true,
          mensagem: 'Multa cadastrada com sucesso.',
          multa: this.mapearRegistro(registro, colunas),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'cadastrar');
    }
  }

  async atualizar(
    idEmpresa: number,
    idMulta: number,
    dados: AtualizarMultaDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    try {
      return this.executarComRls(idEmpresa, async (manager, colunas) => {
        const registroAtual = await this.buscarRegistroPorIdOuFalhar(
          manager,
          colunas,
          idEmpresa,
          idMulta,
        );
        const atual = this.mapearRegistro(registroAtual, colunas);
        const payload = this.normalizarAtualizacao(dados, atual, usuarioJwt);

        this.validarConsistencia(payload);
        await this.validarRelacionamentos(manager, idEmpresa, payload);

        const sets: string[] = [];
        const valores: Array<string | number | Date | null> = [];

        const adicionarSet = (
          coluna: string | null,
          valor: string | number | Date | null,
        ) => {
          if (!coluna) {
            return;
          }
          valores.push(valor);
          sets.push(`${this.quote(coluna)} = $${valores.length}`);
        };

        adicionarSet(colunas.idMotorista, payload.idMotorista);
        adicionarSet(colunas.idVeiculo, payload.idVeiculo);
        adicionarSet(colunas.dataMulta, payload.dataMulta);
        adicionarSet(colunas.dataVencimento, payload.dataVencimento);
        adicionarSet(colunas.horaMulta, payload.horaMulta);
        adicionarSet(colunas.local, payload.local);
        adicionarSet(colunas.logradouro, payload.logradouro);
        adicionarSet(colunas.cidade, payload.cidade);
        adicionarSet(colunas.estado, payload.estado);
        adicionarSet(colunas.cep, payload.cep);
        adicionarSet(colunas.rodovia, payload.rodovia);
        adicionarSet(colunas.kmRodovia, payload.kmRodovia);
        adicionarSet(colunas.descricao, payload.descricao);
        adicionarSet(colunas.valor, payload.valor);
        adicionarSet(colunas.pontos, payload.pontos);
        adicionarSet(colunas.status, payload.status);
        adicionarSet(colunas.orgaoAutuador, payload.orgaoAutuador);
        adicionarSet(colunas.numeroAuto, payload.numeroAuto);
        adicionarSet(colunas.dataPagamento, payload.dataPagamento);
        adicionarSet(colunas.desconto, payload.desconto);
        adicionarSet(colunas.juros, payload.juros);
        adicionarSet(colunas.valorPago, payload.valorPago);
        adicionarSet(colunas.usuarioAtualizacao, payload.usuarioAtualizacao);

        if (sets.length === 0) {
          throw new BadRequestException('Nenhum campo valido foi informado para atualizar a multa.');
        }

        valores.push(idMulta);
        const filtros = [`${this.quote(colunas.idMulta)} = $${valores.length}`];

        if (colunas.idEmpresa) {
          valores.push(String(idEmpresa));
          filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
        }

        const sql = `
          UPDATE app.multas
          SET ${sets.join(', ')}
          WHERE ${filtros.join(' AND ')}
          RETURNING *
        `;

        const rows = (await manager.query(sql, valores)) as RegistroBanco[];
        const registro = rows[0];
        if (!registro) {
          throw new NotFoundException('Multa nao encontrada para a empresa logada.');
        }

        return {
          sucesso: true,
          mensagem: 'Multa atualizada com sucesso.',
          multa: this.mapearRegistro(registro, colunas),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'atualizar');
    }
  }

  async remover(idEmpresa: number, idMulta: number) {
    return this.executarComRls(idEmpresa, async (manager, colunas) => {
      const valores: Array<string | number> = [idMulta];
      const filtros = [`${this.quote(colunas.idMulta)} = $1`];

      if (colunas.idEmpresa) {
        valores.push(String(idEmpresa));
        filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
      }

      const sql = `
        DELETE FROM app.multas
        WHERE ${filtros.join(' AND ')}
        RETURNING *
      `;

      const rows = (await manager.query(sql, valores)) as RegistroBanco[];
      if (!rows[0]) {
        throw new NotFoundException('Multa nao encontrada para a empresa logada.');
      }

      return {
        sucesso: true,
        mensagem: 'Multa removida com sucesso.',
        idMulta,
      };
    });
  }

  private async executarComRls<T>(
    idEmpresa: number,
    callback: (manager: EntityManager, colunas: MapaColunasMulta) => Promise<T>,
  ): Promise<T> {
    return this.dataSource.manager.transaction(async (manager) => {
      await configurarContextoEmpresaRls(manager, idEmpresa);
      const colunas = await this.carregarMapaColunas(manager);
      return callback(manager, colunas);
    });
  }

  private async carregarMapaColunas(manager: EntityManager): Promise<MapaColunasMulta> {
    const rows = (await manager.query(`
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = 'app'
        AND table_name = 'multas'
    `)) as Array<{ column_name?: string }>;

    if (rows.length === 0) {
      throw new BadRequestException('Tabela app.multas nao encontrada.');
    }

    const set = new Set(
      rows
        .map((row) => (typeof row.column_name === 'string' ? row.column_name : ''))
        .filter((value) => value.length > 0),
    );

    return {
      idMulta: this.encontrarColuna(set, ['id_multa', 'id'], 'id da multa')!,
      idEmpresa: this.encontrarColuna(set, ['id_empresa'], 'id da empresa', false),
      usuarioAtualizacao: this.encontrarColuna(
        set,
        ['usuario_atualizacao', 'usuario_update'],
        'usuario de atualizacao',
        false,
      ),
      idMotorista: this.encontrarColuna(set, ['id_motorista', 'motorista_id'], 'id do motorista')!,
      idVeiculo: this.encontrarColuna(set, ['id_veiculo', 'veiculo_id'], 'id do veiculo')!,
      dataMulta: this.encontrarColuna(
        set,
        ['data_multa', 'data', 'data_lancamento'],
        'data da multa',
      )!,
      dataVencimento: this.encontrarColuna(
        set,
        ['data_vencimento', 'vencimento'],
        'data de vencimento',
        false,
      ),
      horaMulta: this.encontrarColuna(set, ['hora_multa', 'hora'], 'hora da multa', false),
      local: this.encontrarColuna(set, ['local'], 'local', false),
      logradouro: this.encontrarColuna(set, ['logradouro'], 'logradouro', false),
      cidade: this.encontrarColuna(set, ['cidade'], 'cidade', false),
      estado: this.encontrarColuna(set, ['estado', 'uf'], 'estado', false),
      cep: this.encontrarColuna(set, ['cep'], 'cep', false),
      rodovia: this.encontrarColuna(set, ['rodovia'], 'rodovia', false),
      kmRodovia: this.encontrarColuna(set, ['km_rodovia', 'km'], 'km da rodovia', false),
      descricao: this.encontrarColuna(set, ['descricao', 'observacao', 'obs'], 'descricao', false),
      valor: this.encontrarColuna(set, ['valor', 'valor_total'], 'valor')!,
      pontos: this.encontrarColuna(set, ['pontos'], 'pontos', false),
      status: this.encontrarColuna(set, ['status'], 'status', false),
      orgaoAutuador: this.encontrarColuna(
        set,
        ['orgao_autuador', 'orgao'],
        'orgao autuador',
        false,
      ),
      numeroAuto: this.encontrarColuna(set, ['numero_auto', 'auto_numero'], 'numero do auto', false),
      dataPagamento: this.encontrarColuna(
        set,
        ['data_pagamento'],
        'data de pagamento',
        false,
      ),
      desconto: this.encontrarColuna(set, ['desconto'], 'desconto', false),
      juros: this.encontrarColuna(set, ['juros'], 'juros', false),
      valorPago: this.encontrarColuna(set, ['valor_pago'], 'valor pago', false),
      criadoEm: this.encontrarColuna(set, ['criado_em', 'created_at'], '', false),
      atualizadoEm: this.encontrarColuna(set, ['atualizado_em', 'updated_at'], '', false),
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
        `Estrutura da tabela app.multas invalida: coluna de ${descricao} nao encontrada.`,
      );
    }

    return null;
  }

  private async buscarRegistroPorIdOuFalhar(
    manager: EntityManager,
    colunas: MapaColunasMulta,
    idEmpresa: number,
    idMulta: number,
  ) {
    const valores: Array<string | number> = [idMulta];
    const filtros = [`${this.quote(colunas.idMulta)} = $1`];

    if (colunas.idEmpresa) {
      valores.push(String(idEmpresa));
      filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
    }

    const sql = `
      SELECT *
      FROM app.multas
      WHERE ${filtros.join(' AND ')}
      LIMIT 1
    `;

    const rows = (await manager.query(sql, valores)) as RegistroBanco[];
    const registro = rows[0];
    if (!registro) {
      throw new NotFoundException('Multa nao encontrada para a empresa logada.');
    }

    return registro;
  }

  private normalizarCriacao(
    dados: CriarMultaDto,
    usuarioJwt: JwtUsuarioPayload,
  ): MultaPersistencia {
    return {
      usuarioAtualizacao: this.normalizarUsuario(usuarioJwt.email),
      idMotorista: dados.idMotorista,
      idVeiculo: dados.idVeiculo,
      dataMulta: this.normalizarDataSomenteData(dados.dataMulta),
      dataVencimento: this.normalizarDataOpcional(dados.dataVencimento),
      horaMulta: this.normalizarHoraOpcional(dados.horaMulta),
      local: this.normalizarTextoOpcional(dados.local),
      logradouro: this.normalizarTextoOpcional(dados.logradouro),
      cidade: this.normalizarTextoOpcional(dados.cidade),
      estado: this.normalizarTextoOpcional(dados.estado),
      cep: this.normalizarTextoOpcional(dados.cep),
      rodovia: this.normalizarTextoOpcional(dados.rodovia),
      kmRodovia: this.normalizarTextoOpcional(dados.kmRodovia),
      descricao: this.normalizarTextoOpcional(dados.descricao),
      valor: dados.valor,
      dataPagamento: null,
      desconto: null,
      juros: null,
      valorPago: null,
      pontos: dados.pontos ?? null,
      status: this.normalizarStatus(dados.status),
      orgaoAutuador: this.normalizarTextoOpcional(dados.orgaoAutuador),
      numeroAuto: this.normalizarTextoOpcional(dados.numeroAuto),
    };
  }

  private normalizarAtualizacao(
    dados: AtualizarMultaDto,
    atual: MultaNormalizada,
    usuarioJwt: JwtUsuarioPayload,
  ): MultaPersistencia {
    return {
      usuarioAtualizacao: this.normalizarUsuario(usuarioJwt.email),
      idMotorista: dados.idMotorista ?? atual.idMotorista,
      idVeiculo: dados.idVeiculo ?? atual.idVeiculo,
      dataMulta: dados.dataMulta ? this.normalizarDataSomenteData(dados.dataMulta) : atual.dataMulta,
      dataVencimento:
        dados.dataVencimento !== undefined
          ? this.normalizarDataOpcional(dados.dataVencimento)
          : atual.dataVencimento,
      horaMulta:
        dados.horaMulta !== undefined
          ? this.normalizarHoraOpcional(dados.horaMulta)
          : atual.horaMulta,
      local:
        dados.local !== undefined
          ? this.normalizarTextoOpcional(dados.local)
          : atual.local,
      logradouro:
        dados.logradouro !== undefined
          ? this.normalizarTextoOpcional(dados.logradouro)
          : atual.logradouro,
      cidade:
        dados.cidade !== undefined
          ? this.normalizarTextoOpcional(dados.cidade)
          : atual.cidade,
      estado:
        dados.estado !== undefined
          ? this.normalizarTextoOpcional(dados.estado)
          : atual.estado,
      cep: dados.cep !== undefined ? this.normalizarTextoOpcional(dados.cep) : atual.cep,
      rodovia:
        dados.rodovia !== undefined
          ? this.normalizarTextoOpcional(dados.rodovia)
          : atual.rodovia,
      kmRodovia:
        dados.kmRodovia !== undefined
          ? this.normalizarTextoOpcional(dados.kmRodovia)
          : atual.kmRodovia,
      descricao:
        dados.descricao !== undefined
          ? this.normalizarTextoOpcional(dados.descricao)
          : atual.descricao,
      valor: dados.valor ?? atual.valor,
      dataPagamento:
        dados.dataPagamento !== undefined
          ? this.normalizarDataOpcional(dados.dataPagamento)
          : atual.dataPagamento,
      desconto:
        dados.desconto !== undefined
          ? this.normalizarNumeroOpcional(dados.desconto)
          : atual.desconto,
      juros:
        dados.juros !== undefined
          ? this.normalizarNumeroOpcional(dados.juros)
          : atual.juros,
      valorPago:
        dados.valorPago !== undefined
          ? this.normalizarNumeroOpcional(dados.valorPago)
          : atual.valorPago,
      pontos: dados.pontos !== undefined ? dados.pontos : atual.pontos,
      status:
        dados.status !== undefined ? this.normalizarStatus(dados.status) : atual.status,
      orgaoAutuador:
        dados.orgaoAutuador !== undefined
          ? this.normalizarTextoOpcional(dados.orgaoAutuador)
          : atual.orgaoAutuador,
      numeroAuto:
        dados.numeroAuto !== undefined
          ? this.normalizarTextoOpcional(dados.numeroAuto)
          : atual.numeroAuto,
    };
  }

  private validarConsistencia(payload: MultaPersistencia) {
    if (!payload.usuarioAtualizacao || payload.usuarioAtualizacao.length < 2) {
      throw new BadRequestException('usuarioAtualizacao invalido.');
    }

    if (!Number.isFinite(payload.idMotorista) || payload.idMotorista <= 0) {
      throw new BadRequestException('idMotorista invalido.');
    }

    if (!Number.isFinite(payload.idVeiculo) || payload.idVeiculo <= 0) {
      throw new BadRequestException('idVeiculo invalido.');
    }

    if (!(payload.dataMulta instanceof Date) || Number.isNaN(payload.dataMulta.getTime())) {
      throw new BadRequestException('dataMulta invalida.');
    }

    if (!Number.isFinite(payload.valor) || payload.valor < 0) {
      throw new BadRequestException('valor da multa invalido.');
    }

    if (
      payload.desconto !== null &&
      (!Number.isFinite(payload.desconto) || payload.desconto < 0)
    ) {
      throw new BadRequestException('desconto invalido.');
    }

    if (
      payload.juros !== null &&
      (!Number.isFinite(payload.juros) || payload.juros < 0)
    ) {
      throw new BadRequestException('juros invalido.');
    }

    if (
      payload.valorPago !== null &&
      (!Number.isFinite(payload.valorPago) || payload.valorPago < 0)
    ) {
      throw new BadRequestException('valorPago invalido.');
    }

    if (
      payload.status === 'PAGA' &&
      (!payload.dataPagamento || payload.valorPago === null)
    ) {
      throw new BadRequestException(
        'Para encerrar a multa como paga, informe dataPagamento e valorPago.',
      );
    }

    if (
      payload.pontos !== null &&
      (!Number.isFinite(payload.pontos) || payload.pontos < 0)
    ) {
      throw new BadRequestException('pontos invalidos.');
    }

    if (!payload.status) {
      throw new BadRequestException('status da multa invalido.');
    }
  }

  private validarIntervalosDoFiltro(filtro: FiltroMultasDto) {
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
      filtro.pontosMin !== undefined &&
      filtro.pontosMax !== undefined &&
      filtro.pontosMax < filtro.pontosMin
    ) {
      throw new BadRequestException(
        'Filtro invalido: pontosMax deve ser maior ou igual a pontosMin.',
      );
    }
  }

  private resolverColunaOrdenacao(
    ordenarPor: FiltroMultasDto['ordenarPor'],
    colunas: MapaColunasMulta,
  ) {
    if (ordenarPor === 'id_multa') return colunas.idMulta;
    if (ordenarPor === 'id_motorista') return colunas.idMotorista;
    if (ordenarPor === 'id_veiculo') return colunas.idVeiculo;
    if (ordenarPor === 'valor') return colunas.valor;
    if (ordenarPor === 'pontos' && colunas.pontos) return colunas.pontos;
    if (ordenarPor === 'status' && colunas.status) return colunas.status;
    if (ordenarPor === 'criado_em' && colunas.criadoEm) return colunas.criadoEm;
    if (ordenarPor === 'atualizado_em' && colunas.atualizadoEm) return colunas.atualizadoEm;
    return colunas.dataMulta;
  }

  private mapearRegistro(
    registro: RegistroBanco,
    colunas: MapaColunasMulta,
  ): MultaNormalizada {
    return {
      idMulta: this.converterNumero(registro[colunas.idMulta]) ?? 0,
      idEmpresa:
        colunas.idEmpresa !== null
          ? this.converterNumero(registro[colunas.idEmpresa])
          : null,
      usuarioAtualizacao:
        colunas.usuarioAtualizacao !== null
          ? this.converterTexto(registro[colunas.usuarioAtualizacao])
          : null,
      idMotorista: this.converterNumero(registro[colunas.idMotorista]) ?? 0,
      idVeiculo: this.converterNumero(registro[colunas.idVeiculo]) ?? 0,
      dataMulta: this.converterData(registro[colunas.dataMulta]) ?? new Date(0),
      dataVencimento:
        colunas.dataVencimento !== null
          ? this.converterData(registro[colunas.dataVencimento])
          : null,
      horaMulta:
        colunas.horaMulta !== null
          ? this.converterHora(registro[colunas.horaMulta])
          : null,
      local: colunas.local !== null ? this.converterTexto(registro[colunas.local]) : null,
      logradouro:
        colunas.logradouro !== null
          ? this.converterTexto(registro[colunas.logradouro])
          : null,
      cidade: colunas.cidade !== null ? this.converterTexto(registro[colunas.cidade]) : null,
      estado: colunas.estado !== null ? this.converterTexto(registro[colunas.estado]) : null,
      cep: colunas.cep !== null ? this.converterTexto(registro[colunas.cep]) : null,
      rodovia:
        colunas.rodovia !== null ? this.converterTexto(registro[colunas.rodovia]) : null,
      kmRodovia:
        colunas.kmRodovia !== null
          ? this.converterTexto(registro[colunas.kmRodovia])
          : null,
      descricao:
        colunas.descricao !== null
          ? this.converterTexto(registro[colunas.descricao])
          : null,
      valor: this.converterNumero(registro[colunas.valor]) ?? 0,
      pontos:
        colunas.pontos !== null ? this.converterNumero(registro[colunas.pontos]) : null,
      status:
        colunas.status !== null
          ? this.converterTexto(registro[colunas.status]) ?? 'PENDENTE'
          : 'PENDENTE',
      orgaoAutuador:
        colunas.orgaoAutuador !== null
          ? this.converterTexto(registro[colunas.orgaoAutuador])
          : null,
      numeroAuto:
        colunas.numeroAuto !== null
          ? this.converterTexto(registro[colunas.numeroAuto])
          : null,
      dataPagamento:
        colunas.dataPagamento !== null
          ? this.converterData(registro[colunas.dataPagamento])
          : null,
      desconto:
        colunas.desconto !== null ? this.converterNumero(registro[colunas.desconto]) : null,
      juros:
        colunas.juros !== null ? this.converterNumero(registro[colunas.juros]) : null,
      valorPago:
        colunas.valorPago !== null ? this.converterNumero(registro[colunas.valorPago]) : null,
      criadoEm:
        colunas.criadoEm !== null
          ? this.converterData(registro[colunas.criadoEm])
          : null,
      atualizadoEm:
        colunas.atualizadoEm !== null
          ? this.converterData(registro[colunas.atualizadoEm])
          : null,
    };
  }

  private async validarRelacionamentos(
    manager: EntityManager,
    idEmpresa: number,
    payload: MultaPersistencia,
  ) {
    const motoristaRows = (await manager.query(
      `
      SELECT 1
      FROM app.motoristas
      WHERE id_motorista = $1
        AND id_empresa = $2
      LIMIT 1
      `,
      [payload.idMotorista, String(idEmpresa)],
    )) as RegistroBanco[];
    const veiculoRows = (await manager.query(
      `
      SELECT 1
      FROM app.veiculo
      WHERE id_veiculo = $1
        AND id_empresa = $2
      LIMIT 1
      `,
      [payload.idVeiculo, String(idEmpresa)],
    )) as RegistroBanco[];

    if (!motoristaRows[0]) {
      throw new BadRequestException('Motorista informado nao existe para a empresa.');
    }

    if (!veiculoRows[0]) {
      throw new BadRequestException('Veiculo informado nao existe para a empresa.');
    }
  }

  private montarFiltroEmpresa(colunas: MapaColunasMulta, idEmpresa: number) {
    if (!colunas.idEmpresa) {
      return { clausula: '', valores: [] as Array<string | number> };
    }

    return {
      clausula: `WHERE ${this.quote(colunas.idEmpresa)} = $1`,
      valores: [String(idEmpresa)] as Array<string | number>,
    };
  }

  private adicionarFiltroEmpresa(
    colunas: MapaColunasMulta,
    idEmpresa: number,
    filtros: string[],
    valores: Array<string | number>,
  ) {
    if (!colunas.idEmpresa) {
      return;
    }
    valores.push(String(idEmpresa));
    filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
  }

  private adicionarCampoOpcional(
    campos: string[],
    valores: Array<string | number | Date | null>,
    coluna: string | null,
    valor: string | number | Date | null,
  ) {
    if (!coluna || valor === null || valor === undefined) {
      return;
    }
    campos.push(coluna);
    valores.push(valor);
  }

  private normalizarDataSomenteData(valor: string) {
    const texto = valor.trim();
    const base = texto.includes('T') ? texto.slice(0, 10) : texto;
    return new Date(`${base}T00:00:00`);
  }

  private normalizarTextoOpcional(valor: string | null | undefined) {
    if (typeof valor !== 'string') {
      return null;
    }
    const texto = valor.trim();
    return texto ? texto.toUpperCase() : null;
  }

  private normalizarDataOpcional(valor: string | null | undefined) {
    if (typeof valor !== 'string') {
      return null;
    }
    const texto = valor.trim();
    return texto ? this.normalizarDataSomenteData(texto) : null;
  }

  private normalizarHoraOpcional(valor: string | null | undefined) {
    if (typeof valor !== 'string') {
      return null;
    }
    const texto = valor.trim();
    return texto || null;
  }

  private normalizarStatus(valor: string | null | undefined) {
    const texto = this.normalizarTextoOpcional(valor);
    return texto ?? 'PENDENTE';
  }

  private normalizarNumeroOpcional(valor: number | null | undefined) {
    if (valor === null || valor === undefined) {
      return null;
    }

    return Number.isFinite(valor) ? Number(valor) : null;
  }

  private normalizarUsuario(valor: string | undefined) {
    const texto = (valor ?? '').trim().toUpperCase();
    return texto || 'APP_WEB';
  }

  private quote(coluna: string): string {
    if (!/^[a-z_][a-z0-9_]*$/.test(coluna)) {
      throw new BadRequestException(`Nome de coluna invalido detectado: ${coluna}`);
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
    return texto || null;
  }

  private converterHora(valor: unknown): string | null {
    if (typeof valor === 'string') {
      const texto = valor.trim();
      return texto || null;
    }
    return null;
  }

  private tratarErroPersistencia(error: unknown, acao: string): never {
    if (error instanceof BadRequestException || error instanceof NotFoundException) {
      throw error;
    }

    if (error instanceof QueryFailedError) {
      const erroPg = error.driverError as { code?: string; message?: string };
      this.logger.error(
        `Falha ao ${acao} multa. code=${erroPg.code ?? 'N/A'} message=${erroPg.message ?? 'Erro desconhecido'}`,
      );

      if (erroPg.code === '23503') {
        throw new BadRequestException(
          'Motorista ou veiculo informado nao existe para a empresa.',
        );
      }

      if (erroPg.code === '23502') {
        throw new BadRequestException(
          'A estrutura atual da tabela exige campos obrigatorios nao preenchidos para esta multa.',
        );
      }

      if (erroPg.code === '23514') {
        throw new BadRequestException('Dados da multa invalidos para as regras da base.');
      }

      if (erroPg.code === '42501') {
        throw new BadRequestException(
          'Usuario do banco sem permissao para gravar em app.multas.',
        );
      }

      if (erroPg.code === '42P01') {
        throw new BadRequestException('Tabela app.multas nao encontrada.');
      }

      if (erroPg.code === '42703') {
        throw new BadRequestException(
          'Estrutura da tabela app.multas esta diferente do esperado.',
        );
      }
    }

    this.logger.error(
      `Falha ao ${acao} multa sem codigo SQL mapeado. message=${error instanceof Error ? error.message : 'Erro desconhecido'}`,
    );
    throw new BadRequestException(`Nao foi possivel ${acao} a multa neste momento.`);
  }
}
