import {
  BadRequestException,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { DataSource, EntityManager, QueryFailedError } from 'typeorm';
import { JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { configurarContextoEmpresaRls } from '../common/database/rls.util';
import { AtualizarVeiculoDto } from './dto/atualizar-veiculo.dto';
import { CriarVeiculoDto } from './dto/criar-veiculo.dto';
import { FiltroVeiculosDto } from './dto/filtro-veiculos.dto';
import { ListarVeiculoDto } from './dto/listar-veiculo.dto';
import { PlacaVeiculoDto } from './dto/placa-veiculo.dto';

type RegistroBanco = Record<string, unknown>;

type MapaColunasVeiculo = {
  idVeiculo: string;
  idEmpresa: string | null;
  idFornecedor: string | null;
  idMarca: string | null;
  idModelo: string | null;
  idCombustivel: string | null;
  idTipo: string | null;
  idCor: string | null;
  placa: string;
  placa2: string | null;
  placa3: string | null;
  placa4: string | null;
  numeroMotor: string | null;
  renavam: string | null;
  anoFabricacao: string | null;
  anoModelo: string | null;
  km: string | null;
  chassi: string | null;
  vencimentoDocumento: string | null;
  observacao: string | null;
  status: string | null;
  idMotoristaAtual: string | null;
  usuarioAtualizacao: string | null;
  criadoEm: string | null;
};

type VeiculoPersistencia = {
  idFornecedor: number;
  idMarca: number;
  idModelo: number;
  idCombustivel: number;
  idTipo: number;
  idCor: number;
  placa: string;
  placa2: string | null;
  placa3: string | null;
  placa4: string | null;
  numeroMotor: string | null;
  renavam: string | null;
  anoFabricacao: number;
  anoModelo: number;
  km: number;
  chassi: string | null;
  vencimentoDocumento: string | null;
  observacao: string | null;
  idMotoristaAtual: number | null;
  status: string | null;
  usuarioAtualizacao: string;
};

type AtualizacaoVeiculoPersistencia = Partial<VeiculoPersistencia>;

@Injectable()
export class VeiculoService {
  private readonly logger = new Logger(VeiculoService.name);

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
        'SELECT * FROM app.veiculo',
        filtros.length > 0 ? `WHERE ${filtros.join(' AND ')}` : '',
        `ORDER BY ${this.quote(colunas.placa)} ASC, ${this.quote(colunas.idVeiculo)} ASC`,
      ]
        .filter(Boolean)
        .join('\n');

      const rows = await manager.query(sql, valores);
      const veiculos = rows.map((row) => this.mapearRegistro(row, colunas));

      return {
        sucesso: true,
        total: veiculos.length,
        veiculos,
      };
    });
  }

  async listarComFiltro(idEmpresa: number, filtro: FiltroVeiculosDto) {
    this.validarIntervalosDoFiltro(filtro);

    return this.executarComRls(idEmpresa, async (manager, colunas) => {
      const filtros: string[] = [];
      const valores: Array<string | number> = [];

      if (colunas.idEmpresa) {
        valores.push(String(idEmpresa));
        filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
      }

      if (filtro.idVeiculo !== undefined) {
        valores.push(filtro.idVeiculo);
        filtros.push(`${this.quote(colunas.idVeiculo)} = $${valores.length}`);
      }

      if (filtro.idFornecedor !== undefined) {
        valores.push(filtro.idFornecedor);
        filtros.push(
          `${this.quote(this.exigirColuna(colunas.idFornecedor, 'idFornecedor'))} = $${valores.length}`,
        );
      }

      if (filtro.idMarca !== undefined) {
        valores.push(filtro.idMarca);
        filtros.push(
          `${this.quote(this.exigirColuna(colunas.idMarca, 'idMarca'))} = $${valores.length}`,
        );
      }

      if (filtro.idModelo !== undefined) {
        valores.push(filtro.idModelo);
        filtros.push(
          `${this.quote(this.exigirColuna(colunas.idModelo, 'idModelo'))} = $${valores.length}`,
        );
      }

      if (filtro.idCombustivel !== undefined) {
        valores.push(filtro.idCombustivel);
        filtros.push(
          `${this.quote(this.exigirColuna(colunas.idCombustivel, 'idCombustivel'))} = $${valores.length}`,
        );
      }

      if (filtro.idTipo !== undefined) {
        valores.push(filtro.idTipo);
        filtros.push(
          `${this.quote(this.exigirColuna(colunas.idTipo, 'idTipo'))} = $${valores.length}`,
        );
      }

      if (filtro.idCor !== undefined) {
        valores.push(filtro.idCor);
        filtros.push(
          `${this.quote(this.exigirColuna(colunas.idCor, 'idCor'))} = $${valores.length}`,
        );
      }

      if (filtro.placa) {
        valores.push(`%${filtro.placa}%`);
        const referencia = `$${valores.length}`;
        const colunasPlaca = [
          colunas.placa,
          colunas.placa2,
          colunas.placa3,
          colunas.placa4,
        ]
          .filter((coluna): coluna is string => coluna !== null)
          .map(
            (coluna) =>
              `COALESCE(${this.quote(coluna)}, '') ILIKE ${referencia}`,
          );

        if (colunasPlaca.length > 0) {
          filtros.push(`(${colunasPlaca.join(' OR ')})`);
        }
      }

      if (filtro.renavam) {
        valores.push(`%${filtro.renavam}%`);
        filtros.push(
          `COALESCE(${this.quote(this.exigirColuna(colunas.renavam, 'renavam'))}, '') ILIKE $${valores.length}`,
        );
      }

      if (filtro.chassi) {
        valores.push(`%${filtro.chassi}%`);
        filtros.push(
          `COALESCE(${this.quote(this.exigirColuna(colunas.chassi, 'chassi'))}, '') ILIKE $${valores.length}`,
        );
      }

      if (filtro.texto?.trim()) {
        valores.push(`%${filtro.texto.trim()}%`);
        const referencia = `$${valores.length}`;
        const colunasTexto = [
          colunas.placa,
          colunas.placa2,
          colunas.placa3,
          colunas.placa4,
          colunas.numeroMotor,
          colunas.renavam,
          colunas.chassi,
          colunas.observacao,
        ]
          .filter((coluna): coluna is string => coluna !== null)
          .map(
            (coluna) =>
              `COALESCE(${this.quote(coluna)}, '') ILIKE ${referencia}`,
          );

        if (colunasTexto.length > 0) {
          filtros.push(`(${colunasTexto.join(' OR ')})`);
        }
      }

      if (filtro.anoFabricacaoDe !== undefined) {
        valores.push(filtro.anoFabricacaoDe);
        filtros.push(
          `${this.quote(this.exigirColuna(colunas.anoFabricacao, 'anoFabricacao'))} >= $${valores.length}`,
        );
      }

      if (filtro.anoFabricacaoAte !== undefined) {
        valores.push(filtro.anoFabricacaoAte);
        filtros.push(
          `${this.quote(this.exigirColuna(colunas.anoFabricacao, 'anoFabricacao'))} <= $${valores.length}`,
        );
      }

      if (filtro.anoModeloDe !== undefined) {
        valores.push(filtro.anoModeloDe);
        filtros.push(
          `${this.quote(this.exigirColuna(colunas.anoModelo, 'anoModelo'))} >= $${valores.length}`,
        );
      }

      if (filtro.anoModeloAte !== undefined) {
        valores.push(filtro.anoModeloAte);
        filtros.push(
          `${this.quote(this.exigirColuna(colunas.anoModelo, 'anoModelo'))} <= $${valores.length}`,
        );
      }

      if (filtro.kmMin !== undefined) {
        valores.push(filtro.kmMin);
        filtros.push(
          `${this.quote(this.exigirColuna(colunas.km, 'km'))} >= $${valores.length}`,
        );
      }

      if (filtro.kmMax !== undefined) {
        valores.push(filtro.kmMax);
        filtros.push(
          `${this.quote(this.exigirColuna(colunas.km, 'km'))} <= $${valores.length}`,
        );
      }

      if (filtro.vencimentoDocumentoDe) {
        valores.push(filtro.vencimentoDocumentoDe);
        filtros.push(
          `${this.quote(this.exigirColuna(colunas.vencimentoDocumento, 'vencimentoDocumento'))} >= $${valores.length}`,
        );
      }

      if (filtro.vencimentoDocumentoAte) {
        valores.push(filtro.vencimentoDocumentoAte);
        filtros.push(
          `${this.quote(this.exigirColuna(colunas.vencimentoDocumento, 'vencimentoDocumento'))} <= $${valores.length}`,
        );
      }

      const whereSql =
        filtros.length > 0 ? `WHERE ${filtros.join(' AND ')}` : '';
      const pagina = filtro.pagina ?? 1;
      const limite = filtro.limite ?? 20;
      const offset = (pagina - 1) * limite;
      const ordem = filtro.ordem ?? 'ASC';
      const colunaOrdenacao = this.resolverColunaOrdenacao(
        filtro.ordenarPor,
        colunas,
      );

      const sqlCount = `
        SELECT COUNT(1)::int AS total
        FROM app.veiculo
        ${whereSql}
      `;

      const sqlDados = `
        SELECT *
        FROM app.veiculo
        ${whereSql}
        ORDER BY ${this.quote(colunaOrdenacao)} ${ordem}, ${this.quote(colunas.idVeiculo)} ASC
        LIMIT $${valores.length + 1}
        OFFSET $${valores.length + 2}
      `;

      const countRows = await manager.query(sqlCount, valores);
      const rows = await manager.query(sqlDados, [...valores, limite, offset]);

      const total = Number(countRows[0]?.total ?? 0);
      const veiculos = rows.map((row) => this.mapearRegistro(row, colunas));

      return {
        sucesso: true,
        paginaAtual: pagina,
        limite,
        total,
        totalPaginas: total > 0 ? Math.ceil(total / limite) : 0,
        veiculos,
      };
    });
  }

  async buscarPorId(idEmpresa: number, idVeiculo: number) {
    return this.executarComRls(idEmpresa, async (manager, colunas) => {
      const registro = await this.buscarRegistroPorIdOuFalhar(
        manager,
        colunas,
        idEmpresa,
        idVeiculo,
      );

      return {
        sucesso: true,
        veiculo: this.mapearRegistro(registro, colunas),
      };
    });
  }

  async listarPlacas(idEmpresa: number) {
    return this.executarComRls(idEmpresa, async (manager, colunas) => {
      const campos = [
        `${this.quote(colunas.idVeiculo)} AS id_veiculo`,
        `${this.quote(colunas.placa)} AS placa`,
      ];

      if (colunas.idMotoristaAtual) {
        campos.push(
          `${this.quote(colunas.idMotoristaAtual)} AS id_motorista_atual`,
        );
      }

      if (colunas.km) {
        campos.push(`${this.quote(colunas.km)} AS km_atual`);
      }

      if (colunas.placa2) {
        campos.push(`${this.quote(colunas.placa2)} AS placa2`);
      }
      if (colunas.placa3) {
        campos.push(`${this.quote(colunas.placa3)} AS placa3`);
      }
      if (colunas.placa4) {
        campos.push(`${this.quote(colunas.placa4)} AS placa4`);
      }

      const filtros: string[] = [];
      const valores: Array<string | number> = [];

      if (colunas.idEmpresa) {
        valores.push(String(idEmpresa));
        filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
      }

      const sql = `
        SELECT ${campos.join(', ')}
        FROM app.veiculo
        ${filtros.length > 0 ? `WHERE ${filtros.join(' AND ')}` : ''}
        ORDER BY placa ASC, id_veiculo ASC
      `;

      const rows = await manager.query(sql, valores);
      const placas = rows.flatMap((row) => this.extrairPlacasDoRegistro(row));
      const unicas = new Map<string, PlacaVeiculoDto>();

      for (const placa of placas) {
        const chave = `${placa.idVeiculo}-${placa.placa}-${placa.origemCampo}`;
        if (!unicas.has(chave)) {
          unicas.set(chave, placa);
        }
      }

      const dados = Array.from(unicas.values()).sort((a, b) =>
        a.placa.localeCompare(b.placa, 'pt-BR'),
      );

      return {
        sucesso: true,
        total: dados.length,
        placas: dados,
      };
    });
  }

  async cadastrar(
    idEmpresa: number,
    dados: CriarVeiculoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    const payload = this.normalizarCriacao(dados, usuarioJwt);
    this.validarConsistencia(payload);

    try {
      return this.executarComRls(idEmpresa, async (manager, colunas) => {
        await this.validarDuplicidadeIdentificadores(
          manager,
          colunas,
          idEmpresa,
          payload,
        );

        const campos: string[] = [];
        const valores: Array<string | number | null> = [];

        const adicionarCampoObrigatorio = (
          coluna: string | null,
          campoApi: string,
          valor: string | number | null,
        ) => {
          campos.push(this.exigirColuna(coluna, campoApi));
          valores.push(valor);
        };

        const adicionarCampoOpcional = (
          coluna: string | null,
          _campoApi: string,
          valor: string | number | null | undefined,
        ) => {
          if (valor === undefined) {
            return;
          }

          if (!coluna) {
            return;
          }

          campos.push(coluna);
          valores.push(valor);
        };

        adicionarCampoObrigatorio(
          colunas.idFornecedor,
          'idFornecedor',
          payload.idFornecedor,
        );
        adicionarCampoObrigatorio(colunas.idMarca, 'idMarca', payload.idMarca);
        adicionarCampoObrigatorio(
          colunas.idModelo,
          'idModelo',
          payload.idModelo,
        );
        adicionarCampoObrigatorio(
          colunas.idCombustivel,
          'idCombustivel',
          payload.idCombustivel,
        );
        adicionarCampoObrigatorio(colunas.idTipo, 'idTipo', payload.idTipo);
        adicionarCampoObrigatorio(colunas.idCor, 'idCor', payload.idCor);
        adicionarCampoObrigatorio(colunas.placa, 'placa', payload.placa);
        adicionarCampoObrigatorio(
          colunas.anoFabricacao,
          'anoFabricacao',
          payload.anoFabricacao,
        );
        adicionarCampoObrigatorio(
          colunas.anoModelo,
          'anoModelo',
          payload.anoModelo,
        );
        adicionarCampoObrigatorio(colunas.km, 'km', payload.km);

        adicionarCampoOpcional(colunas.placa2, 'placa2', payload.placa2);
        adicionarCampoOpcional(colunas.placa3, 'placa3', payload.placa3);
        adicionarCampoOpcional(colunas.placa4, 'placa4', payload.placa4);
        adicionarCampoOpcional(
          colunas.numeroMotor,
          'numeroMotor',
          payload.numeroMotor,
        );
        adicionarCampoOpcional(colunas.renavam, 'renavam', payload.renavam);
        adicionarCampoOpcional(colunas.chassi, 'chassi', payload.chassi);
        adicionarCampoOpcional(
          colunas.vencimentoDocumento,
          'vencimentoDocumento',
          payload.vencimentoDocumento,
        );
        adicionarCampoOpcional(
          colunas.observacao,
          'observacao',
          payload.observacao,
        );
        adicionarCampoOpcional(
          colunas.idMotoristaAtual,
          'idMotoristaAtual',
          payload.idMotoristaAtual,
        );
        adicionarCampoOpcional(colunas.status, 'status', payload.status);

        if (colunas.usuarioAtualizacao) {
          campos.push(colunas.usuarioAtualizacao);
          valores.push(payload.usuarioAtualizacao);
        }

        if (colunas.idEmpresa) {
          campos.push(colunas.idEmpresa);
          valores.push(String(idEmpresa));
        }

        const placeholders = valores
          .map((_, index) => `$${index + 1}`)
          .join(', ');
        const sql = `
          INSERT INTO app.veiculo (${campos.map((coluna) => this.quote(coluna)).join(', ')})
          VALUES (${placeholders})
          RETURNING *
        `;

        const rows = await manager.query(sql, valores);
        const registro = rows[0];

        if (!registro) {
          throw new BadRequestException(
            'Falha ao cadastrar veiculo (retorno vazio da base).',
          );
        }

        return {
          sucesso: true,
          mensagem: 'Veiculo cadastrado com sucesso.',
          veiculo: this.mapearRegistro(registro, colunas),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'cadastrar');
    }
  }

  async atualizar(
    idEmpresa: number,
    idVeiculo: number,
    dados: AtualizarVeiculoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    const payload = this.normalizarAtualizacao(dados, usuarioJwt);
    this.validarConsistencia(payload);

    try {
      return this.executarComRls(idEmpresa, async (manager, colunas) => {
        await this.buscarRegistroPorIdOuFalhar(
          manager,
          colunas,
          idEmpresa,
          idVeiculo,
        );

        await this.validarDuplicidadeIdentificadores(
          manager,
          colunas,
          idEmpresa,
          payload,
          idVeiculo,
        );

        const sets: string[] = [];
        const valores: Array<string | number | null> = [];

        const adicionarSetOpcional = (
          coluna: string | null,
          _campoApi: string,
          valor: string | number | null | undefined,
        ) => {
          if (valor === undefined) {
            return;
          }

          if (!coluna) {
            return;
          }

          valores.push(valor);
          sets.push(`${this.quote(coluna)} = $${valores.length}`);
        };

        adicionarSetOpcional(
          colunas.idFornecedor,
          'idFornecedor',
          payload.idFornecedor,
        );
        adicionarSetOpcional(colunas.idMarca, 'idMarca', payload.idMarca);
        adicionarSetOpcional(colunas.idModelo, 'idModelo', payload.idModelo);
        adicionarSetOpcional(
          colunas.idCombustivel,
          'idCombustivel',
          payload.idCombustivel,
        );
        adicionarSetOpcional(colunas.idTipo, 'idTipo', payload.idTipo);
        adicionarSetOpcional(colunas.idCor, 'idCor', payload.idCor);
        adicionarSetOpcional(colunas.placa, 'placa', payload.placa);
        adicionarSetOpcional(colunas.placa2, 'placa2', payload.placa2);
        adicionarSetOpcional(colunas.placa3, 'placa3', payload.placa3);
        adicionarSetOpcional(colunas.placa4, 'placa4', payload.placa4);
        adicionarSetOpcional(
          colunas.numeroMotor,
          'numeroMotor',
          payload.numeroMotor,
        );
        adicionarSetOpcional(colunas.renavam, 'renavam', payload.renavam);
        adicionarSetOpcional(
          colunas.anoFabricacao,
          'anoFabricacao',
          payload.anoFabricacao,
        );
        adicionarSetOpcional(colunas.anoModelo, 'anoModelo', payload.anoModelo);
        adicionarSetOpcional(colunas.km, 'km', payload.km);
        adicionarSetOpcional(colunas.chassi, 'chassi', payload.chassi);
        adicionarSetOpcional(
          colunas.vencimentoDocumento,
          'vencimentoDocumento',
          payload.vencimentoDocumento,
        );
        adicionarSetOpcional(
          colunas.observacao,
          'observacao',
          payload.observacao,
        );
        adicionarSetOpcional(
          colunas.idMotoristaAtual,
          'idMotoristaAtual',
          payload.idMotoristaAtual,
        );
        adicionarSetOpcional(colunas.status, 'status', payload.status);

        const houveAtualizacao = sets.length > 0;
        if (!houveAtualizacao) {
          throw new BadRequestException(
            'Nenhum campo valido foi informado para atualizar o veiculo.',
          );
        }

        if (colunas.usuarioAtualizacao) {
          valores.push(
            payload.usuarioAtualizacao ??
              this.normalizarUsuario(usuarioJwt.email),
          );
          sets.push(
            `${this.quote(colunas.usuarioAtualizacao)} = $${valores.length}`,
          );
        }

        const filtros: string[] = [];
        valores.push(idVeiculo);
        filtros.push(`${this.quote(colunas.idVeiculo)} = $${valores.length}`);

        if (colunas.idEmpresa) {
          valores.push(String(idEmpresa));
          filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
        }

        const sql = `
          UPDATE app.veiculo
          SET ${sets.join(', ')}
          WHERE ${filtros.join(' AND ')}
          RETURNING *
        `;

        const rows = await manager.query(sql, valores);
        const atualizado = rows[0];

        if (!atualizado) {
          throw new NotFoundException(
            'Veiculo nao encontrado para a empresa logada.',
          );
        }

        return {
          sucesso: true,
          mensagem: 'Veiculo atualizado com sucesso.',
          veiculo: this.mapearRegistro(atualizado, colunas),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'atualizar');
    }
  }

  private async executarComRls<T>(
    idEmpresa: number,
    callback: (
      manager: EntityManager,
      colunas: MapaColunasVeiculo,
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
  ): Promise<MapaColunasVeiculo> {
    const rows = await manager.query(`
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = 'app'
        AND table_name = 'veiculo'
    `);

    if (rows.length === 0) {
      throw new BadRequestException('Tabela app.veiculo nao encontrada.');
    }

    const set = new Set<string>(
      rows
        .map((row) =>
          typeof row.column_name === 'string' ? row.column_name : '',
        )
        .filter((value) => value.length > 0),
    );

    return {
      idVeiculo: this.encontrarColuna(
        set,
        ['id_veiculo', 'id'],
        'id do veiculo',
      )!,
      idEmpresa: this.encontrarColuna(set, ['id_empresa'], '', false),
      idFornecedor: this.encontrarColuna(set, ['id_fornecedor'], '', false),
      idMarca: this.encontrarColuna(
        set,
        ['id_marca_vei', 'id_marca'],
        '',
        false,
      ),
      idModelo: this.encontrarColuna(
        set,
        ['id_modelo_vei', 'id_modelo'],
        '',
        false,
      ),
      idCombustivel: this.encontrarColuna(
        set,
        ['id_combustivel', 'id_combustiveis'],
        '',
        false,
      ),
      idTipo: this.encontrarColuna(
        set,
        ['id_tipos_vei', 'id_tipo_vei', 'id_tipo'],
        '',
        false,
      ),
      idCor: this.encontrarColuna(set, ['id_cor_vei', 'id_cor'], '', false),
      placa: this.encontrarColuna(set, ['placa'], 'placa do veiculo')!,
      placa2: this.encontrarColuna(set, ['placa2', 'placa_2'], '', false),
      placa3: this.encontrarColuna(set, ['placa3', 'placa_3'], '', false),
      placa4: this.encontrarColuna(set, ['placa4', 'placa_4'], '', false),
      numeroMotor: this.encontrarColuna(
        set,
        ['numero_motor', 'nr_motor', 'nro_motor', 'motor'],
        '',
        false,
      ),
      renavam: this.encontrarColuna(set, ['renavam'], '', false),
      anoFabricacao: this.encontrarColuna(set, ['ano_fabricacao'], '', false),
      anoModelo: this.encontrarColuna(set, ['ano_modelo'], '', false),
      km: this.encontrarColuna(set, ['km', 'km_atual'], '', false),
      chassi: this.encontrarColuna(set, ['chassi'], '', false),
      vencimentoDocumento: this.encontrarColuna(
        set,
        ['vencimento_documento', 'data_vencimento', 'venc_documento'],
        '',
        false,
      ),
      observacao: this.encontrarColuna(set, ['observacao', 'obs'], '', false),
      status: this.encontrarColuna(
        set,
        ['status', 'situacao', 'ativo'],
        '',
        false,
      ),
      idMotoristaAtual: this.encontrarColuna(
        set,
        ['id_motorista_atual'],
        '',
        false,
      ),
      usuarioAtualizacao: this.encontrarColuna(
        set,
        ['usuario_atualizacao', 'usuario_update'],
        '',
        false,
      ),
      criadoEm: this.encontrarColuna(
        set,
        ['criado_em', 'data_cadastro', 'created_at'],
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
        `Estrutura da tabela app.veiculo invalida: coluna de ${descricao} nao encontrada.`,
      );
    }

    return null;
  }

  private async buscarRegistroPorIdOuFalhar(
    manager: EntityManager,
    colunas: MapaColunasVeiculo,
    idEmpresa: number,
    idVeiculo: number,
  ): Promise<RegistroBanco> {
    const filtros: string[] = [];
    const valores: Array<string | number> = [];

    valores.push(idVeiculo);
    filtros.push(`${this.quote(colunas.idVeiculo)} = $${valores.length}`);

    if (colunas.idEmpresa) {
      valores.push(String(idEmpresa));
      filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
    }

    const sql = `
      SELECT *
      FROM app.veiculo
      WHERE ${filtros.join(' AND ')}
      LIMIT 1
    `;

    const rows = await manager.query(sql, valores);
    const registro = rows[0];

    if (!registro) {
      throw new NotFoundException(
        'Veiculo nao encontrado para a empresa logada.',
      );
    }

    return registro;
  }

  private async validarDuplicidadeIdentificadores(
    manager: EntityManager,
    colunas: MapaColunasVeiculo,
    idEmpresa: number,
    payload:
      | VeiculoPersistencia
      | AtualizacaoVeiculoPersistencia,
    idVeiculoAtual?: number,
  ) {
    const validacoes: Array<{
      campo: 'renavam' | 'chassi';
      valor: string | null | undefined;
      coluna: string | null;
      mensagem: string;
    }> = [
      {
        campo: 'renavam',
        valor: payload.renavam,
        coluna: colunas.renavam,
        mensagem: 'Ja existe outro veiculo cadastrado com este RENAVAM.',
      },
      {
        campo: 'chassi',
        valor: payload.chassi,
        coluna: colunas.chassi,
        mensagem: 'Ja existe outro veiculo cadastrado com este chassi.',
      },
    ];

    for (const validacao of validacoes) {
      if (!validacao.coluna || !validacao.valor) {
        continue;
      }

      const filtros = [`${this.quote(validacao.coluna)} = $1`];
      const valores: Array<string | number> = [validacao.valor];

      if (colunas.idEmpresa) {
        valores.push(String(idEmpresa));
        filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
      }

      if (idVeiculoAtual && Number.isFinite(idVeiculoAtual)) {
        valores.push(idVeiculoAtual);
        filtros.push(`${this.quote(colunas.idVeiculo)} <> $${valores.length}`);
      }

      const rows = (await manager.query(
        `
          SELECT ${this.quote(colunas.idVeiculo)} AS id_veiculo
          FROM app.veiculo
          WHERE ${filtros.join(' AND ')}
          LIMIT 1
        `,
        valores,
      )) as Array<{ id_veiculo?: string | number }>;

      if (rows[0]?.id_veiculo !== undefined) {
        throw new BadRequestException(validacao.mensagem);
      }
    }
  }

  private normalizarCriacao(
    dados: CriarVeiculoDto,
    usuarioJwt: JwtUsuarioPayload,
  ): VeiculoPersistencia {
    return {
      idFornecedor: dados.idFornecedor,
      idMarca: dados.idMarca,
      idModelo: dados.idModelo,
      idCombustivel: dados.idCombustivel,
      idTipo: dados.idTipo,
      idCor: dados.idCor,
      placa: this.normalizarPlacaObrigatoria(dados.placa),
      placa2: this.normalizarPlacaOpcional(dados.placa2),
      placa3: this.normalizarPlacaOpcional(dados.placa3),
      placa4: this.normalizarPlacaOpcional(dados.placa4),
      numeroMotor: this.normalizarTextoOpcional(dados.numeroMotor),
      renavam: this.normalizarTextoOpcional(dados.renavam),
      anoFabricacao: dados.anoFabricacao,
      anoModelo: dados.anoModelo,
      km: dados.km,
      chassi: this.normalizarTextoOpcional(dados.chassi),
      vencimentoDocumento: dados.vencimentoDocumento
        ? this.normalizarData(dados.vencimentoDocumento)
        : null,
      observacao: this.normalizarTextoOpcional(dados.observacao),
      idMotoristaAtual: dados.idMotoristaAtual ?? null,
      status: this.normalizarTextoOpcional(dados.status),
      usuarioAtualizacao: this.normalizarUsuario(usuarioJwt.email),
    };
  }

  private normalizarAtualizacao(
    dados: AtualizarVeiculoDto,
    usuarioJwt: JwtUsuarioPayload,
  ): AtualizacaoVeiculoPersistencia {
    return {
      idFornecedor: dados.idFornecedor,
      idMarca: dados.idMarca,
      idModelo: dados.idModelo,
      idCombustivel: dados.idCombustivel,
      idTipo: dados.idTipo,
      idCor: dados.idCor,
      placa: dados.placa
        ? this.normalizarPlacaObrigatoria(dados.placa)
        : undefined,
      placa2:
        dados.placa2 !== undefined
          ? this.normalizarPlacaOpcional(dados.placa2)
          : undefined,
      placa3:
        dados.placa3 !== undefined
          ? this.normalizarPlacaOpcional(dados.placa3)
          : undefined,
      placa4:
        dados.placa4 !== undefined
          ? this.normalizarPlacaOpcional(dados.placa4)
          : undefined,
      numeroMotor:
        dados.numeroMotor !== undefined
          ? this.normalizarTextoOpcional(dados.numeroMotor)
          : undefined,
      renavam:
        dados.renavam !== undefined
          ? this.normalizarTextoOpcional(dados.renavam)
          : undefined,
      anoFabricacao: dados.anoFabricacao,
      anoModelo: dados.anoModelo,
      km: dados.km,
      chassi:
        dados.chassi !== undefined
          ? this.normalizarTextoOpcional(dados.chassi)
          : undefined,
      vencimentoDocumento:
        dados.vencimentoDocumento !== undefined
          ? this.normalizarData(dados.vencimentoDocumento)
          : undefined,
      observacao:
        dados.observacao !== undefined
          ? this.normalizarTextoOpcional(dados.observacao)
          : undefined,
      idMotoristaAtual: dados.idMotoristaAtual,
      status:
        dados.status !== undefined
          ? this.normalizarTextoOpcional(dados.status)
          : undefined,
      usuarioAtualizacao: this.normalizarUsuario(usuarioJwt.email),
    };
  }

  private validarConsistencia(payload: Partial<VeiculoPersistencia>) {
    if (
      payload.anoFabricacao !== undefined &&
      (!Number.isFinite(payload.anoFabricacao) || payload.anoFabricacao < 1900)
    ) {
      throw new BadRequestException('anoFabricacao invalido.');
    }

    if (
      payload.anoModelo !== undefined &&
      (!Number.isFinite(payload.anoModelo) || payload.anoModelo < 1900)
    ) {
      throw new BadRequestException('anoModelo invalido.');
    }

    if (
      payload.anoFabricacao !== undefined &&
      payload.anoModelo !== undefined &&
      payload.anoModelo < payload.anoFabricacao
    ) {
      throw new BadRequestException(
        'anoModelo deve ser maior ou igual ao anoFabricacao.',
      );
    }

    if (
      payload.km !== undefined &&
      (!Number.isFinite(payload.km) || payload.km < 0)
    ) {
      throw new BadRequestException('km invalido.');
    }
  }

  private validarIntervalosDoFiltro(filtro: FiltroVeiculosDto) {
    if (
      filtro.anoFabricacaoDe !== undefined &&
      filtro.anoFabricacaoAte !== undefined &&
      filtro.anoFabricacaoAte < filtro.anoFabricacaoDe
    ) {
      throw new BadRequestException(
        'Filtro invalido: anoFabricacaoAte deve ser maior ou igual a anoFabricacaoDe.',
      );
    }

    if (
      filtro.anoModeloDe !== undefined &&
      filtro.anoModeloAte !== undefined &&
      filtro.anoModeloAte < filtro.anoModeloDe
    ) {
      throw new BadRequestException(
        'Filtro invalido: anoModeloAte deve ser maior ou igual a anoModeloDe.',
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

    if (
      filtro.vencimentoDocumentoDe &&
      filtro.vencimentoDocumentoAte &&
      new Date(filtro.vencimentoDocumentoAte) <
        new Date(filtro.vencimentoDocumentoDe)
    ) {
      throw new BadRequestException(
        'Filtro invalido: vencimentoDocumentoAte deve ser maior ou igual a vencimentoDocumentoDe.',
      );
    }
  }

  private resolverColunaOrdenacao(
    ordenarPor: FiltroVeiculosDto['ordenarPor'],
    colunas: MapaColunasVeiculo,
  ): string {
    if (ordenarPor === 'id_veiculo') {
      return colunas.idVeiculo;
    }
    if (ordenarPor === 'id_fornecedor' && colunas.idFornecedor) {
      return colunas.idFornecedor;
    }
    if (ordenarPor === 'id_marca' && colunas.idMarca) {
      return colunas.idMarca;
    }
    if (ordenarPor === 'id_modelo' && colunas.idModelo) {
      return colunas.idModelo;
    }
    if (ordenarPor === 'id_combustivel' && colunas.idCombustivel) {
      return colunas.idCombustivel;
    }
    if (ordenarPor === 'id_tipo' && colunas.idTipo) {
      return colunas.idTipo;
    }
    if (ordenarPor === 'id_cor' && colunas.idCor) {
      return colunas.idCor;
    }
    if (ordenarPor === 'ano_fabricacao' && colunas.anoFabricacao) {
      return colunas.anoFabricacao;
    }
    if (ordenarPor === 'ano_modelo' && colunas.anoModelo) {
      return colunas.anoModelo;
    }
    if (ordenarPor === 'km' && colunas.km) {
      return colunas.km;
    }
    if (ordenarPor === 'vencimento_documento' && colunas.vencimentoDocumento) {
      return colunas.vencimentoDocumento;
    }
    return colunas.placa;
  }

  private mapearRegistro(
    registro: RegistroBanco,
    colunas: MapaColunasVeiculo,
  ): ListarVeiculoDto {
    const placaPrincipal =
      this.converterTexto(registro[colunas.placa])?.toUpperCase() ?? '';

    const placasAdicionais = [colunas.placa2, colunas.placa3, colunas.placa4]
      .filter((coluna): coluna is string => coluna !== null)
      .map(
        (coluna) => this.converterTexto(registro[coluna])?.toUpperCase() ?? '',
      )
      .filter((placa) => placa.length > 0);

    const vencimentoDocumento =
      colunas.vencimentoDocumento !== null
        ? this.converterData(registro[colunas.vencimentoDocumento])
        : null;

    const km =
      colunas.km !== null ? this.converterNumero(registro[colunas.km]) : null;

    return {
      idVeiculo: this.converterNumero(registro[colunas.idVeiculo]) ?? 0,
      idEmpresa:
        colunas.idEmpresa !== null
          ? this.converterNumero(registro[colunas.idEmpresa])
          : null,
      idFornecedor:
        colunas.idFornecedor !== null
          ? this.converterNumero(registro[colunas.idFornecedor])
          : null,
      idMarca:
        colunas.idMarca !== null
          ? this.converterNumero(registro[colunas.idMarca])
          : null,
      idModelo:
        colunas.idModelo !== null
          ? this.converterNumero(registro[colunas.idModelo])
          : null,
      idCombustivel:
        colunas.idCombustivel !== null
          ? this.converterNumero(registro[colunas.idCombustivel])
          : null,
      idTipo:
        colunas.idTipo !== null
          ? this.converterNumero(registro[colunas.idTipo])
          : null,
      idCor:
        colunas.idCor !== null
          ? this.converterNumero(registro[colunas.idCor])
          : null,
      placa: placaPrincipal,
      placasAdicionais,
      numeroMotor:
        colunas.numeroMotor !== null
          ? (this.converterTexto(
              registro[colunas.numeroMotor],
            )?.toUpperCase() ?? null)
          : null,
      renavam:
        colunas.renavam !== null
          ? (this.converterTexto(registro[colunas.renavam])?.toUpperCase() ??
            null)
          : null,
      chassi:
        colunas.chassi !== null
          ? (this.converterTexto(registro[colunas.chassi])?.toUpperCase() ??
            null)
          : null,
      vencimentoDocumento,
      observacao:
        colunas.observacao !== null
          ? (this.converterTexto(registro[colunas.observacao])?.toUpperCase() ??
            null)
          : null,
      status:
        colunas.status !== null
          ? (this.converterTexto(registro[colunas.status])?.toUpperCase() ??
            null)
          : null,
      idMotorista:
        colunas.idMotoristaAtual !== null
          ? this.converterNumero(registro[colunas.idMotoristaAtual])
          : null,
      idMotoristaAtual:
        colunas.idMotoristaAtual !== null
          ? this.converterNumero(registro[colunas.idMotoristaAtual])
          : null,
      km,
      kmAtual: km,
      anoFabricacao:
        colunas.anoFabricacao !== null
          ? this.converterNumero(registro[colunas.anoFabricacao])
          : null,
      anoModelo:
        colunas.anoModelo !== null
          ? this.converterNumero(registro[colunas.anoModelo])
          : null,
      dataVencimento: vencimentoDocumento,
      criadoEm:
        colunas.criadoEm !== null
          ? this.converterData(registro[colunas.criadoEm])
          : null,
    };
  }

  private extrairPlacasDoRegistro(registro: RegistroBanco): PlacaVeiculoDto[] {
    const idVeiculo = this.converterNumero(registro.id_veiculo) ?? 0;
    const idMotoristaAtual =
      this.converterNumero(registro.id_motorista_atual) ?? null;
    const kmAtual = this.converterNumero(registro.km_atual) ?? null;
    const entradas = [
      {
        valor: this.converterTexto(registro.placa),
        origem: 'placa',
        tipo: 'principal',
      },
      {
        valor: this.converterTexto(registro.placa2),
        origem: 'placa2',
        tipo: 'adicional',
      },
      {
        valor: this.converterTexto(registro.placa3),
        origem: 'placa3',
        tipo: 'adicional',
      },
      {
        valor: this.converterTexto(registro.placa4),
        origem: 'placa4',
        tipo: 'adicional',
      },
    ];

    return entradas
      .filter(
        (
          item,
        ): item is {
          valor: string;
          origem: PlacaVeiculoDto['origemCampo'];
          tipo: PlacaVeiculoDto['tipo'];
        } => Boolean(item.valor && item.valor.trim()),
      )
      .map((item) => ({
        idVeiculo,
        idMotorista: idMotoristaAtual,
        idMotoristaAtual,
        kmAtual,
        placa: item.valor.trim().toUpperCase(),
        origemCampo: item.origem,
        tipo: item.tipo,
      }));
  }

  private exigirColuna(coluna: string | null, campoApi: string): string {
    if (!coluna) {
      throw new BadRequestException(
        `Campo ${campoApi} nao esta disponivel na tabela app.veiculo.`,
      );
    }

    return coluna;
  }

  private normalizarPlacaObrigatoria(valor: string): string {
    const placa = valor.trim().toUpperCase();

    if (!/^[A-Z0-9-]{7,8}$/.test(placa)) {
      throw new BadRequestException('Placa invalida informada.');
    }

    return placa;
  }

  private normalizarPlacaOpcional(valor: string | undefined): string | null {
    if (valor === undefined) {
      return null;
    }

    const placa = valor.trim().toUpperCase();
    if (!placa) {
      return null;
    }

    if (!/^[A-Z0-9-]{7,8}$/.test(placa)) {
      throw new BadRequestException('Placa adicional invalida informada.');
    }

    return placa;
  }

  private normalizarTextoOpcional(valor: string | undefined): string | null {
    if (valor === undefined) {
      return null;
    }

    const texto = valor.trim().toUpperCase();
    return texto.length > 0 ? texto : null;
  }

  private normalizarData(valor: string): string {
    const data = new Date(valor);
    if (Number.isNaN(data.getTime())) {
      throw new BadRequestException('Data invalida informada.');
    }

    return data.toISOString().slice(0, 10);
  }

  private normalizarUsuario(valor: string): string {
    return valor.trim().toUpperCase();
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
      valor instanceof Date ||
        typeof valor === 'string' ||
        typeof valor === 'number'
        ? valor
        : '',
    );
    if (Number.isNaN(data.getTime())) {
      return null;
    }

    return data.toISOString().slice(0, 10);
  }

  private tratarErroPersistencia(error: unknown, acao: string): never {
    if (
      error instanceof BadRequestException ||
      error instanceof NotFoundException
    ) {
      throw error;
    }

    if (error instanceof QueryFailedError) {
      const erroPg = error.driverError as { code?: string; message?: string };
      this.logger.error(
        `Falha ao ${acao} veiculo. code=${erroPg.code ?? 'N/A'} message=${erroPg.message ?? 'Erro desconhecido'}`,
      );

      if (erroPg.code === '23503') {
        throw new BadRequestException(
          'Alguma referência informada (fornecedor, marca, modelo, tipo, cor ou combustível) não existe.',
        );
      }

      if (erroPg.code === '23505') {
        throw new BadRequestException(
          'Já existe veículo com a placa informada.',
        );
      }

      if (erroPg.code === '42501') {
        throw new BadRequestException(
          'Usuário do banco sem permissão para gravar em app.veiculo.',
        );
      }

      if (erroPg.code === '42P01') {
        throw new BadRequestException('Tabela app.veiculo não encontrada.');
      }

      if (erroPg.code === '42703') {
        throw new BadRequestException(
          'Estrutura da tabela app.veiculo está diferente do esperado.',
        );
      }
    }

    this.logger.error(
      `Falha ao ${acao} veiculo sem codigo SQL mapeado. message=${error instanceof Error ? error.message : 'Erro desconhecido'}`,
    );
    throw new BadRequestException(
      `Não foi possível ${acao} o veículo neste momento.`,
    );
  }
}
