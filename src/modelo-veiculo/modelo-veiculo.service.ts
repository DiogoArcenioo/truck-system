import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { DataSource, EntityManager, QueryFailedError } from 'typeorm';
import { JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { configurarContextoEmpresaRls } from '../common/database/rls.util';
import { CriarModeloVeiculoDto } from './dto/criar-modelo-veiculo.dto';
import { ListarModeloVeiculoDto } from './dto/listar-modelo-veiculo.dto';

type RegistroBanco = Record<string, unknown>;

type MapaColunasModeloVeiculo = {
  idModelo: string;
  idMarca: string | null;
  descricao: string;
  status: string | null;
  idEmpresa: string | null;
  usuarioAtualizacao: string | null;
  criadoEm: string | null;
  atualizadoEm: string | null;
};

@Injectable()
export class ModeloVeiculoService {
  constructor(private readonly dataSource: DataSource) {}

  async listarTodos(idEmpresa: number, idMarca?: number) {
    return this.executarComRls(idEmpresa, async (manager, colunas) => {
      const filtros: string[] = [];
      const valores: Array<string | number> = [];

      if (colunas.idEmpresa) {
        valores.push(String(idEmpresa));
        filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
      }

      if (idMarca !== undefined) {
        const colunaMarca = this.exigirColuna(colunas.idMarca, 'idMarca');
        valores.push(idMarca);
        filtros.push(`${this.quote(colunaMarca)} = $${valores.length}`);
      }

      const sql = `
        SELECT *
        FROM app.modelo_vei
        ${filtros.length > 0 ? `WHERE ${filtros.join(' AND ')}` : ''}
        ORDER BY ${this.quote(colunas.descricao)} ASC, ${this.quote(colunas.idModelo)} ASC
      `;

      const rows = await manager.query(sql, valores);
      const modelos = rows.map((row) => this.mapear(row, colunas));

      return {
        sucesso: true,
        total: modelos.length,
        modelos,
      };
    });
  }

  async cadastrar(
    idEmpresa: number,
    dados: CriarModeloVeiculoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    try {
      return await this.executarComRls(idEmpresa, async (manager, colunas) => {
        const colunaMarca = this.exigirColuna(colunas.idMarca, 'idMarca');
        const descricao = this.normalizarDescricao(dados.descricao);
        const status = this.normalizarStatus(dados.status);
        const usuarioAtualizacao = this.normalizarUsuario(
          dados.usuarioAtualizacao ?? usuarioJwt.email,
        );

        await this.validarDuplicidadeDescricao(
          manager,
          colunas,
          idEmpresa,
          dados.idMarca,
          descricao,
        );

        const insertCols: string[] = [];
        const values: Array<string | number> = [];
        const placeholders: string[] = [];

        const addValue = (coluna: string, valor: string | number) => {
          insertCols.push(this.quote(coluna));
          values.push(valor);
          placeholders.push(`$${values.length}`);
        };

        addValue(colunaMarca, dados.idMarca);
        addValue(colunas.descricao, descricao);

        if (colunas.status) {
          addValue(colunas.status, status);
        }

        if (colunas.idEmpresa) {
          addValue(colunas.idEmpresa, String(idEmpresa));
        }

        if (colunas.usuarioAtualizacao) {
          addValue(colunas.usuarioAtualizacao, usuarioAtualizacao);
        }

        if (colunas.criadoEm) {
          insertCols.push(this.quote(colunas.criadoEm));
          placeholders.push('NOW()');
        }

        if (colunas.atualizadoEm) {
          insertCols.push(this.quote(colunas.atualizadoEm));
          placeholders.push('NOW()');
        }

        const sql = `
          INSERT INTO app.modelo_vei (${insertCols.join(', ')})
          VALUES (${placeholders.join(', ')})
          RETURNING *
        `;

        const rows = await manager.query(sql, values);
        const registro = rows[0];

        if (!registro) {
          throw new BadRequestException('Falha ao cadastrar modelo de veiculo.');
        }

        return {
          sucesso: true,
          mensagem: 'Modelo de veiculo cadastrado com sucesso.',
          modelo: this.mapear(registro, colunas),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error);
    }
  }

  async buscarPorId(idEmpresa: number, idModelo: number) {
    return this.executarComRls(idEmpresa, async (manager, colunas) => {
      const filtros: string[] = [];
      const valores: Array<string | number> = [];

      valores.push(idModelo);
      filtros.push(`${this.quote(colunas.idModelo)} = $${valores.length}`);

      if (colunas.idEmpresa) {
        valores.push(String(idEmpresa));
        filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
      }

      const sql = `
        SELECT *
        FROM app.modelo_vei
        WHERE ${filtros.join(' AND ')}
        LIMIT 1
      `;

      const rows = await manager.query(sql, valores);
      const registro = rows[0];

      if (!registro) {
        throw new NotFoundException(
          'Modelo de veiculo nao encontrado para a empresa logada.',
        );
      }

      return {
        sucesso: true,
        modelo: this.mapear(registro, colunas),
      };
    });
  }

  async atualizar(
    idEmpresa: number,
    idModelo: number,
    dados: CriarModeloVeiculoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    try {
      return await this.executarComRls(idEmpresa, async (manager, colunas) => {
        const colunaMarca = this.exigirColuna(colunas.idMarca, 'idMarca');
        const descricao = this.normalizarDescricao(dados.descricao);
        const status = this.normalizarStatus(dados.status);
        const usuarioAtualizacao = this.normalizarUsuario(
          dados.usuarioAtualizacao ?? usuarioJwt.email,
        );

        await this.validarDuplicidadeDescricao(
          manager,
          colunas,
          idEmpresa,
          dados.idMarca,
          descricao,
          idModelo,
        );

        const sets: string[] = [];
        const valores: Array<string | number> = [];
        const addSet = (coluna: string, valor: string | number) => {
          valores.push(valor);
          sets.push(`${this.quote(coluna)} = $${valores.length}`);
        };

        addSet(colunaMarca, dados.idMarca);
        addSet(colunas.descricao, descricao);
        if (colunas.status) addSet(colunas.status, status);
        if (colunas.usuarioAtualizacao) addSet(colunas.usuarioAtualizacao, usuarioAtualizacao);
        if (colunas.atualizadoEm) sets.push(`${this.quote(colunas.atualizadoEm)} = NOW()`);

        const filtros: string[] = [];
        valores.push(idModelo);
        filtros.push(`${this.quote(colunas.idModelo)} = $${valores.length}`);
        if (colunas.idEmpresa) {
          valores.push(String(idEmpresa));
          filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
        }

        const rows = await manager.query(
          `
            UPDATE app.modelo_vei
            SET ${sets.join(', ')}
            WHERE ${filtros.join(' AND ')}
            RETURNING *
          `,
          valores,
        );

        const registro = rows[0];
        if (!registro) {
          throw new NotFoundException('Modelo de veiculo nao encontrado para a empresa logada.');
        }

        return {
          sucesso: true,
          mensagem: status === 'I' ? 'Modelo de veiculo inativado com sucesso.' : 'Modelo de veiculo atualizado com sucesso.',
          modelo: this.mapear(registro, colunas),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error);
    }
  }

  private async executarComRls<T>(
    idEmpresa: number,
    callback: (
      manager: EntityManager,
      colunas: MapaColunasModeloVeiculo,
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
  ): Promise<MapaColunasModeloVeiculo> {
    const rows = await manager.query(`
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = 'app'
        AND table_name = 'modelo_vei'
    `);

    if (rows.length === 0) {
      throw new BadRequestException('Tabela app.modelo_vei nao encontrada.');
    }

    const set = new Set<string>(
      rows
        .map((row) =>
          typeof row.column_name === 'string' ? row.column_name : '',
        )
        .filter((value) => value.length > 0),
    );

    return {
      idModelo: this.encontrarColuna(
        set,
        ['id_modelo_vei', 'id_modelo', 'id'],
        'id do modelo',
      )!,
      idMarca: this.encontrarColuna(
        set,
        ['id_marca_vei', 'id_marca'],
        '',
        false,
      ),
      descricao: this.encontrarColuna(
        set,
        ['descricao', 'modelo', 'nome'],
        'descricao do modelo',
      )!,
      status: this.encontrarColuna(
        set,
        ['status', 'situacao', 'ativo'],
        '',
        false,
      ),
      idEmpresa: this.encontrarColuna(set, ['id_empresa'], '', false),
      usuarioAtualizacao: this.encontrarColuna(
        set,
        ['usuario_atualizacao', 'usuario_update', 'usuario'],
        '',
        false,
      ),
      criadoEm: this.encontrarColuna(
        set,
        ['criado_em', 'data_cadastro', 'created_at'],
        '',
        false,
      ),
      atualizadoEm: this.encontrarColuna(
        set,
        ['atualizado_em', 'data_atualizacao', 'updated_at'],
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
        `Estrutura da tabela app.modelo_vei invalida: coluna de ${descricao} nao encontrada.`,
      );
    }

    return null;
  }

  private mapear(
    registro: RegistroBanco,
    colunas: MapaColunasModeloVeiculo,
  ): ListarModeloVeiculoDto {
    return {
      idModelo: this.converterNumero(registro[colunas.idModelo]) ?? 0,
      idMarca:
        colunas.idMarca !== null
          ? this.converterNumero(registro[colunas.idMarca])
          : null,
      descricao: this.converterTexto(registro[colunas.descricao]) ?? '',
      status:
        colunas.status !== null
          ? (this.converterTexto(registro[colunas.status])?.toUpperCase() ??
            null)
          : null,
    };
  }

  private async validarDuplicidadeDescricao(
    manager: EntityManager,
    colunas: MapaColunasModeloVeiculo,
    idEmpresa: number,
    idMarca: number,
    descricao: string,
    idModeloIgnorado?: number,
  ) {
    const colunaMarca = this.exigirColuna(colunas.idMarca, 'idMarca');
    const filtros: string[] = [
      `${this.quote(colunaMarca)} = $1`,
      `UPPER(${this.quote(colunas.descricao)}) = UPPER($2)`,
    ];
    const valores: Array<string | number> = [idMarca, descricao];

    if (colunas.idEmpresa) {
      valores.push(String(idEmpresa));
      filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
    }

    if (idModeloIgnorado !== undefined) {
      valores.push(idModeloIgnorado);
      filtros.push(`${this.quote(colunas.idModelo)} <> $${valores.length}`);
    }

    const rows = await manager.query(
      `
        SELECT 1
        FROM app.modelo_vei
        WHERE ${filtros.join(' AND ')}
        LIMIT 1
      `,
      valores,
    );

    if (rows[0]) {
      throw new BadRequestException(
        'Ja existe um modelo com essa descricao para a marca informada na empresa logada.',
      );
    }
  }

  private normalizarDescricao(valor: string) {
    const texto = valor.trim().toUpperCase();
    if (!texto) {
      throw new BadRequestException('Descricao do modelo e obrigatoria.');
    }
    return texto;
  }

  private normalizarStatus(valor: string | undefined) {
    return valor?.trim().toUpperCase() === 'I' ? 'I' : 'A';
  }

  private normalizarUsuario(valor: string) {
    const texto = valor.trim().toUpperCase();
    return texto.length > 0 ? texto : 'SISTEMA';
  }

  private exigirColuna(coluna: string | null, campoApi: string): string {
    if (!coluna) {
      throw new BadRequestException(
        `Campo ${campoApi} nao esta disponivel na tabela app.modelo_vei.`,
      );
    }

    return coluna;
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

  private tratarErroPersistencia(error: unknown): never {
    if (error instanceof BadRequestException || error instanceof NotFoundException) {
      throw error;
    }

    if (error instanceof QueryFailedError) {
      const erroPg = error.driverError as { code?: string };
      if (erroPg.code === '23505') {
        throw new BadRequestException(
          'Ja existe um modelo com essa descricao para a marca informada na empresa logada.',
        );
      }
    }

    throw new BadRequestException(
      'Nao foi possivel cadastrar modelo de veiculo neste momento.',
    );
  }
}
