import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { DataSource, EntityManager, QueryFailedError } from 'typeorm';
import { JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { configurarContextoEmpresaRls } from '../common/database/rls.util';
import { CriarTipoVeiculoDto } from './dto/criar-tipo-veiculo.dto';
import { ListarTipoVeiculoDto } from './dto/listar-tipo-veiculo.dto';

type RegistroBanco = Record<string, unknown>;

type MapaColunasTipoVeiculo = {
  idTipo: string;
  descricao: string;
  status: string | null;
  idEmpresa: string | null;
  usuarioAtualizacao: string | null;
  criadoEm: string | null;
  atualizadoEm: string | null;
};

@Injectable()
export class TiposVeiculoService {
  constructor(private readonly dataSource: DataSource) {}

  async listarTodos(idEmpresa: number) {
    return this.executarComRls(idEmpresa, async (manager, colunas) => {
      const filtros: string[] = [];
      const valores: Array<string | number> = [];

      if (colunas.idEmpresa) {
        valores.push(String(idEmpresa));
        filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
      }

      const sql = `
        SELECT *
        FROM app.tipos_vei
        ${filtros.length > 0 ? `WHERE ${filtros.join(' AND ')}` : ''}
        ORDER BY ${this.quote(colunas.descricao)} ASC, ${this.quote(colunas.idTipo)} ASC
      `;

      const rows = await manager.query(sql, valores);
      const tipos = rows.map((row) => this.mapear(row, colunas));

      return {
        sucesso: true,
        total: tipos.length,
        tipos,
      };
    });
  }

  async cadastrar(
    idEmpresa: number,
    dados: CriarTipoVeiculoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    try {
      return await this.executarComRls(idEmpresa, async (manager, colunas) => {
        const descricao = this.normalizarDescricao(dados.descricao);
        const status = this.normalizarStatus(dados.status);
        const usuarioAtualizacao = this.normalizarUsuario(
          dados.usuarioAtualizacao ?? usuarioJwt.email,
        );

        await this.validarDuplicidadeDescricao(
          manager,
          colunas,
          idEmpresa,
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
          INSERT INTO app.tipos_vei (${insertCols.join(', ')})
          VALUES (${placeholders.join(', ')})
          RETURNING *
        `;

        const rows = await manager.query(sql, values);
        const registro = rows[0];

        if (!registro) {
          throw new BadRequestException('Falha ao cadastrar tipo de veiculo.');
        }

        return {
          sucesso: true,
          mensagem: 'Tipo de veiculo cadastrado com sucesso.',
          tipo: this.mapear(registro, colunas),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error);
    }
  }

  async buscarPorId(idEmpresa: number, idTipo: number) {
    return this.executarComRls(idEmpresa, async (manager, colunas) => {
      const filtros: string[] = [];
      const valores: Array<string | number> = [];

      valores.push(idTipo);
      filtros.push(`${this.quote(colunas.idTipo)} = $${valores.length}`);

      if (colunas.idEmpresa) {
        valores.push(String(idEmpresa));
        filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
      }

      const sql = `
        SELECT *
        FROM app.tipos_vei
        WHERE ${filtros.join(' AND ')}
        LIMIT 1
      `;

      const rows = await manager.query(sql, valores);
      const registro = rows[0];

      if (!registro) {
        throw new NotFoundException(
          'Tipo de veiculo nao encontrado para a empresa logada.',
        );
      }

      return {
        sucesso: true,
        tipo: this.mapear(registro, colunas),
      };
    });
  }

  async atualizar(
    idEmpresa: number,
    idTipo: number,
    dados: CriarTipoVeiculoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    try {
      return await this.executarComRls(idEmpresa, async (manager, colunas) => {
        const descricao = this.normalizarDescricao(dados.descricao);
        const status = this.normalizarStatus(dados.status);
        const usuarioAtualizacao = this.normalizarUsuario(
          dados.usuarioAtualizacao ?? usuarioJwt.email,
        );

        await this.validarDuplicidadeDescricao(
          manager,
          colunas,
          idEmpresa,
          descricao,
          idTipo,
        );

        const sets: string[] = [];
        const valores: Array<string | number> = [];
        const addSet = (coluna: string, valor: string | number) => {
          valores.push(valor);
          sets.push(`${this.quote(coluna)} = $${valores.length}`);
        };

        addSet(colunas.descricao, descricao);
        if (colunas.status) addSet(colunas.status, status);
        if (colunas.usuarioAtualizacao) addSet(colunas.usuarioAtualizacao, usuarioAtualizacao);
        if (colunas.atualizadoEm) sets.push(`${this.quote(colunas.atualizadoEm)} = NOW()`);

        const filtros: string[] = [];
        valores.push(idTipo);
        filtros.push(`${this.quote(colunas.idTipo)} = $${valores.length}`);
        if (colunas.idEmpresa) {
          valores.push(String(idEmpresa));
          filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
        }

        const rows = await manager.query(
          `
            UPDATE app.tipos_vei
            SET ${sets.join(', ')}
            WHERE ${filtros.join(' AND ')}
            RETURNING *
          `,
          valores,
        );

        const registro = rows[0];
        if (!registro) {
          throw new NotFoundException('Tipo de veiculo nao encontrado para a empresa logada.');
        }

        return {
          sucesso: true,
          mensagem: status === 'I' ? 'Tipo de veiculo inativado com sucesso.' : 'Tipo de veiculo atualizado com sucesso.',
          tipo: this.mapear(registro, colunas),
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
      colunas: MapaColunasTipoVeiculo,
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
  ): Promise<MapaColunasTipoVeiculo> {
    const rows = await manager.query(`
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = 'app'
        AND table_name = 'tipos_vei'
    `);

    if (rows.length === 0) {
      throw new BadRequestException('Tabela app.tipos_vei nao encontrada.');
    }

    const set = new Set<string>(
      rows
        .map((row) =>
          typeof row.column_name === 'string' ? row.column_name : '',
        )
        .filter((value) => value.length > 0),
    );

    return {
      idTipo: this.encontrarColuna(
        set,
        ['id_tipos_vei', 'id_tipo_vei', 'id_tipo', 'id'],
        'id do tipo',
      )!,
      descricao: this.encontrarColuna(
        set,
        ['descricao', 'tipo', 'nome'],
        'descricao do tipo',
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
        `Estrutura da tabela app.tipos_vei invalida: coluna de ${descricao} nao encontrada.`,
      );
    }

    return null;
  }

  private mapear(
    registro: RegistroBanco,
    colunas: MapaColunasTipoVeiculo,
  ): ListarTipoVeiculoDto {
    return {
      idTipo: this.converterNumero(registro[colunas.idTipo]) ?? 0,
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
    colunas: MapaColunasTipoVeiculo,
    idEmpresa: number,
    descricao: string,
    idTipoIgnorado?: number,
  ) {
    const filtros: string[] = [
      `UPPER(${this.quote(colunas.descricao)}) = UPPER($1)`,
    ];
    const valores: Array<string | number> = [descricao];

    if (colunas.idEmpresa) {
      valores.push(String(idEmpresa));
      filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
    }

    if (idTipoIgnorado !== undefined) {
      valores.push(idTipoIgnorado);
      filtros.push(`${this.quote(colunas.idTipo)} <> $${valores.length}`);
    }

    const rows = await manager.query(
      `
        SELECT 1
        FROM app.tipos_vei
        WHERE ${filtros.join(' AND ')}
        LIMIT 1
      `,
      valores,
    );

    if (rows[0]) {
      throw new BadRequestException(
        'Ja existe um tipo de veiculo com essa descricao para a empresa logada.',
      );
    }
  }

  private normalizarDescricao(valor: string) {
    const texto = valor.trim().toUpperCase();
    if (!texto) {
      throw new BadRequestException('Descricao do tipo e obrigatoria.');
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
          'Ja existe um tipo de veiculo com essa descricao para a empresa logada.',
        );
      }
    }

    throw new BadRequestException(
      'Nao foi possivel cadastrar tipo de veiculo neste momento.',
    );
  }
}
