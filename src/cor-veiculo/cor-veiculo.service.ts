import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { DataSource, EntityManager, QueryFailedError } from 'typeorm';
import { JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { configurarContextoEmpresaRls } from '../common/database/rls.util';
import { CriarCorVeiculoDto } from './dto/criar-cor-veiculo.dto';
import { ListarCorVeiculoDto } from './dto/listar-cor-veiculo.dto';

type RegistroBanco = Record<string, unknown>;

type MapaColunasCorVeiculo = {
  idCor: string;
  descricao: string;
  status: string | null;
  idEmpresa: string | null;
  usuarioAtualizacao: string | null;
  criadoEm: string | null;
  atualizadoEm: string | null;
};

@Injectable()
export class CorVeiculoService {
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
        FROM app.cor_vei
        ${filtros.length > 0 ? `WHERE ${filtros.join(' AND ')}` : ''}
        ORDER BY ${this.quote(colunas.descricao)} ASC, ${this.quote(colunas.idCor)} ASC
      `;

      const rows = await manager.query(sql, valores);
      const cores = rows.map((row) => this.mapear(row, colunas));

      return {
        sucesso: true,
        total: cores.length,
        cores,
      };
    });
  }

  async cadastrar(
    idEmpresa: number,
    dados: CriarCorVeiculoDto,
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
          INSERT INTO app.cor_vei (${insertCols.join(', ')})
          VALUES (${placeholders.join(', ')})
          RETURNING *
        `;

        const rows = await manager.query(sql, values);
        const registro = rows[0];

        if (!registro) {
          throw new BadRequestException('Falha ao cadastrar cor de veiculo.');
        }

        return {
          sucesso: true,
          mensagem: 'Cor de veiculo cadastrada com sucesso.',
          cor: this.mapear(registro, colunas),
        };
      });
    } catch (error) {
      this.tratarErroPersistencia(error);
    }
  }

  async buscarPorId(idEmpresa: number, idCor: number) {
    return this.executarComRls(idEmpresa, async (manager, colunas) => {
      const filtros: string[] = [];
      const valores: Array<string | number> = [];

      valores.push(idCor);
      filtros.push(`${this.quote(colunas.idCor)} = $${valores.length}`);

      if (colunas.idEmpresa) {
        valores.push(String(idEmpresa));
        filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
      }

      const sql = `
        SELECT *
        FROM app.cor_vei
        WHERE ${filtros.join(' AND ')}
        LIMIT 1
      `;

      const rows = await manager.query(sql, valores);
      const registro = rows[0];

      if (!registro) {
        throw new NotFoundException(
          'Cor de veiculo nao encontrada para a empresa logada.',
        );
      }

      return {
        sucesso: true,
        cor: this.mapear(registro, colunas),
      };
    });
  }

  async atualizar(
    idEmpresa: number,
    idCor: number,
    dados: CriarCorVeiculoDto,
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
          idCor,
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
        valores.push(idCor);
        filtros.push(`${this.quote(colunas.idCor)} = $${valores.length}`);
        if (colunas.idEmpresa) {
          valores.push(String(idEmpresa));
          filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
        }

        const rows = await manager.query(
          `
            UPDATE app.cor_vei
            SET ${sets.join(', ')}
            WHERE ${filtros.join(' AND ')}
            RETURNING *
          `,
          valores,
        );

        const registro = rows[0];
        if (!registro) {
          throw new NotFoundException('Cor de veiculo nao encontrada para a empresa logada.');
        }

        return {
          sucesso: true,
          mensagem: status === 'I' ? 'Cor de veiculo inativada com sucesso.' : 'Cor de veiculo atualizada com sucesso.',
          cor: this.mapear(registro, colunas),
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
      colunas: MapaColunasCorVeiculo,
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
  ): Promise<MapaColunasCorVeiculo> {
    const rows = await manager.query(`
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = 'app'
        AND table_name = 'cor_vei'
    `);

    if (rows.length === 0) {
      throw new BadRequestException('Tabela app.cor_vei nao encontrada.');
    }

    const set = new Set<string>(
      rows
        .map((row) =>
          typeof row.column_name === 'string' ? row.column_name : '',
        )
        .filter((value) => value.length > 0),
    );

    return {
      idCor: this.encontrarColuna(
        set,
        ['id_cor_vei', 'id_cor', 'id'],
        'id da cor',
      )!,
      descricao: this.encontrarColuna(
        set,
        ['descricao', 'cor', 'nome'],
        'descricao da cor',
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
        `Estrutura da tabela app.cor_vei invalida: coluna de ${descricao} nao encontrada.`,
      );
    }

    return null;
  }

  private mapear(
    registro: RegistroBanco,
    colunas: MapaColunasCorVeiculo,
  ): ListarCorVeiculoDto {
    return {
      idCor: this.converterNumero(registro[colunas.idCor]) ?? 0,
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
    colunas: MapaColunasCorVeiculo,
    idEmpresa: number,
    descricao: string,
    idCorIgnorada?: number,
  ) {
    const filtros: string[] = [
      `UPPER(${this.quote(colunas.descricao)}) = UPPER($1)`,
    ];
    const valores: Array<string | number> = [descricao];

    if (colunas.idEmpresa) {
      valores.push(String(idEmpresa));
      filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
    }

    if (idCorIgnorada !== undefined) {
      valores.push(idCorIgnorada);
      filtros.push(`${this.quote(colunas.idCor)} <> $${valores.length}`);
    }

    const rows = await manager.query(
      `
        SELECT 1
        FROM app.cor_vei
        WHERE ${filtros.join(' AND ')}
        LIMIT 1
      `,
      valores,
    );

    if (rows[0]) {
      throw new BadRequestException(
        'Ja existe uma cor de veiculo com essa descricao para a empresa logada.',
      );
    }
  }

  private normalizarDescricao(valor: string) {
    const texto = valor.trim().toUpperCase();
    if (!texto) {
      throw new BadRequestException('Descricao da cor e obrigatoria.');
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
          'Ja existe uma cor de veiculo com essa descricao para a empresa logada.',
        );
      }
    }

    throw new BadRequestException(
      'Nao foi possivel cadastrar cor de veiculo neste momento.',
    );
  }
}
