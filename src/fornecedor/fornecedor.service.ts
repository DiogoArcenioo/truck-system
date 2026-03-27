import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { DataSource, EntityManager } from 'typeorm';
import { configurarContextoEmpresaRls } from '../common/database/rls.util';
import { ListarFornecedorDto } from './dto/listar-fornecedor.dto';

type RegistroFornecedor = Record<string, unknown>;

type MapaColunasFornecedor = {
  idFornecedor: string;
  nome: string;
  status: string | null;
  idEmpresa: string | null;
};

@Injectable()
export class FornecedorService {
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
        FROM app.fornecedor
        ${filtros.length > 0 ? `WHERE ${filtros.join(' AND ')}` : ''}
        ORDER BY ${this.quote(colunas.nome)} ASC, ${this.quote(colunas.idFornecedor)} ASC
      `;

      const rows = (await manager.query(sql, valores)) as RegistroFornecedor[];
      const fornecedores = rows.map((row) => this.mapear(row, colunas));

      return {
        sucesso: true,
        total: fornecedores.length,
        fornecedores,
      };
    });
  }

  async buscarPorId(idEmpresa: number, idFornecedor: number) {
    return this.executarComRls(idEmpresa, async (manager, colunas) => {
      const filtros: string[] = [];
      const valores: Array<string | number> = [];

      valores.push(idFornecedor);
      filtros.push(`${this.quote(colunas.idFornecedor)} = $${valores.length}`);

      if (colunas.idEmpresa) {
        valores.push(String(idEmpresa));
        filtros.push(`${this.quote(colunas.idEmpresa)} = $${valores.length}`);
      }

      const sql = `
        SELECT *
        FROM app.fornecedor
        WHERE ${filtros.join(' AND ')}
        LIMIT 1
      `;

      const rows = (await manager.query(sql, valores)) as RegistroFornecedor[];
      const registro = rows[0];

      if (!registro) {
        throw new NotFoundException(
          'Fornecedor nao encontrado para a empresa logada.',
        );
      }

      return {
        sucesso: true,
        fornecedor: this.mapear(registro, colunas),
      };
    });
  }

  private async executarComRls<T>(
    idEmpresa: number,
    callback: (
      manager: EntityManager,
      colunas: MapaColunasFornecedor,
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
  ): Promise<MapaColunasFornecedor> {
    const rows = (await manager.query(`
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = 'app'
        AND table_name = 'fornecedor'
    `)) as Array<{ column_name?: string }>;

    if (rows.length === 0) {
      throw new BadRequestException('Tabela app.fornecedor nao encontrada.');
    }

    const set = new Set(
      rows
        .map((row) => (typeof row.column_name === 'string' ? row.column_name : ''))
        .filter((value) => value.length > 0),
    );

    return {
      idFornecedor: this.encontrarColuna(
        set,
        ['id_fornecedor', 'id'],
        'id do fornecedor',
      )!,
      nome: this.encontrarColuna(
        set,
        ['nome_fornecedor', 'nome', 'razao_social', 'fantasia'],
        'nome do fornecedor',
      )!,
      status: this.encontrarColuna(set, ['status', 'ativo'], '', false),
      idEmpresa: this.encontrarColuna(set, ['id_empresa'], '', false),
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
        `Estrutura da tabela app.fornecedor invalida: coluna de ${descricao} nao encontrada.`,
      );
    }

    return null;
  }

  private mapear(
    registro: RegistroFornecedor,
    colunas: MapaColunasFornecedor,
  ): ListarFornecedorDto {
    const idFornecedor = Number(registro[colunas.idFornecedor]);
    const nomeBruto = registro[colunas.nome];

    return {
      idFornecedor: Number.isFinite(idFornecedor) ? idFornecedor : 0,
      nome:
        typeof nomeBruto === 'string' && nomeBruto.trim()
          ? nomeBruto.trim()
          : 'SEM NOME',
      status:
        colunas.status !== null && typeof registro[colunas.status] === 'string'
          ? (registro[colunas.status] as string).trim()
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
}
