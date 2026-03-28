import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { DataSource, EntityManager } from 'typeorm';
import { configurarContextoEmpresaRls } from '../common/database/rls.util';
import { ListarCorVeiculoDto } from './dto/listar-cor-veiculo.dto';

type RegistroBanco = Record<string, unknown>;

type MapaColunasCorVeiculo = {
  idCor: string;
  descricao: string;
  status: string | null;
  idEmpresa: string | null;
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
}
