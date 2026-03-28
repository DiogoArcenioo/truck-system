import { Injectable } from '@nestjs/common';
import { DataSource, EntityManager } from 'typeorm';
import { configurarContextoEmpresaRls } from '../common/database/rls.util';

type RegistroBanco = Record<string, unknown>;

@Injectable()
export class ProdutoReferenciasService {
  constructor(private readonly dataSource: DataSource) {}

  async listarTodos(idEmpresa: number) {
    return this.executarComRls(idEmpresa, async (manager) => {
      const [gruposRows, subgruposRows, unidadesRows, marcasRows] = await Promise.all([
        manager.query(
          `
            SELECT id_grupo_produto, descricao, situacao
            FROM app.grupo_produto
            WHERE id_empresa = $1
            ORDER BY descricao ASC, id_grupo_produto ASC
          `,
          [String(idEmpresa)],
        ) as Promise<RegistroBanco[]>,
        manager.query(
          `
            SELECT id_subgrupo, id_grupo_produto, descricao, situacao
            FROM app.subgrupo_produto
            WHERE id_empresa = $1
            ORDER BY descricao ASC, id_subgrupo ASC
          `,
          [String(idEmpresa)],
        ) as Promise<RegistroBanco[]>,
        manager.query(
          `
            SELECT id_un, tipo_un, descricao, situacao
            FROM app.un_produto
            WHERE id_empresa = $1
            ORDER BY descricao ASC, id_un ASC
          `,
          [String(idEmpresa)],
        ) as Promise<RegistroBanco[]>,
        manager.query(
          `
            SELECT id_marca, descricao, situacao
            FROM app.marca_produto
            WHERE id_empresa = $1
            ORDER BY descricao ASC, id_marca ASC
          `,
          [String(idEmpresa)],
        ) as Promise<RegistroBanco[]>,
      ]);

      return {
        sucesso: true,
        gruposProduto: gruposRows.map((item) => ({
          idGrupoProduto: this.converterNumero(item.id_grupo_produto) ?? 0,
          descricao: this.converterTexto(item.descricao) ?? '',
          situacao: this.converterTexto(item.situacao)?.toUpperCase() ?? 'A',
        })),
        subgruposProduto: subgruposRows.map((item) => ({
          idSubgrupo: this.converterNumero(item.id_subgrupo) ?? 0,
          idGrupoProduto: this.converterNumero(item.id_grupo_produto) ?? 0,
          descricao: this.converterTexto(item.descricao) ?? '',
          situacao: this.converterTexto(item.situacao)?.toUpperCase() ?? 'A',
        })),
        unidadesProduto: unidadesRows.map((item) => ({
          idUn: this.converterNumero(item.id_un) ?? 0,
          tipoUn: this.converterTexto(item.tipo_un) ?? '',
          descricao: this.converterTexto(item.descricao) ?? '',
          situacao: this.converterTexto(item.situacao)?.toUpperCase() ?? 'A',
        })),
        marcasProduto: marcasRows.map((item) => ({
          idMarca: this.converterNumero(item.id_marca) ?? 0,
          descricao: this.converterTexto(item.descricao) ?? '',
          situacao: this.converterTexto(item.situacao)?.toUpperCase() ?? 'A',
        })),
      };
    });
  }

  private async executarComRls<T>(
    idEmpresa: number,
    callback: (manager: EntityManager) => Promise<T>,
  ): Promise<T> {
    return this.dataSource.manager.transaction(async (manager) => {
      await configurarContextoEmpresaRls(manager, idEmpresa);
      return callback(manager);
    });
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
    return texto ? texto : null;
  }
}
