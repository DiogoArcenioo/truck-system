import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { DataSource, EntityManager, QueryFailedError } from 'typeorm';
import { JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { configurarContextoEmpresaRls } from '../common/database/rls.util';
import { AtualizarFornecedorDto } from './dto/atualizar-fornecedor.dto';
import { CriarFornecedorDto } from './dto/criar-fornecedor.dto';
import { FiltroFornecedorDto } from './dto/filtro-fornecedor.dto';
import {
  AtualizarFornecedorContatoDto,
  CriarFornecedorContatoDto,
} from './dto/fornecedor-contato.dto';
import {
  AtualizarFornecedorEnderecoDto,
  CriarFornecedorEnderecoDto,
} from './dto/fornecedor-endereco.dto';
import {
  ListarFornecedorContatoDto,
  ListarFornecedorDto,
  ListarFornecedorEnderecoDto,
} from './dto/listar-fornecedor.dto';
import { TIPO_PESSOA_OPCOES } from './fornecedor.constants';

type RegistroBanco = Record<string, unknown>;

type PayloadFornecedor = {
  tipoPessoa: string;
  razaoSocial: string | null;
  nomeFantasia: string | null;
  nomePessoa: string | null;
  cpf: string | null;
  cnpj: string | null;
  inscricaoEstadual: string | null;
  inscricaoMunicipal: string | null;
  observacoes: string | null;
  ativo: boolean;
  usuarioAtualizacao: string;
};

type PayloadContato = {
  nomeContato: string;
  telefone: string | null;
  celular: string | null;
  email: string | null;
  principal: boolean;
  usuarioAtualizacao: string;
};

type PayloadEndereco = {
  logradouro: string;
  numero: string | null;
  complemento: string | null;
  bairro: string | null;
  cidade: string | null;
  estado: string | null;
  cep: string | null;
  principal: boolean;
  usuarioAtualizacao: string;
};

@Injectable()
export class FornecedorService {
  constructor(private readonly dataSource: DataSource) {}

  listarOpcoes() {
    return { sucesso: true, tipoPessoa: TIPO_PESSOA_OPCOES };
  }

  async listarTodos(idEmpresa: number) {
    return this.executarComRls(idEmpresa, async (manager) => {
      const rows = await this.listarRowsFornecedor(manager, idEmpresa);
      const fornecedores = rows.map((row) => this.mapearFornecedor(row));

      return { sucesso: true, total: fornecedores.length, fornecedores };
    });
  }

  async listarComFiltro(idEmpresa: number, filtro: FiltroFornecedorDto) {
    return this.executarComRls(idEmpresa, async (manager) => {
      const filtros: string[] = ['f.id_empresa = $1'];
      const valores: Array<string | number | boolean> = [String(idEmpresa)];

      if (filtro.idFornecedor !== undefined) {
        valores.push(filtro.idFornecedor);
        filtros.push(`f.id_fornecedor = $${valores.length}`);
      }

      if (filtro.tipoPessoa) {
        valores.push(filtro.tipoPessoa);
        filtros.push(`f.tipo_pessoa = $${valores.length}`);
      }

      if (filtro.ativo !== undefined) {
        valores.push(filtro.ativo);
        filtros.push(`f.ativo = $${valores.length}`);
      }

      if (filtro.texto?.trim()) {
        valores.push(`%${filtro.texto.trim().toUpperCase()}%`);
        const idx = valores.length;
        filtros.push(
          `(UPPER(COALESCE(f.nome_fantasia, '')) LIKE $${idx} OR UPPER(COALESCE(f.razao_social, '')) LIKE $${idx} OR UPPER(COALESCE(f.nome, '')) LIKE $${idx} OR UPPER(COALESCE(f.cnpj, '')) LIKE $${idx} OR UPPER(COALESCE(f.cpf, '')) LIKE $${idx})`,
        );
      }

      const whereSql = `WHERE ${filtros.join(' AND ')}`;
      const pagina = filtro.pagina ?? 1;
      const limite = filtro.limite ?? 20;
      const offset = (pagina - 1) * limite;
      const ordem = filtro.ordem ?? 'ASC';
      const colunaOrdenacao = this.resolverColunaOrdenacao(filtro.ordenarPor);

      const sqlCount = `
        SELECT COUNT(1)::int AS total
        FROM app.fornecedor f
        ${whereSql}
      `;

      const sqlDados = `
        SELECT
          f.*,
          (SELECT COUNT(1)::int FROM app.fornecedor_contatos c WHERE c.id_fornecedor = f.id_fornecedor) AS qtd_contatos,
          (SELECT COUNT(1)::int FROM app.fornecedor_enderecos e WHERE e.id_fornecedor = f.id_fornecedor) AS qtd_enderecos,
          (
            SELECT COALESCE(NULLIF(c.celular, ''), NULLIF(c.telefone, ''))
            FROM app.fornecedor_contatos c
            WHERE c.id_fornecedor = f.id_fornecedor
            ORDER BY c.principal DESC, c.id_contato ASC
            LIMIT 1
          ) AS resumo_contato,
          (
            SELECT TRIM(
              BOTH ' /' FROM CONCAT(
                COALESCE(NULLIF(e.cidade, ''), ''),
                CASE
                  WHEN NULLIF(e.estado, '') IS NOT NULL AND NULLIF(e.cidade, '') IS NOT NULL THEN '/'
                  ELSE ''
                END,
                COALESCE(NULLIF(e.estado, ''), '')
              )
            )
            FROM app.fornecedor_enderecos e
            WHERE e.id_fornecedor = f.id_fornecedor
            ORDER BY e.principal DESC, e.id_endereco ASC
            LIMIT 1
          ) AS resumo_endereco
        FROM app.fornecedor f
        ${whereSql}
        ORDER BY ${colunaOrdenacao} ${ordem}, f.id_fornecedor ASC
        LIMIT $${valores.length + 1}
        OFFSET $${valores.length + 2}
      `;

      const countRows = (await manager.query(sqlCount, valores)) as Array<{ total: number }>;
      const rows = (await manager.query(sqlDados, [
        ...valores,
        limite,
        offset,
      ])) as RegistroBanco[];

      const total = Number(countRows[0]?.total ?? 0);
      const fornecedores = rows.map((row) => this.mapearFornecedor(row));
      return {
        sucesso: true,
        paginaAtual: pagina,
        limite,
        total,
        totalPaginas: total > 0 ? Math.ceil(total / limite) : 0,
        fornecedores,
      };
    });
  }

  async buscarPorId(idEmpresa: number, idFornecedor: number) {
    return this.executarComRls(idEmpresa, async (manager) => {
      const fornecedor = await this.carregarFornecedorCompleto(
        manager,
        idEmpresa,
        idFornecedor,
      );
      return { sucesso: true, fornecedor };
    });
  }

  async cadastrar(
    idEmpresa: number,
    dados: CriarFornecedorDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    try {
      return await this.executarComRls(idEmpresa, async (manager) => {
        const payload = this.normalizarFornecedorCriacao(dados, usuarioJwt);
        await this.validarDocumentoDuplicadoEmpresa(manager, idEmpresa, payload);
        const rows = (await manager.query(
          `
            INSERT INTO app.fornecedor (
              tipo_pessoa, razao_social, nome_fantasia, nome, cpf, cnpj,
              inscricao_estadual, inscricao_municipal, observacoes, ativo,
              data_cadastro, usuario_atualizacao, id_empresa, atualizado_em
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW(),$11,$12,NOW())
            RETURNING id_fornecedor
          `,
          [
            payload.tipoPessoa,
            payload.razaoSocial,
            payload.nomeFantasia,
            payload.nomePessoa,
            payload.cpf,
            payload.cnpj,
            payload.inscricaoEstadual,
            payload.inscricaoMunicipal,
            payload.observacoes,
            payload.ativo,
            payload.usuarioAtualizacao,
            String(idEmpresa),
          ],
        )) as Array<{ id_fornecedor?: string | number }>;

        const idFornecedor = this.toNum(rows[0]?.id_fornecedor);
        if (!idFornecedor || idFornecedor <= 0) {
          throw new BadRequestException('Falha ao cadastrar fornecedor.');
        }

        const fornecedor = await this.carregarFornecedorCompleto(
          manager,
          idEmpresa,
          idFornecedor,
        );
        return { sucesso: true, mensagem: 'Fornecedor cadastrado com sucesso.', fornecedor };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'cadastrar');
    }
  }

  async atualizar(
    idEmpresa: number,
    idFornecedor: number,
    dados: AtualizarFornecedorDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    try {
      return await this.executarComRls(idEmpresa, async (manager) => {
        const atual = await this.buscarRegistroFornecedorOuFalhar(
          manager,
          idEmpresa,
          idFornecedor,
        );
        const payload = this.normalizarFornecedorAtualizacao(dados, usuarioJwt, atual);
        await this.validarDocumentoDuplicadoEmpresa(
          manager,
          idEmpresa,
          payload,
          idFornecedor,
        );

        const rows = (await manager.query(
          `
            UPDATE app.fornecedor
            SET
              tipo_pessoa = $1,
              razao_social = $2,
              nome_fantasia = $3,
              nome = $4,
              cpf = $5,
              cnpj = $6,
              inscricao_estadual = $7,
              inscricao_municipal = $8,
              observacoes = $9,
              ativo = $10,
              usuario_atualizacao = $11,
              atualizado_em = NOW()
            WHERE id_fornecedor = $12
              AND id_empresa = $13
            RETURNING id_fornecedor
          `,
          [
            payload.tipoPessoa,
            payload.razaoSocial,
            payload.nomeFantasia,
            payload.nomePessoa,
            payload.cpf,
            payload.cnpj,
            payload.inscricaoEstadual,
            payload.inscricaoMunicipal,
            payload.observacoes,
            payload.ativo,
            payload.usuarioAtualizacao,
            idFornecedor,
            String(idEmpresa),
          ],
        )) as Array<{ id_fornecedor?: string | number }>;

        if (!rows[0]) {
          throw new NotFoundException('Fornecedor não encontrado para a empresa logada.');
        }

        const fornecedor = await this.carregarFornecedorCompleto(
          manager,
          idEmpresa,
          idFornecedor,
        );
        return { sucesso: true, mensagem: 'Fornecedor atualizado com sucesso.', fornecedor };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'atualizar');
    }
  }

  async remover(idEmpresa: number, idFornecedor: number) {
    try {
      return await this.executarComRls(idEmpresa, async (manager) => {
        await this.buscarRegistroFornecedorOuFalhar(manager, idEmpresa, idFornecedor);
        await manager.query(`DELETE FROM app.fornecedor_contatos WHERE id_fornecedor = $1`, [
          idFornecedor,
        ]);
        await manager.query(`DELETE FROM app.fornecedor_enderecos WHERE id_fornecedor = $1`, [
          idFornecedor,
        ]);
        await manager.query(
          `DELETE FROM app.fornecedor WHERE id_fornecedor = $1 AND id_empresa = $2`,
          [idFornecedor, String(idEmpresa)],
        );
        return { sucesso: true, mensagem: 'Fornecedor removido com sucesso.' };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'remover');
    }
  }

  async adicionarContato(
    idEmpresa: number,
    idFornecedor: number,
    dados: CriarFornecedorContatoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    try {
      return await this.executarComRls(idEmpresa, async (manager) => {
        await this.buscarRegistroFornecedorOuFalhar(manager, idEmpresa, idFornecedor);
        const payload = this.normalizarContatoCriacao(dados, usuarioJwt);
        const rows = (await manager.query(
          `
            INSERT INTO app.fornecedor_contatos (
              id_fornecedor, nome_contato, telefone, celular, email,
              principal, usuario_atualizacao, criado_em, atualizado_em
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,NOW(),NOW())
            RETURNING id_contato
          `,
          [
            idFornecedor,
            payload.nomeContato,
            payload.telefone,
            payload.celular,
            payload.email,
            payload.principal,
            payload.usuarioAtualizacao,
          ],
        )) as Array<{ id_contato?: string | number }>;

        const idContato = this.toNum(rows[0]?.id_contato);
        if (!idContato || idContato <= 0) {
          throw new BadRequestException('Falha ao cadastrar contato.');
        }

        if (payload.principal) {
          await this.marcarContatoPrincipal(manager, idFornecedor, idContato);
        } else {
          await this.garantirContatoPrincipal(manager, idFornecedor);
        }

        const fornecedor = await this.carregarFornecedorCompleto(
          manager,
          idEmpresa,
          idFornecedor,
        );
        return { sucesso: true, mensagem: 'Contato cadastrado com sucesso.', fornecedor };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'cadastrar contato');
    }
  }

  async atualizarContato(
    idEmpresa: number,
    idFornecedor: number,
    idContato: number,
    dados: AtualizarFornecedorContatoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    try {
      return await this.executarComRls(idEmpresa, async (manager) => {
        const atual = await this.buscarContatoOuFalhar(
          manager,
          idEmpresa,
          idFornecedor,
          idContato,
        );
        const payload = this.normalizarContatoAtualizacao(dados, usuarioJwt, atual);

        await manager.query(
          `
            UPDATE app.fornecedor_contatos
            SET
              nome_contato = $1,
              telefone = $2,
              celular = $3,
              email = $4,
              principal = $5,
              usuario_atualizacao = $6,
              atualizado_em = NOW()
            WHERE id_fornecedor = $7
              AND id_contato = $8
          `,
          [
            payload.nomeContato,
            payload.telefone,
            payload.celular,
            payload.email,
            payload.principal,
            payload.usuarioAtualizacao,
            idFornecedor,
            idContato,
          ],
        );

        if (payload.principal) {
          await this.marcarContatoPrincipal(manager, idFornecedor, idContato);
        } else {
          await this.garantirContatoPrincipal(manager, idFornecedor);
        }

        const fornecedor = await this.carregarFornecedorCompleto(
          manager,
          idEmpresa,
          idFornecedor,
        );
        return { sucesso: true, mensagem: 'Contato atualizado com sucesso.', fornecedor };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'atualizar contato');
    }
  }

  async removerContato(idEmpresa: number, idFornecedor: number, idContato: number) {
    try {
      return await this.executarComRls(idEmpresa, async (manager) => {
        await this.buscarContatoOuFalhar(manager, idEmpresa, idFornecedor, idContato);
        await manager.query(
          `DELETE FROM app.fornecedor_contatos WHERE id_fornecedor = $1 AND id_contato = $2`,
          [idFornecedor, idContato],
        );
        await this.garantirContatoPrincipal(manager, idFornecedor);

        const fornecedor = await this.carregarFornecedorCompleto(
          manager,
          idEmpresa,
          idFornecedor,
        );
        return { sucesso: true, mensagem: 'Contato removido com sucesso.', fornecedor };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'remover contato');
    }
  }

  async adicionarEndereco(
    idEmpresa: number,
    idFornecedor: number,
    dados: CriarFornecedorEnderecoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    try {
      return await this.executarComRls(idEmpresa, async (manager) => {
        await this.buscarRegistroFornecedorOuFalhar(manager, idEmpresa, idFornecedor);
        const payload = this.normalizarEnderecoCriacao(dados, usuarioJwt);
        const rows = (await manager.query(
          `
            INSERT INTO app.fornecedor_enderecos (
              id_fornecedor, logradouro, numero, complemento, bairro, cidade,
              estado, cep, principal, usuario_atualizacao, criado_em, atualizado_em
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW(),NOW())
            RETURNING id_endereco
          `,
          [
            idFornecedor,
            payload.logradouro,
            payload.numero,
            payload.complemento,
            payload.bairro,
            payload.cidade,
            payload.estado,
            payload.cep,
            payload.principal,
            payload.usuarioAtualizacao,
          ],
        )) as Array<{ id_endereco?: string | number }>;

        const idEndereco = this.toNum(rows[0]?.id_endereco);
        if (!idEndereco || idEndereco <= 0) {
          throw new BadRequestException('Falha ao cadastrar endereço.');
        }

        if (payload.principal) {
          await this.marcarEnderecoPrincipal(manager, idFornecedor, idEndereco);
        } else {
          await this.garantirEnderecoPrincipal(manager, idFornecedor);
        }

        const fornecedor = await this.carregarFornecedorCompleto(
          manager,
          idEmpresa,
          idFornecedor,
        );
        return { sucesso: true, mensagem: 'Endereço cadastrado com sucesso.', fornecedor };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'cadastrar endereço');
    }
  }

  async atualizarEndereco(
    idEmpresa: number,
    idFornecedor: number,
    idEndereco: number,
    dados: AtualizarFornecedorEnderecoDto,
    usuarioJwt: JwtUsuarioPayload,
  ) {
    try {
      return await this.executarComRls(idEmpresa, async (manager) => {
        const atual = await this.buscarEnderecoOuFalhar(
          manager,
          idEmpresa,
          idFornecedor,
          idEndereco,
        );
        const payload = this.normalizarEnderecoAtualizacao(dados, usuarioJwt, atual);

        await manager.query(
          `
            UPDATE app.fornecedor_enderecos
            SET
              logradouro = $1,
              numero = $2,
              complemento = $3,
              bairro = $4,
              cidade = $5,
              estado = $6,
              cep = $7,
              principal = $8,
              usuario_atualizacao = $9,
              atualizado_em = NOW()
            WHERE id_fornecedor = $10
              AND id_endereco = $11
          `,
          [
            payload.logradouro,
            payload.numero,
            payload.complemento,
            payload.bairro,
            payload.cidade,
            payload.estado,
            payload.cep,
            payload.principal,
            payload.usuarioAtualizacao,
            idFornecedor,
            idEndereco,
          ],
        );

        if (payload.principal) {
          await this.marcarEnderecoPrincipal(manager, idFornecedor, idEndereco);
        } else {
          await this.garantirEnderecoPrincipal(manager, idFornecedor);
        }

        const fornecedor = await this.carregarFornecedorCompleto(
          manager,
          idEmpresa,
          idFornecedor,
        );
        return { sucesso: true, mensagem: 'Endereço atualizado com sucesso.', fornecedor };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'atualizar endereço');
    }
  }

  async removerEndereco(idEmpresa: number, idFornecedor: number, idEndereco: number) {
    try {
      return await this.executarComRls(idEmpresa, async (manager) => {
        await this.buscarEnderecoOuFalhar(manager, idEmpresa, idFornecedor, idEndereco);
        await manager.query(
          `DELETE FROM app.fornecedor_enderecos WHERE id_fornecedor = $1 AND id_endereco = $2`,
          [idFornecedor, idEndereco],
        );
        await this.garantirEnderecoPrincipal(manager, idFornecedor);

        const fornecedor = await this.carregarFornecedorCompleto(
          manager,
          idEmpresa,
          idFornecedor,
        );
        return { sucesso: true, mensagem: 'Endereço removido com sucesso.', fornecedor };
      });
    } catch (error) {
      this.tratarErroPersistencia(error, 'remover endereço');
    }
  }

  private async listarRowsFornecedor(manager: EntityManager, idEmpresa: number) {
    return (await manager.query(
      `
        SELECT
          f.*,
          (SELECT COUNT(1)::int FROM app.fornecedor_contatos c WHERE c.id_fornecedor = f.id_fornecedor) AS qtd_contatos,
          (SELECT COUNT(1)::int FROM app.fornecedor_enderecos e WHERE e.id_fornecedor = f.id_fornecedor) AS qtd_enderecos,
          (
            SELECT COALESCE(NULLIF(c.celular, ''), NULLIF(c.telefone, ''))
            FROM app.fornecedor_contatos c
            WHERE c.id_fornecedor = f.id_fornecedor
            ORDER BY c.principal DESC, c.id_contato ASC
            LIMIT 1
          ) AS resumo_contato,
          (
            SELECT TRIM(
              BOTH ' /' FROM CONCAT(
                COALESCE(NULLIF(e.cidade, ''), ''),
                CASE
                  WHEN NULLIF(e.estado, '') IS NOT NULL AND NULLIF(e.cidade, '') IS NOT NULL THEN '/'
                  ELSE ''
                END,
                COALESCE(NULLIF(e.estado, ''), '')
              )
            )
            FROM app.fornecedor_enderecos e
            WHERE e.id_fornecedor = f.id_fornecedor
            ORDER BY e.principal DESC, e.id_endereco ASC
            LIMIT 1
          ) AS resumo_endereco
        FROM app.fornecedor f
        WHERE f.id_empresa = $1
        ORDER BY COALESCE(NULLIF(f.nome_fantasia, ''), NULLIF(f.razao_social, ''), NULLIF(f.nome, ''), CAST(f.id_fornecedor AS TEXT)) ASC, f.id_fornecedor ASC
      `,
      [String(idEmpresa)],
    )) as RegistroBanco[];
  }

  private async carregarFornecedorCompleto(
    manager: EntityManager,
    idEmpresa: number,
    idFornecedor: number,
  ): Promise<ListarFornecedorDto> {
    const rows = (await manager.query(
      `
        SELECT
          f.*,
          (SELECT COUNT(1)::int FROM app.fornecedor_contatos c WHERE c.id_fornecedor = f.id_fornecedor) AS qtd_contatos,
          (SELECT COUNT(1)::int FROM app.fornecedor_enderecos e WHERE e.id_fornecedor = f.id_fornecedor) AS qtd_enderecos,
          (
            SELECT COALESCE(NULLIF(c.celular, ''), NULLIF(c.telefone, ''))
            FROM app.fornecedor_contatos c
            WHERE c.id_fornecedor = f.id_fornecedor
            ORDER BY c.principal DESC, c.id_contato ASC
            LIMIT 1
          ) AS resumo_contato,
          (
            SELECT TRIM(
              BOTH ' /' FROM CONCAT(
                COALESCE(NULLIF(e.cidade, ''), ''),
                CASE
                  WHEN NULLIF(e.estado, '') IS NOT NULL AND NULLIF(e.cidade, '') IS NOT NULL THEN '/'
                  ELSE ''
                END,
                COALESCE(NULLIF(e.estado, ''), '')
              )
            )
            FROM app.fornecedor_enderecos e
            WHERE e.id_fornecedor = f.id_fornecedor
            ORDER BY e.principal DESC, e.id_endereco ASC
            LIMIT 1
          ) AS resumo_endereco
        FROM app.fornecedor f
        WHERE f.id_fornecedor = $1
          AND f.id_empresa = $2
        LIMIT 1
      `,
      [idFornecedor, String(idEmpresa)],
    )) as RegistroBanco[];
    const row = rows[0];
    if (!row) {
      throw new NotFoundException('Fornecedor não encontrado para a empresa logada.');
    }

    const contatos = await this.listarContatosFornecedor(manager, idEmpresa, idFornecedor);
    const enderecos = await this.listarEnderecosFornecedor(manager, idEmpresa, idFornecedor);

    return this.mapearFornecedor(row, contatos, enderecos);
  }

  private async buscarRegistroFornecedorOuFalhar(
    manager: EntityManager,
    idEmpresa: number,
    idFornecedor: number,
  ) {
    const rows = (await manager.query(
      `
        SELECT *
        FROM app.fornecedor
        WHERE id_fornecedor = $1
          AND id_empresa = $2
        LIMIT 1
      `,
      [idFornecedor, String(idEmpresa)],
    )) as RegistroBanco[];
    if (!rows[0]) {
      throw new NotFoundException('Fornecedor não encontrado para a empresa logada.');
    }
    return rows[0];
  }

  private async listarContatosFornecedor(
    manager: EntityManager,
    idEmpresa: number,
    idFornecedor: number,
  ): Promise<ListarFornecedorContatoDto[]> {
    const rows = (await manager.query(
      `
        SELECT c.*
        FROM app.fornecedor_contatos c
        INNER JOIN app.fornecedor f ON f.id_fornecedor = c.id_fornecedor
        WHERE c.id_fornecedor = $1
          AND f.id_empresa = $2
        ORDER BY c.principal DESC, c.id_contato ASC
      `,
      [idFornecedor, String(idEmpresa)],
    )) as RegistroBanco[];
    return rows.map((row) => this.mapearContato(row));
  }

  private async listarEnderecosFornecedor(
    manager: EntityManager,
    idEmpresa: number,
    idFornecedor: number,
  ): Promise<ListarFornecedorEnderecoDto[]> {
    const rows = (await manager.query(
      `
        SELECT e.*
        FROM app.fornecedor_enderecos e
        INNER JOIN app.fornecedor f ON f.id_fornecedor = e.id_fornecedor
        WHERE e.id_fornecedor = $1
          AND f.id_empresa = $2
        ORDER BY e.principal DESC, e.id_endereco ASC
      `,
      [idFornecedor, String(idEmpresa)],
    )) as RegistroBanco[];
    return rows.map((row) => this.mapearEndereco(row));
  }

  private async buscarContatoOuFalhar(
    manager: EntityManager,
    idEmpresa: number,
    idFornecedor: number,
    idContato: number,
  ) {
    const rows = (await manager.query(
      `
        SELECT c.*
        FROM app.fornecedor_contatos c
        INNER JOIN app.fornecedor f ON f.id_fornecedor = c.id_fornecedor
        WHERE c.id_fornecedor = $1
          AND c.id_contato = $2
          AND f.id_empresa = $3
        LIMIT 1
      `,
      [idFornecedor, idContato, String(idEmpresa)],
    )) as RegistroBanco[];
    if (!rows[0]) {
      throw new NotFoundException('Contato do fornecedor não encontrado para a empresa logada.');
    }
    return rows[0];
  }

  private async buscarEnderecoOuFalhar(
    manager: EntityManager,
    idEmpresa: number,
    idFornecedor: number,
    idEndereco: number,
  ) {
    const rows = (await manager.query(
      `
        SELECT e.*
        FROM app.fornecedor_enderecos e
        INNER JOIN app.fornecedor f ON f.id_fornecedor = e.id_fornecedor
        WHERE e.id_fornecedor = $1
          AND e.id_endereco = $2
          AND f.id_empresa = $3
        LIMIT 1
      `,
      [idFornecedor, idEndereco, String(idEmpresa)],
    )) as RegistroBanco[];
    if (!rows[0]) {
      throw new NotFoundException(
        'Endereço do fornecedor não encontrado para a empresa logada.',
      );
    }
    return rows[0];
  }

  private async marcarContatoPrincipal(
    manager: EntityManager,
    idFornecedor: number,
    idContatoPrincipal: number,
  ) {
    await manager.query(
      `
        UPDATE app.fornecedor_contatos
        SET principal = CASE WHEN id_contato = $2 THEN true ELSE false END,
            atualizado_em = NOW()
        WHERE id_fornecedor = $1
      `,
      [idFornecedor, idContatoPrincipal],
    );
  }

  private async garantirContatoPrincipal(manager: EntityManager, idFornecedor: number) {
    const rows = (await manager.query(
      `
        SELECT id_contato, principal
        FROM app.fornecedor_contatos
        WHERE id_fornecedor = $1
        ORDER BY principal DESC, id_contato ASC
      `,
      [idFornecedor],
    )) as Array<{ id_contato?: number | string; principal?: boolean }>;
    if (rows.length === 0 || rows.some((row) => row.principal === true)) return;
    const primeiro = this.toNum(rows[0]?.id_contato);
    if (!primeiro) return;
    await this.marcarContatoPrincipal(manager, idFornecedor, primeiro);
  }

  private async marcarEnderecoPrincipal(
    manager: EntityManager,
    idFornecedor: number,
    idEnderecoPrincipal: number,
  ) {
    await manager.query(
      `
        UPDATE app.fornecedor_enderecos
        SET principal = CASE WHEN id_endereco = $2 THEN true ELSE false END,
            atualizado_em = NOW()
        WHERE id_fornecedor = $1
      `,
      [idFornecedor, idEnderecoPrincipal],
    );
  }

  private async garantirEnderecoPrincipal(manager: EntityManager, idFornecedor: number) {
    const rows = (await manager.query(
      `
        SELECT id_endereco, principal
        FROM app.fornecedor_enderecos
        WHERE id_fornecedor = $1
        ORDER BY principal DESC, id_endereco ASC
      `,
      [idFornecedor],
    )) as Array<{ id_endereco?: number | string; principal?: boolean }>;
    if (rows.length === 0 || rows.some((row) => row.principal === true)) return;
    const primeiro = this.toNum(rows[0]?.id_endereco);
    if (!primeiro) return;
    await this.marcarEnderecoPrincipal(manager, idFornecedor, primeiro);
  }

  private async validarDocumentoDuplicadoEmpresa(
    manager: EntityManager,
    idEmpresa: number,
    payload: PayloadFornecedor,
    idFornecedorAtual?: number,
  ) {
    const filtrosDocumento: string[] = [];
    const valores: Array<string | number> = [String(idEmpresa)];

    if (payload.cnpj) {
      valores.push(payload.cnpj);
      filtrosDocumento.push(`cnpj = $${valores.length}`);
    }

    if (payload.cpf) {
      valores.push(payload.cpf);
      filtrosDocumento.push(`cpf = $${valores.length}`);
    }

    if (filtrosDocumento.length === 0) {
      return;
    }

    let sql = `
      SELECT id_fornecedor, cnpj, cpf
      FROM app.fornecedor
      WHERE id_empresa = $1
        AND (${filtrosDocumento.join(' OR ')})
    `;

    if (idFornecedorAtual && Number.isFinite(idFornecedorAtual) && idFornecedorAtual > 0) {
      valores.push(idFornecedorAtual);
      sql += ` AND id_fornecedor <> $${valores.length}`;
    }

    sql += ` LIMIT 1`;

    const rows = (await manager.query(sql, valores)) as RegistroBanco[];
    const registro = rows[0];

    if (!registro) {
      return;
    }

    const cnpjExistente = this.str(registro.cnpj);
    const cpfExistente = this.str(registro.cpf);

    if (payload.cnpj && cnpjExistente === payload.cnpj) {
      throw new BadRequestException(
        'Já existe fornecedor com este CNPJ na empresa logada.',
      );
    }

    if (payload.cpf && cpfExistente === payload.cpf) {
      throw new BadRequestException(
        'Já existe fornecedor com este CPF na empresa logada.',
      );
    }
  }

  private normalizarFornecedorCriacao(
    dados: CriarFornecedorDto,
    usuarioJwt: JwtUsuarioPayload,
  ): PayloadFornecedor {
    const tipoPessoa = this.resolverTipoPessoa(dados.tipoPessoa, dados.cpf, dados.cnpj);
    const payload: PayloadFornecedor = {
      tipoPessoa,
      razaoSocial: this.textoOpt(dados.razaoSocial, true),
      nomeFantasia: this.textoOpt(dados.nomeFantasia, true),
      nomePessoa: this.textoOpt(dados.nomePessoa, true),
      cpf: this.cpfOpt(dados.cpf),
      cnpj: this.cnpjOpt(dados.cnpj),
      inscricaoEstadual: this.textoOpt(dados.inscricaoEstadual, true),
      inscricaoMunicipal: this.textoOpt(dados.inscricaoMunicipal, true),
      observacoes: this.textoOpt(dados.observacoes, true),
      ativo: dados.ativo ?? true,
      usuarioAtualizacao: this.texto(
        dados.usuarioAtualizacao ?? usuarioJwt.email,
        'usuarioAtualizacao',
        true,
      ),
    };
    this.validarFornecedor(payload);
    return payload;
  }

  private normalizarFornecedorAtualizacao(
    dados: AtualizarFornecedorDto,
    usuarioJwt: JwtUsuarioPayload,
    atual: RegistroBanco,
  ): PayloadFornecedor {
    const tipoPessoa = this.resolverTipoPessoa(
      dados.tipoPessoa ?? this.str(atual.tipo_pessoa) ?? 'J',
      dados.cpf !== undefined ? dados.cpf : this.str(atual.cpf),
      dados.cnpj !== undefined ? dados.cnpj : this.str(atual.cnpj),
    );
    const payload: PayloadFornecedor = {
      tipoPessoa,
      razaoSocial:
        dados.razaoSocial !== undefined
          ? this.textoOpt(dados.razaoSocial, true)
          : this.textoOpt(this.str(atual.razao_social), true),
      nomeFantasia:
        dados.nomeFantasia !== undefined
          ? this.textoOpt(dados.nomeFantasia, true)
          : this.textoOpt(this.str(atual.nome_fantasia), true),
      nomePessoa:
        dados.nomePessoa !== undefined
          ? this.textoOpt(dados.nomePessoa, true)
          : this.textoOpt(this.str(atual.nome), true),
      cpf:
        dados.cpf !== undefined ? this.cpfOpt(dados.cpf) : this.cpfOpt(this.str(atual.cpf)),
      cnpj:
        dados.cnpj !== undefined
          ? this.cnpjOpt(dados.cnpj)
          : this.cnpjOpt(this.str(atual.cnpj)),
      inscricaoEstadual:
        dados.inscricaoEstadual !== undefined
          ? this.textoOpt(dados.inscricaoEstadual, true)
          : this.textoOpt(this.str(atual.inscricao_estadual), true),
      inscricaoMunicipal:
        dados.inscricaoMunicipal !== undefined
          ? this.textoOpt(dados.inscricaoMunicipal, true)
          : this.textoOpt(this.str(atual.inscricao_municipal), true),
      observacoes:
        dados.observacoes !== undefined
          ? this.textoOpt(dados.observacoes, true)
          : this.textoOpt(this.str(atual.observacoes), true),
      ativo: dados.ativo ?? this.bool(atual.ativo) ?? true,
      usuarioAtualizacao: this.texto(
        dados.usuarioAtualizacao ?? usuarioJwt.email,
        'usuarioAtualizacao',
        true,
      ),
    };
    this.validarFornecedor(payload);
    return payload;
  }

  private normalizarContatoCriacao(
    dados: CriarFornecedorContatoDto,
    usuarioJwt: JwtUsuarioPayload,
  ): PayloadContato {
    return {
      nomeContato: this.texto(dados.nomeContato, 'nomeContato', true),
      telefone: this.textoOpt(dados.telefone, false),
      celular: this.textoOpt(dados.celular, false),
      email: this.emailOpt(dados.email),
      principal: dados.principal ?? false,
      usuarioAtualizacao: this.texto(
        dados.usuarioAtualizacao ?? usuarioJwt.email,
        'usuarioAtualizacao',
        true,
      ),
    };
  }

  private normalizarContatoAtualizacao(
    dados: AtualizarFornecedorContatoDto,
    usuarioJwt: JwtUsuarioPayload,
    atual: RegistroBanco,
  ): PayloadContato {
    return {
      nomeContato:
        dados.nomeContato !== undefined
          ? this.texto(dados.nomeContato, 'nomeContato', true)
          : this.texto(this.str(atual.nome_contato) ?? '', 'nomeContato', true),
      telefone:
        dados.telefone !== undefined
          ? this.textoOpt(dados.telefone, false)
          : this.textoOpt(this.str(atual.telefone), false),
      celular:
        dados.celular !== undefined
          ? this.textoOpt(dados.celular, false)
          : this.textoOpt(this.str(atual.celular), false),
      email:
        dados.email !== undefined
          ? this.emailOpt(dados.email)
          : this.emailOpt(this.str(atual.email)),
      principal: dados.principal ?? this.bool(atual.principal) ?? false,
      usuarioAtualizacao: this.texto(
        dados.usuarioAtualizacao ?? usuarioJwt.email,
        'usuarioAtualizacao',
        true,
      ),
    };
  }

  private normalizarEnderecoCriacao(
    dados: CriarFornecedorEnderecoDto,
    usuarioJwt: JwtUsuarioPayload,
  ): PayloadEndereco {
    return {
      logradouro: this.texto(dados.logradouro, 'logradouro', true),
      numero: this.textoOpt(dados.numero, true),
      complemento: this.textoOpt(dados.complemento, true),
      bairro: this.textoOpt(dados.bairro, true),
      cidade: this.textoOpt(dados.cidade, true),
      estado: this.estadoOpt(dados.estado),
      cep: this.cepOpt(dados.cep),
      principal: dados.principal ?? false,
      usuarioAtualizacao: this.texto(
        dados.usuarioAtualizacao ?? usuarioJwt.email,
        'usuarioAtualizacao',
        true,
      ),
    };
  }

  private normalizarEnderecoAtualizacao(
    dados: AtualizarFornecedorEnderecoDto,
    usuarioJwt: JwtUsuarioPayload,
    atual: RegistroBanco,
  ): PayloadEndereco {
    return {
      logradouro:
        dados.logradouro !== undefined
          ? this.texto(dados.logradouro, 'logradouro', true)
          : this.texto(this.str(atual.logradouro) ?? '', 'logradouro', true),
      numero:
        dados.numero !== undefined
          ? this.textoOpt(dados.numero, true)
          : this.textoOpt(this.str(atual.numero), true),
      complemento:
        dados.complemento !== undefined
          ? this.textoOpt(dados.complemento, true)
          : this.textoOpt(this.str(atual.complemento), true),
      bairro:
        dados.bairro !== undefined
          ? this.textoOpt(dados.bairro, true)
          : this.textoOpt(this.str(atual.bairro), true),
      cidade:
        dados.cidade !== undefined
          ? this.textoOpt(dados.cidade, true)
          : this.textoOpt(this.str(atual.cidade), true),
      estado:
        dados.estado !== undefined
          ? this.estadoOpt(dados.estado)
          : this.estadoOpt(this.str(atual.estado)),
      cep: dados.cep !== undefined ? this.cepOpt(dados.cep) : this.cepOpt(this.str(atual.cep)),
      principal: dados.principal ?? this.bool(atual.principal) ?? false,
      usuarioAtualizacao: this.texto(
        dados.usuarioAtualizacao ?? usuarioJwt.email,
        'usuarioAtualizacao',
        true,
      ),
    };
  }

  private mapearFornecedor(
    row: RegistroBanco,
    contatos?: ListarFornecedorContatoDto[],
    enderecos?: ListarFornecedorEnderecoDto[],
  ): ListarFornecedorDto {
    const ativo = this.bool(row.ativo) ?? false;
    const nomeFantasia = this.str(row.nome_fantasia);
    const razaoSocial = this.str(row.razao_social);
    const nomePessoa = this.str(row.nome);
    const idFornecedor = this.toNum(row.id_fornecedor) ?? 0;

    return {
      idFornecedor,
      tipoPessoa: this.str(row.tipo_pessoa)?.toUpperCase() ?? 'J',
      razaoSocial,
      nomeFantasia,
      nomePessoa,
      cpf: this.str(row.cpf),
      cnpj: this.str(row.cnpj),
      inscricaoEstadual: this.str(row.inscricao_estadual),
      inscricaoMunicipal: this.str(row.inscricao_municipal),
      observacoes: this.str(row.observacoes),
      ativo,
      status: ativo ? 'A' : 'I',
      nome: nomeFantasia ?? razaoSocial ?? nomePessoa ?? `FORNECEDOR #${idFornecedor}`,
      dataCadastro: this.iso(row.data_cadastro),
      usuarioAtualizacao: this.str(row.usuario_atualizacao),
      atualizadoEm: this.iso(row.atualizado_em),
      qtdContatos: this.toNum(row.qtd_contatos) ?? contatos?.length ?? 0,
      qtdEnderecos: this.toNum(row.qtd_enderecos) ?? enderecos?.length ?? 0,
      resumoContato:
        this.str(row.resumo_contato) ??
        contatos?.find((item) => item.principal)?.celular ??
        contatos?.find((item) => item.principal)?.telefone ??
        contatos?.[0]?.celular ??
        contatos?.[0]?.telefone ??
        null,
      resumoEndereco:
        this.str(row.resumo_endereco) ??
        (() => {
          const enderecoPrincipal =
            enderecos?.find((item) => item.principal) ?? enderecos?.[0];
          if (!enderecoPrincipal) return null;
          const cidade = enderecoPrincipal.cidade?.trim() ?? '';
          const estado = enderecoPrincipal.estado?.trim() ?? '';
          if (cidade && estado) return `${cidade}/${estado}`;
          return cidade || estado || null;
        })(),
      contatos,
      enderecos,
    };
  }

  private mapearContato(row: RegistroBanco): ListarFornecedorContatoDto {
    return {
      idContato: this.toNum(row.id_contato) ?? 0,
      idFornecedor: this.toNum(row.id_fornecedor) ?? 0,
      nomeContato: this.str(row.nome_contato) ?? '',
      telefone: this.str(row.telefone),
      celular: this.str(row.celular),
      email: this.str(row.email),
      principal: this.bool(row.principal) ?? false,
      usuarioAtualizacao: this.str(row.usuario_atualizacao),
      criadoEm: this.iso(row.criado_em),
      atualizadoEm: this.iso(row.atualizado_em),
    };
  }

  private mapearEndereco(row: RegistroBanco): ListarFornecedorEnderecoDto {
    return {
      idEndereco: this.toNum(row.id_endereco) ?? 0,
      idFornecedor: this.toNum(row.id_fornecedor) ?? 0,
      logradouro: this.str(row.logradouro) ?? '',
      numero: this.str(row.numero),
      complemento: this.str(row.complemento),
      bairro: this.str(row.bairro),
      cidade: this.str(row.cidade),
      estado: this.str(row.estado),
      cep: this.str(row.cep),
      principal: this.bool(row.principal) ?? false,
      usuarioAtualizacao: this.str(row.usuario_atualizacao),
      criadoEm: this.iso(row.criado_em),
      atualizadoEm: this.iso(row.atualizado_em),
    };
  }

  private resolverColunaOrdenacao(ordenarPor: FiltroFornecedorDto['ordenarPor']) {
    if (ordenarPor === 'id_fornecedor') return 'f.id_fornecedor';
    if (ordenarPor === 'razao_social') return 'f.razao_social';
    if (ordenarPor === 'nome') return 'f.nome';
    if (ordenarPor === 'data_cadastro') return 'f.data_cadastro';
    if (ordenarPor === 'atualizado_em') return 'f.atualizado_em';
    return 'f.nome_fantasia';
  }

  private resolverTipoPessoa(
    tipoPessoa: string | undefined,
    cpf: string | null | undefined,
    cnpj: string | null | undefined,
  ) {
    const tipo = (tipoPessoa ?? '').trim().toUpperCase();
    if (tipo === 'F' || tipo === 'J') return tipo;
    if (this.cpfOpt(cpf)) return 'F';
    if (this.cnpjOpt(cnpj)) return 'J';
    return 'J';
  }

  private validarFornecedor(payload: PayloadFornecedor) {
    if (payload.tipoPessoa === 'J') {
      if (!payload.razaoSocial) throw new BadRequestException('Razão social é obrigatória.');
      if (!payload.cnpj) throw new BadRequestException('CNPJ é obrigatório.');
      return;
    }
    if (!payload.nomePessoa) throw new BadRequestException('Nome da pessoa é obrigatório.');
    if (!payload.cpf) throw new BadRequestException('CPF é obrigatório.');
  }

  private texto(valor: string, campo: string, upper = false) {
    const texto = valor.trim();
    if (!texto) throw new BadRequestException(`${campo} inválido.`);
    return upper ? texto.toUpperCase() : texto;
  }

  private textoOpt(valor: string | null | undefined, upper = false) {
    if (valor === null || valor === undefined) return null;
    const texto = valor.trim();
    if (!texto) return null;
    return upper ? texto.toUpperCase() : texto;
  }

  private cpfOpt(valor: string | null | undefined) {
    const texto = this.textoOpt(valor, false);
    if (!texto) return null;
    const digits = texto.replace(/\D/g, '');
    if (digits.length !== 11) throw new BadRequestException('CPF inválido.');
    return digits;
  }

  private cnpjOpt(valor: string | null | undefined) {
    const texto = this.textoOpt(valor, false);
    if (!texto) return null;
    const digits = texto.replace(/\D/g, '');
    if (digits.length !== 14) throw new BadRequestException('CNPJ inválido.');
    return digits;
  }

  private emailOpt(valor: string | null | undefined) {
    const texto = this.textoOpt(valor, false);
    return texto ? texto.toLowerCase() : null;
  }

  private estadoOpt(valor: string | null | undefined) {
    const estado = this.textoOpt(valor, true);
    if (!estado) return null;
    if (estado.length !== 2) throw new BadRequestException('Estado inválido.');
    return estado;
  }

  private cepOpt(valor: string | null | undefined) {
    const texto = this.textoOpt(valor, false);
    if (!texto) return null;
    const digits = texto.replace(/\D/g, '');
    if (digits.length !== 8) throw new BadRequestException('CEP inválido.');
    return digits;
  }

  private str(valor: unknown) {
    if (typeof valor !== 'string') return null;
    const texto = valor.trim();
    return texto ? texto : null;
  }

  private toNum(valor: unknown) {
    if (valor === null || valor === undefined) return null;
    const numero = Number(valor);
    return Number.isFinite(numero) ? numero : null;
  }

  private bool(valor: unknown): boolean | null {
    if (typeof valor === 'boolean') return valor;
    if (typeof valor === 'string') {
      const v = valor.trim().toLowerCase();
      if (v === 'true' || v === 't' || v === '1') return true;
      if (v === 'false' || v === 'f' || v === '0') return false;
    }
    if (typeof valor === 'number') {
      if (valor === 1) return true;
      if (valor === 0) return false;
    }
    return null;
  }

  private iso(valor: unknown) {
    if (valor === null || valor === undefined) return null;
    const data = new Date(
      valor instanceof Date || typeof valor === 'string' || typeof valor === 'number'
        ? valor
        : '',
    );
    return Number.isNaN(data.getTime()) ? null : data.toISOString();
  }

  private tratarErroPersistencia(error: unknown, acao: string): never {
    if (error instanceof BadRequestException || error instanceof NotFoundException) {
      throw error;
    }

    if (error instanceof QueryFailedError) {
      const erroPg = error.driverError as { code?: string; detail?: string; message?: string };

      if (erroPg.code === '23503') {
        if (acao === 'remover') {
          throw new BadRequestException(
            'Fornecedor possui registros vinculados e não pode ser excluído. Altere o status para Inativo.',
          );
        }
        throw new BadRequestException('Registro relacionado não encontrado para a operação.');
      }
      if (erroPg.code === '23505') {
        const detalhe =
          `${erroPg.detail ?? ''} ${erroPg.message ?? ''}`.toLowerCase();
        const possuiEscopoEmpresa = detalhe.includes('id_empresa');

        if (detalhe.includes('cnpj')) {
          if (possuiEscopoEmpresa) {
            throw new BadRequestException(
              'Já existe fornecedor com este CNPJ na empresa logada.',
            );
          }
          throw new BadRequestException(
            'Conflito de CNPJ detectado no banco em escopo global. Execute o script sql/fornecedor_unicidade_por_empresa.sql para aplicar a regra por empresa.',
          );
        }

        if (detalhe.includes('cpf')) {
          if (possuiEscopoEmpresa) {
            throw new BadRequestException(
              'Já existe fornecedor com este CPF na empresa logada.',
            );
          }
          throw new BadRequestException(
            'Conflito de CPF detectado no banco em escopo global. Execute o script sql/fornecedor_unicidade_por_empresa.sql para aplicar a regra por empresa.',
          );
        }

        throw new BadRequestException('Ja existe um registro igual para os dados informados.');
      }
      if (erroPg.code === '23514') {
        throw new BadRequestException('Dados inválidos para fornecedor, contato ou endereço.');
      }
      if (erroPg.code === '22P02' || erroPg.code === '22007') {
        throw new BadRequestException('Formato de número ou data inválido.');
      }
      if (erroPg.code === '23502') {
        throw new BadRequestException('Campos obrigatórios não foram informados.');
      }
      if (erroPg.code === '42501') {
        throw new BadRequestException(
          'Permissao insuficiente no banco (RLS/sequence). Verifique policy e grants.',
        );
      }

      const detalhe = [erroPg.code, erroPg.detail ?? erroPg.message]
        .filter((parte): parte is string => Boolean(parte))
        .join(' - ');
      if (detalhe) {
        throw new BadRequestException(`Falha ao ${acao} fornecedor: ${detalhe}.`);
      }
    }

      throw new BadRequestException(`Não foi possível ${acao} fornecedor neste momento.`);
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
}
