import { BadRequestException, Injectable, Logger } from '@nestjs/common';
import { randomBytes, scryptSync } from 'crypto';
import { PoolClient } from 'pg';
import { DatabaseService } from '../database/database.service';

// Dados recebidos da tela de criacao de conta.
export interface DadosCadastroEmpresa {
  nomeEmpresa?: string;
  razaoSocial?: string;
  cnpj?: string;
  emailEmpresa?: string;
  telefoneEmpresa?: string;
  idEmpresa?: number | string;
  nomeAdministrador?: string;
  emailAdministrador?: string;
  senha?: string;
  // Compatibilidade com payload antigo da tela.
  password?: string;
}

interface LinhaClienteInserido {
  id_cliente: number;
  codigo: string;
  nome: string;
}

interface LinhaUsuarioInserido {
  id_usuario: number;
  nome: string;
  email: string;
}

type NomeTabelaCliente = 'cliente' | 'clientes';

@Injectable()
export class ServicoAuth {
  private static readonly USUARIO_ADMIN_CADASTRO = 'kodigo';
  private static readonly ID_EMPRESA_RLS_PADRAO = 1;
  // Evita recriar tabelas e indices a cada requisicao.
  private esquemaPronto = false;
  private tabelaClienteResolvida: NomeTabelaCliente | null = null;
  private readonly logger = new Logger(ServicoAuth.name);

  constructor(private readonly servicoBanco: DatabaseService) {}

  // Fluxo principal: valida dados, garante schema e salva cliente+usuario em transacao unica.
  async registrarEmpresa(dadosRecebidos: DadosCadastroEmpresa) {
    const dados = this.normalizarEValidar(dadosRecebidos);

    try {
      await this.garantirEstruturaDoBanco();

      const resultado = await this.servicoBanco.withSignupTransaction(
        async (clienteDb) => {
          const idEmpresaRls = this.definirIdEmpresaParaRls(dados.idEmpresa);
          await this.configurarContextoRls(clienteDb, idEmpresaRls);
          const tabelaCliente = await this.resolverNomeTabelaCliente(clienteDb);
          const tabelaClienteSql = `app.${tabelaCliente}`;

          // Calcula o proximo codigo no formato CLI-0001 com base nos codigos existentes.
          const codigoResult = await clienteDb.query<{ proximo: number }>(
            `
            SELECT COALESCE(
              MAX(
                COALESCE(
                  NULLIF(REGEXP_REPLACE(codigo, '[^0-9]', '', 'g'), ''),
                  '0'
                )::BIGINT
              ),
              0
            ) + 1 AS proximo
            FROM ${tabelaClienteSql}
            `,
          );

          const proximoCodigo = codigoResult.rows[0]?.proximo ?? 1;
          const codigoCliente = `CLI-${String(proximoCodigo).padStart(4, '0')}`;

          const resultadoCliente = await clienteDb.query<LinhaClienteInserido>(
            `
            INSERT INTO ${tabelaClienteSql} (codigo, nome, ativo, usuario_atualizacao, id_empresa)
            VALUES ($1, $2, TRUE, $3, $4)
            RETURNING id_cliente, codigo, nome
            `,
            [
              codigoCliente,
              dados.nomeEmpresa,
              ServicoAuth.USUARIO_ADMIN_CADASTRO,
              idEmpresaRls,
            ],
          );

          const cliente = resultadoCliente.rows[0];

          // Cadastra o primeiro usuario administrador usando a estrutura de app.usuarios.
          const resultadoUsuario = await clienteDb.query<LinhaUsuarioInserido>(
            `
            INSERT INTO app.usuarios (id_empresa, nome, email, senha_hash, perfil, ativo, usuario_atualizacao)
            VALUES ($1, $2, $3, $4, 'ADM', TRUE, $5)
            RETURNING id_usuario, nome, email
            `,
            [
              idEmpresaRls,
              dados.nomeAdministrador,
              dados.emailAdministrador,
              this.gerarHashDaSenha(dados.senha),
              ServicoAuth.USUARIO_ADMIN_CADASTRO,
            ],
          );

          const usuario = resultadoUsuario.rows[0];

          return {
            cliente,
            usuario,
          };
        },
      );

      return {
        sucesso: true,
        mensagem: 'Empresa e usuario administrador cadastrados com sucesso.',
        clienteId: resultado.cliente.id_cliente,
        usuarioId: resultado.usuario.id_usuario,
      };
    } catch (erro) {
      const erroPg = erro as { code?: string; message?: string };
      const mensagemErro = erroPg.message ?? 'Erro desconhecido';

      this.logger.error(
        `Falha ao registrar empresa. code=${erroPg.code ?? 'N/A'} message=${mensagemErro}`,
      );

      if (erroPg.message?.includes('Database env vars are missing')) {
        throw new BadRequestException(
          'Configuracao de banco ausente. Defina DB_HOST, DB_PORT, DB_NAME, DB_USER e DB_PASSWORD no arquivo .env.',
        );
      }

      if (erroPg.message?.includes('Signup database env vars are missing')) {
        throw new BadRequestException(
          'Configuracao de cadastro ausente. Defina DB_SIGNUP_HOST, DB_SIGNUP_PORT, DB_SIGNUP_NAME, DB_SIGNUP_USER e DB_SIGNUP_PASSWORD.',
        );
      }

      // Erros de conectividade com banco (host/porta indisponiveis).
      if (
        /ENOTFOUND|EAI_AGAIN|ECONNREFUSED|ETIMEDOUT|timeout/i.test(mensagemErro)
      ) {
        throw new BadRequestException(
          'Nao foi possivel conectar ao banco de dados. Verifique DB_HOST, DB_PORT e se o Postgres esta acessivel no servidor.',
        );
      }

      // 28P01 = senha/usuario invalidos no Postgres.
      if (erroPg.code === '28P01') {
        throw new BadRequestException(
          'Falha de autenticacao no banco. Verifique as credenciais DB_SIGNUP_USER/DB_SIGNUP_PASSWORD (cadastro) e DB_USER/DB_PASSWORD (fluxo normal).',
        );
      }

      // 3D000 = banco informado em DB_NAME nao existe.
      if (erroPg.code === '3D000') {
        throw new BadRequestException(
          'Banco informado em DB_NAME nao existe no Postgres.',
        );
      }

      // 42P01 = tabela/schema nao encontrado.
      if (erroPg.code === '42P01') {
        throw new BadRequestException(
          'Estrutura do banco nao encontrada (schema/tabela). Verifique se app.cliente (ou app.clientes legado) e app.usuarios existem.',
        );
      }

      // 42501 = sem permissao (inclui bloqueio por RLS).
      if (erroPg.code === '42501') {
        if (/row-level security/i.test(mensagemErro)) {
          throw new BadRequestException(
            `Cadastro bloqueado por RLS. Execute o cadastro com o usuario ${ServicoAuth.USUARIO_ADMIN_CADASTRO} e desabilite RLS em app.cliente/app.clientes e app.usuarios.`,
          );
        }

        throw new BadRequestException(
          `Usuario do banco sem permissao para executar o cadastro. Garanta DB_SIGNUP_USER=${ServicoAuth.USUARIO_ADMIN_CADASTRO} com GRANTs no schema app e nas tabelas.`,
        );
      }

      // 23505 = violacao de chave unica (email/codigo/cnpj etc).
      if (erroPg.code === '23505') {
        throw new BadRequestException(
          'Ja existe cadastro com os dados informados. Verifique e tente novamente.',
        );
      }

      throw new BadRequestException(
        'Nao foi possivel finalizar o cadastro neste momento.',
      );
    }
  }

  // Normaliza campos e aplica validacoes obrigatorias antes de salvar.
  private normalizarEValidar(dadosRecebidos: DadosCadastroEmpresa) {
    const nomeEmpresa = this.normalizarTexto(dadosRecebidos.nomeEmpresa);
    const razaoSocial = this.paraNuloSeVazio(dadosRecebidos.razaoSocial);
    const cnpj = this.paraNuloSeVazio(dadosRecebidos.cnpj);
    const emailEmpresa = this.normalizarEmail(dadosRecebidos.emailEmpresa);
    const telefoneEmpresa = this.paraNuloSeVazio(
      dadosRecebidos.telefoneEmpresa,
    );
    const idEmpresa = this.definirIdEmpresaParaRls(dadosRecebidos.idEmpresa);
    const nomeAdministrador = this.normalizarTexto(
      dadosRecebidos.nomeAdministrador,
    );
    const emailAdministrador = this.normalizarEmail(
      dadosRecebidos.emailAdministrador,
    );
    const senha = dadosRecebidos.senha ?? dadosRecebidos.password ?? '';

    if (!nomeEmpresa) {
      throw new BadRequestException('Informe o nome da empresa.');
    }

    if (!emailEmpresa) {
      throw new BadRequestException('Informe o e-mail da empresa.');
    }

    if (!nomeAdministrador) {
      throw new BadRequestException('Informe o nome do administrador.');
    }

    if (!emailAdministrador) {
      throw new BadRequestException('Informe o e-mail do administrador.');
    }

    const senhaAtendeRegras =
      senha.length >= 8 &&
      /[A-ZÀ-Ý]/.test(senha) &&
      /[a-zà-ÿ]/.test(senha) &&
      /\d/.test(senha) &&
      /[^A-Za-z0-9\s]/.test(senha);

    if (!senhaAtendeRegras) {
      throw new BadRequestException(
        'A senha deve ter no minimo 8 caracteres, com letra maiuscula, minuscula, numero e simbolo.',
      );
    }

    return {
      nomeEmpresa,
      razaoSocial,
      cnpj,
      emailEmpresa,
      telefoneEmpresa,
      idEmpresa,
      nomeAdministrador,
      emailAdministrador,
      senha,
    };
  }

  // Cria schema/tabelas necessarias para cadastro e garante execucao com kodigo sem RLS.
  private async garantirEstruturaDoBanco(): Promise<void> {
    if (this.esquemaPronto) {
      return;
    }

    try {
      await this.servicoBanco.queryWithSignup(
        'CREATE SCHEMA IF NOT EXISTS app;',
      );
      const tabelaCliente = await this.resolverNomeTabelaCliente();
      const tabelaClienteSql = this.obterTabelaClienteSql(tabelaCliente);
      const nomeIndiceCodigo =
        tabelaCliente === 'cliente'
          ? 'uq_app_cliente_codigo'
          : 'uq_app_clientes_codigo';

      // Estrutura da tabela de cliente (app.cliente preferencial, app.clientes legado).
      await this.servicoBanco.queryWithSignup(`
        CREATE TABLE IF NOT EXISTS ${tabelaClienteSql} (
          id_cliente BIGSERIAL PRIMARY KEY,
          codigo TEXT,
          nome TEXT NOT NULL,
          ativo BOOLEAN NOT NULL DEFAULT TRUE,
          criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          usuario_atualizacao TEXT,
          id_empresa BIGINT NOT NULL,
          atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `);

      await this.servicoBanco.queryWithSignup(
        `CREATE UNIQUE INDEX IF NOT EXISTS ${nomeIndiceCodigo} ON ${tabelaClienteSql} (codigo);`,
      );

      // Tabela de usuarios com a estrutura informada para app.usuarios.
      await this.servicoBanco.queryWithSignup(`
        CREATE TABLE IF NOT EXISTS app.usuarios (
          id_usuario BIGSERIAL PRIMARY KEY,
          id_empresa BIGINT NOT NULL,
          nome VARCHAR(150) NOT NULL,
          email VARCHAR(150) NOT NULL,
          senha_hash TEXT NOT NULL,
          perfil VARCHAR(20) NOT NULL DEFAULT 'ADM',
          ativo BOOLEAN NOT NULL DEFAULT TRUE,
          ultimo_login_em TIMESTAMPTZ,
          criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          usuario_atualizacao TEXT,
          atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `);

      await this.servicoBanco.queryWithSignup(
        'CREATE UNIQUE INDEX IF NOT EXISTS uq_app_usuarios_email_lower ON app.usuarios (LOWER(email));',
      );

      await this.servicoBanco.queryWithSignup(
        'CREATE INDEX IF NOT EXISTS idx_app_usuarios_empresa_id ON app.usuarios (id_empresa);',
      );

      await this.desativarRlsETirarForce(tabelaClienteSql);
      await this.desativarRlsETirarForce('app.usuarios');
      await this.garantirPermissoesDoUsuarioKodigo(tabelaClienteSql);
    } catch (erro) {
      const erroPg = erro as { code?: string; message?: string };

      // Em producao, usuario de app pode nao ter permissao de DDL.
      // Nesse caso seguimos e tentamos o fluxo de INSERT em tabelas ja existentes.
      if (erroPg.code === '42501') {
        this.logger.warn(
          `Sem permissao total para DDL/ALTER no banco (code=42501). Mantendo fluxo com estruturas ja existentes. message=${erroPg.message ?? 'N/A'}`,
        );
        this.esquemaPronto = true;
        return;
      }

      throw erro;
    }

    this.esquemaPronto = true;
  }

  private definirIdEmpresaParaRls(
    idEmpresaRecebido?: number | string,
  ): number {
    const idEmpresa = Number(idEmpresaRecebido);
    if (!Number.isInteger(idEmpresa) || idEmpresa <= 0) {
      return ServicoAuth.ID_EMPRESA_RLS_PADRAO;
    }

    return idEmpresa;
  }

  private async configurarContextoRls(
    clienteDb: PoolClient,
    idEmpresa: number,
  ): Promise<void> {
    const idEmpresaTexto = String(idEmpresa);
    await clienteDb.query(`SELECT set_config('app.id_empresa', $1, true);`, [
      idEmpresaTexto,
    ]);
    await clienteDb.query(`SELECT set_config('app.empresa_id', $1, true);`, [
      idEmpresaTexto,
    ]);
    await clienteDb.query(`SELECT set_config('app.codigo_empresa', $1, true);`, [
      idEmpresaTexto,
    ]);
  }

  private async resolverNomeTabelaCliente(
    clienteDb?: PoolClient,
  ): Promise<NomeTabelaCliente> {
    if (this.tabelaClienteResolvida) {
      return this.tabelaClienteResolvida;
    }

    const query = `
      SELECT
        to_regclass('app.cliente') IS NOT NULL AS possui_cliente,
        to_regclass('app.clientes') IS NOT NULL AS possui_clientes;
    `;

    const resultado = clienteDb
      ? await clienteDb.query<{
          possui_cliente: boolean;
          possui_clientes: boolean;
        }>(query)
      : await this.servicoBanco.queryWithSignup<{
          possui_cliente: boolean;
          possui_clientes: boolean;
        }>(query);

    const linha = resultado.rows[0];
    if (linha?.possui_cliente) {
      this.tabelaClienteResolvida = 'cliente';
    } else if (linha?.possui_clientes) {
      this.tabelaClienteResolvida = 'clientes';
    } else {
      // Se nenhuma existir, criamos o padrao novo app.cliente.
      this.tabelaClienteResolvida = 'cliente';
    }

    return this.tabelaClienteResolvida;
  }

  private obterTabelaClienteSql(nomeTabela: NomeTabelaCliente): string {
    return nomeTabela === 'cliente' ? 'app.cliente' : 'app.clientes';
  }

  private async desativarRlsETirarForce(tabelaSql: string): Promise<void> {
    await this.servicoBanco.queryWithSignup(
      `ALTER TABLE ${tabelaSql} DISABLE ROW LEVEL SECURITY;`,
    );
    await this.servicoBanco.queryWithSignup(
      `ALTER TABLE ${tabelaSql} NO FORCE ROW LEVEL SECURITY;`,
    );
  }

  private async garantirPermissoesDoUsuarioKodigo(
    tabelaClienteSql: string,
  ): Promise<void> {
    await this.servicoBanco.queryWithSignup(
      `GRANT USAGE ON SCHEMA app TO ${ServicoAuth.USUARIO_ADMIN_CADASTRO};`,
    );
    await this.servicoBanco.queryWithSignup(
      `GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE ${tabelaClienteSql}, app.usuarios TO ${ServicoAuth.USUARIO_ADMIN_CADASTRO};`,
    );
    await this.servicoBanco.queryWithSignup(
      `GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA app TO ${ServicoAuth.USUARIO_ADMIN_CADASTRO};`,
    );
  }

  // Padroniza e-mail para armazenamento em caixa alta.
  private normalizarEmail(valor?: string): string {
    return this.normalizarTexto(valor);
  }

  // Padroniza textos para armazenamento em caixa alta.
  private normalizarTexto(valor?: string): string {
    return (valor ?? '').trim().toUpperCase();
  }

  // Retorna null para campos opcionais vazios.
  private paraNuloSeVazio(valor?: string): string | null {
    const texto = this.normalizarTexto(valor);
    return texto.length > 0 ? texto : null;
  }

  // Gera hash de senha para nao armazenar senha em texto puro.
  private gerarHashDaSenha(senha: string): string {
    // Formato salvo: scrypt$N=16384,r=8,p=1$<salt_base64>$<hash_base64>
    // Isso deixa claro no banco qual algoritmo foi usado e permite validacao futura.
    const salt = randomBytes(16);
    const hash = scryptSync(senha, salt, 64);
    return `scrypt$N=16384,r=8,p=1$${salt.toString('base64')}$${hash.toString('base64')}`;
  }
}
