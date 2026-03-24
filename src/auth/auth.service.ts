import { BadRequestException, Injectable, Logger } from '@nestjs/common';
import { randomBytes, scryptSync } from 'crypto';
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

@Injectable()
export class ServicoAuth {
  // Evita recriar tabelas e indices a cada requisicao.
  private esquemaPronto = false;
  private readonly logger = new Logger(ServicoAuth.name);

  constructor(private readonly servicoBanco: DatabaseService) {}

  // Fluxo principal: valida dados, garante schema e salva cliente+usuario em transacao unica.
  async registrarEmpresa(dadosRecebidos: DadosCadastroEmpresa) {
    const dados = this.normalizarEValidar(dadosRecebidos);

    try {
      await this.garantirEstruturaDoBanco();

      const resultado = await this.servicoBanco.withTransaction(async (clienteDb) => {
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
          FROM app.clientes
          `,
        );

        const proximoCodigo = codigoResult.rows[0]?.proximo ?? 1;
        const codigoCliente = `CLI-${String(proximoCodigo).padStart(4, '0')}`;

        // Cadastra a empresa na tabela solicitada: app.clientes.
        const resultadoCliente = await clienteDb.query<LinhaClienteInserido>(
          `
          INSERT INTO app.clientes (codigo, nome, ativo, usuario_atualizacao, id_empresa)
          VALUES ($1, $2, TRUE, $3, $4)
          RETURNING id_cliente, codigo, nome
          `,
          [codigoCliente, dados.nomeEmpresa, 'CADASTRO_WEB', dados.idEmpresa],
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
            dados.idEmpresa,
            dados.nomeAdministrador,
            dados.emailAdministrador,
            this.gerarHashDaSenha(dados.senha),
            dados.nomeAdministrador,
          ],
        );

        const usuario = resultadoUsuario.rows[0];

        return {
          cliente,
          usuario,
        };
      });

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

      // Erros de conectividade com banco (host/porta indisponiveis).
      if (
        /ENOTFOUND|EAI_AGAIN|ECONNREFUSED|ETIMEDOUT|timeout/i.test(
          mensagemErro,
        )
      ) {
        throw new BadRequestException(
          'Nao foi possivel conectar ao banco de dados. Verifique DB_HOST, DB_PORT e se o Postgres esta acessivel no servidor.',
        );
      }

      // 28P01 = senha/usuario invalidos no Postgres.
      if (erroPg.code === '28P01') {
        throw new BadRequestException(
          'Falha de autenticacao no banco. Verifique DB_USER e DB_PASSWORD.',
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
          'Estrutura do banco nao encontrada (schema/tabela). Verifique se app.clientes e app.usuarios existem.',
        );
      }

      // 42501 = sem permissao (inclui bloqueio por RLS).
      if (erroPg.code === '42501') {
        if (/row-level security/i.test(mensagemErro)) {
          throw new BadRequestException(
            'Cadastro bloqueado por politica de seguranca (RLS). Ajuste a policy para permitir INSERT do usuario atual.',
          );
        }

        throw new BadRequestException(
          'Usuario do banco sem permissao para executar o cadastro. Verifique GRANTs no schema app e nas tabelas.',
        );
      }

      // 23505 = violacao de chave unica (email/codigo/cnpj etc).
      if (erroPg.code === '23505') {
        throw new BadRequestException(
          'Ja existe cadastro com os dados informados. Verifique e tente novamente.',
        );
      }

      throw new BadRequestException('Nao foi possivel finalizar o cadastro neste momento.');
    }
  }

  // Normaliza campos e aplica validacoes obrigatorias antes de salvar.
  private normalizarEValidar(dadosRecebidos: DadosCadastroEmpresa) {
    const nomeEmpresa = this.normalizarTexto(dadosRecebidos.nomeEmpresa);
    const razaoSocial = this.paraNuloSeVazio(dadosRecebidos.razaoSocial);
    const cnpj = this.paraNuloSeVazio(dadosRecebidos.cnpj);
    const emailEmpresa = this.normalizarEmail(dadosRecebidos.emailEmpresa);
    const telefoneEmpresa = this.paraNuloSeVazio(dadosRecebidos.telefoneEmpresa);
    const idEmpresa = Number(dadosRecebidos.idEmpresa ?? 1);
    const nomeAdministrador = this.normalizarTexto(dadosRecebidos.nomeAdministrador);
    const emailAdministrador = this.normalizarEmail(dadosRecebidos.emailAdministrador);
    const senha = dadosRecebidos.senha ?? dadosRecebidos.password ?? '';

    if (!nomeEmpresa) {
      throw new BadRequestException('Informe o nome da empresa.');
    }

    if (!emailEmpresa) {
      throw new BadRequestException('Informe o e-mail da empresa.');
    }

    if (!Number.isInteger(idEmpresa) || idEmpresa <= 0) {
      throw new BadRequestException('Informe um idEmpresa valido.');
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

  // Cria schema/tabelas necessarias para cadastro, respeitando o uso de app.clientes.
  private async garantirEstruturaDoBanco(): Promise<void> {
    if (this.esquemaPronto) {
      return;
    }

    try {
      await this.servicoBanco.query('CREATE SCHEMA IF NOT EXISTS app;');

      // Estrutura da tabela app.clientes conforme informado.
      await this.servicoBanco.query(`
        CREATE TABLE IF NOT EXISTS app.clientes (
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

      await this.servicoBanco.query(
        'CREATE UNIQUE INDEX IF NOT EXISTS uq_app_clientes_codigo ON app.clientes (codigo);',
      );

      // Tabela de usuarios com a estrutura informada para app.usuarios.
      await this.servicoBanco.query(`
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

      await this.servicoBanco.query(
        'CREATE UNIQUE INDEX IF NOT EXISTS uq_app_usuarios_email_lower ON app.usuarios (LOWER(email));',
      );

      await this.servicoBanco.query(
        'CREATE INDEX IF NOT EXISTS idx_app_usuarios_empresa_id ON app.usuarios (id_empresa);',
      );
    } catch (erro) {
      const erroPg = erro as { code?: string; message?: string };

      // Em producao, usuario de app pode nao ter permissao de DDL.
      // Nesse caso seguimos e tentamos o fluxo de INSERT em tabelas ja existentes.
      if (erroPg.code === '42501') {
        this.logger.warn(
          `Sem permissao para DDL no banco (code=42501). Pulando auto-criacao de estrutura. message=${erroPg.message ?? 'N/A'}`,
        );
        this.esquemaPronto = true;
        return;
      }

      throw erro;
    }

    this.esquemaPronto = true;
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
