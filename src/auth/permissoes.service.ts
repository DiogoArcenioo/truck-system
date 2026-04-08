import { BadRequestException, Injectable, Logger } from '@nestjs/common';
import { DataSource } from 'typeorm';
import {
  ACOES_PERMISSAO,
  AcaoPermissao,
  MODULOS_SISTEMA,
  MODULOS_SISTEMA_METADATA,
  ModuloSistema,
  PERFIL_BASE_PADRAO,
  PERFIL_CODIGO_REGEX,
  PERFIS_BASE,
  PerfilBase,
  PERMISSOES_PADRAO_PERFIL,
  PermissoesParciaisSistema,
  PermissoesSistema,
} from './permissoes.constants';

type PerfilPermissoesRow = {
  perfil?: unknown;
  permissoes?: unknown;
};

type UsuarioPermissoesRow = {
  id_usuario?: unknown;
  permissoes?: unknown;
};

type UsuarioPerfilVinculoRow = {
  id_usuario?: unknown;
  codigo_perfil?: unknown;
};

type PerfilCadastroRow = {
  codigo?: unknown;
  nome?: unknown;
  descricao?: unknown;
  perfil_base?: unknown;
  ativo?: unknown;
  fixo?: unknown;
};

type UsuarioPerfilRow = {
  id_usuario?: unknown;
  nome?: unknown;
  email?: unknown;
  ativo?: unknown;
  perfil?: unknown;
};

export type PerfilUsuarioVinculado = {
  idUsuario: number;
  nome: string;
  email: string;
  ativo: boolean;
};

export type PerfilSistemaCadastro = {
  codigo: string;
  nome: string;
  descricao: string;
  ativo: boolean;
  fixo: boolean;
  perfilBase: PerfilBase;
  quantidadeUsuarios: number;
  usuariosVinculados: PerfilUsuarioVinculado[];
  permissoes: PermissoesSistema;
};

type ResultadoAcessoRota = {
  permitido: boolean;
  acao: AcaoPermissao | null;
  modulos: ModuloSistema[];
  permissoesEfetivas: PermissoesSistema;
};

const ROTAS_SISTEMA_POR_MODULO: Array<{
  prefixos: string[];
  modulos: ModuloSistema[];
}> = [
  { prefixos: ['/api/dashboard', '/api/sistema/dashboard'], modulos: ['home'] },
  { prefixos: ['/api/viagens', '/api/sistema/viagens'], modulos: ['viagens'] },
  {
    prefixos: ['/api/requisicao', '/api/sistema/requisicao'],
    modulos: ['requisicao'],
  },
  {
    prefixos: ['/api/ordem-servico', '/api/sistema/ordem-servico'],
    modulos: ['ordem_servico'],
  },
  {
    prefixos: ['/api/veiculo', '/api/sistema/veiculo'],
    modulos: ['veiculos'],
  },
  {
    prefixos: ['/api/pneus', '/api/sistema/pneus'],
    modulos: ['pneus'],
  },
  {
    prefixos: ['/api/engate-desengate', '/api/sistema/engate-desengate'],
    modulos: ['engate_desengate'],
  },
  {
    prefixos: ['/api/marca-veiculo', '/api/sistema/marca-veiculo'],
    modulos: ['veiculos'],
  },
  {
    prefixos: ['/api/modelo-veiculo', '/api/sistema/modelo-veiculo'],
    modulos: ['veiculos'],
  },
  {
    prefixos: ['/api/tipos-veiculo', '/api/sistema/tipos-veiculo'],
    modulos: ['veiculos'],
  },
  {
    prefixos: ['/api/cor-veiculo', '/api/sistema/cor-veiculo'],
    modulos: ['veiculos'],
  },
  {
    prefixos: ['/api/combustiveis', '/api/sistema/combustiveis'],
    modulos: ['veiculos', 'abastecimentos'],
  },
  {
    prefixos: ['/api/abastecimentos', '/api/sistema/abastecimentos'],
    modulos: ['abastecimentos'],
  },
  {
    prefixos: ['/api/despesas', '/api/sistema/despesas'],
    modulos: ['despesas'],
  },
  {
    prefixos: ['/api/multas', '/api/sistema/multas'],
    modulos: ['multas'],
  },
  {
    prefixos: ['/api/motoristas', '/api/sistema/motoristas'],
    modulos: ['motoristas'],
  },
  {
    prefixos: ['/api/fornecedor', '/api/sistema/fornecedor'],
    modulos: ['fornecedores'],
  },
  {
    prefixos: ['/api/cancelamentos', '/api/sistema/cancelamentos'],
    modulos: ['cancelamentos'],
  },
  { prefixos: ['/api/produto', '/api/sistema/produto'], modulos: ['produtos'] },
  {
    prefixos: ['/api/produto-referencias', '/api/sistema/produto-referencias'],
    modulos: ['produtos', 'requisicao'],
  },
  { prefixos: ['/api/sistema/usuarios'], modulos: ['usuarios'] },
  { prefixos: ['/api/sistema/licenca'], modulos: ['home'] },
];

@Injectable()
export class PermissoesService {
  private readonly logger = new Logger(PermissoesService.name);
  private estruturaInicializada = false;
  private persistenciaPermissoesDisponivel = true;
  private inicializacaoEmAndamento: Promise<void> | null = null;

  constructor(private readonly dataSource: DataSource) {}

  normalizarPerfil(perfil: string | undefined): string {
    const valor = this.normalizarCodigoPerfil(perfil ?? PERFIL_BASE_PADRAO);
    if (!PERFIL_CODIGO_REGEX.test(valor)) {
      throw new BadRequestException(
        'Perfil inválido. Use de 2 a 40 caracteres com letras, números, "_" ou "-".',
      );
    }

    return valor;
  }

  normalizarPerfilBase(
    perfilBase: string | undefined,
    estrito = true,
  ): PerfilBase {
    const valor = (perfilBase ?? PERFIL_BASE_PADRAO).trim().toUpperCase();
    if (!PERFIS_BASE.includes(valor as PerfilBase)) {
      if (!estrito) {
        return PERFIL_BASE_PADRAO;
      }
      throw new BadRequestException(
        'Perfil base inválido. Valores permitidos: ADM, GESTOR, OPERADOR.',
      );
    }

    return valor as PerfilBase;
  }

  async listarPermissoesEmpresa(idEmpresa: number) {
    await this.garantirEstrutura();
    await this.garantirPerfisEmpresa(idEmpresa);
    const [perfis, overrides, perfisCadastro] = await Promise.all([
      this.obterPermissoesPerfisEmpresa(idEmpresa),
      this.obterOverridesUsuariosEmpresa(idEmpresa),
      this.listarPerfisEmpresa(idEmpresa),
    ]);

    return {
      modulos: MODULOS_SISTEMA_METADATA,
      perfis,
      perfisCadastro,
      overridesUsuarios: overrides,
    };
  }

  async obterMapeamentoPerfisUsuariosEmpresa(idEmpresa: number) {
    await this.garantirEstrutura();
    if (!this.persistenciaPermissoesDisponivel) {
      return {} as Record<number, string>;
    }

    const linhas = (await this.dataSource.query(
      `
      SELECT id_usuario, codigo_perfil
      FROM app.usuario_perfil_vinculos
      WHERE id_empresa = $1
      `,
      [idEmpresa],
    )) as UsuarioPerfilVinculoRow[];

    const mapa: Record<number, string> = {};
    for (const linha of linhas) {
      const idUsuario = this.lerNumero(linha.id_usuario);
      const codigoPerfil = this.normalizarPerfilTalvez(
        this.lerTexto(linha.codigo_perfil),
      );
      if (!idUsuario || idUsuario <= 0 || !codigoPerfil) {
        continue;
      }
      mapa[idUsuario] = codigoPerfil;
    }

    return mapa;
  }

  async resolverPerfilUsuario(
    idEmpresa: number,
    idUsuario: number,
    perfilFallback: string | undefined,
  ) {
    const fallback = this.normalizarPerfil(perfilFallback ?? PERFIL_BASE_PADRAO);
    await this.garantirEstrutura();
    if (!this.persistenciaPermissoesDisponivel) {
      return fallback;
    }

    const linhas = (await this.dataSource.query(
      `
      SELECT codigo_perfil
      FROM app.usuario_perfil_vinculos
      WHERE id_empresa = $1
        AND id_usuario = $2
      LIMIT 1
      `,
      [idEmpresa, idUsuario],
    )) as Array<{ codigo_perfil?: unknown }>;

    if (!Array.isArray(linhas) || linhas.length === 0) {
      return fallback;
    }

    const codigoPerfil = this.normalizarPerfilTalvez(
      this.lerTexto(linhas[0].codigo_perfil),
    );

    return codigoPerfil ?? fallback;
  }

  async atualizarPerfilVinculadoUsuario(
    idEmpresa: number,
    idUsuario: number,
    codigoPerfil: string,
    usuarioAtualizacao: string,
  ) {
    await this.garantirEstrutura();
    this.exigirPersistenciaDisponivel();

    const perfil = await this.validarPerfilParaVinculo(idEmpresa, codigoPerfil, false);

    await this.dataSource.query(
      `
      INSERT INTO app.usuario_perfil_vinculos (
        id_empresa,
        id_usuario,
        codigo_perfil,
        usuario_atualizacao,
        criado_em,
        atualizado_em
      )
      VALUES ($1, $2, $3, $4, NOW(), NOW())
      ON CONFLICT (id_empresa, id_usuario)
      DO UPDATE SET
        codigo_perfil = EXCLUDED.codigo_perfil,
        usuario_atualizacao = EXCLUDED.usuario_atualizacao,
        atualizado_em = NOW()
      `,
      [idEmpresa, idUsuario, perfil.codigo, usuarioAtualizacao],
    );

    return perfil;
  }

  async listarPerfisEmpresa(idEmpresa: number): Promise<PerfilSistemaCadastro[]> {
    await this.garantirEstrutura();
    await this.garantirPerfisEmpresa(idEmpresa);

    if (!this.persistenciaPermissoesDisponivel) {
      return this.listarPerfisFallbackSemPersistencia(idEmpresa);
    }

    const [linhasPerfil, permissoesPerfil, vinculos] = await Promise.all([
      this.dataSource.query(
        `
        SELECT
          codigo,
          nome,
          descricao,
          perfil_base,
          ativo,
          fixo
        FROM app.perfis_usuario
        WHERE id_empresa = $1
        ORDER BY fixo DESC, nome ASC, codigo ASC
        `,
        [idEmpresa],
      ) as Promise<PerfilCadastroRow[]>,
      this.obterPermissoesPerfisEmpresa(idEmpresa),
      this.obterVinculosUsuariosPorPerfil(idEmpresa),
    ]);

    return linhasPerfil
      .map((linha) => this.mapearPerfilCadastro(linha, permissoesPerfil, vinculos))
      .filter((perfil): perfil is PerfilSistemaCadastro => Boolean(perfil));
  }

  async obterPerfilCadastroPorCodigo(
    idEmpresa: number,
    codigoPerfil: string,
  ): Promise<PerfilSistemaCadastro | null> {
    await this.garantirEstrutura();
    await this.garantirPerfisEmpresa(idEmpresa);
    const codigo = this.normalizarPerfil(codigoPerfil);

    if (!this.persistenciaPermissoesDisponivel) {
      const perfis = await this.listarPerfisFallbackSemPersistencia(idEmpresa);
      return perfis.find((perfil) => perfil.codigo === codigo) ?? null;
    }

    const [linhas, permissoes, vinculos] = await Promise.all([
      this.dataSource.query(
        `
        SELECT
          codigo,
          nome,
          descricao,
          perfil_base,
          ativo,
          fixo
        FROM app.perfis_usuario
        WHERE id_empresa = $1
          AND codigo = $2
        LIMIT 1
        `,
        [idEmpresa, codigo],
      ) as Promise<PerfilCadastroRow[]>,
      this.obterPermissoesPerfisEmpresa(idEmpresa),
      this.obterVinculosUsuariosPorPerfil(idEmpresa),
    ]);

    if (!Array.isArray(linhas) || linhas.length === 0) {
      return null;
    }

    return this.mapearPerfilCadastro(linhas[0], permissoes, vinculos);
  }

  async validarPerfilParaVinculo(
    idEmpresa: number,
    codigoPerfil: string,
    exigirAtivo = true,
  ): Promise<PerfilSistemaCadastro> {
    const perfil = await this.obterPerfilCadastroPorCodigo(idEmpresa, codigoPerfil);
    if (!perfil) {
      throw new BadRequestException('Perfil nao encontrado para a empresa logada.');
    }

    if (exigirAtivo && !perfil.ativo) {
      throw new BadRequestException(
        `Perfil ${perfil.codigo} esta inativo e nao pode receber novos usuarios.`,
      );
    }

    return perfil;
  }

  async criarPerfil(
    idEmpresa: number,
    dados: {
      nome: string;
      codigo?: string;
      descricao?: string;
      ativo?: boolean;
      perfilBase?: string;
      permissoes?: unknown;
    },
    usuarioAtualizacao: string,
  ): Promise<PerfilSistemaCadastro> {
    await this.garantirEstrutura();
    this.exigirPersistenciaDisponivel();
    await this.garantirPerfisEmpresa(idEmpresa);

    const nome = this.normalizarTextoObrigatorio(dados.nome, 'Nome do perfil');
    const descricao = this.normalizarTextoOpcional(dados.descricao);
    const codigo = dados.codigo?.trim()
      ? this.normalizarPerfil(dados.codigo)
      : this.gerarCodigoPerfil(nome);
    const perfilBase = this.normalizarPerfilBase(dados.perfilBase);
    const ativo = dados.ativo ?? true;

    const existe = await this.dataSource.query(
      `
      SELECT 1
      FROM app.perfis_usuario
      WHERE id_empresa = $1
        AND codigo = $2
      LIMIT 1
      `,
      [idEmpresa, codigo],
    );

    if (Array.isArray(existe) && existe.length > 0) {
      throw new BadRequestException(
        `Ja existe um perfil cadastrado com o codigo ${codigo}.`,
      );
    }

    await this.dataSource.query(
      `
      INSERT INTO app.perfis_usuario (
        id_empresa,
        codigo,
        nome,
        descricao,
        perfil_base,
        ativo,
        fixo,
        usuario_atualizacao,
        criado_em,
        atualizado_em
      )
      VALUES ($1, $2, $3, $4, $5, $6, FALSE, $7, NOW(), NOW())
      `,
      [idEmpresa, codigo, nome, descricao, perfilBase, ativo, usuarioAtualizacao],
    );

    if (dados.permissoes) {
      await this.atualizarPermissoesPerfil(
        idEmpresa,
        codigo,
        dados.permissoes,
        usuarioAtualizacao,
      );
    }

    const perfilCriado = await this.obterPerfilCadastroPorCodigo(idEmpresa, codigo);
    if (!perfilCriado) {
      throw new BadRequestException('Nao foi possivel carregar o perfil criado.');
    }

    return perfilCriado;
  }

  async atualizarPerfilCadastro(
    idEmpresa: number,
    codigoPerfil: string,
    dados: {
      nome?: string;
      descricao?: string;
      ativo?: boolean;
      perfilBase?: string;
      permissoes?: unknown;
    },
    usuarioAtualizacao: string,
  ): Promise<PerfilSistemaCadastro> {
    await this.garantirEstrutura();
    this.exigirPersistenciaDisponivel();
    await this.garantirPerfisEmpresa(idEmpresa);

    const codigo = this.normalizarPerfil(codigoPerfil);
    const perfilAtual = await this.obterPerfilCadastroPorCodigo(idEmpresa, codigo);

    if (!perfilAtual) {
      throw new BadRequestException('Perfil nao encontrado para a empresa logada.');
    }

    if (perfilAtual.fixo && codigo === 'ADM' && dados.ativo === false) {
      throw new BadRequestException('Nao e permitido inativar o perfil ADM.');
    }

    const nome =
      dados.nome !== undefined
        ? this.normalizarTextoObrigatorio(dados.nome, 'Nome do perfil')
        : perfilAtual.nome;
    const descricao =
      dados.descricao !== undefined
        ? this.normalizarTextoOpcional(dados.descricao)
        : perfilAtual.descricao;
    const ativo = dados.ativo ?? perfilAtual.ativo;
    const perfilBase = this.normalizarPerfilBase(
      dados.perfilBase ?? perfilAtual.perfilBase,
    );

    await this.dataSource.query(
      `
      UPDATE app.perfis_usuario
      SET
        nome = $3,
        descricao = $4,
        ativo = $5,
        perfil_base = $6,
        usuario_atualizacao = $7,
        atualizado_em = NOW()
      WHERE id_empresa = $1
        AND codigo = $2
      `,
      [idEmpresa, codigo, nome, descricao, ativo, perfilBase, usuarioAtualizacao],
    );

    if (dados.permissoes !== undefined) {
      await this.atualizarPermissoesPerfil(
        idEmpresa,
        codigo,
        dados.permissoes,
        usuarioAtualizacao,
      );
    }

    const perfilAtualizado = await this.obterPerfilCadastroPorCodigo(idEmpresa, codigo);
    if (!perfilAtualizado) {
      throw new BadRequestException('Nao foi possivel carregar o perfil atualizado.');
    }

    return perfilAtualizado;
  }

  async obterPermissoesPerfisEmpresa(idEmpresa: number) {
    await this.garantirEstrutura();
    await this.garantirPerfisEmpresa(idEmpresa);
    if (!this.persistenciaPermissoesDisponivel) {
      return {
        ADM: this.clonarPermissoes(PERMISSOES_PADRAO_PERFIL.ADM),
        GESTOR: this.clonarPermissoes(PERMISSOES_PADRAO_PERFIL.GESTOR),
        OPERADOR: this.clonarPermissoes(PERMISSOES_PADRAO_PERFIL.OPERADOR),
      } as Record<string, PermissoesSistema>;
    }

    const [linhasPerfil, linhasPermissao] = await Promise.all([
      this.dataSource.query(
        `
        SELECT codigo, perfil_base
        FROM app.perfis_usuario
        WHERE id_empresa = $1
        `,
        [idEmpresa],
      ) as Promise<PerfilCadastroRow[]>,
      this.dataSource.query(
        `
        SELECT perfil, permissoes
        FROM app.perfil_permissoes
        WHERE id_empresa = $1
        `,
        [idEmpresa],
      ) as Promise<PerfilPermissoesRow[]>,
    ]);

    const permissoesCustomizadasPorPerfil = new Map<string, PermissoesSistema>();

    for (const linha of linhasPermissao) {
      const perfil = this.normalizarPerfilTalvez(this.lerTexto(linha.perfil));
      if (!perfil) {
        continue;
      }

      const permissoesCompletas = this.normalizarPermissoesCompletas(
        this.parseJsonTalvez(linha.permissoes),
      );
      permissoesCustomizadasPorPerfil.set(perfil, permissoesCompletas);
    }

    const resposta = {} as Record<string, PermissoesSistema>;
    for (const linha of linhasPerfil) {
      const codigoPerfil = this.normalizarPerfilTalvez(this.lerTexto(linha.codigo));
      if (!codigoPerfil) {
        continue;
      }

      if (codigoPerfil === 'ADM') {
        resposta[codigoPerfil] = this.clonarPermissoes(PERMISSOES_PADRAO_PERFIL.ADM);
        continue;
      }

      const perfilBase = this.normalizarPerfilBase(
        this.lerTexto(linha.perfil_base) || PERFIL_BASE_PADRAO,
        false,
      );
      const base = this.clonarPermissoes(PERMISSOES_PADRAO_PERFIL[perfilBase]);
      resposta[codigoPerfil] = permissoesCustomizadasPorPerfil.get(codigoPerfil) ?? base;
    }

    for (const perfilBase of PERFIS_BASE) {
      if (!resposta[perfilBase]) {
        resposta[perfilBase] = this.clonarPermissoes(
          PERMISSOES_PADRAO_PERFIL[perfilBase],
        );
      }
    }

    return resposta;
  }

  async obterOverridesUsuariosEmpresa(idEmpresa: number) {
    await this.garantirEstrutura();
    if (!this.persistenciaPermissoesDisponivel) {
      return {};
    }

    const linhas = await this.dataSource.query(
      `
      SELECT id_usuario, permissoes
      FROM app.usuario_permissoes
      WHERE id_empresa = $1
      `,
      [idEmpresa],
    );

    const overrides: Record<number, PermissoesParciaisSistema> = {};

    for (const linha of linhas as UsuarioPermissoesRow[]) {
      const idUsuario = this.lerNumero(linha.id_usuario);
      if (!idUsuario || idUsuario <= 0) {
        continue;
      }

      overrides[idUsuario] = this.normalizarPermissoesParciais(
        this.parseJsonTalvez(linha.permissoes),
      );
    }

    return overrides;
  }

  async obterPermissoesEfetivasUsuario(
    idEmpresa: number,
    idUsuario: number,
    perfil: string,
  ): Promise<PermissoesSistema> {
    const perfilNormalizado = this.normalizarPerfil(perfil);
    if (perfilNormalizado === 'ADM') {
      return this.clonarPermissoes(PERMISSOES_PADRAO_PERFIL.ADM);
    }

    const [permissoesPerfis, overrideUsuario] = await Promise.all([
      this.obterPermissoesPerfisEmpresa(idEmpresa),
      this.obterOverrideUsuario(idEmpresa, idUsuario),
    ]);

    const basePerfil = this.clonarPermissoes(
      permissoesPerfis[perfilNormalizado] ??
        this.clonarPermissoes(PERMISSOES_PADRAO_PERFIL.OPERADOR),
    );
    return this.aplicarOverrides(basePerfil, overrideUsuario ?? {});
  }

  combinarPermissoes(
    base: PermissoesSistema,
    overrideUsuario: PermissoesParciaisSistema | null | undefined,
  ) {
    return this.aplicarOverrides(
      this.clonarPermissoes(base),
      overrideUsuario ?? {},
    );
  }

  async atualizarPermissoesPerfil(
    idEmpresa: number,
    perfil: string,
    permissoes: unknown,
    usuarioAtualizacao: string,
  ) {
    await this.garantirEstrutura();
    this.exigirPersistenciaDisponivel();
    await this.garantirPerfisEmpresa(idEmpresa);

    const perfilNormalizado = this.normalizarPerfil(perfil);
    if (perfilNormalizado === 'ADM') {
      throw new BadRequestException(
        'Perfil ADM possui acesso total fixo e nao pode ser restringido.',
      );
    }

    const perfilCadastro = await this.obterPerfilCadastroPorCodigo(
      idEmpresa,
      perfilNormalizado,
    );
    if (!perfilCadastro) {
      throw new BadRequestException('Perfil nao encontrado para a empresa logada.');
    }

    const base = this.clonarPermissoes(
      PERMISSOES_PADRAO_PERFIL[perfilCadastro.perfilBase],
    );
    const overrides = this.normalizarPermissoesParciais(permissoes, true);
    const efetivas = this.aplicarOverrides(base, overrides);

    await this.dataSource.query(
      `
      INSERT INTO app.perfil_permissoes (
        id_empresa,
        perfil,
        permissoes,
        usuario_atualizacao,
        criado_em,
        atualizado_em
      )
      VALUES ($1, $2, $3::jsonb, $4, NOW(), NOW())
      ON CONFLICT (id_empresa, perfil)
      DO UPDATE SET
        permissoes = EXCLUDED.permissoes,
        usuario_atualizacao = EXCLUDED.usuario_atualizacao,
        atualizado_em = NOW()
      `,
      [idEmpresa, perfilNormalizado, JSON.stringify(efetivas), usuarioAtualizacao],
    );

    return efetivas;
  }

  async atualizarPermissoesUsuario(
    idEmpresa: number,
    idUsuario: number,
    permissoes: unknown,
    usuarioAtualizacao: string,
  ) {
    await this.garantirEstrutura();
    this.exigirPersistenciaDisponivel();
    const overrides = this.normalizarPermissoesParciais(permissoes, true);

    await this.dataSource.query(
      `
      INSERT INTO app.usuario_permissoes (
        id_empresa,
        id_usuario,
        permissoes,
        usuario_atualizacao,
        criado_em,
        atualizado_em
      )
      VALUES ($1, $2, $3::jsonb, $4, NOW(), NOW())
      ON CONFLICT (id_empresa, id_usuario)
      DO UPDATE SET
        permissoes = EXCLUDED.permissoes,
        usuario_atualizacao = EXCLUDED.usuario_atualizacao,
        atualizado_em = NOW()
      `,
      [idEmpresa, idUsuario, JSON.stringify(overrides), usuarioAtualizacao],
    );

    return overrides;
  }

  async obterPermissoesUsuarioCustomizadas(idEmpresa: number, idUsuario: number) {
    return this.obterOverrideUsuario(idEmpresa, idUsuario);
  }

  async limparPermissoesUsuario(idEmpresa: number, idUsuario: number) {
    await this.garantirEstrutura();
    this.exigirPersistenciaDisponivel();
    await this.dataSource.query(
      `
      DELETE FROM app.usuario_permissoes
      WHERE id_empresa = $1
        AND id_usuario = $2
      `,
      [idEmpresa, idUsuario],
    );
  }

  async avaliarPermissaoPorRota(
    idEmpresa: number,
    idUsuario: number,
    perfil: string,
    url: string | undefined,
    method: string | undefined,
  ): Promise<ResultadoAcessoRota> {
    const permissoesEfetivas = await this.obterPermissoesEfetivasUsuario(
      idEmpresa,
      idUsuario,
      perfil,
    );
    const acao = this.mapearAcaoPorMetodo(method);
    const modulos = this.mapearModulosPorRota(url);

    if (!acao || modulos.length === 0) {
      return {
        permitido: true,
        acao,
        modulos,
        permissoesEfetivas,
      };
    }

    const permitido = modulos.some((modulo) =>
      this.permiteAcao(permissoesEfetivas, modulo, acao),
    );

    return {
      permitido,
      acao,
      modulos,
      permissoesEfetivas,
    };
  }

  private async obterOverrideUsuario(
    idEmpresa: number,
    idUsuario: number,
  ): Promise<PermissoesParciaisSistema | null> {
    await this.garantirEstrutura();
    if (!this.persistenciaPermissoesDisponivel) {
      return null;
    }

    const linhas = await this.dataSource.query(
      `
      SELECT permissoes
      FROM app.usuario_permissoes
      WHERE id_empresa = $1
        AND id_usuario = $2
      LIMIT 1
      `,
      [idEmpresa, idUsuario],
    );

    if (!Array.isArray(linhas) || linhas.length === 0) {
      return null;
    }

    return this.normalizarPermissoesParciais(
      this.parseJsonTalvez((linhas[0] as UsuarioPermissoesRow).permissoes),
    );
  }

  private permiteAcao(
    permissoes: PermissoesSistema,
    modulo: ModuloSistema,
    acao: AcaoPermissao,
  ) {
    return Boolean(permissoes[modulo]?.[acao]);
  }

  private mapearAcaoPorMetodo(method: string | undefined): AcaoPermissao | null {
    if (!method) {
      return null;
    }

    const metodo = method.trim().toUpperCase();

    if (metodo === 'GET' || metodo === 'HEAD' || metodo === 'OPTIONS') {
      return 'visualizar';
    }

    if (metodo === 'POST') {
      return 'criar';
    }

    if (metodo === 'PUT' || metodo === 'PATCH') {
      return 'editar';
    }

    if (metodo === 'DELETE') {
      return 'excluir';
    }

    return null;
  }

  private mapearModulosPorRota(url: string | undefined): ModuloSistema[] {
    const caminho = this.normalizarUrl(url);
    if (!caminho) {
      return [];
    }

    for (const item of ROTAS_SISTEMA_POR_MODULO) {
      if (
        item.prefixos.some(
          (prefixo) =>
            caminho === prefixo || caminho.startsWith(`${prefixo}/`),
        )
      ) {
        return item.modulos;
      }
    }

    return [];
  }

  private normalizarUrl(url: string | undefined) {
    if (!url || !url.trim()) {
      return '';
    }

    const semQuery = url.split('?')[0]?.trim();
    if (!semQuery) {
      return '';
    }

    return semQuery.startsWith('/') ? semQuery : `/${semQuery}`;
  }

  private normalizarPermissoesCompletas(valor: unknown): PermissoesSistema {
    const base = this.clonarPermissoes(PERMISSOES_PADRAO_PERFIL.OPERADOR);
    if (!valor || typeof valor !== 'object' || Array.isArray(valor)) {
      return base;
    }

    const entrada = valor as Record<string, unknown>;
    for (const modulo of MODULOS_SISTEMA) {
      const moduloValor = entrada[modulo];
      if (
        !moduloValor ||
        typeof moduloValor !== 'object' ||
        Array.isArray(moduloValor)
      ) {
        continue;
      }

      const parcial = moduloValor as Record<string, unknown>;
      for (const acao of ACOES_PERMISSAO) {
        if (typeof parcial[acao] === 'boolean') {
          base[modulo][acao] = Boolean(parcial[acao]);
        }
      }
    }

    return base;
  }

  private normalizarPermissoesParciais(
    valor: unknown,
    estrito = false,
  ): PermissoesParciaisSistema {
    if (valor === null || valor === undefined) {
      return {};
    }

    if (typeof valor !== 'object') {
      throw new BadRequestException('Formato de permissoes invalido.');
    }

    const entrada = valor as Record<string, unknown>;
    const resultado: PermissoesParciaisSistema = {};

    for (const [moduloBruto, permissoesBrutas] of Object.entries(entrada)) {
      if (!MODULOS_SISTEMA.includes(moduloBruto as ModuloSistema)) {
        throw new BadRequestException(
          `Modulo de permissao invalido: ${moduloBruto}.`,
        );
      }

      if (
        permissoesBrutas === null ||
        typeof permissoesBrutas !== 'object' ||
        Array.isArray(permissoesBrutas)
      ) {
        throw new BadRequestException(
          `Permissoes invalidas para o modulo ${moduloBruto}.`,
        );
      }

      const modulo = moduloBruto as ModuloSistema;
      const parcial = permissoesBrutas as Record<string, unknown>;
      const modResultado: Partial<Record<AcaoPermissao, boolean>> = {};

      for (const [acaoBruta, valorBruto] of Object.entries(parcial)) {
        if (!ACOES_PERMISSAO.includes(acaoBruta as AcaoPermissao)) {
          throw new BadRequestException(
            `Acao de permissao invalida: ${acaoBruta}.`,
          );
        }

        if (typeof valorBruto !== 'boolean') {
          throw new BadRequestException(
            `Permissao ${modulo}.${acaoBruta} deve ser booleana.`,
          );
        }

        modResultado[acaoBruta as AcaoPermissao] = valorBruto;
      }

      if (estrito && Object.keys(modResultado).length === 0) {
        throw new BadRequestException(
          `Informe ao menos uma acao para o modulo ${modulo}.`,
        );
      }

      resultado[modulo] = modResultado;
    }

    if (estrito && Object.keys(resultado).length === 0) {
      throw new BadRequestException(
        'Informe ao menos um modulo para atualizar permissoes.',
      );
    }

    return resultado;
  }

  private aplicarOverrides(
    base: PermissoesSistema,
    overrides: PermissoesParciaisSistema,
  ): PermissoesSistema {
    const resultado = this.clonarPermissoes(base);

    for (const modulo of MODULOS_SISTEMA) {
      const overrideModulo = overrides[modulo];
      if (!overrideModulo) {
        continue;
      }

      for (const acao of ACOES_PERMISSAO) {
        if (typeof overrideModulo[acao] === 'boolean') {
          resultado[modulo][acao] = Boolean(overrideModulo[acao]);
        }
      }
    }

    return resultado;
  }

  private clonarPermissoes(origem: PermissoesSistema): PermissoesSistema {
    const clone = {} as PermissoesSistema;

    for (const modulo of MODULOS_SISTEMA) {
      clone[modulo] = {
        visualizar: Boolean(origem[modulo]?.visualizar),
        criar: Boolean(origem[modulo]?.criar),
        editar: Boolean(origem[modulo]?.editar),
        excluir: Boolean(origem[modulo]?.excluir),
      };
    }

    return clone;
  }

  private parseJsonTalvez(valor: unknown): unknown {
    if (typeof valor !== 'string') {
      return valor;
    }

    try {
      return JSON.parse(valor);
    } catch {
      return {};
    }
  }

  private lerTexto(valor: unknown) {
    if (typeof valor !== 'string') {
      return '';
    }
    return valor.trim();
  }

  private lerNumero(valor: unknown) {
    if (typeof valor === 'number' && Number.isFinite(valor)) {
      return Math.trunc(valor);
    }

    if (typeof valor === 'string' && valor.trim()) {
      const numero = Number(valor);
      if (Number.isFinite(numero)) {
        return Math.trunc(numero);
      }
    }

    return 0;
  }

  private lerBoolean(valor: unknown, fallback = false) {
    if (typeof valor === 'boolean') {
      return valor;
    }

    if (typeof valor === 'number') {
      return valor !== 0;
    }

    if (typeof valor === 'string') {
      const texto = valor.trim().toLowerCase();
      if (texto === 'true' || texto === 't' || texto === '1') {
        return true;
      }
      if (texto === 'false' || texto === 'f' || texto === '0') {
        return false;
      }
    }

    return fallback;
  }

  private normalizarTextoOpcional(valor: string | undefined): string {
    if (!valor) {
      return '';
    }
    return valor.trim();
  }

  private normalizarTextoObrigatorio(valor: string | undefined, campo: string): string {
    const texto = (valor ?? '').trim();
    if (!texto) {
      throw new BadRequestException(`${campo} e obrigatorio.`);
    }
    return texto;
  }

  private normalizarCodigoPerfil(valor: string | undefined): string {
    return (valor ?? '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .trim()
      .toUpperCase()
      .replace(/\s+/g, '_')
      .replace(/[^A-Z0-9_-]/g, '_')
      .replace(/_+/g, '_')
      .replace(/^_+|_+$/g, '');
  }

  private normalizarPerfilTalvez(valor: string | undefined): string | null {
    const codigo = this.normalizarCodigoPerfil(valor);
    if (!PERFIL_CODIGO_REGEX.test(codigo)) {
      return null;
    }
    return codigo;
  }

  private gerarCodigoPerfil(nome: string): string {
    const base = this.normalizarCodigoPerfil(nome);
    if (PERFIL_CODIGO_REGEX.test(base)) {
      return base;
    }

    const timestamp = Date.now().toString().slice(-6);
    const fallback = `PERFIL_${timestamp}`;
    if (PERFIL_CODIGO_REGEX.test(fallback)) {
      return fallback;
    }

    return 'PERFIL_CUSTOM';
  }

  private async obterVinculosUsuariosPorPerfil(idEmpresa: number) {
    const linhas = this.persistenciaPermissoesDisponivel
      ? ((await this.dataSource.query(
          `
          SELECT
            u.id_usuario,
            u.nome,
            u.email,
            u.ativo,
            COALESCE(
              NULLIF(TRIM(up.codigo_perfil), ''),
              u.perfil
            ) AS perfil
          FROM app.usuarios u
          LEFT JOIN app.usuario_perfil_vinculos up
            ON up.id_empresa = u.id_empresa
           AND up.id_usuario = u.id_usuario
          WHERE u.id_empresa = $1
          ORDER BY u.nome ASC
          `,
          [idEmpresa],
        )) as UsuarioPerfilRow[])
      : ((await this.dataSource.query(
          `
          SELECT
            id_usuario,
            nome,
            email,
            ativo,
            perfil
          FROM app.usuarios
          WHERE id_empresa = $1
          ORDER BY nome ASC
          `,
          [idEmpresa],
        )) as UsuarioPerfilRow[]);

    const vinculos: Record<string, PerfilUsuarioVinculado[]> = {};

    for (const linha of linhas) {
      const perfil = this.normalizarPerfilTalvez(this.lerTexto(linha.perfil));
      const idUsuario = this.lerNumero(linha.id_usuario);

      if (!perfil || !idUsuario || idUsuario <= 0) {
        continue;
      }

      if (!vinculos[perfil]) {
        vinculos[perfil] = [];
      }

      vinculos[perfil].push({
        idUsuario,
        nome: this.lerTexto(linha.nome) || `USUARIO ${idUsuario}`,
        email: this.lerTexto(linha.email),
        ativo: this.lerBoolean(linha.ativo, true),
      });
    }

    return vinculos;
  }

  private mapearPerfilCadastro(
    linha: PerfilCadastroRow,
    permissoesPerfil: Record<string, PermissoesSistema>,
    vinculos: Record<string, PerfilUsuarioVinculado[]>,
  ): PerfilSistemaCadastro | null {
    const codigo = this.normalizarPerfilTalvez(this.lerTexto(linha.codigo));
    if (!codigo) {
      return null;
    }

    const perfilBase = this.normalizarPerfilBase(
      this.lerTexto(linha.perfil_base) || PERFIL_BASE_PADRAO,
      false,
    );
    const usuariosVinculados = vinculos[codigo] ?? [];
    const permissoes =
      permissoesPerfil[codigo] ??
      this.clonarPermissoes(PERMISSOES_PADRAO_PERFIL[perfilBase]);

    return {
      codigo,
      nome: this.lerTexto(linha.nome) || codigo,
      descricao: this.lerTexto(linha.descricao),
      ativo: this.lerBoolean(linha.ativo, true),
      fixo: this.lerBoolean(linha.fixo, false),
      perfilBase,
      quantidadeUsuarios: usuariosVinculados.length,
      usuariosVinculados,
      permissoes,
    };
  }

  private async listarPerfisFallbackSemPersistencia(idEmpresa: number) {
    const perfisBase = PERFIS_BASE.map((perfilBase) => ({
      codigo: perfilBase,
      nome:
        perfilBase === 'ADM'
          ? 'Administrador'
          : perfilBase === 'GESTOR'
            ? 'Gestor'
            : 'Operador',
      descricao: '',
      ativo: true,
      fixo: perfilBase === 'ADM',
      perfilBase,
      quantidadeUsuarios: 0,
      usuariosVinculados: [] as PerfilUsuarioVinculado[],
      permissoes: this.clonarPermissoes(PERMISSOES_PADRAO_PERFIL[perfilBase]),
    }));

    const vinculos = await this.obterVinculosUsuariosPorPerfil(idEmpresa);
    for (const perfil of perfisBase) {
      const usuarios = vinculos[perfil.codigo] ?? [];
      perfil.usuariosVinculados = usuarios;
      perfil.quantidadeUsuarios = usuarios.length;
    }

    return perfisBase;
  }

  private async garantirPerfisEmpresa(idEmpresa: number) {
    if (!this.persistenciaPermissoesDisponivel) {
      return;
    }

    await this.inserirPerfisBaseEmpresa(idEmpresa);
    await this.sincronizarPerfisComUsuarios(idEmpresa);
  }

  private async inserirPerfisBaseEmpresa(idEmpresa: number) {
    await this.dataSource.query(
      `
      INSERT INTO app.perfis_usuario (
        id_empresa,
        codigo,
        nome,
        descricao,
        perfil_base,
        ativo,
        fixo,
        usuario_atualizacao,
        criado_em,
        atualizado_em
      )
      VALUES
        ($1, 'ADM', 'Administrador', 'Acesso total do sistema.', 'ADM', TRUE, TRUE, 'SISTEMA', NOW(), NOW()),
        ($1, 'GESTOR', 'Gestor', 'Acesso amplo sem administracao de usuarios por padrao.', 'GESTOR', TRUE, FALSE, 'SISTEMA', NOW(), NOW()),
        ($1, 'OPERADOR', 'Operador', 'Operacao diaria com permissoes restritas por padrao.', 'OPERADOR', TRUE, FALSE, 'SISTEMA', NOW(), NOW())
      ON CONFLICT (id_empresa, codigo)
      DO NOTHING
      `,
      [idEmpresa],
    );
  }

  private async sincronizarPerfisComUsuarios(idEmpresa: number) {
    await this.dataSource.query(
      `
      INSERT INTO app.perfis_usuario (
        id_empresa,
        codigo,
        nome,
        descricao,
        perfil_base,
        ativo,
        fixo,
        usuario_atualizacao,
        criado_em,
        atualizado_em
      )
      SELECT
        $1,
        base.perfil_codigo,
        base.perfil_codigo,
        '',
        $2,
        TRUE,
        FALSE,
        'SISTEMA',
        NOW(),
        NOW()
      FROM (
        SELECT DISTINCT
          NULLIF(
            REGEXP_REPLACE(
              UPPER(TRIM(perfil)),
              '[^A-Z0-9_-]+',
              '_',
              'g'
            ),
            ''
          ) AS perfil_codigo
        FROM app.usuarios
        WHERE id_empresa = $1
          AND perfil IS NOT NULL
          AND TRIM(perfil) <> ''
        UNION
        SELECT DISTINCT
          NULLIF(
            REGEXP_REPLACE(
              UPPER(TRIM(codigo_perfil)),
              '[^A-Z0-9_-]+',
              '_',
              'g'
            ),
            ''
          ) AS perfil_codigo
        FROM app.usuario_perfil_vinculos
        WHERE id_empresa = $1
          AND codigo_perfil IS NOT NULL
          AND TRIM(codigo_perfil) <> ''
      ) AS base
      LEFT JOIN app.perfis_usuario existente
        ON existente.id_empresa = $1
       AND existente.codigo = base.perfil_codigo
      WHERE base.perfil_codigo IS NOT NULL
        AND CHAR_LENGTH(base.perfil_codigo) >= 2
        AND existente.id_perfil IS NULL
      `,
      [idEmpresa, PERFIL_BASE_PADRAO],
    );
  }

  private async ajustarRestricaoPerfilUsuarios() {
    try {
      const constraints = (await this.dataSource.query(
        `
        SELECT c.conname AS nome
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'app'
          AND t.relname = 'usuarios'
          AND c.contype = 'c'
          AND pg_get_constraintdef(c.oid) ILIKE '%perfil%'
        `,
      )) as Array<{ nome?: unknown }>;
      let jaTemConstraintPadrao = false;

      for (const item of constraints) {
        const nome = this.lerTexto(item.nome);
        if (!nome) {
          continue;
        }

        if (nome === 'ck_usuarios_perfil_nao_vazio') {
          jaTemConstraintPadrao = true;
          continue;
        }

        const nomeEscapado = nome.replace(/"/g, '""');
        await this.dataSource.query(
          `ALTER TABLE app.usuarios DROP CONSTRAINT IF EXISTS "${nomeEscapado}"`,
        );
      }

      if (!jaTemConstraintPadrao) {
        await this.dataSource.query(`
          ALTER TABLE app.usuarios
          ADD CONSTRAINT ck_usuarios_perfil_nao_vazio
          CHECK (CHAR_LENGTH(TRIM(perfil)) >= 2)
        `);
      }
    } catch (error) {
      this.logger.warn(
        `Nao foi possivel ajustar constraint de perfil em app.usuarios. message=${error instanceof Error ? error.message : 'Erro desconhecido'}`,
      );
    }
  }

  private async ajustarComprimentoColunasPerfil() {
    try {
      await this.dataSource.query(`
        ALTER TABLE app.perfil_permissoes
        ALTER COLUMN perfil TYPE VARCHAR(40)
      `);

      await this.dataSource.query(`
        ALTER TABLE app.perfis_usuario
        ALTER COLUMN codigo TYPE VARCHAR(40)
      `);

      await this.dataSource.query(`
        ALTER TABLE app.usuario_perfil_vinculos
        ALTER COLUMN codigo_perfil TYPE VARCHAR(40)
      `);
    } catch (error) {
      this.logger.warn(
        `Nao foi possivel ajustar comprimento das colunas de perfil. message=${error instanceof Error ? error.message : 'Erro desconhecido'}`,
      );
    }
  }

  private async garantirEstrutura() {
    if (this.estruturaInicializada) {
      return;
    }

    if (this.inicializacaoEmAndamento) {
      await this.inicializacaoEmAndamento;
      return;
    }

    this.inicializacaoEmAndamento = (async () => {
      try {
        await this.dataSource.query(`
          CREATE TABLE IF NOT EXISTS app.perfil_permissoes (
            id_perfil_permissao BIGSERIAL PRIMARY KEY,
            id_empresa BIGINT NOT NULL,
            perfil VARCHAR(40) NOT NULL,
            permissoes JSONB NOT NULL DEFAULT '{}'::jsonb,
            usuario_atualizacao TEXT NOT NULL DEFAULT 'SISTEMA',
            criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
          )
        `);

        await this.dataSource.query(`
          CREATE UNIQUE INDEX IF NOT EXISTS ux_perfil_permissoes_empresa_perfil
          ON app.perfil_permissoes (id_empresa, perfil)
        `);

        await this.dataSource.query(`
          CREATE TABLE IF NOT EXISTS app.usuario_permissoes (
            id_usuario_permissao BIGSERIAL PRIMARY KEY,
            id_empresa BIGINT NOT NULL,
            id_usuario BIGINT NOT NULL,
            permissoes JSONB NOT NULL DEFAULT '{}'::jsonb,
            usuario_atualizacao TEXT NOT NULL DEFAULT 'SISTEMA',
            criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
          )
        `);

        await this.dataSource.query(`
          CREATE UNIQUE INDEX IF NOT EXISTS ux_usuario_permissoes_empresa_usuario
          ON app.usuario_permissoes (id_empresa, id_usuario)
        `);

        await this.dataSource.query(`
          CREATE TABLE IF NOT EXISTS app.usuario_perfil_vinculos (
            id_usuario_perfil BIGSERIAL PRIMARY KEY,
            id_empresa BIGINT NOT NULL,
            id_usuario BIGINT NOT NULL,
            codigo_perfil VARCHAR(40) NOT NULL,
            usuario_atualizacao TEXT NOT NULL DEFAULT 'SISTEMA',
            criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
          )
        `);

        await this.dataSource.query(`
          CREATE UNIQUE INDEX IF NOT EXISTS ux_usuario_perfil_vinculos_empresa_usuario
          ON app.usuario_perfil_vinculos (id_empresa, id_usuario)
        `);

        await this.dataSource.query(`
          CREATE INDEX IF NOT EXISTS ix_usuario_perfil_vinculos_empresa_perfil
          ON app.usuario_perfil_vinculos (id_empresa, codigo_perfil)
        `);

        await this.dataSource.query(`
          CREATE TABLE IF NOT EXISTS app.perfis_usuario (
            id_perfil BIGSERIAL PRIMARY KEY,
            id_empresa BIGINT NOT NULL,
            codigo VARCHAR(40) NOT NULL,
            nome VARCHAR(120) NOT NULL,
            descricao TEXT NOT NULL DEFAULT '',
            perfil_base VARCHAR(20) NOT NULL DEFAULT 'OPERADOR',
            ativo BOOLEAN NOT NULL DEFAULT TRUE,
            fixo BOOLEAN NOT NULL DEFAULT FALSE,
            usuario_atualizacao TEXT NOT NULL DEFAULT 'SISTEMA',
            criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
          )
        `);

        await this.dataSource.query(`
          CREATE UNIQUE INDEX IF NOT EXISTS ux_perfis_usuario_empresa_codigo
          ON app.perfis_usuario (id_empresa, codigo)
        `);

        await this.ajustarComprimentoColunasPerfil();
        await this.ajustarRestricaoPerfilUsuarios();
        this.estruturaInicializada = true;
      } catch (error) {
        this.logger.error(
          `Falha ao garantir estrutura de permissoes. message=${error instanceof Error ? error.message : 'Erro desconhecido'}`,
        );
        this.persistenciaPermissoesDisponivel = false;
        this.estruturaInicializada = true;
      } finally {
        this.inicializacaoEmAndamento = null;
      }
    })();

    await this.inicializacaoEmAndamento;
  }

  private exigirPersistenciaDisponivel() {
    if (this.persistenciaPermissoesDisponivel) {
      return;
    }

    throw new BadRequestException(
      'Estrutura de permissoes nao disponivel no banco. Contate o suporte para habilitar app.perfil_permissoes, app.usuario_permissoes, app.usuario_perfil_vinculos e app.perfis_usuario.',
    );
  }
}
