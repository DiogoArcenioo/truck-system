import { BadRequestException, Injectable, Logger } from '@nestjs/common';
import { DataSource } from 'typeorm';
import {
  ACOES_PERMISSAO,
  AcaoPermissao,
  MODULOS_SISTEMA,
  MODULOS_SISTEMA_METADATA,
  ModuloSistema,
  PERFIS_USUARIO,
  PERMISSOES_PADRAO_PERFIL,
  PerfilUsuario,
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
    prefixos: ['/api/motoristas', '/api/sistema/motoristas'],
    modulos: ['motoristas'],
  },
  {
    prefixos: ['/api/fornecedor', '/api/sistema/fornecedor'],
    modulos: ['fornecedores'],
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

  normalizarPerfil(perfil: string | undefined): PerfilUsuario {
    const valor = (perfil ?? 'OPERADOR').trim().toUpperCase();
    if (!PERFIS_USUARIO.includes(valor as PerfilUsuario)) {
      throw new BadRequestException(
        'Perfil invalido. Valores permitidos: ADM, GESTOR, OPERADOR.',
      );
    }

    return valor as PerfilUsuario;
  }

  async listarPermissoesEmpresa(idEmpresa: number) {
    await this.garantirEstrutura();
    const [perfis, overrides] = await Promise.all([
      this.obterPermissoesPerfisEmpresa(idEmpresa),
      this.obterOverridesUsuariosEmpresa(idEmpresa),
    ]);

    return {
      modulos: MODULOS_SISTEMA_METADATA,
      perfis,
      overridesUsuarios: overrides,
    };
  }

  async obterPermissoesPerfisEmpresa(idEmpresa: number) {
    await this.garantirEstrutura();
    if (!this.persistenciaPermissoesDisponivel) {
      return {
        ADM: this.clonarPermissoes(PERMISSOES_PADRAO_PERFIL.ADM),
        GESTOR: this.clonarPermissoes(PERMISSOES_PADRAO_PERFIL.GESTOR),
        OPERADOR: this.clonarPermissoes(PERMISSOES_PADRAO_PERFIL.OPERADOR),
      };
    }

    const linhas = await this.dataSource.query(
      `
      SELECT perfil, permissoes
      FROM app.perfil_permissoes
      WHERE id_empresa = $1
      `,
      [idEmpresa],
    );

    const porPerfil = new Map<PerfilUsuario, PermissoesParciaisSistema>();

    for (const linha of linhas as PerfilPermissoesRow[]) {
      const perfil = this.normalizarPerfil(this.lerTexto(linha.perfil));
      const permissoesParciais = this.normalizarPermissoesParciais(
        this.parseJsonTalvez(linha.permissoes),
      );
      porPerfil.set(perfil, permissoesParciais);
    }

    const resposta = {} as Record<PerfilUsuario, PermissoesSistema>;
    for (const perfil of PERFIS_USUARIO) {
      const base = this.clonarPermissoes(PERMISSOES_PADRAO_PERFIL[perfil]);
      if (perfil === 'ADM') {
        resposta[perfil] = base;
      } else {
        resposta[perfil] = this.aplicarOverrides(base, porPerfil.get(perfil) ?? {});
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

    const basePerfil = this.clonarPermissoes(permissoesPerfis[perfilNormalizado]);
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

    const perfilNormalizado = this.normalizarPerfil(perfil);
    if (perfilNormalizado === 'ADM') {
      throw new BadRequestException(
        'Perfil ADM possui acesso total fixo e nao pode ser restringido.',
      );
    }

    const base = this.clonarPermissoes(PERMISSOES_PADRAO_PERFIL[perfilNormalizado]);
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
            perfil VARCHAR(20) NOT NULL,
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
      'Estrutura de permissoes nao disponivel no banco. Contate o suporte para habilitar app.perfil_permissoes e app.usuario_permissoes.',
    );
  }
}
