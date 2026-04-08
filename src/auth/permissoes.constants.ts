export const PERFIS_BASE = ['ADM', 'GESTOR', 'OPERADOR'] as const;

export type PerfilBase = (typeof PERFIS_BASE)[number];

export const PERFIL_BASE_PADRAO: PerfilBase = 'OPERADOR';

// Mantido por compatibilidade com imports existentes.
export const PERFIS_USUARIO = PERFIS_BASE;

export type PerfilUsuario = string;

export const PERFIL_CODIGO_REGEX = /^[A-Z0-9_-]{2,40}$/;

export const ACOES_PERMISSAO = [
  'visualizar',
  'criar',
  'editar',
  'excluir',
] as const;

export type AcaoPermissao = (typeof ACOES_PERMISSAO)[number];

export const MODULOS_SISTEMA = [
  'home',
  'requisicao',
  'ordem_servico',
  'veiculos',
  'pneus',
  'engate_desengate',
  'viagens',
  'abastecimentos',
  'despesas',
  'multas',
  'motoristas',
  'produtos',
  'fornecedores',
  'cancelamentos',
  'relatorios',
  'usuarios',
] as const;

export type ModuloSistema = (typeof MODULOS_SISTEMA)[number];

export type PermissaoCrud = Record<AcaoPermissao, boolean>;

export type PermissoesSistema = Record<ModuloSistema, PermissaoCrud>;

export type PermissoesParciaisSistema = Partial<
  Record<ModuloSistema, Partial<Record<AcaoPermissao, boolean>>>
>;

type ModuloMetadata = {
  id: ModuloSistema;
  label: string;
  descricao: string;
};

const moduloMetadata: Record<ModuloSistema, Omit<ModuloMetadata, 'id'>> = {
  home: {
    label: 'Home',
    descricao: 'Dashboard principal e indicadores gerais.',
  },
  requisicao: {
    label: 'Requisicao',
    descricao: 'Gestao de requisicoes de produtos e materiais.',
  },
  ordem_servico: {
    label: 'Ordem de servico',
    descricao: 'Cadastro e acompanhamento de OS.',
  },
  veiculos: {
    label: 'Cadastro de veiculos',
    descricao: 'Cadastros de veiculos e catalogos de veiculo.',
  },
  pneus: {
    label: 'Pneus',
    descricao: 'Cadastro, vínculo e movimentação de pneus da frota.',
  },
  engate_desengate: {
    label: 'Engate e desengate',
    descricao: 'Movimentacoes de engate e desengate da frota.',
  },
  viagens: {
    label: 'Viagens',
    descricao: 'Operacao e acompanhamento de viagens.',
  },
  abastecimentos: {
    label: 'Abastecimentos',
    descricao: 'Controle de abastecimentos da frota.',
  },
  despesas: {
    label: 'Despesas',
    descricao: 'Lancamentos de pedagio, multa, alimentacao e custos da viagem.',
  },
  multas: {
    label: 'Multas',
    descricao: 'Lancamento e acompanhamento de multas da frota.',
  },
  motoristas: {
    label: 'Motoristas',
    descricao: 'Cadastro e manutencao de motoristas.',
  },
  produtos: {
    label: 'Cadastro produto',
    descricao: 'Produtos e catalogos de itens.',
  },
  fornecedores: {
    label: 'Fornecedores',
    descricao: 'Cadastro, contatos e enderecos de fornecedores.',
  },
  cancelamentos: {
    label: 'Reabertura',
    descricao:
      'Reabertura controlada de documentos finalizados com trilha de auditoria.',
  },
  relatorios: {
    label: 'Relatorios',
    descricao: 'Painel analitico e consolidacao de dados.',
  },
  usuarios: {
    label: 'Usuarios',
    descricao: 'Gestao de usuarios e permissao de acesso.',
  },
};

function crud(
  visualizar: boolean,
  criar: boolean,
  editar: boolean,
  excluir: boolean,
): PermissaoCrud {
  return {
    visualizar,
    criar,
    editar,
    excluir,
  };
}

function criarPermissoesComBase(
  base: Partial<Record<ModuloSistema, PermissaoCrud>>,
): PermissoesSistema {
  const resultado = {} as PermissoesSistema;

  for (const modulo of MODULOS_SISTEMA) {
    const valor = base[modulo] ?? crud(false, false, false, false);
    resultado[modulo] = {
      visualizar: Boolean(valor.visualizar),
      criar: Boolean(valor.criar),
      editar: Boolean(valor.editar),
      excluir: Boolean(valor.excluir),
    };
  }

  return resultado;
}

const acessoTotal = crud(true, true, true, true);
const semAcesso = crud(false, false, false, false);

export const PERMISSOES_PADRAO_PERFIL: Record<PerfilBase, PermissoesSistema> =
  {
    ADM: criarPermissoesComBase({
      home: acessoTotal,
      requisicao: acessoTotal,
      ordem_servico: acessoTotal,
      veiculos: acessoTotal,
      pneus: acessoTotal,
      engate_desengate: acessoTotal,
      viagens: acessoTotal,
      abastecimentos: acessoTotal,
      despesas: acessoTotal,
      multas: acessoTotal,
      motoristas: acessoTotal,
      produtos: acessoTotal,
      fornecedores: acessoTotal,
      cancelamentos: acessoTotal,
      relatorios: acessoTotal,
      usuarios: acessoTotal,
    }),
    GESTOR: criarPermissoesComBase({
      home: acessoTotal,
      requisicao: acessoTotal,
      ordem_servico: acessoTotal,
      veiculos: acessoTotal,
      pneus: acessoTotal,
      engate_desengate: acessoTotal,
      viagens: acessoTotal,
      abastecimentos: acessoTotal,
      despesas: acessoTotal,
      multas: acessoTotal,
      motoristas: acessoTotal,
      produtos: acessoTotal,
      fornecedores: acessoTotal,
      cancelamentos: acessoTotal,
      relatorios: acessoTotal,
      usuarios: semAcesso,
    }),
    OPERADOR: criarPermissoesComBase({
      home: acessoTotal,
      requisicao: acessoTotal,
      ordem_servico: acessoTotal,
      veiculos: acessoTotal,
      pneus: acessoTotal,
      engate_desengate: acessoTotal,
      viagens: acessoTotal,
      abastecimentos: acessoTotal,
      despesas: acessoTotal,
      multas: acessoTotal,
      motoristas: acessoTotal,
      produtos: acessoTotal,
      fornecedores: acessoTotal,
      cancelamentos: acessoTotal,
      relatorios: semAcesso,
      usuarios: semAcesso,
    }),
  };

export const MODULOS_SISTEMA_METADATA: ModuloMetadata[] = MODULOS_SISTEMA.map(
  (modulo) => ({
    id: modulo,
    label: moduloMetadata[modulo].label,
    descricao: moduloMetadata[modulo].descricao,
  }),
);
