const DIA_EM_MS = 24 * 60 * 60 * 1000;
export const TRIAL_DIAS_PADRAO = 7;

export type ModoLicenca =
  | 'trial_ativo'
  | 'trial_expirado'
  | 'assinatura_ativa'
  | 'somente_leitura';

export type ResumoLicenca = {
  statusEmpresa: string;
  planoEmpresa: string;
  trialInicioEm: string | null;
  trialTerminoEm: string | null;
  diasTrial: number;
  diasRestantesTrial: number;
  trialExpirado: boolean;
  permiteEscrita: boolean;
  modoAcesso: ModoLicenca;
  mensagem: string;
};

type AvaliarLicencaInput = {
  status?: string | null;
  plano?: string | null;
  criadoEm?: Date | string | null;
  agora?: Date;
  diasTrial?: number;
};

export function avaliarLicencaEmpresa(
  input: AvaliarLicencaInput,
): ResumoLicenca {
  const diasTrial = sanitizarDiasTrial(input.diasTrial);
  const agora = input.agora ?? new Date();
  const statusEmpresa = normalizar(input.status, 'ativo');
  const planoEmpresa = normalizar(input.plano, 'basico');
  const trialInicio = parseData(input.criadoEm);
  const trialTermino = trialInicio
    ? new Date(trialInicio.getTime() + diasTrial * DIA_EM_MS)
    : null;

  const trialExpirado =
    statusEmpresa === 'trial' &&
    trialTermino !== null &&
    agora.getTime() >= trialTermino.getTime();
  const diasRestantesTrial =
    statusEmpresa === 'trial' && trialTermino !== null
      ? Math.max(0, Math.ceil((trialTermino.getTime() - agora.getTime()) / DIA_EM_MS))
      : 0;

  const statusSomenteLeitura = ['inativo', 'bloqueado', 'cancelado'].includes(
    statusEmpresa,
  );
  const assinaturaAtiva = statusEmpresa === 'ativo';
  const trialAtivo = statusEmpresa === 'trial' && !trialExpirado;
  const permiteEscrita =
    assinaturaAtiva || trialAtivo || (!statusSomenteLeitura && statusEmpresa !== 'trial');

  const modoAcesso = calcularModoAcesso(
    assinaturaAtiva,
    trialAtivo,
    trialExpirado,
    permiteEscrita,
  );

  return {
    statusEmpresa,
    planoEmpresa,
    trialInicioEm: trialInicio?.toISOString() ?? null,
    trialTerminoEm: trialTermino?.toISOString() ?? null,
    diasTrial,
    diasRestantesTrial,
    trialExpirado,
    permiteEscrita,
    modoAcesso,
    mensagem: mensagemPorModo({
      modoAcesso,
      diasTrial,
      diasRestantesTrial,
      trialTermino,
    }),
  };
}

function normalizar(valor: string | null | undefined, fallback: string) {
  if (!valor || !valor.trim()) {
    return fallback;
  }
  return valor.trim().toLowerCase();
}

function parseData(data: Date | string | null | undefined): Date | null {
  if (!data) {
    return null;
  }

  if (data instanceof Date && !Number.isNaN(data.getTime())) {
    return data;
  }

  const parsed = new Date(data);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed;
}

function sanitizarDiasTrial(valor: number | undefined) {
  if (!valor || !Number.isFinite(valor) || valor <= 0) {
    return TRIAL_DIAS_PADRAO;
  }
  return Math.trunc(valor);
}

function calcularModoAcesso(
  assinaturaAtiva: boolean,
  trialAtivo: boolean,
  trialExpirado: boolean,
  permiteEscrita: boolean,
): ModoLicenca {
  if (assinaturaAtiva) {
    return 'assinatura_ativa';
  }

  if (trialAtivo) {
    return 'trial_ativo';
  }

  if (trialExpirado) {
    return 'trial_expirado';
  }

  if (!permiteEscrita) {
    return 'somente_leitura';
  }

  return 'assinatura_ativa';
}

function mensagemPorModo({
  modoAcesso,
  diasTrial,
  diasRestantesTrial,
  trialTermino,
}: {
  modoAcesso: ModoLicenca;
  diasTrial: number;
  diasRestantesTrial: number;
  trialTermino: Date | null;
}) {
  if (modoAcesso === 'trial_ativo') {
    const dataFim = trialTermino
      ? new Intl.DateTimeFormat('pt-BR').format(trialTermino)
      : null;

    const diasTexto =
      diasRestantesTrial === 1
        ? 'Resta 1 dia'
        : `Restam ${diasRestantesTrial} dias`;

    return dataFim
      ? `${diasTexto} do seu período de teste (${diasTrial} dias). Encerramento em ${dataFim}.`
      : `${diasTexto} do seu período de teste (${diasTrial} dias).`;
  }

  if (modoAcesso === 'trial_expirado') {
    return `Seu período de teste (${diasTrial} dias) terminou. Você pode visualizar os dados, mas não pode cadastrar, editar ou excluir enquanto não ativar a assinatura.`;
  }

  if (modoAcesso === 'somente_leitura') {
    return 'Seu acesso está em modo somente leitura. Visualização liberada, operações de escrita bloqueadas.';
  }

  return 'Assinatura ativa. Operacoes de cadastro, edicao e exclusao estao liberadas.';
}
