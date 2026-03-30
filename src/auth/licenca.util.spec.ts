import { avaliarLicencaEmpresa } from './licenca.util';

describe('licenca.util', () => {
  it('deve manter escrita liberada durante trial de 7 dias', () => {
    const agora = new Date('2026-03-30T12:00:00.000Z');
    const resultado = avaliarLicencaEmpresa({
      status: 'trial',
      plano: 'basico',
      criadoEm: '2026-03-30T12:00:00.000Z',
      agora,
    });

    expect(resultado.permiteEscrita).toBe(true);
    expect(resultado.modoAcesso).toBe('trial_ativo');
    expect(resultado.diasRestantesTrial).toBe(7);
  });

  it('deve bloquear escrita quando trial expirar', () => {
    const agora = new Date('2026-04-08T12:00:00.000Z');
    const resultado = avaliarLicencaEmpresa({
      status: 'trial',
      plano: 'basico',
      criadoEm: '2026-03-30T12:00:00.000Z',
      agora,
    });

    expect(resultado.permiteEscrita).toBe(false);
    expect(resultado.modoAcesso).toBe('trial_expirado');
    expect(resultado.diasRestantesTrial).toBe(0);
  });

  it('deve liberar escrita quando assinatura estiver ativa', () => {
    const resultado = avaliarLicencaEmpresa({
      status: 'ativo',
      plano: 'pro',
      criadoEm: '2026-03-30T12:00:00.000Z',
      agora: new Date('2026-04-20T12:00:00.000Z'),
    });

    expect(resultado.permiteEscrita).toBe(true);
    expect(resultado.modoAcesso).toBe('assinatura_ativa');
  });
});
