import {
  CanActivate,
  ExecutionContext,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createHmac, timingSafeEqual } from 'crypto';
import {
  ACOES_PERMISSAO,
  MODULOS_SISTEMA,
  PermissoesSistema,
} from '../permissoes.constants';

type JwtHeader = {
  alg?: string;
  typ?: string;
};

type JwtPayloadBruto = {
  sub?: unknown;
  idEmpresa?: unknown;
  codigoEmpresa?: unknown;
  nomeEmpresa?: unknown;
  nomeUsuario?: unknown;
  email?: unknown;
  perfil?: unknown;
  permissoes?: unknown;
  sessao?: unknown;
  licencaModo?: unknown;
  licencaTerminoEm?: unknown;
  licencaDiasTrial?: unknown;
  licencaDiasRestantes?: unknown;
  iss?: unknown;
  iat?: unknown;
  exp?: unknown;
  jti?: unknown;
};

export type JwtUsuarioPayload = {
  sub: number;
  idEmpresa: number;
  codigoEmpresa: string;
  nomeEmpresa?: string;
  nomeUsuario?: string;
  email: string;
  perfil: string;
  permissoes?: PermissoesSistema;
  sessao?: number;
  licencaModo?: string;
  licencaTerminoEm?: string;
  licencaDiasTrial?: number;
  licencaDiasRestantes?: number;
  iss?: string;
  iat?: number;
  exp?: number;
  jti?: string;
};

type RequestComUsuario = {
  headers: Record<string, string | string[] | undefined>;
  usuario?: JwtUsuarioPayload;
};

@Injectable()
export class JwtAuthGuard implements CanActivate {
  constructor(private readonly configService: ConfigService) {}

  canActivate(context: ExecutionContext): boolean {
    const request = context.switchToHttp().getRequest<RequestComUsuario>();
    const token = this.extrairBearerToken(request.headers.authorization);
    request.usuario = this.validarJwt(token);
    return true;
  }

  private extrairBearerToken(
    authorization: string | string[] | undefined,
  ): string {
    const valor =
      typeof authorization === 'string'
        ? authorization
        : Array.isArray(authorization)
          ? authorization[0]
          : '';

    if (!valor.trim()) {
      throw new UnauthorizedException(
        'Header Authorization nao informado no formato Bearer.',
      );
    }

    const [tipo, token] = valor.split(' ');

    if (tipo !== 'Bearer' || !token?.trim()) {
      throw new UnauthorizedException(
        'Header Authorization deve usar o formato Bearer <token>.',
      );
    }

    return token.trim();
  }

  private validarJwt(token: string): JwtUsuarioPayload {
    const jwtSecret = this.configService.get<string>('JWT_SECRET')?.trim();

    if (!jwtSecret) {
      throw new UnauthorizedException(
        'JWT_SECRET nao configurado no servidor.',
      );
    }

    const partes = token.split('.');

    if (partes.length !== 3) {
      throw new UnauthorizedException('Token JWT invalido.');
    }

    const [headerRaw, payloadRaw, signatureRaw] = partes;
    const header = this.parseBase64UrlJson<JwtHeader>(headerRaw);
    const payload = this.parseBase64UrlJson<JwtPayloadBruto>(payloadRaw);

    if (header.alg !== 'HS256') {
      throw new UnauthorizedException('Algoritmo JWT nao suportado.');
    }

    if (header.typ && header.typ.toUpperCase() !== 'JWT') {
      throw new UnauthorizedException('Cabecalho JWT invalido.');
    }

    const assinaturaCalculada = createHmac('sha256', jwtSecret)
      .update(`${headerRaw}.${payloadRaw}`)
      .digest('base64url');

    if (!this.assinaturaValida(signatureRaw, assinaturaCalculada)) {
      throw new UnauthorizedException('Assinatura JWT invalida.');
    }

    const agora = Math.floor(Date.now() / 1000);
    const exp = this.toNumero(payload.exp);
    const iat = this.toNumero(payload.iat);

    if (!exp || exp <= agora) {
      throw new UnauthorizedException('Token expirado.');
    }

    if (iat && iat > agora + 30) {
      throw new UnauthorizedException('Token invalido para o horario atual.');
    }

    const sub = this.validarNumeroPositivo(payload.sub, 'sub');
    const idEmpresa = this.validarNumeroPositivo(
      payload.idEmpresa,
      'idEmpresa',
    );
    const codigoEmpresa = this.validarTexto(
      payload.codigoEmpresa,
      'codigoEmpresa',
    );
    const nomeEmpresa = this.validarTextoOpcional(payload.nomeEmpresa);
    const nomeUsuario = this.validarTextoOpcional(payload.nomeUsuario);
    const email = this.validarTexto(payload.email, 'email');
    const perfil = this.validarTexto(payload.perfil, 'perfil');
    const permissoes = this.validarPermissoesOpcional(payload.permissoes);
    const sessao = this.validarNumeroPositivoOpcional(payload.sessao);
    const licencaDiasTrial = this.validarNumeroPositivoOpcional(
      payload.licencaDiasTrial,
    );
    const licencaDiasRestantes = this.validarNumeroNaoNegativoOpcional(
      payload.licencaDiasRestantes,
    );

    return {
      sub,
      idEmpresa,
      codigoEmpresa,
      nomeEmpresa,
      nomeUsuario,
      email,
      perfil,
      permissoes,
      sessao,
      licencaModo: this.validarTextoOpcional(payload.licencaModo),
      licencaTerminoEm: this.validarTextoOpcional(payload.licencaTerminoEm),
      licencaDiasTrial,
      licencaDiasRestantes,
      iss: typeof payload.iss === 'string' ? payload.iss : undefined,
      iat: iat ?? undefined,
      exp,
      jti: typeof payload.jti === 'string' ? payload.jti : undefined,
    };
  }

  private assinaturaValida(recebida: string, esperada: string): boolean {
    const assinaturaRecebida = Buffer.from(recebida, 'utf8');
    const assinaturaEsperada = Buffer.from(esperada, 'utf8');

    if (assinaturaRecebida.length !== assinaturaEsperada.length) {
      return false;
    }

    return timingSafeEqual(assinaturaRecebida, assinaturaEsperada);
  }

  private parseBase64UrlJson<T>(value: string): T {
    try {
      return JSON.parse(Buffer.from(value, 'base64url').toString('utf8')) as T;
    } catch {
      throw new UnauthorizedException('Token JWT malformado.');
    }
  }

  private validarNumeroPositivo(valor: unknown, campo: string): number {
    const numero = this.toNumero(valor);

    if (!numero || numero <= 0) {
      throw new UnauthorizedException(`Token JWT sem campo ${campo} valido.`);
    }

    return Math.trunc(numero);
  }

  private validarTexto(valor: unknown, campo: string): string {
    if (typeof valor !== 'string' || !valor.trim()) {
      throw new UnauthorizedException(`Token JWT sem campo ${campo} valido.`);
    }

    return valor.trim();
  }

  private validarTextoOpcional(valor: unknown): string | undefined {
    if (typeof valor !== 'string') {
      return undefined;
    }

    const texto = valor.trim();
    return texto.length > 0 ? texto : undefined;
  }

  private validarNumeroPositivoOpcional(valor: unknown): number | undefined {
    const numero = this.toNumero(valor);
    if (!numero || numero <= 0) {
      return undefined;
    }
    return Math.trunc(numero);
  }

  private validarNumeroNaoNegativoOpcional(
    valor: unknown,
  ): number | undefined {
    const numero = this.toNumero(valor);
    if (numero === null || numero < 0) {
      return undefined;
    }
    return Math.trunc(numero);
  }

  private toNumero(valor: unknown): number | null {
    if (typeof valor === 'number' && Number.isFinite(valor)) {
      return valor;
    }

    if (typeof valor === 'string' && valor.trim()) {
      const numero = Number(valor);
      if (Number.isFinite(numero)) {
        return numero;
      }
    }

    return null;
  }

  private validarPermissoesOpcional(
    valor: unknown,
  ): PermissoesSistema | undefined {
    if (valor === undefined || valor === null) {
      return undefined;
    }

    if (typeof valor !== 'object' || Array.isArray(valor)) {
      return undefined;
    }

    const entrada = valor as Record<string, unknown>;
    const resultado = {} as PermissoesSistema;

    for (const modulo of MODULOS_SISTEMA) {
      const moduloValor = entrada[modulo];
      if (
        !moduloValor ||
        typeof moduloValor !== 'object' ||
        Array.isArray(moduloValor)
      ) {
        continue;
      }

      const moduloObj = moduloValor as Record<string, unknown>;
      resultado[modulo] = {
        visualizar: Boolean(moduloObj.visualizar),
        criar: Boolean(moduloObj.criar),
        editar: Boolean(moduloObj.editar),
        excluir: Boolean(moduloObj.excluir),
      };
    }

    for (const modulo of MODULOS_SISTEMA) {
      if (!resultado[modulo]) {
        resultado[modulo] = {
          visualizar: false,
          criar: false,
          editar: false,
          excluir: false,
        };
      } else {
        for (const acao of ACOES_PERMISSAO) {
          resultado[modulo][acao] = Boolean(resultado[modulo][acao]);
        }
      }
    }

    return resultado;
  }
}
