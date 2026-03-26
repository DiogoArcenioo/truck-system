import {
  CanActivate,
  ExecutionContext,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createHmac, timingSafeEqual } from 'crypto';

type JwtHeader = {
  alg?: string;
  typ?: string;
};

type JwtPayloadBruto = {
  sub?: unknown;
  idEmpresa?: unknown;
  codigoEmpresa?: unknown;
  email?: unknown;
  perfil?: unknown;
  iss?: unknown;
  iat?: unknown;
  exp?: unknown;
  jti?: unknown;
};

export type JwtUsuarioPayload = {
  sub: number;
  idEmpresa: number;
  codigoEmpresa: string;
  email: string;
  perfil: string;
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
    const email = this.validarTexto(payload.email, 'email');
    const perfil = this.validarTexto(payload.perfil, 'perfil');

    return {
      sub,
      idEmpresa,
      codigoEmpresa,
      email,
      perfil,
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
}
