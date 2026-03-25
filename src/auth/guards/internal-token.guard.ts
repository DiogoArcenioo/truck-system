import {
  CanActivate,
  ExecutionContext,
  ForbiddenException,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { timingSafeEqual } from 'crypto';

@Injectable()
export class InternalTokenGuard implements CanActivate {
  constructor(private readonly configService: ConfigService) {}

  canActivate(context: ExecutionContext): boolean {
    const tokenEsperado = this.configService.get<string>('INTERNAL_API_TOKEN');
    const request = context.switchToHttp().getRequest<{
      headers: Record<string, string | string[] | undefined>;
    }>();

    if (!tokenEsperado?.trim()) {
      throw new UnauthorizedException(
        'Acesso bloqueado. Configure INTERNAL_API_TOKEN no servidor.',
      );
    }

    const valorHeader = request.headers['x-internal-token'];
    const tokenRecebido = Array.isArray(valorHeader)
      ? valorHeader[0]
      : valorHeader;

    if (!tokenRecebido?.trim()) {
      throw new UnauthorizedException('Header x-internal-token nao informado.');
    }

    if (!this.tokenValido(tokenRecebido.trim(), tokenEsperado.trim())) {
      throw new ForbiddenException('Token interno invalido.');
    }

    return true;
  }

  private tokenValido(tokenRecebido: string, tokenEsperado: string): boolean {
    const recebido = Buffer.from(tokenRecebido, 'utf8');
    const esperado = Buffer.from(tokenEsperado, 'utf8');

    if (recebido.length !== esperado.length) {
      return false;
    }

    return timingSafeEqual(recebido, esperado);
  }
}
