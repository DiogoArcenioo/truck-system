import {
  CallHandler,
  ExecutionContext,
  ForbiddenException,
  Injectable,
  NestInterceptor,
  UnauthorizedException,
} from '@nestjs/common';
import { Observable } from 'rxjs';
import { DataSource } from 'typeorm';
import { EmpresaEntity } from '../entities/empresa.entity';
import { JwtUsuarioPayload } from '../guards/jwt-auth.guard';
import { avaliarLicencaEmpresa } from '../licenca.util';

type RequestComUsuario = {
  method?: string;
  usuario?: JwtUsuarioPayload;
};

@Injectable()
export class AssinaturaAcessoInterceptor implements NestInterceptor {
  constructor(private readonly dataSource: DataSource) {}

  async intercept(
    context: ExecutionContext,
    next: CallHandler,
  ): Promise<Observable<unknown>> {
    if (context.getType() !== 'http') {
      return next.handle();
    }

    const request = context.switchToHttp().getRequest<RequestComUsuario>();
    const usuario = request?.usuario;

    if (!usuario || this.ehMetodoSomenteLeitura(request.method)) {
      return next.handle();
    }

    const empresa = await this.dataSource.getRepository(EmpresaEntity).findOne({
      select: {
        idEmpresa: true,
        ativo: true,
        status: true,
        plano: true,
        criadoEm: true,
      },
      where: { idEmpresa: String(usuario.idEmpresa) },
    });

    if (!empresa || !empresa.ativo) {
      throw new UnauthorizedException('Empresa vinculada nao disponivel.');
    }

    const licenca = avaliarLicencaEmpresa({
      status: empresa.status,
      plano: empresa.plano,
      criadoEm: empresa.criadoEm,
    });

    if (!licenca.permiteEscrita) {
      throw new ForbiddenException(licenca.mensagem);
    }

    return next.handle();
  }

  private ehMetodoSomenteLeitura(method: string | undefined) {
    if (!method) {
      return false;
    }

    const metodo = method.trim().toUpperCase();
    return metodo === 'GET' || metodo === 'HEAD' || metodo === 'OPTIONS';
  }
}
