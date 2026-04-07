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
import { UsuarioEntity } from '../entities/usuario.entity';
import { JwtUsuarioPayload } from '../guards/jwt-auth.guard';
import { avaliarLicencaEmpresa } from '../licenca.util';
import { MODULOS_SISTEMA_METADATA } from '../permissoes.constants';
import { PermissoesService } from '../permissoes.service';

type RequestComUsuario = {
  method?: string;
  url?: string;
  originalUrl?: string;
  path?: string;
  usuario?: JwtUsuarioPayload;
};

@Injectable()
export class AssinaturaAcessoInterceptor implements NestInterceptor {
  constructor(
    private readonly dataSource: DataSource,
    private readonly permissoesService: PermissoesService,
  ) {}

  async intercept(
    context: ExecutionContext,
    next: CallHandler,
  ): Promise<Observable<unknown>> {
    if (context.getType() !== 'http') {
      return next.handle();
    }

    const request = context.switchToHttp().getRequest<RequestComUsuario>();
    const usuario = request?.usuario;

    if (!usuario) {
      return next.handle();
    }

    const usuarioAtual = await this.dataSource.getRepository(UsuarioEntity).findOne({
      select: {
        idUsuario: true,
        idEmpresa: true,
        perfil: true,
        ativo: true,
        ultimoLoginEm: true,
      },
      where: {
        idUsuario: String(usuario.sub),
        idEmpresa: String(usuario.idEmpresa),
      },
    });

    if (!usuarioAtual || !usuarioAtual.ativo) {
      throw new UnauthorizedException('Usuario nao disponivel para a empresa logada.');
    }

    if (!this.sessaoTokenValida(usuario.sessao, usuarioAtual.ultimoLoginEm)) {
      throw new UnauthorizedException(
        'Sua sessao foi encerrada porque houve um novo login com esta conta.',
      );
    }

    let perfilAtual = 'OPERADOR';
    try {
      perfilAtual = await this.permissoesService.resolverPerfilUsuario(
        usuario.idEmpresa,
        usuario.sub,
        usuarioAtual.perfil ?? usuario.perfil,
      );
    } catch {
      perfilAtual = 'OPERADOR';
    }
    const avaliacaoAcesso = await this.permissoesService.avaliarPermissaoPorRota(
      usuario.idEmpresa,
      usuario.sub,
      perfilAtual,
      request.originalUrl ?? request.url ?? request.path,
      request.method,
    );

    request.usuario = {
      ...usuario,
      perfil: perfilAtual,
      permissoes: avaliacaoAcesso.permissoesEfetivas,
    };

    if (!avaliacaoAcesso.permitido) {
      const acao = this.descreverAcao(avaliacaoAcesso.acao);
      const modulos = this.descreverModulos(avaliacaoAcesso.modulos);
      throw new ForbiddenException(
        `Sem permissao para ${acao} no modulo ${modulos}.`,
      );
    }

    if (this.ehMetodoSomenteLeitura(request.method)) {
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

  private descreverAcao(acao: string | null) {
    if (acao === 'criar') return 'criar';
    if (acao === 'editar') return 'editar';
    if (acao === 'excluir') return 'excluir';
    return 'visualizar';
  }

  private descreverModulos(modulos: string[]) {
    const labels = modulos
      .map((modulo) =>
        MODULOS_SISTEMA_METADATA.find((item) => item.id === modulo)?.label,
      )
      .filter((item): item is string => Boolean(item));

    if (labels.length === 0) {
      return 'solicitado';
    }

    return labels.join(', ');
  }

  private sessaoTokenValida(
    sessaoToken: number | undefined,
    ultimoLoginEm: Date | null,
  ) {
    if (!sessaoToken || !ultimoLoginEm) {
      return true;
    }

    const tokenMs = Math.trunc(sessaoToken);
    const ultimoLoginMs = Math.trunc(ultimoLoginEm.getTime());
    return tokenMs >= ultimoLoginMs;
  }
}
