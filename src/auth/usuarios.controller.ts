import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Param,
  ParseIntPipe,
  Post,
  Put,
  Req,
  UnauthorizedException,
  UseGuards,
} from '@nestjs/common';
import { AuthService } from './auth.service';
import { AtualizarUsuarioSistemaDto } from './dto/atualizar-usuario-sistema.dto';
import { CriarUsuarioSistemaDto } from './dto/criar-usuario-sistema.dto';
import { JwtAuthGuard, JwtUsuarioPayload } from './guards/jwt-auth.guard';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/sistema/usuarios')
@UseGuards(JwtAuthGuard)
export class UsuariosController {
  constructor(private readonly authService: AuthService) {}

  @Get()
  async listar(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    this.exigirPerfilAdm(usuario);
    return this.authService.listarUsuariosEmpresa(usuario.idEmpresa);
  }

  @Post()
  async cadastrar(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarUsuarioSistemaDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    this.exigirPerfilAdm(usuario);
    return this.authService.cadastrarUsuarioSistema(
      usuario.idEmpresa,
      dados,
      usuario.email,
    );
  }

  @Put(':idUsuario')
  async atualizar(
    @Req() request: RequisicaoAutenticada,
    @Param('idUsuario', ParseIntPipe) idUsuario: number,
    @Body() dados: AtualizarUsuarioSistemaDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    this.exigirPerfilAdm(usuario);
    return this.authService.atualizarUsuarioSistema(
      usuario.idEmpresa,
      idUsuario,
      dados,
      usuario.email,
      usuario.sub,
    );
  }

  private obterUsuarioAutenticado(
    request: RequisicaoAutenticada,
  ): JwtUsuarioPayload {
    if (!request.usuario) {
      throw new UnauthorizedException('Usuario nao autenticado.');
    }

    return request.usuario;
  }

  private exigirPerfilAdm(usuario: JwtUsuarioPayload) {
    if (usuario.perfil.trim().toUpperCase() !== 'ADM') {
      throw new BadRequestException(
        'Apenas usuarios com perfil ADM podem gerenciar usuarios do sistema.',
      );
    }
  }
}
