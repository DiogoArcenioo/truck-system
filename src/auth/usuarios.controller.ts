import {
  Body,
  Controller,
  Delete,
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
import { AtualizarPermissoesDto } from './dto/atualizar-permissoes.dto';
import { AtualizarPerfilSistemaDto } from './dto/atualizar-perfil-sistema.dto';
import { AtualizarUsuarioSistemaDto } from './dto/atualizar-usuario-sistema.dto';
import { CriarPerfilSistemaDto } from './dto/criar-perfil-sistema.dto';
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
    return this.authService.listarUsuariosEmpresa(usuario.idEmpresa);
  }

  @Post()
  async cadastrar(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarUsuarioSistemaDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.authService.cadastrarUsuarioSistema(
      usuario.idEmpresa,
      dados,
      usuario.email,
    );
  }

  @Get('perfis')
  async listarPerfis(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.authService.listarPerfisSistema(usuario.idEmpresa);
  }

  @Post('perfis')
  async cadastrarPerfil(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarPerfilSistemaDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.authService.criarPerfilSistema(
      usuario.idEmpresa,
      dados,
      dados.usuarioAtualizacao ?? usuario.email,
    );
  }

  @Put('perfis/:perfil')
  async atualizarPerfil(
    @Req() request: RequisicaoAutenticada,
    @Param('perfil') perfil: string,
    @Body() dados: AtualizarPerfilSistemaDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);

    return this.authService.atualizarPerfilSistema(
      usuario.idEmpresa,
      perfil,
      dados,
      dados.usuarioAtualizacao ?? usuario.email,
    );
  }

  @Put('perfis/:perfil/permissoes')
  async atualizarPermissoesPerfil(
    @Req() request: RequisicaoAutenticada,
    @Param('perfil') perfil: string,
    @Body() dados: AtualizarPermissoesDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);

    return this.authService.atualizarPermissoesPerfilSistema(
      usuario.idEmpresa,
      perfil,
      dados.permissoes,
      dados.usuarioAtualizacao ?? usuario.email,
    );
  }

  @Put(':idUsuario/permissoes')
  async atualizarPermissoesUsuario(
    @Req() request: RequisicaoAutenticada,
    @Param('idUsuario', ParseIntPipe) idUsuario: number,
    @Body() dados: AtualizarPermissoesDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);

    return this.authService.atualizarPermissoesUsuarioSistema(
      usuario.idEmpresa,
      idUsuario,
      dados.permissoes,
      dados.usuarioAtualizacao ?? usuario.email,
    );
  }

  @Delete(':idUsuario/permissoes')
  async limparPermissoesUsuario(
    @Req() request: RequisicaoAutenticada,
    @Param('idUsuario', ParseIntPipe) idUsuario: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);

    return this.authService.limparPermissoesUsuarioSistema(
      usuario.idEmpresa,
      idUsuario,
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
}
