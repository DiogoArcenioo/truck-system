import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  ParseIntPipe,
  Post,
  Put,
  Query,
  Req,
  UnauthorizedException,
  UseGuards,
} from '@nestjs/common';
import { JwtAuthGuard, JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { AtualizarManifestoDto } from './dto/atualizar-manifesto.dto';
import { CriarManifestoDto } from './dto/criar-manifesto.dto';
import { FiltroManifestoDto } from './dto/filtro-manifesto.dto';
import { ManifestoService } from './manifesto.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/manifesto')
@UseGuards(JwtAuthGuard)
export class ManifestoController {
  constructor(private readonly manifestoService: ManifestoService) {}

  @Get()
  async listarTodas(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.manifestoService.listarTodas(usuario.idEmpresa);
  }

  @Get('filtro')
  async listarComFiltro(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroManifestoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.manifestoService.listarComFiltro(usuario.idEmpresa, filtro);
  }

  @Get(':idManifesto')
  async buscarPorId(
    @Req() request: RequisicaoAutenticada,
    @Param('idManifesto', ParseIntPipe) idManifesto: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.manifestoService.buscarPorId(usuario.idEmpresa, idManifesto);
  }

  @Post()
  async cadastrar(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarManifestoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.manifestoService.cadastrar(usuario.idEmpresa, dados, usuario);
  }

  @Put(':idManifesto')
  async atualizar(
    @Req() request: RequisicaoAutenticada,
    @Param('idManifesto', ParseIntPipe) idManifesto: number,
    @Body() dados: AtualizarManifestoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.manifestoService.atualizar(
      usuario.idEmpresa,
      idManifesto,
      dados,
      usuario,
    );
  }

  @Delete(':idManifesto')
  async remover(
    @Req() request: RequisicaoAutenticada,
    @Param('idManifesto', ParseIntPipe) idManifesto: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.manifestoService.remover(usuario.idEmpresa, idManifesto, usuario);
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
