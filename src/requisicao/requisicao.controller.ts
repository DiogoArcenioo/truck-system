import {
  Body,
  Controller,
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
import { AtualizarRequisicaoDto } from './dto/atualizar-requisicao.dto';
import { CriarRequisicaoDto } from './dto/criar-requisicao.dto';
import { FiltroRequisicaoDto } from './dto/filtro-requisicao.dto';
import { RequisicaoService } from './requisicao.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/requisicao')
@UseGuards(JwtAuthGuard)
export class RequisicaoController {
  constructor(private readonly requisicaoService: RequisicaoService) {}

  @Get('opcoes')
  async listarOpcoes() {
    return this.requisicaoService.listarOpcoes();
  }

  @Get()
  async listarTodos(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.requisicaoService.listarTodos(usuario.idEmpresa);
  }

  @Get('filtro')
  async listarComFiltro(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroRequisicaoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.requisicaoService.listarComFiltro(usuario.idEmpresa, filtro);
  }

  @Get(':idRequisicao')
  async buscarPorId(
    @Req() request: RequisicaoAutenticada,
    @Param('idRequisicao', ParseIntPipe) idRequisicao: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.requisicaoService.buscarPorId(usuario.idEmpresa, idRequisicao);
  }

  @Post()
  async cadastrar(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarRequisicaoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.requisicaoService.cadastrar(usuario.idEmpresa, dados, usuario);
  }

  @Put(':idRequisicao')
  async atualizar(
    @Req() request: RequisicaoAutenticada,
    @Param('idRequisicao', ParseIntPipe) idRequisicao: number,
    @Body() dados: AtualizarRequisicaoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.requisicaoService.atualizar(
      usuario.idEmpresa,
      idRequisicao,
      dados,
      usuario,
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
