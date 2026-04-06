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
import { AtualizarPneuDto } from './dto/atualizar-pneu.dto';
import { CriarPneuDto } from './dto/criar-pneu.dto';
import { FiltroMovimentacoesPneuDto } from './dto/filtro-movimentacoes-pneu.dto';
import { FiltroPneusDto } from './dto/filtro-pneus.dto';
import { MovimentarPneuDto } from './dto/movimentar-pneu.dto';
import { PneusService } from './pneus.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/pneus')
@UseGuards(JwtAuthGuard)
export class PneusController {
  constructor(private readonly pneusService: PneusService) {}

  @Get()
  async listarTodos(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroPneusDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.pneusService.listarComFiltro(usuario.idEmpresa, filtro);
  }

  @Get('filtro')
  async listarComFiltro(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroPneusDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.pneusService.listarComFiltro(usuario.idEmpresa, filtro);
  }

  @Get('opcoes')
  async listarOpcoes(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.pneusService.listarOpcoes(usuario.idEmpresa);
  }

  @Get('movimentacoes')
  async listarMovimentacoes(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroMovimentacoesPneuDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.pneusService.listarMovimentacoes(usuario.idEmpresa, filtro);
  }

  @Get(':idPneu')
  async buscarPorId(
    @Req() request: RequisicaoAutenticada,
    @Param('idPneu', ParseIntPipe) idPneu: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.pneusService.buscarPorId(usuario.idEmpresa, idPneu);
  }

  @Post()
  async cadastrar(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarPneuDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.pneusService.cadastrar(usuario.idEmpresa, dados, usuario);
  }

  @Put(':idPneu')
  async atualizar(
    @Req() request: RequisicaoAutenticada,
    @Param('idPneu', ParseIntPipe) idPneu: number,
    @Body() dados: AtualizarPneuDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.pneusService.atualizar(usuario.idEmpresa, idPneu, dados, usuario);
  }

  @Delete(':idPneu')
  async remover(
    @Req() request: RequisicaoAutenticada,
    @Param('idPneu', ParseIntPipe) idPneu: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.pneusService.remover(usuario.idEmpresa, idPneu, usuario);
  }

  @Post('movimentacoes')
  async movimentar(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: MovimentarPneuDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.pneusService.movimentar(usuario.idEmpresa, dados, usuario);
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

