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
import { AtualizarCteDto } from './dto/atualizar-cte.dto';
import { CriarCteDto } from './dto/criar-cte.dto';
import { FiltroCteDto } from './dto/filtro-cte.dto';
import { CteService } from './cte.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/cte')
@UseGuards(JwtAuthGuard)
export class CteController {
  constructor(private readonly cteService: CteService) {}

  @Get()
  async listarTodas(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.cteService.listarTodas(usuario.idEmpresa);
  }

  @Get('filtro')
  async listarComFiltro(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroCteDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.cteService.listarComFiltro(usuario.idEmpresa, filtro);
  }

  @Get(':idCte')
  async buscarPorId(
    @Req() request: RequisicaoAutenticada,
    @Param('idCte', ParseIntPipe) idCte: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.cteService.buscarPorId(usuario.idEmpresa, idCte);
  }

  @Post()
  async cadastrar(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarCteDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.cteService.cadastrar(usuario.idEmpresa, dados, usuario);
  }

  @Put(':idCte')
  async atualizar(
    @Req() request: RequisicaoAutenticada,
    @Param('idCte', ParseIntPipe) idCte: number,
    @Body() dados: AtualizarCteDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.cteService.atualizar(usuario.idEmpresa, idCte, dados, usuario);
  }

  @Delete(':idCte')
  async remover(
    @Req() request: RequisicaoAutenticada,
    @Param('idCte', ParseIntPipe) idCte: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.cteService.remover(usuario.idEmpresa, idCte, usuario);
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
