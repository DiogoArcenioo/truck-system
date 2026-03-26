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
import { AtualizarViagemDto } from './dto/atualizar-viagem.dto';
import { CriarViagemDto } from './dto/criar-viagem.dto';
import { FiltroViagensDto } from './dto/filtro-viagens.dto';
import { ViagensService } from './viagens.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/viagens')
@UseGuards(JwtAuthGuard)
export class ViagensController {
  constructor(private readonly viagensService: ViagensService) {}

  @Get()
  async listarTodas(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.viagensService.listarTodas(usuario.idEmpresa);
  }

  @Get('filtro')
  async listarComFiltro(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroViagensDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.viagensService.listarComFiltro(usuario.idEmpresa, filtro);
  }

  @Get(':idViagem')
  async buscarPorId(
    @Req() request: RequisicaoAutenticada,
    @Param('idViagem', ParseIntPipe) idViagem: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.viagensService.buscarPorId(usuario.idEmpresa, idViagem);
  }

  @Post()
  async cadastrar(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarViagemDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.viagensService.cadastrar(usuario.idEmpresa, dados, usuario);
  }

  @Put(':idViagem')
  async atualizar(
    @Req() request: RequisicaoAutenticada,
    @Param('idViagem', ParseIntPipe) idViagem: number,
    @Body() dados: AtualizarViagemDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.viagensService.atualizar(
      usuario.idEmpresa,
      idViagem,
      dados,
      usuario,
    );
  }

  @Delete(':idViagem')
  async remover(
    @Req() request: RequisicaoAutenticada,
    @Param('idViagem', ParseIntPipe) idViagem: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.viagensService.remover(usuario.idEmpresa, idViagem);
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
