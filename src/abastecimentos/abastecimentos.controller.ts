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
import { AbastecimentosService } from './abastecimentos.service';
import { AtualizarAbastecimentoDto } from './dto/atualizar-abastecimento.dto';
import { CriarAbastecimentoDto } from './dto/criar-abastecimento.dto';
import { FiltroAbastecimentosDto } from './dto/filtro-abastecimentos.dto';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/abastecimentos')
@UseGuards(JwtAuthGuard)
export class AbastecimentosController {
  constructor(private readonly abastecimentosService: AbastecimentosService) {}

  @Get()
  async listarTodos(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.abastecimentosService.listarTodos(usuario.idEmpresa);
  }

  @Get('filtro')
  async listarComFiltro(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroAbastecimentosDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.abastecimentosService.listarComFiltro(usuario.idEmpresa, filtro);
  }

  @Get(':idAbastecimento')
  async buscarPorId(
    @Req() request: RequisicaoAutenticada,
    @Param('idAbastecimento', ParseIntPipe) idAbastecimento: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.abastecimentosService.buscarPorId(
      usuario.idEmpresa,
      idAbastecimento,
    );
  }

  @Post()
  async cadastrar(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarAbastecimentoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.abastecimentosService.cadastrar(usuario.idEmpresa, dados, usuario);
  }

  @Put(':idAbastecimento')
  async atualizar(
    @Req() request: RequisicaoAutenticada,
    @Param('idAbastecimento', ParseIntPipe) idAbastecimento: number,
    @Body() dados: AtualizarAbastecimentoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.abastecimentosService.atualizar(
      usuario.idEmpresa,
      idAbastecimento,
      dados,
      usuario,
    );
  }

  @Delete(':idAbastecimento')
  async remover(
    @Req() request: RequisicaoAutenticada,
    @Param('idAbastecimento', ParseIntPipe) idAbastecimento: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.abastecimentosService.remover(usuario.idEmpresa, idAbastecimento);
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
