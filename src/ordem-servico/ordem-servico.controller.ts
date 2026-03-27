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
import { AtualizarOrdemServicoDto } from './dto/atualizar-ordem-servico.dto';
import { CriarOrdemServicoDto } from './dto/criar-ordem-servico.dto';
import { FiltroOrdemServicoDto } from './dto/filtro-ordem-servico.dto';
import { OrdemServicoService } from './ordem-servico.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/ordem-servico')
@UseGuards(JwtAuthGuard)
export class OrdemServicoController {
  constructor(private readonly ordemServicoService: OrdemServicoService) {}

  @Get('opcoes')
  async listarOpcoes() {
    return this.ordemServicoService.listarOpcoes();
  }

  @Get()
  async listarTodos(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.ordemServicoService.listarTodos(usuario.idEmpresa);
  }

  @Get('filtro')
  async listarComFiltro(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroOrdemServicoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.ordemServicoService.listarComFiltro(usuario.idEmpresa, filtro);
  }

  @Get(':idOs')
  async buscarPorId(
    @Req() request: RequisicaoAutenticada,
    @Param('idOs', ParseIntPipe) idOs: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.ordemServicoService.buscarPorId(usuario.idEmpresa, idOs);
  }

  @Post()
  async cadastrar(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarOrdemServicoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.ordemServicoService.cadastrar(usuario.idEmpresa, dados, usuario);
  }

  @Put(':idOs')
  async atualizar(
    @Req() request: RequisicaoAutenticada,
    @Param('idOs', ParseIntPipe) idOs: number,
    @Body() dados: AtualizarOrdemServicoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.ordemServicoService.atualizar(usuario.idEmpresa, idOs, dados, usuario);
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
