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
import { AtualizarVeiculoDto } from './dto/atualizar-veiculo.dto';
import { CriarVeiculoDto } from './dto/criar-veiculo.dto';
import { FiltroVeiculosDto } from './dto/filtro-veiculos.dto';
import { VeiculoService } from './veiculo.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/veiculo')
@UseGuards(JwtAuthGuard)
export class VeiculoController {
  constructor(private readonly veiculoService: VeiculoService) {}

  @Get()
  async listarTodos(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.veiculoService.listarTodos(usuario.idEmpresa);
  }

  @Get('filtro')
  async listarComFiltro(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroVeiculosDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.veiculoService.listarComFiltro(usuario.idEmpresa, filtro);
  }

  @Get('placas')
  async listarPlacas(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.veiculoService.listarPlacas(usuario.idEmpresa);
  }

  @Get(':idVeiculo')
  async buscarPorId(
    @Req() request: RequisicaoAutenticada,
    @Param('idVeiculo', ParseIntPipe) idVeiculo: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.veiculoService.buscarPorId(usuario.idEmpresa, idVeiculo);
  }

  @Post()
  async cadastrar(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarVeiculoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.veiculoService.cadastrar(usuario.idEmpresa, dados, usuario);
  }

  @Put(':idVeiculo')
  async atualizar(
    @Req() request: RequisicaoAutenticada,
    @Param('idVeiculo', ParseIntPipe) idVeiculo: number,
    @Body() dados: AtualizarVeiculoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.veiculoService.atualizar(
      usuario.idEmpresa,
      idVeiculo,
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
