import {
  Controller,
  Get,
  Param,
  ParseIntPipe,
  Query,
  Req,
  UnauthorizedException,
  UseGuards,
} from '@nestjs/common';
import { JwtAuthGuard, JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { FiltroModeloVeiculoDto } from './dto/filtro-modelo-veiculo.dto';
import { ModeloVeiculoService } from './modelo-veiculo.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/modelo-veiculo')
@UseGuards(JwtAuthGuard)
export class ModeloVeiculoController {
  constructor(private readonly modeloVeiculoService: ModeloVeiculoService) {}

  @Get()
  async listarTodos(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroModeloVeiculoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.modeloVeiculoService.listarTodos(
      usuario.idEmpresa,
      filtro.idMarca,
    );
  }

  @Get(':idModelo')
  async buscarPorId(
    @Req() request: RequisicaoAutenticada,
    @Param('idModelo', ParseIntPipe) idModelo: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.modeloVeiculoService.buscarPorId(usuario.idEmpresa, idModelo);
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
