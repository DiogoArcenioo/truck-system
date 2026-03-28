import {
  Controller,
  Get,
  Param,
  ParseIntPipe,
  Req,
  UnauthorizedException,
  UseGuards,
} from '@nestjs/common';
import { JwtAuthGuard, JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { MarcaVeiculoService } from './marca-veiculo.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/marca-veiculo')
@UseGuards(JwtAuthGuard)
export class MarcaVeiculoController {
  constructor(private readonly marcaVeiculoService: MarcaVeiculoService) {}

  @Get()
  async listarTodos(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.marcaVeiculoService.listarTodos(usuario.idEmpresa);
  }

  @Get(':idMarca')
  async buscarPorId(
    @Req() request: RequisicaoAutenticada,
    @Param('idMarca', ParseIntPipe) idMarca: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.marcaVeiculoService.buscarPorId(usuario.idEmpresa, idMarca);
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
