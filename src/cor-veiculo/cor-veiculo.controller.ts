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
import { CorVeiculoService } from './cor-veiculo.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/cor-veiculo')
@UseGuards(JwtAuthGuard)
export class CorVeiculoController {
  constructor(private readonly corVeiculoService: CorVeiculoService) {}

  @Get()
  async listarTodos(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.corVeiculoService.listarTodos(usuario.idEmpresa);
  }

  @Get(':idCor')
  async buscarPorId(
    @Req() request: RequisicaoAutenticada,
    @Param('idCor', ParseIntPipe) idCor: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.corVeiculoService.buscarPorId(usuario.idEmpresa, idCor);
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
