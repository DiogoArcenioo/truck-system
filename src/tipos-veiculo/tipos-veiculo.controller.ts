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
import { TiposVeiculoService } from './tipos-veiculo.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/tipos-veiculo')
@UseGuards(JwtAuthGuard)
export class TiposVeiculoController {
  constructor(private readonly tiposVeiculoService: TiposVeiculoService) {}

  @Get()
  async listarTodos(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.tiposVeiculoService.listarTodos(usuario.idEmpresa);
  }

  @Get(':idTipo')
  async buscarPorId(
    @Req() request: RequisicaoAutenticada,
    @Param('idTipo', ParseIntPipe) idTipo: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.tiposVeiculoService.buscarPorId(usuario.idEmpresa, idTipo);
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
