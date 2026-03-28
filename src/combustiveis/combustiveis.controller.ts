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
import { CombustiveisService } from './combustiveis.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/combustiveis')
@UseGuards(JwtAuthGuard)
export class CombustiveisController {
  constructor(private readonly combustiveisService: CombustiveisService) {}

  @Get()
  async listarTodos(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.combustiveisService.listarTodos(usuario.idEmpresa);
  }

  @Get(':idCombustivel')
  async buscarPorId(
    @Req() request: RequisicaoAutenticada,
    @Param('idCombustivel', ParseIntPipe) idCombustivel: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.combustiveisService.buscarPorId(
      usuario.idEmpresa,
      idCombustivel,
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
