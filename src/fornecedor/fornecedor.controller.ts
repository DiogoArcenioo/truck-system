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
import { FornecedorService } from './fornecedor.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/fornecedor')
@UseGuards(JwtAuthGuard)
export class FornecedorController {
  constructor(private readonly fornecedorService: FornecedorService) {}

  @Get()
  async listarTodos(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.fornecedorService.listarTodos(usuario.idEmpresa);
  }

  @Get(':idFornecedor')
  async buscarPorId(
    @Req() request: RequisicaoAutenticada,
    @Param('idFornecedor', ParseIntPipe) idFornecedor: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.fornecedorService.buscarPorId(usuario.idEmpresa, idFornecedor);
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
