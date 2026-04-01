import {
  Body,
  Controller,
  Get,
  Param,
  ParseIntPipe,
  Post,
  Req,
  UnauthorizedException,
  UseGuards,
} from '@nestjs/common';
import { JwtAuthGuard, JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { CriarCombustivelDto } from './dto/criar-combustivel.dto';
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

  @Post()
  async cadastrar(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarCombustivelDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.combustiveisService.cadastrar(usuario.idEmpresa, dados, usuario);
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
