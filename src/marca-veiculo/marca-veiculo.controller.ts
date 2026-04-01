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
import { CriarMarcaVeiculoDto } from './dto/criar-marca-veiculo.dto';
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

  @Post()
  async cadastrar(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarMarcaVeiculoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.marcaVeiculoService.cadastrar(usuario.idEmpresa, dados, usuario);
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
