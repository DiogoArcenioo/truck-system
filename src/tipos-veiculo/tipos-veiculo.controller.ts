import {
  Body,
  Controller,
  Get,
  Param,
  ParseIntPipe,
  Post,
  Put,
  Req,
  UnauthorizedException,
  UseGuards,
} from '@nestjs/common';
import { JwtAuthGuard, JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { CriarTipoVeiculoDto } from './dto/criar-tipo-veiculo.dto';
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

  @Post()
  async cadastrar(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarTipoVeiculoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.tiposVeiculoService.cadastrar(usuario.idEmpresa, dados, usuario);
  }

  @Get(':idTipo')
  async buscarPorId(
    @Req() request: RequisicaoAutenticada,
    @Param('idTipo', ParseIntPipe) idTipo: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.tiposVeiculoService.buscarPorId(usuario.idEmpresa, idTipo);
  }

  @Put(':idTipo')
  async atualizar(
    @Req() request: RequisicaoAutenticada,
    @Param('idTipo', ParseIntPipe) idTipo: number,
    @Body() dados: CriarTipoVeiculoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.tiposVeiculoService.atualizar(usuario.idEmpresa, idTipo, dados, usuario);
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
