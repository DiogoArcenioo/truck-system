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
import { CriarCorVeiculoDto } from './dto/criar-cor-veiculo.dto';
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

  @Post()
  async cadastrar(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarCorVeiculoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.corVeiculoService.cadastrar(usuario.idEmpresa, dados, usuario);
  }

  @Get(':idCor')
  async buscarPorId(
    @Req() request: RequisicaoAutenticada,
    @Param('idCor', ParseIntPipe) idCor: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.corVeiculoService.buscarPorId(usuario.idEmpresa, idCor);
  }

  @Put(':idCor')
  async atualizar(
    @Req() request: RequisicaoAutenticada,
    @Param('idCor', ParseIntPipe) idCor: number,
    @Body() dados: CriarCorVeiculoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.corVeiculoService.atualizar(usuario.idEmpresa, idCor, dados, usuario);
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
