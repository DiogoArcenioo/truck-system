import {
  Body,
  Controller,
  Get,
  Param,
  ParseIntPipe,
  Post,
  Put,
  Query,
  Req,
  UnauthorizedException,
  UseGuards,
} from '@nestjs/common';
import { JwtAuthGuard, JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { CriarModeloVeiculoDto } from './dto/criar-modelo-veiculo.dto';
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

  @Post()
  async cadastrar(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarModeloVeiculoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.modeloVeiculoService.cadastrar(usuario.idEmpresa, dados, usuario);
  }

  @Get(':idModelo')
  async buscarPorId(
    @Req() request: RequisicaoAutenticada,
    @Param('idModelo', ParseIntPipe) idModelo: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.modeloVeiculoService.buscarPorId(usuario.idEmpresa, idModelo);
  }

  @Put(':idModelo')
  async atualizar(
    @Req() request: RequisicaoAutenticada,
    @Param('idModelo', ParseIntPipe) idModelo: number,
    @Body() dados: CriarModeloVeiculoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.modeloVeiculoService.atualizar(usuario.idEmpresa, idModelo, dados, usuario);
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
