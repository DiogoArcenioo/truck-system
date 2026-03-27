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
import { AtualizarMotoristaDto } from './dto/atualizar-motorista.dto';
import { CriarMotoristaDto } from './dto/criar-motorista.dto';
import { FiltroMotoristasDto } from './dto/filtro-motoristas.dto';
import { MotoristasService } from './motoristas.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/motoristas')
@UseGuards(JwtAuthGuard)
export class MotoristasController {
  constructor(private readonly motoristasService: MotoristasService) {}

  @Get('opcoes')
  async listarOpcoes() {
    return this.motoristasService.listarOpcoes();
  }

  @Get()
  async listarTodos(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.motoristasService.listarTodos(usuario.idEmpresa);
  }

  @Get('filtro')
  async listarComFiltro(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroMotoristasDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.motoristasService.listarComFiltro(usuario.idEmpresa, filtro);
  }

  @Get(':idMotorista')
  async buscarPorId(
    @Req() request: RequisicaoAutenticada,
    @Param('idMotorista', ParseIntPipe) idMotorista: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.motoristasService.buscarPorId(usuario.idEmpresa, idMotorista);
  }

  @Post()
  async cadastrar(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarMotoristaDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.motoristasService.cadastrar(usuario.idEmpresa, dados, usuario);
  }

  @Put(':idMotorista')
  async atualizar(
    @Req() request: RequisicaoAutenticada,
    @Param('idMotorista', ParseIntPipe) idMotorista: number,
    @Body() dados: AtualizarMotoristaDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.motoristasService.atualizar(
      usuario.idEmpresa,
      idMotorista,
      dados,
      usuario,
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
