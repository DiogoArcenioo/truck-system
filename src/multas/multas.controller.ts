import {
  Body,
  Controller,
  Delete,
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
import { AtualizarMultaDto } from './dto/atualizar-multa.dto';
import { CriarMultaDto } from './dto/criar-multa.dto';
import { FiltroMultasDto } from './dto/filtro-multas.dto';
import { MultasService } from './multas.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/multas')
@UseGuards(JwtAuthGuard)
export class MultasController {
  constructor(private readonly multasService: MultasService) {}

  @Get()
  async listarTodas(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.multasService.listarTodas(usuario.idEmpresa);
  }

  @Get('filtro')
  async listarComFiltro(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroMultasDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.multasService.listarComFiltro(usuario.idEmpresa, filtro);
  }

  @Get(':idMulta')
  async buscarPorId(
    @Req() request: RequisicaoAutenticada,
    @Param('idMulta', ParseIntPipe) idMulta: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.multasService.buscarPorId(usuario.idEmpresa, idMulta);
  }

  @Post()
  async cadastrar(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarMultaDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.multasService.cadastrar(usuario.idEmpresa, dados, usuario);
  }

  @Put(':idMulta')
  async atualizar(
    @Req() request: RequisicaoAutenticada,
    @Param('idMulta', ParseIntPipe) idMulta: number,
    @Body() dados: AtualizarMultaDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.multasService.atualizar(usuario.idEmpresa, idMulta, dados, usuario);
  }

  @Delete(':idMulta')
  async remover(
    @Req() request: RequisicaoAutenticada,
    @Param('idMulta', ParseIntPipe) idMulta: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.multasService.remover(usuario.idEmpresa, idMulta);
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
