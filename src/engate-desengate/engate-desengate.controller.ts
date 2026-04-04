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
import { AtualizarEngateDesengateDto } from './dto/atualizar-engate-desengate.dto';
import { CriarEngateDesengateDto } from './dto/criar-engate-desengate.dto';
import { FiltroEngateDesengateDto } from './dto/filtro-engate-desengate.dto';
import { EngateDesengateService } from './engate-desengate.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/engate-desengate')
@UseGuards(JwtAuthGuard)
export class EngateDesengateController {
  constructor(
    private readonly engateDesengateService: EngateDesengateService,
  ) {}

  @Get()
  async listarTodos(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.engateDesengateService.listarTodos(usuario.idEmpresa);
  }

  @Get('filtro')
  async listarComFiltro(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroEngateDesengateDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.engateDesengateService.listarComFiltro(
      usuario.idEmpresa,
      filtro,
    );
  }

  @Get(':idEngate')
  async buscarPorId(
    @Req() request: RequisicaoAutenticada,
    @Param('idEngate', ParseIntPipe) idEngate: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.engateDesengateService.buscarPorId(usuario.idEmpresa, idEngate);
  }

  @Post()
  async cadastrar(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarEngateDesengateDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.engateDesengateService.cadastrar(
      usuario.idEmpresa,
      dados,
      usuario,
    );
  }

  @Put(':idEngate')
  async atualizar(
    @Req() request: RequisicaoAutenticada,
    @Param('idEngate', ParseIntPipe) idEngate: number,
    @Body() dados: AtualizarEngateDesengateDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.engateDesengateService.atualizar(
      usuario.idEmpresa,
      idEngate,
      dados,
      usuario,
    );
  }

  @Delete(':idEngate')
  async remover(
    @Req() request: RequisicaoAutenticada,
    @Param('idEngate', ParseIntPipe) idEngate: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.engateDesengateService.remover(usuario.idEmpresa, idEngate);
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
