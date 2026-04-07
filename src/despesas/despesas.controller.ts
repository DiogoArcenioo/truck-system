import {
  Body,
  Controller,
  Delete,
  ForbiddenException,
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
import { AtualizarDespesaDto } from './dto/atualizar-despesa.dto';
import { CriarDespesaDto } from './dto/criar-despesa.dto';
import { FiltroDespesasDto } from './dto/filtro-despesas.dto';
import { DespesasService } from './despesas.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/despesas')
@UseGuards(JwtAuthGuard)
export class DespesasController {
  constructor(private readonly despesasService: DespesasService) {}

  @Get()
  async listarTodas(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.despesasService.listarTodas(usuario.idEmpresa);
  }

  @Get('filtro')
  async listarComFiltro(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroDespesasDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.despesasService.listarComFiltro(usuario.idEmpresa, filtro);
  }

  @Get('tipos')
  async listarTipos(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.despesasService.listarTipos(usuario.idEmpresa);
  }

  @Get(':idDespesa')
  async buscarPorId(
    @Req() request: RequisicaoAutenticada,
    @Param('idDespesa', ParseIntPipe) idDespesa: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.despesasService.buscarPorId(usuario.idEmpresa, idDespesa);
  }

  @Post()
  async cadastrar(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarDespesaDto,
  ) {
    this.obterUsuarioAutenticado(request);
    void dados;
    throw new ForbiddenException(
      'Cadastro de despesas desabilitado para este perfil.',
    );
  }

  @Put(':idDespesa')
  async atualizar(
    @Req() request: RequisicaoAutenticada,
    @Param('idDespesa', ParseIntPipe) idDespesa: number,
    @Body() dados: AtualizarDespesaDto,
  ) {
    this.obterUsuarioAutenticado(request);
    void idDespesa;
    void dados;
    throw new ForbiddenException(
      'Edicao de despesas desabilitada para este perfil.',
    );
  }

  @Delete(':idDespesa')
  async remover(
    @Req() request: RequisicaoAutenticada,
    @Param('idDespesa', ParseIntPipe) idDespesa: number,
  ) {
    this.obterUsuarioAutenticado(request);
    void idDespesa;
    throw new ForbiddenException(
      'Exclusao de despesas desabilitada para este perfil.',
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
