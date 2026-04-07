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
import { CancelamentosService } from './cancelamentos.service';
import { AtualizarMotivoCancelamentoDto } from './dto/atualizar-motivo-cancelamento.dto';
import { ConsultarDocumentoCancelamentoDto } from './dto/consultar-documento-cancelamento.dto';
import { CriarCancelamentoDto } from './dto/criar-cancelamento.dto';
import { CriarMotivoCancelamentoDto } from './dto/criar-motivo-cancelamento.dto';
import { FiltroCancelamentosDto } from './dto/filtro-cancelamentos.dto';
import { FiltroMotivosCancelamentoDto } from './dto/filtro-motivos-cancelamento.dto';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/cancelamentos')
@UseGuards(JwtAuthGuard)
export class CancelamentosController {
  constructor(private readonly cancelamentosService: CancelamentosService) {}

  @Get('opcoes')
  async listarOpcoes(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.cancelamentosService.listarOpcoes(usuario.idEmpresa);
  }

  @Get('motivos')
  async listarMotivos(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroMotivosCancelamentoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.cancelamentosService.listarMotivos(usuario.idEmpresa, filtro);
  }

  @Post('motivos')
  async criarMotivo(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarMotivoCancelamentoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.cancelamentosService.criarMotivo(usuario.idEmpresa, dados, usuario);
  }

  @Put('motivos/:idMotivo')
  async atualizarMotivo(
    @Req() request: RequisicaoAutenticada,
    @Param('idMotivo', ParseIntPipe) idMotivo: number,
    @Body() dados: AtualizarMotivoCancelamentoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.cancelamentosService.atualizarMotivo(
      usuario.idEmpresa,
      idMotivo,
      dados,
      usuario,
    );
  }

  @Delete('motivos/:idMotivo')
  async removerMotivo(
    @Req() request: RequisicaoAutenticada,
    @Param('idMotivo', ParseIntPipe) idMotivo: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.cancelamentosService.removerMotivo(usuario.idEmpresa, idMotivo, usuario);
  }

  @Get('documento')
  async consultarDocumento(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: ConsultarDocumentoCancelamentoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.cancelamentosService.consultarDocumento(
      usuario.idEmpresa,
      filtro.tipoDocumento,
      filtro.idDocumento,
    );
  }

  @Post('reabrir')
  async reabrirDocumento(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarCancelamentoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.cancelamentosService.reabrirDocumento(usuario.idEmpresa, dados, usuario);
  }

  @Get('historico')
  async listarHistorico(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroCancelamentosDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.cancelamentosService.listarHistorico(usuario.idEmpresa, filtro);
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
