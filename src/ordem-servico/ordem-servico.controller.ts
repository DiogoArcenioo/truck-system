import {
  Body,
  Controller,
  Get,
  HttpException,
  InternalServerErrorException,
  Logger,
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
import { QueryFailedError } from 'typeorm';
import { AtualizarOrdemServicoDto } from './dto/atualizar-ordem-servico.dto';
import { CriarOrdemServicoDto } from './dto/criar-ordem-servico.dto';
import { FiltroOrdemServicoDto } from './dto/filtro-ordem-servico.dto';
import { OrdemServicoService } from './ordem-servico.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/ordem-servico')
@UseGuards(JwtAuthGuard)
export class OrdemServicoController {
  private readonly logger = new Logger(OrdemServicoController.name);

  constructor(private readonly ordemServicoService: OrdemServicoService) {}

  @Get('opcoes')
  async listarOpcoes() {
    return this.ordemServicoService.listarOpcoes();
  }

  @Get()
  async listarTodos(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.ordemServicoService.listarTodos(usuario.idEmpresa);
  }

  @Get('filtro')
  async listarComFiltro(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroOrdemServicoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.ordemServicoService.listarComFiltro(usuario.idEmpresa, filtro);
  }

  @Get(':idOs')
  async buscarPorId(
    @Req() request: RequisicaoAutenticada,
    @Param('idOs', ParseIntPipe) idOs: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.ordemServicoService.buscarPorId(usuario.idEmpresa, idOs);
  }

  @Post()
  async cadastrar(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarOrdemServicoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    try {
      return await this.ordemServicoService.cadastrar(
        usuario.idEmpresa,
        dados,
        usuario,
      );
    } catch (error) {
      if (error instanceof HttpException) {
        throw error;
      }

      const detalhe = this.descreverErro(error);
      this.logger.error(
        `Erro inesperado ao cadastrar ordem de servico. empresa=${usuario.idEmpresa}, idVeiculo=${dados.idVeiculo}`,
        error instanceof Error ? error.stack : undefined,
      );

      throw new InternalServerErrorException(
        `Falha inesperada ao cadastrar ordem de servico. DEBUG: ${detalhe}`,
      );
    }
  }

  @Put(':idOs')
  async atualizar(
    @Req() request: RequisicaoAutenticada,
    @Param('idOs', ParseIntPipe) idOs: number,
    @Body() dados: AtualizarOrdemServicoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    try {
      return await this.ordemServicoService.atualizar(
        usuario.idEmpresa,
        idOs,
        dados,
        usuario,
      );
    } catch (error) {
      if (error instanceof HttpException) {
        throw error;
      }

      const detalhe = this.descreverErro(error);
      this.logger.error(
        `Erro inesperado ao atualizar ordem de servico. empresa=${usuario.idEmpresa}, idOs=${idOs}`,
        error instanceof Error ? error.stack : undefined,
      );

      throw new InternalServerErrorException(
        `Falha inesperada ao atualizar ordem de servico. DEBUG: ${detalhe}`,
      );
    }
  }

  private obterUsuarioAutenticado(
    request: RequisicaoAutenticada,
  ): JwtUsuarioPayload {
    if (!request.usuario) {
      throw new UnauthorizedException('Usuario nao autenticado.');
    }

    return request.usuario;
  }

  private descreverErro(error: unknown): string {
    if (error instanceof QueryFailedError) {
      const erroPg = error.driverError as {
        code?: string;
        detail?: string;
        message?: string;
      };
      return `QueryFailedError code=${erroPg.code ?? 'N/A'} detail=${erroPg.detail ?? erroPg.message ?? 'N/A'}`;
    }

    if (error instanceof Error) {
      return `${error.name}: ${error.message}`;
    }

    try {
      return JSON.stringify(error);
    } catch {
      return String(error);
    }
  }
}
