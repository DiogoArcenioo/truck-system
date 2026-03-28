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
import { AtualizarRequisicaoDto } from './dto/atualizar-requisicao.dto';
import { CriarRequisicaoDto } from './dto/criar-requisicao.dto';
import { FiltroRequisicaoDto } from './dto/filtro-requisicao.dto';
import { RequisicaoService } from './requisicao.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/requisicao')
@UseGuards(JwtAuthGuard)
export class RequisicaoController {
  private readonly logger = new Logger(RequisicaoController.name);

  constructor(private readonly requisicaoService: RequisicaoService) {}

  @Get('opcoes')
  async listarOpcoes() {
    return this.requisicaoService.listarOpcoes();
  }

  @Get()
  async listarTodos(@Req() request: RequisicaoAutenticada) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.requisicaoService.listarTodos(usuario.idEmpresa);
  }

  @Get('filtro')
  async listarComFiltro(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroRequisicaoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.requisicaoService.listarComFiltro(usuario.idEmpresa, filtro);
  }

  @Get(':idRequisicao')
  async buscarPorId(
    @Req() request: RequisicaoAutenticada,
    @Param('idRequisicao', ParseIntPipe) idRequisicao: number,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.requisicaoService.buscarPorId(usuario.idEmpresa, idRequisicao);
  }

  @Post()
  async cadastrar(
    @Req() request: RequisicaoAutenticada,
    @Body() dados: CriarRequisicaoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    try {
      return await this.requisicaoService.cadastrar(usuario.idEmpresa, dados, usuario);
    } catch (error) {
      if (error instanceof HttpException) {
        throw error;
      }

      const detalhe = this.descreverErro(error);

      this.logger.error(
        `Erro inesperado ao cadastrar requisicao. empresa=${usuario.idEmpresa}, idOs=${dados.idOs}, itens=${dados.itens?.length ?? 0}`,
        error instanceof Error ? error.stack : undefined,
      );

      throw new InternalServerErrorException(
        `Falha inesperada ao cadastrar requisicao. DEBUG: ${detalhe}`,
      );
    }
  }

  @Put(':idRequisicao')
  async atualizar(
    @Req() request: RequisicaoAutenticada,
    @Param('idRequisicao', ParseIntPipe) idRequisicao: number,
    @Body() dados: AtualizarRequisicaoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    try {
      return await this.requisicaoService.atualizar(
        usuario.idEmpresa,
        idRequisicao,
        dados,
        usuario,
      );
    } catch (error) {
      if (error instanceof HttpException) {
        throw error;
      }

      const detalhe = this.descreverErro(error);

      this.logger.error(
        `Erro inesperado ao atualizar requisicao. empresa=${usuario.idEmpresa}, idRequisicao=${idRequisicao}`,
        error instanceof Error ? error.stack : undefined,
      );

      throw new InternalServerErrorException(
        `Falha inesperada ao atualizar requisicao. DEBUG: ${detalhe}`,
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
