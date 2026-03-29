import {
  Controller,
  Get,
  HttpException,
  InternalServerErrorException,
  Logger,
  Param,
  Query,
  Req,
  UnauthorizedException,
  UseGuards,
} from '@nestjs/common';
import { JwtAuthGuard, JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { QueryFailedError } from 'typeorm';
import { FiltroDashboardDto } from './dto/filtro-dashboard.dto';
import { DashboardService } from './dashboard.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/dashboard')
@UseGuards(JwtAuthGuard)
export class DashboardController {
  private readonly logger = new Logger(DashboardController.name);

  constructor(private readonly dashboardService: DashboardService) {}

  @Get()
  @Get('home')
  async obterHome(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroDashboardDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    try {
      return await this.dashboardService.obterHome(usuario.idEmpresa, filtro);
    } catch (error) {
      if (error instanceof HttpException) {
        throw error;
      }

      const detalhe = this.descreverErro(error);
      this.logger.error(
        `Erro inesperado ao carregar dashboard. empresa=${usuario.idEmpresa}`,
        error instanceof Error ? error.stack : undefined,
      );

      throw new InternalServerErrorException(
        `Falha inesperada ao carregar dashboard. DEBUG: ${detalhe}`,
      );
    }
  }

  @Get('indicadores')
  async listarIndicadores(@Req() request: RequisicaoAutenticada) {
    this.obterUsuarioAutenticado(request);
    return this.dashboardService.listarIndicadores();
  }

  @Get('indicadores/:indicadorId')
  async detalharIndicador(
    @Req() request: RequisicaoAutenticada,
    @Param('indicadorId') indicadorId: string,
    @Query() filtro: FiltroDashboardDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    try {
      return await this.dashboardService.obterDetalheIndicador(
        usuario.idEmpresa,
        indicadorId,
        filtro,
      );
    } catch (error) {
      if (error instanceof HttpException) {
        throw error;
      }

      const detalhe = this.descreverErro(error);
      this.logger.error(
        `Erro inesperado ao detalhar indicador. empresa=${usuario.idEmpresa}, indicador=${indicadorId}`,
        error instanceof Error ? error.stack : undefined,
      );

      throw new InternalServerErrorException(
        `Falha inesperada ao detalhar indicador. DEBUG: ${detalhe}`,
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
