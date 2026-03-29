import {
  Controller,
  Get,
  HttpException,
  InternalServerErrorException,
  Logger,
  Query,
  Req,
  UnauthorizedException,
  UseGuards,
} from '@nestjs/common';
import { QueryFailedError } from 'typeorm';
import { JwtAuthGuard, JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { DashboardService } from './dashboard.service';
import { FiltroDashboardDto } from './dto/filtro-dashboard.dto';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/sistema')
@UseGuards(JwtAuthGuard)
export class SistemaDashboardController {
  private readonly logger = new Logger(SistemaDashboardController.name);

  constructor(private readonly dashboardService: DashboardService) {}

  @Get('dashboard')
  async obterDashboard(
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
        `Erro inesperado ao carregar dashboard (api/sistema). empresa=${usuario.idEmpresa}`,
        error instanceof Error ? error.stack : undefined,
      );

      throw new InternalServerErrorException(
        `Falha inesperada ao carregar dashboard. DEBUG: ${detalhe}`,
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

