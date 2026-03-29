import {
  Controller,
  Get,
  Query,
  Req,
  UnauthorizedException,
  UseGuards,
} from '@nestjs/common';
import { JwtAuthGuard, JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { FiltroDashboardDto } from './dto/filtro-dashboard.dto';
import { DashboardService } from './dashboard.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/dashboard')
@UseGuards(JwtAuthGuard)
export class DashboardController {
  constructor(private readonly dashboardService: DashboardService) {}

  @Get()
  @Get('home')
  async obterHome(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroDashboardDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.dashboardService.obterHome(usuario.idEmpresa, filtro);
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

