import {
  Controller,
  Get,
  Query,
  Req,
  UnauthorizedException,
  UseGuards,
} from '@nestjs/common';
import { JwtAuthGuard, JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { DetalheRelatorioFaturamentoDto } from './dto/detalhe-relatorio-faturamento.dto';
import { FiltroRelatorioFaturamentoDto } from './dto/filtro-relatorio-faturamento.dto';
import { FiltroSerieRelatorioFaturamentoDto } from './dto/filtro-serie-relatorio-faturamento.dto';
import { RelatoriosFaturamentoService } from './relatorios-faturamento.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/relatorios/faturamento')
@UseGuards(JwtAuthGuard)
export class RelatoriosFaturamentoController {
  constructor(
    private readonly relatoriosFaturamentoService: RelatoriosFaturamentoService,
  ) {}

  @Get()
  async obterResumo(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroRelatorioFaturamentoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.relatoriosFaturamentoService.obterResumo(usuario.idEmpresa, filtro);
  }

  @Get('detalhes')
  async obterDetalhes(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: DetalheRelatorioFaturamentoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.relatoriosFaturamentoService.obterDetalhes(
      usuario.idEmpresa,
      filtro,
    );
  }

  @Get('serie-mensal')
  async obterSerieMensal(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroSerieRelatorioFaturamentoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.relatoriosFaturamentoService.obterSerieMensal(
      usuario.idEmpresa,
      filtro,
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
