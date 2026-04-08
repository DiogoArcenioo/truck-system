import {
  Controller,
  Get,
  Query,
  Req,
  UnauthorizedException,
  UseGuards,
} from '@nestjs/common';
import { JwtAuthGuard, JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { FiltroRelatorioDespesaDto } from './dto/filtro-relatorio-despesa.dto';
import { RelatoriosDespesaService } from './relatorios-despesa.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/relatorios/despesa')
@UseGuards(JwtAuthGuard)
export class RelatoriosDespesaController {
  constructor(
    private readonly relatoriosDespesaService: RelatoriosDespesaService,
  ) {}

  @Get()
  async obterRelatorio(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroRelatorioDespesaDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.relatoriosDespesaService.obterRelatorio(
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
