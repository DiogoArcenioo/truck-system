import {
  Controller,
  Get,
  Query,
  Req,
  UnauthorizedException,
  UseGuards,
} from '@nestjs/common';
import { JwtAuthGuard, JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { FiltroRelatorioResultadoDto } from './dto/filtro-relatorio-resultado.dto';
import { RelatoriosResultadoService } from './relatorios-resultado.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/relatorios/resultado')
@UseGuards(JwtAuthGuard)
export class RelatoriosResultadoController {
  constructor(
    private readonly relatoriosResultadoService: RelatoriosResultadoService,
  ) {}

  @Get()
  async obterRelatorio(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroRelatorioResultadoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.relatoriosResultadoService.obterRelatorio(
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
