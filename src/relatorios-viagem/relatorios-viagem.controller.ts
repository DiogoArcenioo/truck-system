import {
  Controller,
  Get,
  Query,
  Req,
  UnauthorizedException,
  UseGuards,
} from '@nestjs/common';
import { JwtAuthGuard, JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { FiltroRelatorioViagemDto } from './dto/filtro-relatorio-viagem.dto';
import { RelatoriosViagemService } from './relatorios-viagem.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/relatorios/viagem')
@UseGuards(JwtAuthGuard)
export class RelatoriosViagemController {
  constructor(
    private readonly relatoriosViagemService: RelatoriosViagemService,
  ) {}

  @Get()
  async obterRelatorio(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroRelatorioViagemDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.relatoriosViagemService.obterRelatorio(usuario.idEmpresa, filtro);
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
