import {
  Controller,
  Get,
  Query,
  Req,
  UnauthorizedException,
  UseGuards,
} from '@nestjs/common';
import { JwtAuthGuard, JwtUsuarioPayload } from '../auth/guards/jwt-auth.guard';
import { FiltroRelatorioAbastecimentoDto } from './dto/filtro-relatorio-abastecimento.dto';
import { RelatoriosAbastecimentoService } from './relatorios-abastecimento.service';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/relatorios/abastecimento')
@UseGuards(JwtAuthGuard)
export class RelatoriosAbastecimentoController {
  constructor(
    private readonly relatoriosAbastecimentoService: RelatoriosAbastecimentoService,
  ) {}

  @Get()
  async obterRelatorio(
    @Req() request: RequisicaoAutenticada,
    @Query() filtro: FiltroRelatorioAbastecimentoDto,
  ) {
    const usuario = this.obterUsuarioAutenticado(request);
    return this.relatoriosAbastecimentoService.obterRelatorio(
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
