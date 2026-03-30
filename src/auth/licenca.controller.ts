import {
  Controller,
  Get,
  Req,
  UnauthorizedException,
  UseGuards,
} from '@nestjs/common';
import { AuthService } from './auth.service';
import { JwtAuthGuard, JwtUsuarioPayload } from './guards/jwt-auth.guard';

type RequisicaoAutenticada = {
  usuario?: JwtUsuarioPayload;
};

@Controller('api/sistema/licenca')
@UseGuards(JwtAuthGuard)
export class LicencaController {
  constructor(private readonly authService: AuthService) {}

  @Get('status')
  async obterStatus(@Req() request: RequisicaoAutenticada) {
    const usuario = request.usuario;
    if (!usuario) {
      throw new UnauthorizedException('Usuario nao autenticado.');
    }

    return this.authService.obterStatusLicenca(usuario.idEmpresa);
  }
}
