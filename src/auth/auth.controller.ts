import { Body, Controller, Post, UseGuards } from '@nestjs/common';
import { AuthService } from './auth.service';
import { CreateEmpresaDto } from './dto/create-empresa.dto';
import { CreateUsuarioDto } from './dto/create-usuario.dto';
import { InternalTokenGuard } from './guards/internal-token.guard';

@Controller('api/auth')
export class AuthController {
  constructor(private readonly authService: AuthService) {}

  @Post('registrar-empresa')
  @UseGuards(InternalTokenGuard)
  async registrarEmpresa(@Body() dados: CreateEmpresaDto) {
    return this.authService.registrarEmpresa(dados);
  }

  @Post('registrar-usuario')
  @UseGuards(InternalTokenGuard)
  async registrarUsuario(@Body() dados: CreateUsuarioDto) {
    return this.authService.registrarUsuario(dados);
  }
}
