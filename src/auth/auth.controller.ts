import { Body, Controller, Post, UseGuards } from '@nestjs/common';
import { AuthService } from './auth.service';
import { AtivarAssinaturaDto } from './dto/ativar-assinatura.dto';
import { CreateEmpresaDto } from './dto/create-empresa.dto';
import { CreateUsuarioDto } from './dto/create-usuario.dto';
import { LoginDto } from './dto/login.dto';
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

  @Post('login')
  @UseGuards(InternalTokenGuard)
  async login(@Body() dados: LoginDto) {
    return this.authService.login(dados);
  }

  @Post('ativar-assinatura')
  @UseGuards(InternalTokenGuard)
  async ativarAssinatura(@Body() dados: AtivarAssinaturaDto) {
    return this.authService.ativarAssinatura(dados);
  }
}
