import { Body, Controller, Post } from '@nestjs/common';
import { AuthService } from './auth.service';
import { CreateEmpresaDto } from './dto/create-empresa.dto';

@Controller('api/auth')
export class AuthController {
  constructor(private readonly authService: AuthService) {}

  @Post('registrar-empresa')
  async registrarEmpresa(@Body() dados: CreateEmpresaDto) {
    return this.authService.registrarEmpresa(dados);
  }
}
