import { Body, Controller, Post } from '@nestjs/common';
import { ServicoAuth } from './auth.service';
import type { DadosCadastroEmpresa } from './auth.service';

// Controlador responsavel pelos endpoints de autenticacao/cadastro.
@Controller('api/auth')
export class ControladorAuth {
  constructor(private readonly servicoAuth: ServicoAuth) {}

  // Endpoint que recebe os dados da empresa e do primeiro usuario.
  @Post('registrar-empresa')
  async registrarEmpresa(@Body() dados: DadosCadastroEmpresa) {
    return this.servicoAuth.registrarEmpresa(dados);
  }
}
