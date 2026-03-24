import { Module } from '@nestjs/common';
import { DatabaseModule } from '../database/database.module';
import { ControladorAuth } from './auth.controller';
import { ServicoAuth } from './auth.service';

// Modulo de autenticacao e registro de novas contas.
@Module({
  imports: [DatabaseModule],
  controllers: [ControladorAuth],
  providers: [ServicoAuth],
})
export class ModuloAuth {}
