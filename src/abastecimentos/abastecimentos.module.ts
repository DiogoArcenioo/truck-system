import { Module } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { AbastecimentosController } from './abastecimentos.controller';
import { AbastecimentosService } from './abastecimentos.service';

@Module({
  controllers: [AbastecimentosController],
  providers: [AbastecimentosService, JwtAuthGuard],
})
export class AbastecimentosModule {}
