import { Module } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { DespesasController } from './despesas.controller';
import { DespesasService } from './despesas.service';

@Module({
  controllers: [DespesasController],
  providers: [DespesasService, JwtAuthGuard],
})
export class DespesasModule {}
