import { Module } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { CancelamentosController } from './cancelamentos.controller';
import { CancelamentosService } from './cancelamentos.service';

@Module({
  controllers: [CancelamentosController],
  providers: [CancelamentosService, JwtAuthGuard],
})
export class CancelamentosModule {}
