import { Module } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { OrdemServicoController } from './ordem-servico.controller';
import { OrdemServicoService } from './ordem-servico.service';

@Module({
  controllers: [OrdemServicoController],
  providers: [OrdemServicoService, JwtAuthGuard],
})
export class OrdemServicoModule {}
