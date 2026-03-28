import { Module } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { RequisicaoController } from './requisicao.controller';
import { RequisicaoService } from './requisicao.service';

@Module({
  controllers: [RequisicaoController],
  providers: [RequisicaoService, JwtAuthGuard],
})
export class RequisicaoModule {}
