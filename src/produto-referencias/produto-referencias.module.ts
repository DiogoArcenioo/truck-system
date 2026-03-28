import { Module } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { ProdutoReferenciasController } from './produto-referencias.controller';
import { ProdutoReferenciasService } from './produto-referencias.service';

@Module({
  controllers: [ProdutoReferenciasController],
  providers: [ProdutoReferenciasService, JwtAuthGuard],
})
export class ProdutoReferenciasModule {}
