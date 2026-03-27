import { Module } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { ProdutoController } from './produto.controller';
import { ProdutoService } from './produto.service';

@Module({
  controllers: [ProdutoController],
  providers: [ProdutoService, JwtAuthGuard],
})
export class ProdutoModule {}
