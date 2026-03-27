import { Module } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { FornecedorController } from './fornecedor.controller';
import { FornecedorService } from './fornecedor.service';

@Module({
  controllers: [FornecedorController],
  providers: [FornecedorService, JwtAuthGuard],
})
export class FornecedorModule {}
