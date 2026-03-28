import { Module } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { VeiculoController } from './veiculo.controller';
import { VeiculoService } from './veiculo.service';

@Module({
  controllers: [VeiculoController],
  providers: [VeiculoService, JwtAuthGuard],
})
export class VeiculoModule {}
