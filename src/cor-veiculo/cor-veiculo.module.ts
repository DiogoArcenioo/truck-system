import { Module } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { CorVeiculoController } from './cor-veiculo.controller';
import { CorVeiculoService } from './cor-veiculo.service';

@Module({
  controllers: [CorVeiculoController],
  providers: [CorVeiculoService, JwtAuthGuard],
})
export class CorVeiculoModule {}
