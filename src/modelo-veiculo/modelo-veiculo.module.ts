import { Module } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { ModeloVeiculoController } from './modelo-veiculo.controller';
import { ModeloVeiculoService } from './modelo-veiculo.service';

@Module({
  controllers: [ModeloVeiculoController],
  providers: [ModeloVeiculoService, JwtAuthGuard],
})
export class ModeloVeiculoModule {}
