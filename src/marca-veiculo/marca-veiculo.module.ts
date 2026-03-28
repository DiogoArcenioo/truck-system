import { Module } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { MarcaVeiculoController } from './marca-veiculo.controller';
import { MarcaVeiculoService } from './marca-veiculo.service';

@Module({
  controllers: [MarcaVeiculoController],
  providers: [MarcaVeiculoService, JwtAuthGuard],
})
export class MarcaVeiculoModule {}
