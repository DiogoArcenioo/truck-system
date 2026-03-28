import { Module } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { TiposVeiculoController } from './tipos-veiculo.controller';
import { TiposVeiculoService } from './tipos-veiculo.service';

@Module({
  controllers: [TiposVeiculoController],
  providers: [TiposVeiculoService, JwtAuthGuard],
})
export class TiposVeiculoModule {}
