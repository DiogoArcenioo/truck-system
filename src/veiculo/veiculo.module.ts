import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { VeiculoEntity } from './entities/veiculo.entity';
import { VeiculoController } from './veiculo.controller';
import { VeiculoService } from './veiculo.service';

@Module({
  imports: [TypeOrmModule.forFeature([VeiculoEntity])],
  controllers: [VeiculoController],
  providers: [VeiculoService, JwtAuthGuard],
})
export class VeiculoModule {}
