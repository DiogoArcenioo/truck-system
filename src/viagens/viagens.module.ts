import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { ViagemEntity } from './entities/viagem.entity';
import { ViagensController } from './viagens.controller';
import { ViagensService } from './viagens.service';

@Module({
  imports: [TypeOrmModule.forFeature([ViagemEntity])],
  controllers: [ViagensController],
  providers: [ViagensService, JwtAuthGuard],
})
export class ViagensModule {}
