import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { MotoristaEntity } from './entities/motorista.entity';
import { MotoristasController } from './motoristas.controller';
import { MotoristasService } from './motoristas.service';

@Module({
  imports: [TypeOrmModule.forFeature([MotoristaEntity])],
  controllers: [MotoristasController],
  providers: [MotoristasService, JwtAuthGuard],
})
export class MotoristasModule {}
