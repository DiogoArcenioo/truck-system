import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { CteController } from './cte.controller';
import { CteService } from './cte.service';
import { CteEntity } from './entities/cte.entity';

@Module({
  imports: [TypeOrmModule.forFeature([CteEntity])],
  controllers: [CteController],
  providers: [CteService, JwtAuthGuard],
  exports: [CteService],
})
export class CteModule {}
