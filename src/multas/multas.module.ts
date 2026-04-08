import { Module } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { MultasController } from './multas.controller';
import { MultasService } from './multas.service';

@Module({
  controllers: [MultasController],
  providers: [MultasService, JwtAuthGuard],
  exports: [MultasService],
})
export class MultasModule {}
