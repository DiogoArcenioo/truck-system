import { Module } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { PneusController } from './pneus.controller';
import { PneusService } from './pneus.service';

@Module({
  controllers: [PneusController],
  providers: [PneusService, JwtAuthGuard],
})
export class PneusModule {}

