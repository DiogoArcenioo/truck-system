import { Module } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { CombustiveisController } from './combustiveis.controller';
import { CombustiveisService } from './combustiveis.service';

@Module({
  controllers: [CombustiveisController],
  providers: [CombustiveisService, JwtAuthGuard],
})
export class CombustiveisModule {}
