import { Module } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { EngateDesengateController } from './engate-desengate.controller';
import { EngateDesengateService } from './engate-desengate.service';

@Module({
  controllers: [EngateDesengateController],
  providers: [EngateDesengateService, JwtAuthGuard],
})
export class EngateDesengateModule {}
