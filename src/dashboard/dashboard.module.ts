import { Module } from '@nestjs/common';
import { DashboardController } from './dashboard.controller';
import { DashboardService } from './dashboard.service';
import { SistemaDashboardController } from './sistema-dashboard.controller';

@Module({
  controllers: [DashboardController, SistemaDashboardController],
  providers: [DashboardService],
})
export class DashboardModule {}
