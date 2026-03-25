import { Controller, Get, ServiceUnavailableException } from '@nestjs/common';
import { DataSource } from 'typeorm';

@Controller()
export class HealthController {
  constructor(private readonly dataSource: DataSource) {}

  @Get('health')
  health() {
    return {
      status: 'ok',
      service: 'truck-system',
      timestamp: new Date().toISOString(),
    };
  }

  @Get('health/db')
  async healthDb() {
    try {
      await this.dataSource.query('SELECT 1');
      return {
        status: 'ok',
        database: 'connected',
      };
    } catch (error) {
      throw new ServiceUnavailableException({
        status: 'error',
        database: 'disconnected',
        message:
          error instanceof Error ? error.message : 'Unknown database error',
      });
    }
  }
}
