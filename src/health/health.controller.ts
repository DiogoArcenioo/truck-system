import { Controller, Get, ServiceUnavailableException } from '@nestjs/common';
import { DatabaseService } from '../database/database.service';

@Controller()
export class HealthController {
  constructor(private readonly databaseService: DatabaseService) {}

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
      await this.databaseService.checkConnection();
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

  @Get('health/db-signup')
  async healthDbSignup() {
    try {
      await this.databaseService.checkSignupConnection();
      const currentUser = await this.databaseService.getSignupCurrentUser();

      return {
        status: 'ok',
        database: 'connected',
        mode: 'signup',
        currentUser,
      };
    } catch (error) {
      throw new ServiceUnavailableException({
        status: 'error',
        database: 'disconnected',
        mode: 'signup',
        message:
          error instanceof Error ? error.message : 'Unknown database error',
      });
    }
  }
}
