import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Pool, PoolClient, QueryResult, QueryResultRow } from 'pg';

@Injectable()
export class DatabaseService implements OnModuleDestroy {
  private pool: Pool | null = null;

  constructor(private readonly configService: ConfigService) {}

  private getPool(): Pool {
    if (this.pool) {
      return this.pool;
    }

    const host = this.configService.get<string>('DB_HOST');
    const portRaw = this.configService.get<string>('DB_PORT') ?? '5432';
    const database = this.configService.get<string>('DB_NAME');
    const user = this.configService.get<string>('DB_USER');
    const password = this.configService.get<string>('DB_PASSWORD');

    if (!host || !database || !user || !password) {
      throw new Error(
        'Database env vars are missing. Set DB_HOST, DB_PORT, DB_NAME, DB_USER and DB_PASSWORD.',
      );
    }

    const port = Number(portRaw);
    if (Number.isNaN(port)) {
      throw new Error('DB_PORT must be a valid number.');
    }

    this.pool = new Pool({
      host,
      port,
      database,
      user,
      password,
      max: 10,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 5000,
    });

    return this.pool;
  }

  async checkConnection(): Promise<void> {
    const pool = this.getPool();
    await pool.query('SELECT 1');
  }

  async query<T extends QueryResultRow = QueryResultRow>(
    text: string,
    params: readonly unknown[] = [],
  ): Promise<QueryResult<T>> {
    const pool = this.getPool();
    return pool.query<T>(text, params);
  }

  async withTransaction<T>(callback: (client: PoolClient) => Promise<T>): Promise<T> {
    const pool = this.getPool();
    const client = await pool.connect();

    try {
      await client.query('BEGIN');
      const result = await callback(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async onModuleDestroy(): Promise<void> {
    if (this.pool) {
      await this.pool.end();
      this.pool = null;
    }
  }
}
