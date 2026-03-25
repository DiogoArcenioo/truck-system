import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Pool, PoolClient, QueryResult, QueryResultRow } from 'pg';

@Injectable()
export class DatabaseService implements OnModuleDestroy {
  private static readonly SIGNUP_ADMIN_USER = 'kodigo';
  private pool: Pool | null = null;
  private signupPool: Pool | null = null;

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

  private getSignupPool(): Pool {
    if (this.signupPool) {
      return this.signupPool;
    }

    const signupHost =
      this.configService.get<string>('DB_SIGNUP_HOST') ??
      this.configService.get<string>('DB_HOST');
    const signupPortRaw =
      this.configService.get<string>('DB_SIGNUP_PORT') ??
      this.configService.get<string>('DB_PORT') ??
      '5432';
    const signupDatabase =
      this.configService.get<string>('DB_SIGNUP_NAME') ??
      this.configService.get<string>('DB_NAME');
    const signupUser =
      this.configService.get<string>('DB_SIGNUP_USER')?.trim() ||
      DatabaseService.SIGNUP_ADMIN_USER;
    const signupPassword = this.configService.get<string>('DB_SIGNUP_PASSWORD');

    if (!signupHost || !signupDatabase || !signupPassword) {
      throw new Error(
        'Signup database env vars are missing. Set DB_SIGNUP_HOST, DB_SIGNUP_PORT, DB_SIGNUP_NAME and DB_SIGNUP_PASSWORD.',
      );
    }

    const port = Number(signupPortRaw);
    if (Number.isNaN(port)) {
      throw new Error('DB_SIGNUP_PORT must be a valid number.');
    }

    if (signupUser.toLowerCase() !== DatabaseService.SIGNUP_ADMIN_USER) {
      throw new Error(
        `DB_SIGNUP_USER must be "${DatabaseService.SIGNUP_ADMIN_USER}" to run signup as admin without RLS restrictions.`,
      );
    }

    this.signupPool = new Pool({
      host: signupHost,
      port,
      database: signupDatabase,
      user: signupUser,
      password: signupPassword,
      max: 4,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 5000,
    });

    return this.signupPool;
  }

  async checkConnection(): Promise<void> {
    const pool = this.getPool();
    await pool.query('SELECT 1');
  }

  async checkSignupConnection(): Promise<void> {
    const pool = this.getSignupPool();
    await pool.query('SELECT 1');
  }

  async getSignupCurrentUser(): Promise<string> {
    const pool = this.getSignupPool();
    const resultado = await pool.query<{ current_user: string }>(
      'SELECT current_user;',
    );
    return resultado.rows[0]?.current_user ?? 'unknown';
  }

  async query<T extends QueryResultRow = QueryResultRow>(
    text: string,
    params: readonly unknown[] = [],
  ): Promise<QueryResult<T>> {
    const pool = this.getPool();
    return pool.query<T>(text, params);
  }

  async queryWithSignup<T extends QueryResultRow = QueryResultRow>(
    text: string,
    params: readonly unknown[] = [],
  ): Promise<QueryResult<T>> {
    const pool = this.getSignupPool();
    return pool.query<T>(text, params);
  }

  async withTransaction<T>(
    callback: (client: PoolClient) => Promise<T>,
  ): Promise<T> {
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

  async withSignupTransaction<T>(
    callback: (client: PoolClient) => Promise<T>,
  ): Promise<T> {
    const pool = this.getSignupPool();
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

    if (this.signupPool && this.signupPool !== this.pool) {
      await this.signupPool.end();
      this.signupPool = null;
    }
  }
}
