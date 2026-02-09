import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { createPool, Pool, PoolConnection } from 'mysql2/promise';
import { AppConfigService } from '../config/app-config.service';

@Injectable()
export class MysqlService implements OnModuleDestroy {
  private readonly pool: Pool;

  constructor(private readonly appConfigService: AppConfigService) {
    this.pool = createPool({
      host: appConfigService.db.host,
      port: appConfigService.db.port,
      user: appConfigService.db.user,
      password: appConfigService.db.password,
      database: appConfigService.db.database,
      waitForConnections: true,
      connectionLimit: 20,
      queueLimit: 0,
      charset: 'utf8mb4',
      timezone: 'Z',
    });
  }

  getPool(): Pool {
    return this.pool;
  }

  async getConnection(): Promise<PoolConnection> {
    return this.pool.getConnection();
  }

  async onModuleDestroy(): Promise<void> {
    await this.pool.end();
  }
}
