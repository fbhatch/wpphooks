import { Injectable } from '@nestjs/common';
import { PoolConnection, RowDataPacket } from 'mysql2/promise';
import { MysqlService } from '../db/mysql.service';

export interface IntegrationMapping extends RowDataPacket {
  id: number;
  company_id: number;
  gupshup_app_id: string;
  is_active: number;
}

@Injectable()
export class IntegrationRepo {
  constructor(private readonly mysqlService: MysqlService) {}

  async findActiveByAppId(
    appId: string,
    connection?: PoolConnection,
  ): Promise<IntegrationMapping | null> {
    const executor = connection ?? this.mysqlService.getPool();
    const sql = `
      SELECT id, company_id, gupshup_app_id, is_active
      FROM wpp_company_integration
      WHERE gupshup_app_id = ?
        AND is_active = 1
      LIMIT 1
    `;

    const [rows] = await executor.query<IntegrationMapping[]>(sql, [appId]);
    return rows.length > 0 ? rows[0] : null;
  }
}
