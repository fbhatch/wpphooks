import { Injectable } from '@nestjs/common';
import { PoolConnection, RowDataPacket } from 'mysql2/promise';
import { MysqlService } from '../db/mysql.service';

type ConsentType = 'OPT_IN' | 'OPT_OUT';
type CurrentStatus = 'UNKNOWN' | 'OPT_IN' | 'OPT_OUT';

interface MarketingCurrentRow extends RowDataPacket {
  user_id: number;
  company_id: number;
  status: CurrentStatus;
  last_opt_in_at: Date | null;
  last_opt_out_at: Date | null;
}

interface UserRow extends RowDataPacket {
  id: number;
}

@Injectable()
export class ConsentRepo {
  constructor(private readonly mysqlService: MysqlService) {}

  async findUserIdByPhone(
    connection: PoolConnection,
    phone: string,
    phoneColumn: string,
  ): Promise<number | null> {
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(phoneColumn)) {
      throw new Error('Invalid USER_PHONE_COLUMN');
    }

    const sql = `SELECT id FROM \`awer_core\`.\`user\` WHERE \`${phoneColumn}\` = ? LIMIT 1`;
    const [rows] = await connection.query<UserRow[]>(sql, [phone]);
    return rows.length > 0 ? rows[0].id : null;
  }

  async insertConsentEvent(
    connection: PoolConnection,
    userId: number,
    companyId: number,
    eventType: ConsentType,
    eventAt: Date,
  ): Promise<void> {
    const sql = `
      INSERT INTO whatsapp_marketing_consent_event (user_id, company_id, event_type, event_at)
      VALUES (?, ?, ?, ?)
    `;
    await connection.query(sql, [userId, companyId, eventType, eventAt]);
  }

  async upsertMarketingCurrent(
    connection: PoolConnection,
    userId: number,
    companyId: number,
    eventType: ConsentType,
    eventAt: Date,
  ): Promise<void> {
    const lockSql = `
      SELECT user_id, company_id, status, last_opt_in_at, last_opt_out_at
      FROM whatsapp_marketing_current
      WHERE user_id = ? AND company_id = ?
      FOR UPDATE
    `;
    const [rows] = await connection.query<MarketingCurrentRow[]>(lockSql, [userId, companyId]);
    const current = rows.length > 0 ? rows[0] : null;

    const nextLastOptIn =
      eventType === 'OPT_IN' ? this.maxDate(current?.last_opt_in_at ?? null, eventAt) : current?.last_opt_in_at ?? null;
    const nextLastOptOut =
      eventType === 'OPT_OUT'
        ? this.maxDate(current?.last_opt_out_at ?? null, eventAt)
        : current?.last_opt_out_at ?? null;
    const nextStatus = this.computeStatus(nextLastOptIn, nextLastOptOut);

    if (!current) {
      const insertSql = `
        INSERT INTO whatsapp_marketing_current (
          user_id,
          company_id,
          status,
          last_opt_in_at,
          last_opt_out_at,
          updated_at
        )
        VALUES (?, ?, ?, ?, ?, NOW())
      `;
      await connection.query(insertSql, [userId, companyId, nextStatus, nextLastOptIn, nextLastOptOut]);
      return;
    }

    const updateSql = `
      UPDATE whatsapp_marketing_current
      SET status = ?,
          last_opt_in_at = ?,
          last_opt_out_at = ?,
          updated_at = NOW()
      WHERE user_id = ? AND company_id = ?
    `;
    await connection.query(updateSql, [nextStatus, nextLastOptIn, nextLastOptOut, userId, companyId]);
  }

  private maxDate(existing: Date | null, candidate: Date): Date {
    if (!existing) {
      return candidate;
    }
    return existing.getTime() >= candidate.getTime() ? existing : candidate;
  }

  private computeStatus(lastOptIn: Date | null, lastOptOut: Date | null): CurrentStatus {
    if (lastOptIn && lastOptOut) {
      return lastOptIn.getTime() >= lastOptOut.getTime() ? 'OPT_IN' : 'OPT_OUT';
    }
    if (lastOptIn) {
      return 'OPT_IN';
    }
    if (lastOptOut) {
      return 'OPT_OUT';
    }
    return 'UNKNOWN';
  }
}
