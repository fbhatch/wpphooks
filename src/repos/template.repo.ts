import { Injectable } from '@nestjs/common';
import { PoolConnection, RowDataPacket } from 'mysql2/promise';
import { MysqlService } from '../db/mysql.service';
import { TemplateStatus } from '../normalizer/normalizer.types';

interface TemplateRow extends RowDataPacket {
  id: number;
  company_id: number;
  integration_id: number;
  name: string;
  language: string | null;
}

interface TemplateVersionRow extends RowDataPacket {
  id: number;
  status: 'DRAFT' | 'SUBMITTED' | 'PENDING' | 'APPROVED' | 'REJECTED';
  submitted_at: Date | null;
  approved_at: Date | null;
  rejected_at: Date | null;
}

@Injectable()
export class TemplateRepo {
  constructor(private readonly mysqlService: MysqlService) {}

  async findTemplateByProviderId(
    connection: PoolConnection,
    integrationId: number,
    providerTemplateId: string,
  ): Promise<TemplateRow | null> {
    const sql = `
      SELECT id, company_id, integration_id, name, language
      FROM wpp_template
      WHERE integration_id = ?
        AND provider_template_id = ?
      LIMIT 1
    `;
    const [rows] = await connection.query<TemplateRow[]>(sql, [integrationId, providerTemplateId]);
    return rows.length > 0 ? rows[0] : null;
  }

  async findTemplateByName(
    connection: PoolConnection,
    companyId: number,
    templateName: string,
    language: string | null,
  ): Promise<TemplateRow | null> {
    let sql = `
      SELECT id, company_id, integration_id, name, language
      FROM wpp_template
      WHERE company_id = ?
        AND name = ?
    `;
    const params: unknown[] = [companyId, templateName];

    if (language) {
      sql += ' AND language = ?';
      params.push(language);
    }

    sql += ' ORDER BY id DESC LIMIT 1';
    const [rows] = await connection.query<TemplateRow[]>(sql, params);
    return rows.length > 0 ? rows[0] : null;
  }

  async updateTemplateStatus(
    connection: PoolConnection,
    templateId: number,
    status: TemplateStatus,
    rejectionReason: string | null,
    correctCategory: string | null,
  ): Promise<void> {
    const sql = `
      UPDATE wpp_template
      SET status = ?,
          rejection_reason = ?,
          correct_category = ?,
          last_synced_at = NOW(),
          updated_at = NOW()
      WHERE id = ?
    `;
    await connection.query(sql, [
      status,
      status === 'REJECTED' ? rejectionReason : null,
      status === 'REJECTED' ? correctCategory : null,
      templateId,
    ]);
  }

  async updateLatestTemplateVersion(
    connection: PoolConnection,
    templateId: number,
    status: TemplateStatus,
    rejectionReason: string | null,
    eventAt: Date | null,
  ): Promise<void> {
    const fetchSql = `
      SELECT id, status, submitted_at, approved_at, rejected_at
      FROM wpp_template_version
      WHERE template_id = ?
      ORDER BY version_no DESC
      LIMIT 1
      FOR UPDATE
    `;
    const [rows] = await connection.query<TemplateVersionRow[]>(fetchSql, [templateId]);
    if (rows.length === 0) {
      return;
    }

    const version = rows[0];
    const changeSet: string[] = ['status = ?'];
    const params: unknown[] = [status];
    const timestamp = eventAt ?? new Date();

    if (status === 'SUBMITTED' && !version.submitted_at) {
      changeSet.push('submitted_at = ?');
      params.push(timestamp);
    }
    if (status === 'APPROVED' && !version.approved_at) {
      changeSet.push('approved_at = ?');
      params.push(timestamp);
    }
    if (status === 'REJECTED') {
      if (!version.rejected_at) {
        changeSet.push('rejected_at = ?');
        params.push(timestamp);
      }
      changeSet.push('rejection_reason = ?');
      params.push(rejectionReason);
    }

    changeSet.push('updated_at = NOW()');
    const updateSql = `UPDATE wpp_template_version SET ${changeSet.join(', ')} WHERE id = ?`;
    params.push(version.id);
    await connection.query(updateSql, params);
  }
}
