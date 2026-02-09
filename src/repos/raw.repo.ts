import { Injectable } from '@nestjs/common';
import { ResultSetHeader, RowDataPacket } from 'mysql2/promise';
import { MysqlService } from '../db/mysql.service';
import { EventKind } from '../normalizer/normalizer.types';

export interface RawEventInsertInput {
  appId: string;
  eventKind: EventKind;
  providerEventId: string | null;
  messageId: string | null;
  whatsappMessageId: string | null;
  templateName: string | null;
  templateProviderId: string | null;
  eventStatus: string | null;
  payloadJson: unknown;
  dedupeKey: string;
}

export interface RawEventRow extends RowDataPacket {
  id: number;
  app_id: string;
  event_kind: EventKind;
  provider_event_id: string | null;
  message_id: string | null;
  whatsapp_message_id: string | null;
  template_name: string | null;
  template_provider_id: string | null;
  event_status: string | null;
  received_at: Date;
  payload_json: unknown;
  processed: number;
  attempts: number;
  last_error: string | null;
  processed_at: Date | null;
  dedupe_key: string;
}

@Injectable()
export class RawRepo {
  constructor(private readonly mysqlService: MysqlService) {}

  async insertRawEvent(input: RawEventInsertInput): Promise<{ inserted: boolean }> {
    const sql = `
      INSERT INTO wpp_webhook_event_raw (
        app_id,
        event_kind,
        provider_event_id,
        message_id,
        whatsapp_message_id,
        template_name,
        template_provider_id,
        event_status,
        payload_json,
        dedupe_key
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS JSON), ?)
      ON DUPLICATE KEY UPDATE id = id
    `;

    const [result] = await this.mysqlService.getPool().query<ResultSetHeader>(sql, [
      input.appId,
      input.eventKind,
      input.providerEventId,
      input.messageId,
      input.whatsappMessageId,
      input.templateName,
      input.templateProviderId,
      input.eventStatus,
      JSON.stringify(input.payloadJson),
      input.dedupeKey,
    ]);

    return {
      inserted: result.affectedRows === 1,
    };
  }

  async lockNextBatch(connection: import('mysql2/promise').PoolConnection, batchSize: number): Promise<RawEventRow[]> {
    const sql = `
      SELECT
        id,
        app_id,
        event_kind,
        provider_event_id,
        message_id,
        whatsapp_message_id,
        template_name,
        template_provider_id,
        event_status,
        received_at,
        payload_json,
        processed,
        attempts,
        last_error,
        processed_at,
        dedupe_key
      FROM wpp_webhook_event_raw
      WHERE processed = 0
      ORDER BY received_at ASC
      LIMIT ?
      FOR UPDATE SKIP LOCKED
    `;

    const [rows] = await connection.query<RawEventRow[]>(sql, [batchSize]);
    return rows;
  }

  async markProcessed(
    connection: import('mysql2/promise').PoolConnection,
    id: number,
    lastError: string | null,
  ): Promise<void> {
    const sql = `
      UPDATE wpp_webhook_event_raw
      SET processed = 1,
          processed_at = NOW(),
          last_error = ?
      WHERE id = ?
    `;
    await connection.query(sql, [this.trimError(lastError), id]);
  }

  async markFailedAttempt(
    connection: import('mysql2/promise').PoolConnection,
    id: number,
    attempts: number,
    errorMessage: string,
    finalize: boolean,
  ): Promise<void> {
    if (finalize) {
      const finalizeSql = `
        UPDATE wpp_webhook_event_raw
        SET processed = 1,
            processed_at = NOW(),
            attempts = ?,
            last_error = ?
        WHERE id = ?
      `;
      await connection.query(finalizeSql, [attempts, this.trimError(errorMessage), id]);
      return;
    }

    const retrySql = `
      UPDATE wpp_webhook_event_raw
      SET processed = 0,
          attempts = ?,
          last_error = ?
      WHERE id = ?
    `;
    await connection.query(retrySql, [attempts, this.trimError(errorMessage), id]);
  }

  parsePayloadJson(payload: unknown): unknown {
    if (payload === null || payload === undefined) {
      return null;
    }

    if (typeof payload === 'string') {
      try {
        return JSON.parse(payload);
      } catch {
        return { _raw: payload };
      }
    }

    if (Buffer.isBuffer(payload)) {
      const raw = payload.toString('utf8');
      try {
        return JSON.parse(raw);
      } catch {
        return { _raw: raw };
      }
    }

    return payload;
  }

  private trimError(message: string | null): string | null {
    if (!message) {
      return null;
    }
    if (message.length <= 255) {
      return message;
    }
    return message.slice(0, 252) + '...';
  }
}
