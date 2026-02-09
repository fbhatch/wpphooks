import { Injectable } from '@nestjs/common';
import { PoolConnection, RowDataPacket } from 'mysql2/promise';
import { MysqlService } from '../db/mysql.service';
import { MessageStatus, NormalizedMessageEvent } from '../normalizer/normalizer.types';

type RecipientStatus =
  | 'PENDING'
  | 'SKIPPED'
  | 'SUBMITTED'
  | 'SENT'
  | 'DELIVERED'
  | 'READ'
  | 'FAILED'
  | 'RETRYING';

interface RecipientRow extends RowDataPacket {
  id: number;
  status: RecipientStatus;
  gupshup_message_id: string | null;
  whatsapp_message_id: string | null;
  accepted_at: Date | null;
  sent_at: Date | null;
  reached_at: Date | null;
  failed_at: Date | null;
  last_event_at: Date | null;
}

const STATUS_RANK: Record<RecipientStatus, number> = {
  PENDING: 0,
  SKIPPED: 0,
  SUBMITTED: 1,
  RETRYING: 1,
  SENT: 2,
  DELIVERED: 3,
  READ: 4,
  FAILED: 5,
};

const TARGET_STATUS_BY_MESSAGE_EVENT: Record<MessageStatus, RecipientStatus> = {
  accepted: 'SUBMITTED',
  sent: 'SENT',
  delivered: 'DELIVERED',
  read: 'READ',
  failed: 'FAILED',
};

@Injectable()
export class RecipientRepo {
  constructor(private readonly mysqlService: MysqlService) {}

  async applyMessageEvent(
    connection: PoolConnection,
    event: NormalizedMessageEvent,
  ): Promise<'UPDATED' | 'NOOP' | 'NOT_FOUND'> {
    if (!event.messageId && !event.whatsappMessageId) {
      return 'NOT_FOUND';
    }

    const recipient = await this.findRecipient(connection, event.messageId, event.whatsappMessageId);
    if (!recipient) {
      return 'NOT_FOUND';
    }
    if (!event.status) {
      return 'NOOP';
    }

    const eventAt = event.eventAt ?? new Date();
    const targetStatus = TARGET_STATUS_BY_MESSAGE_EVENT[event.status];
    const transition = this.evaluateTransition(recipient.status, targetStatus, event.status);
    if (transition === 'IGNORE') {
      return 'NOOP';
    }

    const nextStatus =
      transition === 'UPGRADE' || (event.status === 'failed' && recipient.status !== 'FAILED')
        ? targetStatus
        : null;

    const updates: string[] = [];
    const params: unknown[] = [];

    if (nextStatus && nextStatus !== recipient.status) {
      updates.push('status = ?');
      params.push(nextStatus);
    }

    if (!recipient.whatsapp_message_id && event.whatsappMessageId) {
      updates.push('whatsapp_message_id = ?');
      params.push(event.whatsappMessageId);
    }

    if (transition === 'UPGRADE' && this.shouldSetLastEventAt(recipient.last_event_at, eventAt)) {
      updates.push('last_event_at = ?');
      params.push(eventAt);
    }

    if (event.status === 'accepted' && !recipient.accepted_at) {
      updates.push('accepted_at = ?');
      params.push(eventAt);
    }

    if (event.status === 'sent' && !recipient.sent_at) {
      updates.push('sent_at = ?');
      params.push(eventAt);
    }

    if ((event.status === 'delivered' || event.status === 'read') && !recipient.reached_at) {
      updates.push('reached_at = ?');
      params.push(eventAt);
    }

    if (event.status === 'failed' && recipient.status !== 'READ') {
      if (!recipient.failed_at) {
        updates.push('failed_at = ?');
        params.push(eventAt);
      }
      if (event.errorCode) {
        updates.push('last_error_code = ?');
        params.push(event.errorCode);
      }
      if (event.errorReason) {
        updates.push('last_error_reason = ?');
        params.push(event.errorReason);
      }
      if (event.errorPayload) {
        updates.push('error = CAST(? AS JSON)');
        params.push(JSON.stringify(event.errorPayload));
      }
    }

    if (updates.length === 0) {
      return 'NOOP';
    }

    updates.push('updated_at = NOW()');
    const sql = `UPDATE wpp_campaign_recipient SET ${updates.join(', ')} WHERE id = ?`;
    params.push(recipient.id);
    await connection.query(sql, params);
    return 'UPDATED';
  }

  private async findRecipient(
    connection: PoolConnection,
    messageId: string | null,
    whatsappMessageId: string | null,
  ): Promise<RecipientRow | null> {
    const baseColumns = `
      SELECT
        id,
        status,
        gupshup_message_id,
        whatsapp_message_id,
        accepted_at,
        sent_at,
        reached_at,
        failed_at,
        last_event_at
      FROM wpp_campaign_recipient
    `;

    if (messageId) {
      const [rows] = await connection.query<RecipientRow[]>(
        `${baseColumns} WHERE gupshup_message_id = ? LIMIT 1`,
        [messageId],
      );
      if (rows.length > 0) {
        return rows[0];
      }
    }

    if (whatsappMessageId) {
      const [rows] = await connection.query<RecipientRow[]>(
        `${baseColumns} WHERE whatsapp_message_id = ? LIMIT 1`,
        [whatsappMessageId],
      );
      if (rows.length > 0) {
        return rows[0];
      }
    }

    return null;
  }

  private evaluateTransition(
    current: RecipientStatus,
    target: RecipientStatus,
    sourceStatus: MessageStatus,
  ): 'UPGRADE' | 'SAME' | 'IGNORE' {
    if (sourceStatus === 'failed') {
      if (current === 'READ') {
        return 'IGNORE';
      }
      if (current === 'FAILED') {
        return 'SAME';
      }
      return 'UPGRADE';
    }

    if (current === 'FAILED') {
      return 'IGNORE';
    }

    if (STATUS_RANK[target] > STATUS_RANK[current]) {
      return 'UPGRADE';
    }

    if (STATUS_RANK[target] === STATUS_RANK[current] && target === current) {
      return 'SAME';
    }

    return 'IGNORE';
  }

  private shouldSetLastEventAt(current: Date | null, candidate: Date): boolean {
    if (!current) {
      return true;
    }
    return candidate.getTime() > current.getTime();
  }
}
