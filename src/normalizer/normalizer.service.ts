import { Injectable } from '@nestjs/common';
import {
  ConsentEvent,
  MessageStatus,
  NormalizedMessageEvent,
  NormalizedTemplateEvent,
  NormalizedUnknownEvent,
  NormalizedUserEvent,
  NormalizedWebhookEvent,
  TemplateStatus,
} from './normalizer.types';

@Injectable()
export class NormalizerService {
  normalize(payload: unknown): NormalizedWebhookEvent {
    const templateEvent = this.normalizeTemplate(payload);
    if (templateEvent) {
      return templateEvent;
    }

    const messageEvent = this.normalizeMessage(payload);
    if (messageEvent) {
      return messageEvent;
    }

    const userEvent = this.normalizeUser(payload);
    if (userEvent) {
      return userEvent;
    }

    const fallback: NormalizedUnknownEvent = {
      kind: 'UNKNOWN',
      providerEventId: this.extractProviderEventId(payload),
      eventAt: this.parseTimestamp(this.pickFirst(payload, ['timestamp', 'time', 'eventTime', 'event_ts'])),
    };
    return fallback;
  }

  private normalizeMessage(payload: unknown): NormalizedMessageEvent | null {
    const messageId = this.extractString(
      this.pickFirst(payload, [
        'messages[0].id',
        'statuses[0].id',
        'message.id',
        'messageId',
        'message_id',
        'payload.messages[0].id',
        'payload.statuses[0].id',
        'data.messages[0].id',
        'data.statuses[0].id',
      ]) ?? this.findByKeys(payload, ['message_id', 'messageId']),
    );

    const whatsappMessageId = this.extractString(
      this.pickFirst(payload, [
        'messages[0].whatsappMessageId',
        'messages[0].waMessageId',
        'statuses[0].whatsappMessageId',
        'statuses[0].waMessageId',
        'whatsapp_message_id',
        'whatsappMessageId',
        'wa_message_id',
        'waMessageId',
      ]) ?? this.findByKeys(payload, ['whatsapp_message_id', 'whatsappMessageId', 'waMessageId', 'wa_id']),
    );

    const status = this.mapMessageStatus(
      this.extractString(
        this.pickFirst(payload, [
          'statuses[0].status',
          'status',
          'eventStatus',
          'event_status',
          'message.status',
          'messages[0].status',
          'data.status',
        ]) ?? this.findByKeys(payload, ['status', 'eventStatus', 'event_status']),
      ),
    );

    const templateHint =
      this.extractString(
        this.pickFirst(payload, ['template.name', 'templateName', 'template_name', 'elementName']) ??
          this.findByKeys(payload, ['template_name', 'templateName', 'elementName']),
      ) ?? null;

    const hasMessageSignals = Boolean(messageId || whatsappMessageId || status);
    if (!messageId && !whatsappMessageId && templateHint) {
      return null;
    }
    if (!hasMessageSignals) {
      return null;
    }

    const errorsNode =
      this.pickFirst(payload, ['statuses[0].errors[0]', 'messages[0].errors[0]', 'error', 'errors[0]']) ??
      this.findByKeys(payload, ['error', 'errors']);
    const errorObject = this.toRecord(errorsNode);

    const errorCode =
      this.extractString(
        this.pickFirst(payload, [
          'statuses[0].errors[0].code',
          'messages[0].errors[0].code',
          'errorCode',
          'error_code',
        ]) ?? this.findByKeys(errorsNode, ['code', 'errorCode', 'error_code']),
      ) ?? null;

    const errorReason =
      this.extractString(
        this.pickFirst(payload, [
          'statuses[0].errors[0].message',
          'messages[0].errors[0].message',
          'reason',
          'errorReason',
          'error_reason',
        ]) ?? this.findByKeys(errorsNode, ['message', 'reason', 'errorReason', 'error_reason']),
      ) ?? null;

    return {
      kind: 'MESSAGE',
      providerEventId: this.extractProviderEventId(payload),
      messageId: messageId ?? null,
      whatsappMessageId: whatsappMessageId ?? null,
      status,
      eventAt: this.parseTimestamp(
        this.pickFirst(payload, [
          'statuses[0].timestamp',
          'messages[0].timestamp',
          'timestamp',
          'time',
          'eventTime',
          'event_ts',
        ]) ?? this.findByKeys(payload, ['timestamp', 'eventTime', 'event_ts']),
      ),
      errorCode,
      errorReason,
      errorPayload: errorObject,
    };
  }

  private normalizeTemplate(payload: unknown): NormalizedTemplateEvent | null {
    const templateStatus = this.mapTemplateStatus(
      this.extractString(
        this.pickFirst(payload, [
          'template.status',
          'status',
          'eventStatus',
          'event_status',
          'templateStatus',
          'approvalStatus',
        ]) ?? this.findByKeys(payload, ['templateStatus', 'approvalStatus', 'status', 'event_status']),
      ),
    );

    const templateName =
      this.extractString(
        this.pickFirst(payload, [
          'template.name',
          'template_name',
          'templateName',
          'elementName',
          'name',
          'payload.template_name',
        ]) ?? this.findByKeys(payload, ['template_name', 'templateName', 'elementName']),
      ) ?? null;

    const templateProviderId =
      this.extractString(
        this.pickFirst(payload, [
          'template.id',
          'template_provider_id',
          'templateProviderId',
          'providerTemplateId',
        ]) ?? this.findByKeys(payload, ['template_provider_id', 'templateProviderId', 'providerTemplateId']),
      ) ?? null;

    const templateSignal = Boolean(templateStatus || templateName || templateProviderId);
    if (!templateSignal) {
      return null;
    }

    const eventTypeHint = this.extractString(this.pickFirst(payload, ['event', 'eventType', 'type']))?.toLowerCase();
    if (!templateStatus && !eventTypeHint?.includes('template')) {
      return null;
    }

    return {
      kind: 'TEMPLATE',
      providerEventId: this.extractProviderEventId(payload),
      templateName,
      templateProviderId,
      templateStatus,
      language:
        this.extractString(
          this.pickFirst(payload, ['template.language', 'language', 'lang']) ??
            this.findByKeys(payload, ['language', 'lang']),
        ) ?? null,
      rejectionReason:
        this.extractString(
          this.pickFirst(payload, [
            'template.rejectionReason',
            'rejectionReason',
            'rejection_reason',
            'reason',
          ]) ?? this.findByKeys(payload, ['rejectionReason', 'rejection_reason', 'reason']),
        ) ?? null,
      correctCategory:
        this.extractString(
          this.pickFirst(payload, ['template.correctCategory', 'correctCategory', 'correct_category']) ??
            this.findByKeys(payload, ['correctCategory', 'correct_category']),
        ) ?? null,
      eventAt: this.parseTimestamp(
        this.pickFirst(payload, ['timestamp', 'time', 'eventTime', 'event_ts']) ??
          this.findByKeys(payload, ['timestamp', 'eventTime', 'event_ts']),
      ),
    };
  }

  private normalizeUser(payload: unknown): NormalizedUserEvent | null {
    const consentEvent = this.mapConsentEvent(
      this.extractString(
        this.pickFirst(payload, ['event', 'eventType', 'status', 'consent', 'opt']) ??
          this.findByKeys(payload, ['event', 'eventType', 'consent', 'opt', 'status']),
      ),
    );
    const phone =
      this.extractString(
        this.pickFirst(payload, [
          'phone',
          'phone_number',
          'msisdn',
          'user.phone',
          'user.phone_number',
          'payload.phone',
          'payload.msisdn',
          'data.phone',
        ]) ?? this.findByKeys(payload, ['phone', 'phone_number', 'msisdn']),
      ) ?? null;

    if (!consentEvent && !phone) {
      return null;
    }

    return {
      kind: 'USER',
      providerEventId: this.extractProviderEventId(payload),
      phone: this.normalizePhone(phone),
      consentEvent,
      eventAt: this.parseTimestamp(
        this.pickFirst(payload, ['timestamp', 'time', 'eventTime', 'event_ts']) ??
          this.findByKeys(payload, ['timestamp', 'eventTime', 'event_ts']),
      ),
    };
  }

  private extractProviderEventId(payload: unknown): string | null {
    return (
      this.extractString(
        this.pickFirst(payload, ['eventId', 'event_id', 'event.id', 'gs_event_id']) ??
          this.findByKeys(payload, ['eventId', 'event_id', 'gs_event_id']),
      ) ?? null
    );
  }

  private mapMessageStatus(value: string | null | undefined): MessageStatus | null {
    if (!value) {
      return null;
    }
    const normalized = value.trim().toLowerCase();
    if (normalized === 'accepted') {
      return 'accepted';
    }
    if (normalized === 'sent') {
      return 'sent';
    }
    if (normalized === 'delivered') {
      return 'delivered';
    }
    if (normalized === 'read') {
      return 'read';
    }
    if (['failed', 'error', 'undelivered'].includes(normalized)) {
      return 'failed';
    }
    return null;
  }

  private mapTemplateStatus(value: string | null | undefined): TemplateStatus | null {
    if (!value) {
      return null;
    }
    const normalized = value.trim().toUpperCase();
    if (normalized === 'APPROVED') {
      return 'APPROVED';
    }
    if (normalized === 'REJECTED') {
      return 'REJECTED';
    }
    if (normalized === 'PENDING') {
      return 'PENDING';
    }
    if (['SUBMITTED', 'IN_REVIEW'].includes(normalized)) {
      return 'SUBMITTED';
    }
    return null;
  }

  private mapConsentEvent(value: string | null | undefined): ConsentEvent | null {
    if (!value) {
      return null;
    }
    const normalized = value.trim().toUpperCase();
    if (['OPT_IN', 'SUBSCRIBE', 'CONSENT_GRANTED'].includes(normalized)) {
      return 'OPT_IN';
    }
    if (['OPT_OUT', 'UNSUBSCRIBE', 'CONSENT_REVOKED'].includes(normalized)) {
      return 'OPT_OUT';
    }
    if (['BLOCKED', 'BLOCK', 'USER_BLOCKED'].includes(normalized)) {
      return 'BLOCKED';
    }
    return null;
  }

  private parseTimestamp(value: unknown): Date | null {
    if (value === null || value === undefined) {
      return null;
    }

    if (value instanceof Date) {
      return Number.isNaN(value.getTime()) ? null : value;
    }

    if (typeof value === 'number') {
      return this.fromEpoch(value);
    }

    if (typeof value === 'string') {
      const trimmed = value.trim();
      if (trimmed.length === 0) {
        return null;
      }
      if (/^\d+$/.test(trimmed)) {
        return this.fromEpoch(Number(trimmed));
      }
      const parsed = new Date(trimmed);
      return Number.isNaN(parsed.getTime()) ? null : parsed;
    }

    return null;
  }

  private fromEpoch(value: number): Date | null {
    if (!Number.isFinite(value)) {
      return null;
    }
    const milliseconds = value > 9999999999 ? value : value * 1000;
    const date = new Date(milliseconds);
    return Number.isNaN(date.getTime()) ? null : date;
  }

  private pickFirst(payload: unknown, paths: string[]): unknown {
    for (const path of paths) {
      const value = this.getByPath(payload, path);
      if (!this.isEmpty(value)) {
        return value;
      }
    }
    return undefined;
  }

  private getByPath(payload: unknown, path: string): unknown {
    if (!payload || typeof payload !== 'object') {
      return undefined;
    }
    const normalizedPath = path.replace(/\[(\d+)\]/g, '.$1');
    const parts = normalizedPath.split('.');
    let cursor: unknown = payload;

    for (const part of parts) {
      if (cursor === null || cursor === undefined) {
        return undefined;
      }
      if (typeof cursor !== 'object') {
        return undefined;
      }
      const node = cursor as Record<string, unknown>;
      cursor = node[part];
    }

    return cursor;
  }

  private findByKeys(payload: unknown, keys: string[]): unknown {
    if (!payload || typeof payload !== 'object') {
      return undefined;
    }
    const wanted = new Set(keys.map((key) => key.toLowerCase()));
    const queue: unknown[] = [payload];

    while (queue.length > 0) {
      const node = queue.shift();
      if (!node || typeof node !== 'object') {
        continue;
      }

      if (Array.isArray(node)) {
        queue.push(...node);
        continue;
      }

      const record = node as Record<string, unknown>;
      for (const [key, value] of Object.entries(record)) {
        if (wanted.has(key.toLowerCase()) && !this.isEmpty(value)) {
          return value;
        }
        if (value && typeof value === 'object') {
          queue.push(value);
        }
      }
    }

    return undefined;
  }

  private extractString(value: unknown): string | null {
    if (value === null || value === undefined) {
      return null;
    }
    if (typeof value === 'string') {
      const trimmed = value.trim();
      return trimmed.length > 0 ? trimmed : null;
    }
    if (typeof value === 'number' || typeof value === 'boolean') {
      return String(value);
    }
    return null;
  }

  private isEmpty(value: unknown): boolean {
    if (value === null || value === undefined) {
      return true;
    }
    if (typeof value === 'string') {
      return value.trim().length === 0;
    }
    if (Array.isArray(value)) {
      return value.length === 0;
    }
    return false;
  }

  private toRecord(value: unknown): Record<string, unknown> | null {
    if (!value || typeof value !== 'object') {
      return null;
    }
    if (Array.isArray(value)) {
      if (value.length === 0) {
        return null;
      }
      const first = value[0];
      if (!first || typeof first !== 'object') {
        return { value: first };
      }
      return first as Record<string, unknown>;
    }
    return value as Record<string, unknown>;
  }

  private normalizePhone(value: string | null): string | null {
    if (!value) {
      return null;
    }
    const cleaned = value.replace(/\s+/g, '');
    return cleaned.length > 0 ? cleaned : null;
  }
}
