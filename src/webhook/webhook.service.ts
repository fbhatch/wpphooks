import { Injectable } from '@nestjs/common';
import { createHash, timingSafeEqual } from 'crypto';
import { AppConfigService } from '../config/app-config.service';
import { buildPayloadPreview, extractTopLevelKeys } from '../logging/log-sanitizer.util';
import { StructuredLoggerService } from '../logging/structured-logger.service';
import { NormalizerService } from '../normalizer/normalizer.service';
import { EventKind, NormalizedWebhookEvent } from '../normalizer/normalizer.types';
import { RawRepo } from '../repos/raw.repo';

type PayloadFormat = 'json' | 'text' | 'empty';

export interface WebhookIngestResult {
  inserted: boolean;
  dedupeKey: string;
  eventKind: EventKind;
  payloadFormat: PayloadFormat;
  topLevelKeys: string[];
  normalizedSummary: Record<string, unknown>;
}

@Injectable()
export class WebhookService {
  constructor(
    private readonly appConfigService: AppConfigService,
    private readonly rawRepo: RawRepo,
    private readonly normalizerService: NormalizerService,
    private readonly logger: StructuredLoggerService,
  ) {}

  validateSecret(providedSecret: string | null): boolean {
    if (!providedSecret) {
      return false;
    }
    const expected = Buffer.from(this.appConfigService.gupshupWebhookSecret, 'utf8');
    const provided = Buffer.from(providedSecret, 'utf8');
    if (expected.length !== provided.length) {
      return false;
    }
    return timingSafeEqual(expected, provided);
  }

  async ingest(appId: string, rawBody: string): Promise<WebhookIngestResult> {
    const parsedPayload = this.parseRawBody(rawBody);
    const normalized = this.normalizerService.normalize(parsedPayload.normalizedPayload);
    const dedupeKey = this.buildDedupeKey(appId, normalized, rawBody);
    const topLevelKeys = extractTopLevelKeys(parsedPayload.normalizedPayload);
    const normalizedSummary = this.buildNormalizedSummary(normalized);
    const payloadPreview = buildPayloadPreview(
      parsedPayload.payloadJson,
      this.appConfigService.webhookPayloadPreviewChars,
    );

    const persisted = await this.rawRepo.insertRawEvent({
      appId,
      eventKind: normalized.kind,
      providerEventId: normalized.providerEventId,
      messageId: this.pickMessageId(normalized),
      whatsappMessageId: this.pickWhatsappMessageId(normalized),
      templateName: this.pickTemplateName(normalized),
      templateProviderId: this.pickTemplateProviderId(normalized),
      eventStatus: this.pickEventStatus(normalized),
      payloadJson: parsedPayload.payloadJson,
      dedupeKey,
    });

    this.logPayloadShape('webhook_payload_observed', {
      appId,
      inserted: persisted.inserted,
      eventKind: normalized.kind,
      dedupeKeyPrefix: dedupeKey.slice(0, 16),
      payloadFormat: parsedPayload.format,
      bodyBytes: Buffer.byteLength(rawBody, 'utf8'),
      topLevelKeys,
      normalized: normalizedSummary,
      payloadPreview,
    });

    if (!persisted.inserted) {
      this.logger.info('webhook_duplicate_ignored', {
        appId,
        eventKind: normalized.kind,
        dedupeKeyPrefix: dedupeKey.slice(0, 16),
      });
    }

    return {
      inserted: persisted.inserted,
      dedupeKey,
      eventKind: normalized.kind,
      payloadFormat: parsedPayload.format,
      topLevelKeys,
      normalizedSummary,
    };
  }

  private parseRawBody(rawBody: string): { normalizedPayload: unknown; payloadJson: unknown; format: PayloadFormat } {
    const trimmed = rawBody.trim();
    if (trimmed.length === 0) {
      return {
        normalizedPayload: {},
        payloadJson: { _raw: rawBody, _empty: true },
        format: 'empty',
      };
    }

    try {
      const parsed = JSON.parse(rawBody) as unknown;
      return {
        normalizedPayload: parsed,
        payloadJson: parsed,
        format: 'json',
      };
    } catch {
      return {
        normalizedPayload: { _raw: rawBody },
        payloadJson: { _raw: rawBody, _format: 'text/plain' },
        format: 'text',
      };
    }
  }

  private buildDedupeKey(appId: string, normalized: NormalizedWebhookEvent, rawBody: string): string {
    const providerEventId = normalized.providerEventId;
    const eventKind = normalized.kind;

    let material: string;
    if (providerEventId) {
      material = `${appId}|${eventKind}|${providerEventId}`;
    } else {
      const messageId = this.pickMessageId(normalized);
      const eventStatus = this.pickEventStatus(normalized);
      const timestamp = normalized.eventAt ? normalized.eventAt.toISOString() : '';
      if (messageId || eventStatus || timestamp) {
        material = `${appId}|${eventKind}|${messageId ?? ''}|${eventStatus ?? ''}|${timestamp}`;
      } else {
        material = rawBody;
      }
    }

    return createHash('sha256').update(material).digest('hex');
  }

  private pickEventStatus(normalized: NormalizedWebhookEvent): string | null {
    if (normalized.kind === 'MESSAGE') {
      return normalized.status;
    }
    if (normalized.kind === 'TEMPLATE') {
      return normalized.templateStatus;
    }
    if (normalized.kind === 'USER') {
      return normalized.consentEvent;
    }
    return null;
  }

  private pickMessageId(normalized: NormalizedWebhookEvent): string | null {
    return normalized.kind === 'MESSAGE' ? normalized.messageId : null;
  }

  private pickWhatsappMessageId(normalized: NormalizedWebhookEvent): string | null {
    return normalized.kind === 'MESSAGE' ? normalized.whatsappMessageId : null;
  }

  private pickTemplateName(normalized: NormalizedWebhookEvent): string | null {
    return normalized.kind === 'TEMPLATE' ? normalized.templateName : null;
  }

  private pickTemplateProviderId(normalized: NormalizedWebhookEvent): string | null {
    return normalized.kind === 'TEMPLATE' ? normalized.templateProviderId : null;
  }

  private buildNormalizedSummary(normalized: NormalizedWebhookEvent): Record<string, unknown> {
    if (normalized.kind === 'MESSAGE') {
      return {
        kind: normalized.kind,
        providerEventId: normalized.providerEventId,
        messageId: normalized.messageId,
        whatsappMessageId: normalized.whatsappMessageId,
        status: normalized.status,
        eventAt: normalized.eventAt?.toISOString() ?? null,
        errorCode: normalized.errorCode,
        errorReason: normalized.errorReason,
      };
    }

    if (normalized.kind === 'TEMPLATE') {
      return {
        kind: normalized.kind,
        providerEventId: normalized.providerEventId,
        templateName: normalized.templateName,
        templateProviderId: normalized.templateProviderId,
        templateStatus: normalized.templateStatus,
        language: normalized.language,
        rejectionReason: normalized.rejectionReason,
        correctCategory: normalized.correctCategory,
        eventAt: normalized.eventAt?.toISOString() ?? null,
      };
    }

    if (normalized.kind === 'USER') {
      return {
        kind: normalized.kind,
        providerEventId: normalized.providerEventId,
        consentEvent: normalized.consentEvent,
        phone: StructuredLoggerService.maskPhone(normalized.phone),
        eventAt: normalized.eventAt?.toISOString() ?? null,
      };
    }

    return {
      kind: normalized.kind,
      providerEventId: normalized.providerEventId,
      eventAt: normalized.eventAt?.toISOString() ?? null,
    };
  }

  private logPayloadShape(message: string, meta: Record<string, unknown>): void {
    if (this.appConfigService.webhookVerboseLogs) {
      this.logger.info(message, meta);
      return;
    }
    this.logger.debug(message, meta);
  }
}
