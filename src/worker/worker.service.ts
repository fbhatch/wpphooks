import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { PoolConnection } from 'mysql2/promise';
import { AppConfigService } from '../config/app-config.service';
import { MysqlService } from '../db/mysql.service';
import { buildPayloadPreview, extractTopLevelKeys } from '../logging/log-sanitizer.util';
import { StructuredLoggerService } from '../logging/structured-logger.service';
import { NormalizerService } from '../normalizer/normalizer.service';
import {
  MessageStatus,
  NormalizedMessageEvent,
  NormalizedTemplateEvent,
  NormalizedUserEvent,
  NormalizedWebhookEvent,
} from '../normalizer/normalizer.types';
import { ConsentRepo } from '../repos/consent.repo';
import { IntegrationRepo } from '../repos/integration.repo';
import { RawEventRow, RawRepo } from '../repos/raw.repo';
import { RecipientRepo } from '../repos/recipient.repo';
import { TemplateRepo } from '../repos/template.repo';

@Injectable()
export class WorkerService implements OnModuleInit, OnModuleDestroy {
  private timer: NodeJS.Timeout | null = null;
  private running = false;

  constructor(
    private readonly appConfigService: AppConfigService,
    private readonly mysqlService: MysqlService,
    private readonly rawRepo: RawRepo,
    private readonly normalizerService: NormalizerService,
    private readonly integrationRepo: IntegrationRepo,
    private readonly recipientRepo: RecipientRepo,
    private readonly templateRepo: TemplateRepo,
    private readonly consentRepo: ConsentRepo,
    private readonly logger: StructuredLoggerService,
  ) {}

  onModuleInit(): void {
    const { intervalMs, batchSize } = this.appConfigService.worker;
    this.timer = setInterval(() => {
      void this.safeTick();
    }, intervalMs);
    this.timer.unref();
    void this.safeTick();
    this.logger.info('worker_started', { intervalMs, batchSize });
  }

  onModuleDestroy(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  private async safeTick(): Promise<void> {
    if (this.running) {
      return;
    }

    this.running = true;
    try {
      await this.processBatch();
    } catch (error) {
      const reason = error instanceof Error ? error.message : 'unknown_error';
      this.logger.error('worker_tick_failed', { reason });
    } finally {
      this.running = false;
    }
  }

  private async processBatch(): Promise<void> {
    const connection = await this.mysqlService.getConnection();
    try {
      await connection.beginTransaction();
      const rows = await this.rawRepo.lockNextBatch(connection, this.appConfigService.worker.batchSize);
      if (rows.length === 0) {
        await connection.commit();
        return;
      }
      this.logVerbose('worker_batch_locked', {
        count: rows.length,
        ids: rows.slice(0, 10).map((item) => item.id),
        kinds: this.countKinds(rows),
      });

      for (const row of rows) {
        await this.processSingleRow(connection, row);
      }

      await connection.commit();
      this.logVerbose('worker_batch_processed', { count: rows.length });
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
  }

  private async processSingleRow(connection: PoolConnection, row: RawEventRow): Promise<void> {
    const payload = this.rawRepo.parsePayloadJson(row.payload_json);
    const normalized = this.normalizerService.normalize(payload);
    const normalizedSummary = this.summarizeNormalized(normalized);
    this.logVerbose('worker_event_processing_started', {
      rawId: row.id,
      appId: row.app_id,
      eventKind: row.event_kind,
      attempts: row.attempts,
      receivedAt: row.received_at,
      topLevelKeys: extractTopLevelKeys(payload),
      normalized: normalizedSummary,
      payloadPreview: buildPayloadPreview(payload, this.appConfigService.webhookPayloadPreviewChars),
    });

    try {
      switch (row.event_kind) {
        case 'MESSAGE':
          await this.processMessageEvent(connection, row, normalized.kind === 'MESSAGE' ? normalized : null);
          break;
        case 'TEMPLATE':
          await this.processTemplateEvent(connection, row, normalized.kind === 'TEMPLATE' ? normalized : null);
          break;
        case 'USER':
          await this.processUserEvent(connection, row, normalized.kind === 'USER' ? normalized : null);
          break;
        default:
          this.logger.warn('worker_event_unrecognized_kind', {
            rawId: row.id,
            eventKind: row.event_kind,
          });
          await this.rawRepo.markProcessed(connection, row.id, 'Unrecognized payload');
          break;
      }
    } catch (error) {
      const attempts = row.attempts + 1;
      const finalize = attempts > this.appConfigService.worker.maxAttempts;
      const reason = error instanceof Error ? error.message : 'unknown_error';
      await this.rawRepo.markFailedAttempt(connection, row.id, attempts, reason, finalize);
      this.logger.warn('worker_row_failed', {
        rawId: row.id,
        appId: row.app_id,
        eventKind: row.event_kind,
        attempts,
        finalized: finalize,
        reason,
        normalized: normalizedSummary,
      });
    }
  }

  private async processMessageEvent(
    connection: PoolConnection,
    row: RawEventRow,
    normalizedEvent: NormalizedMessageEvent | null,
  ): Promise<void> {
    const messageStatus = normalizedEvent?.status ?? this.parseMessageStatus(row.event_status);
    const event: NormalizedMessageEvent = {
      kind: 'MESSAGE',
      providerEventId: normalizedEvent?.providerEventId ?? row.provider_event_id,
      messageId: normalizedEvent?.messageId ?? row.message_id,
      whatsappMessageId: normalizedEvent?.whatsappMessageId ?? row.whatsapp_message_id,
      status: messageStatus,
      eventAt: normalizedEvent?.eventAt ?? null,
      errorCode: normalizedEvent?.errorCode ?? null,
      errorReason: normalizedEvent?.errorReason ?? null,
      errorPayload: normalizedEvent?.errorPayload ?? null,
    };

    if (!event.status) {
      this.logger.warn('worker_message_unrecognized_payload', {
        rawId: row.id,
        appId: row.app_id,
        messageId: event.messageId,
        whatsappMessageId: event.whatsappMessageId,
        eventStatus: row.event_status,
      });
      await this.rawRepo.markProcessed(connection, row.id, 'Unrecognized payload');
      return;
    }

    this.logVerbose('worker_message_normalized', {
      rawId: row.id,
      appId: row.app_id,
      providerEventId: event.providerEventId,
      messageId: event.messageId,
      whatsappMessageId: event.whatsappMessageId,
      status: event.status,
      eventAt: event.eventAt?.toISOString() ?? null,
      errorCode: event.errorCode,
      errorReason: event.errorReason,
    });

    const result = await this.recipientRepo.applyMessageEvent(connection, event);
    if (result === 'NOT_FOUND') {
      this.logger.warn('worker_message_recipient_not_found', {
        rawId: row.id,
        appId: row.app_id,
        messageId: event.messageId,
        whatsappMessageId: event.whatsappMessageId,
      });
      await this.rawRepo.markProcessed(connection, row.id, 'Recipient not found');
      return;
    }

    this.logVerbose('worker_message_applied', {
      rawId: row.id,
      appId: row.app_id,
      messageId: event.messageId,
      whatsappMessageId: event.whatsappMessageId,
      status: event.status,
      applyResult: result,
    });

    await this.rawRepo.markProcessed(connection, row.id, null);
  }

  private async processTemplateEvent(
    connection: PoolConnection,
    row: RawEventRow,
    normalizedEvent: NormalizedTemplateEvent | null,
  ): Promise<void> {
    const integration = await this.integrationRepo.findActiveByAppId(row.app_id, connection);
    if (!integration) {
      this.logger.warn('worker_template_integration_not_found', {
        rawId: row.id,
        appId: row.app_id,
      });
      await this.rawRepo.markProcessed(connection, row.id, 'Integration not found for appId');
      return;
    }

    const templateStatus = normalizedEvent?.templateStatus ?? this.parseTemplateStatus(row.event_status);
    const event: NormalizedTemplateEvent = {
      kind: 'TEMPLATE',
      providerEventId: normalizedEvent?.providerEventId ?? row.provider_event_id,
      templateName: normalizedEvent?.templateName ?? row.template_name,
      templateProviderId: normalizedEvent?.templateProviderId ?? row.template_provider_id,
      templateStatus,
      language: normalizedEvent?.language ?? null,
      rejectionReason: normalizedEvent?.rejectionReason ?? null,
      correctCategory: normalizedEvent?.correctCategory ?? null,
      eventAt: normalizedEvent?.eventAt ?? null,
    };

    if (!event.templateStatus) {
      this.logger.warn('worker_template_unrecognized_payload', {
        rawId: row.id,
        appId: row.app_id,
        templateName: event.templateName,
        templateProviderId: event.templateProviderId,
        eventStatus: row.event_status,
      });
      await this.rawRepo.markProcessed(connection, row.id, 'Unrecognized payload');
      return;
    }

    this.logVerbose('worker_template_normalized', {
      rawId: row.id,
      appId: row.app_id,
      integrationId: integration.id,
      companyId: integration.company_id,
      templateName: event.templateName,
      templateProviderId: event.templateProviderId,
      status: event.templateStatus,
      language: event.language,
      eventAt: event.eventAt?.toISOString() ?? null,
    });

    let template = null;
    if (event.templateProviderId) {
      template = await this.templateRepo.findTemplateByProviderId(
        connection,
        integration.id,
        event.templateProviderId,
      );
    }

    if (!template && event.templateName) {
      template = await this.templateRepo.findTemplateByName(
        connection,
        integration.company_id,
        event.templateName,
        event.language,
      );
    }

    if (!template) {
      this.logger.warn('worker_template_not_found', {
        rawId: row.id,
        appId: row.app_id,
        integrationId: integration.id,
        companyId: integration.company_id,
        templateName: event.templateName,
        templateProviderId: event.templateProviderId,
      });
      await this.rawRepo.markProcessed(connection, row.id, 'Template not found');
      return;
    }

    await this.templateRepo.updateTemplateStatus(
      connection,
      template.id,
      event.templateStatus,
      event.rejectionReason,
      event.correctCategory,
    );
    await this.templateRepo.updateLatestTemplateVersion(
      connection,
      template.id,
      event.templateStatus,
      event.rejectionReason,
      event.eventAt,
    );
    this.logVerbose('worker_template_applied', {
      rawId: row.id,
      appId: row.app_id,
      templateId: template.id,
      templateName: template.name,
      templateStatus: event.templateStatus,
      integrationId: integration.id,
      companyId: integration.company_id,
    });
    await this.rawRepo.markProcessed(connection, row.id, null);
  }

  private async processUserEvent(
    connection: PoolConnection,
    row: RawEventRow,
    normalizedEvent: NormalizedUserEvent | null,
  ): Promise<void> {
    const integration = await this.integrationRepo.findActiveByAppId(row.app_id, connection);
    if (!integration) {
      this.logger.warn('worker_user_integration_not_found', {
        rawId: row.id,
        appId: row.app_id,
      });
      await this.rawRepo.markProcessed(connection, row.id, 'Integration not found for appId');
      return;
    }

    const event: NormalizedUserEvent = {
      kind: 'USER',
      providerEventId: normalizedEvent?.providerEventId ?? row.provider_event_id,
      phone: normalizedEvent?.phone ?? null,
      consentEvent: normalizedEvent?.consentEvent ?? this.parseConsentEvent(row.event_status),
      eventAt: normalizedEvent?.eventAt ?? null,
    };

    if (!event.phone) {
      this.logger.warn('worker_user_phone_missing', {
        rawId: row.id,
        appId: row.app_id,
      });
      await this.rawRepo.markProcessed(connection, row.id, 'User not found for phone');
      return;
    }
    if (!event.consentEvent) {
      this.logger.warn('worker_user_unrecognized_payload', {
        rawId: row.id,
        appId: row.app_id,
        phone: StructuredLoggerService.maskPhone(event.phone),
        eventStatus: row.event_status,
      });
      await this.rawRepo.markProcessed(connection, row.id, 'Unrecognized payload');
      return;
    }

    this.logVerbose('worker_user_normalized', {
      rawId: row.id,
      appId: row.app_id,
      companyId: integration.company_id,
      phone: StructuredLoggerService.maskPhone(event.phone),
      consentEvent: event.consentEvent,
      eventAt: event.eventAt?.toISOString() ?? null,
    });

    const normalizedConsent = this.normalizeConsentEvent(event.consentEvent);
    if (!normalizedConsent) {
      this.logger.info('worker_user_blocked_ignored_by_config', {
        rawId: row.id,
        appId: row.app_id,
        companyId: integration.company_id,
        phone: StructuredLoggerService.maskPhone(event.phone),
      });
      await this.rawRepo.markProcessed(connection, row.id, 'Blocked event ignored by configuration');
      return;
    }

    const userId = await this.consentRepo.findUserIdByPhone(
      connection,
      event.phone,
      this.appConfigService.userPhoneColumn,
    );
    if (!userId) {
      await this.rawRepo.markProcessed(connection, row.id, 'User not found for phone');
      this.logger.warn('consent_user_not_found', {
        companyId: integration.company_id,
        phone: StructuredLoggerService.maskPhone(event.phone),
      });
      return;
    }

    const eventAt = event.eventAt ?? new Date();
    await this.consentRepo.insertConsentEvent(
      connection,
      userId,
      integration.company_id,
      normalizedConsent,
      eventAt,
    );
    await this.consentRepo.upsertMarketingCurrent(
      connection,
      userId,
      integration.company_id,
      normalizedConsent,
      eventAt,
    );
    this.logVerbose('worker_user_applied', {
      rawId: row.id,
      appId: row.app_id,
      companyId: integration.company_id,
      userId,
      phone: StructuredLoggerService.maskPhone(event.phone),
      consentEvent: normalizedConsent,
      eventAt: eventAt.toISOString(),
    });
    await this.rawRepo.markProcessed(connection, row.id, null);
  }

  private parseMessageStatus(value: string | null): MessageStatus | null {
    if (!value) {
      return null;
    }
    const normalized = value.toLowerCase();
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
    if (normalized === 'failed') {
      return 'failed';
    }
    return null;
  }

  private parseTemplateStatus(value: string | null): 'APPROVED' | 'REJECTED' | 'PENDING' | 'SUBMITTED' | null {
    if (!value) {
      return null;
    }
    const normalized = value.toUpperCase();
    if (['APPROVED', 'REJECTED', 'PENDING', 'SUBMITTED'].includes(normalized)) {
      return normalized as 'APPROVED' | 'REJECTED' | 'PENDING' | 'SUBMITTED';
    }
    return null;
  }

  private parseConsentEvent(value: string | null): 'OPT_IN' | 'OPT_OUT' | 'BLOCKED' | null {
    if (!value) {
      return null;
    }
    const normalized = value.toUpperCase();
    if (['OPT_IN', 'OPT_OUT', 'BLOCKED'].includes(normalized)) {
      return normalized as 'OPT_IN' | 'OPT_OUT' | 'BLOCKED';
    }
    return null;
  }

  private normalizeConsentEvent(event: 'OPT_IN' | 'OPT_OUT' | 'BLOCKED'): 'OPT_IN' | 'OPT_OUT' | null {
    if (event === 'OPT_IN') {
      return 'OPT_IN';
    }
    if (event === 'OPT_OUT') {
      return 'OPT_OUT';
    }
    if (event === 'BLOCKED') {
      return this.appConfigService.blockedAsOptOut ? 'OPT_OUT' : null;
    }
    return null;
  }

  private logVerbose(message: string, meta: Record<string, unknown>): void {
    if (this.appConfigService.webhookVerboseLogs) {
      this.logger.info(message, meta);
      return;
    }
    this.logger.debug(message, meta);
  }

  private summarizeNormalized(event: NormalizedWebhookEvent): Record<string, unknown> {
    if (event.kind === 'MESSAGE') {
      return {
        kind: event.kind,
        providerEventId: event.providerEventId,
        messageId: event.messageId,
        whatsappMessageId: event.whatsappMessageId,
        status: event.status,
        eventAt: event.eventAt?.toISOString() ?? null,
      };
    }

    if (event.kind === 'TEMPLATE') {
      return {
        kind: event.kind,
        providerEventId: event.providerEventId,
        templateName: event.templateName,
        templateProviderId: event.templateProviderId,
        templateStatus: event.templateStatus,
        language: event.language,
        eventAt: event.eventAt?.toISOString() ?? null,
      };
    }

    if (event.kind === 'USER') {
      return {
        kind: event.kind,
        providerEventId: event.providerEventId,
        phone: StructuredLoggerService.maskPhone(event.phone),
        consentEvent: event.consentEvent,
        eventAt: event.eventAt?.toISOString() ?? null,
      };
    }

    return {
      kind: event.kind,
      providerEventId: event.providerEventId,
      eventAt: event.eventAt?.toISOString() ?? null,
    };
  }

  private countKinds(rows: RawEventRow[]): Record<string, number> {
    const counts: Record<string, number> = {};
    for (const row of rows) {
      counts[row.event_kind] = (counts[row.event_kind] ?? 0) + 1;
    }
    return counts;
  }
}
