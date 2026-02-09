import {
  Controller,
  Get,
  Headers,
  HttpCode,
  InternalServerErrorException,
  Param,
  Post,
  Req,
  UnauthorizedException,
} from '@nestjs/common';
import { FastifyRequest } from 'fastify';
import { StructuredLoggerService } from '../logging/structured-logger.service';
import { WebhookService } from './webhook.service';

interface RawBodyRequest extends FastifyRequest {
  rawBody?: Buffer;
}

@Controller()
export class WebhookController {
  constructor(
    private readonly webhookService: WebhookService,
    private readonly logger: StructuredLoggerService,
  ) {}

  @Get('health')
  @HttpCode(200)
  health(): string {
    return 'ok';
  }

  @Post('webhooks/gupshup/:appId/events')
  @HttpCode(200)
  async receiveGupshupWebhook(
    @Param('appId') appId: string,
    @Headers('x-gupshup-secret') secretHeader: string | string[] | undefined,
    @Req() request: RawBodyRequest,
  ): Promise<{ ok: true }> {
    const startedAt = Date.now();
    const contentType = this.headerToString(request.headers['content-type']) ?? 'unknown';
    const contentLength = this.parseContentLength(this.headerToString(request.headers['content-length']));
    const userAgent = this.truncate(this.headerToString(request.headers['user-agent']), 180);
    const requestId = this.headerToString(request.headers['x-request-id']) ?? null;

    this.logger.info('webhook_request_received', {
      appId,
      path: request.url,
      method: request.method,
      ip: request.ip,
      requestId,
      contentType,
      contentLength,
      userAgent,
    });

    const secret = Array.isArray(secretHeader) ? secretHeader[0] : secretHeader ?? null;
    if (!this.webhookService.validateSecret(secret)) {
      this.logger.warn('webhook_secret_rejected', {
        appId,
        ip: request.ip,
        requestId,
        providedSecretLength: secret?.length ?? 0,
      });
      throw new UnauthorizedException('Invalid webhook secret');
    }

    const rawBody = this.extractRawBody(request);
    try {
      const result = await this.webhookService.ingest(appId, rawBody);
      const latencyMs = Date.now() - startedAt;
      this.logger.info('webhook_acknowledged', {
        appId,
        eventKind: result.eventKind,
        inserted: result.inserted,
        payloadFormat: result.payloadFormat,
        topLevelKeys: result.topLevelKeys,
        normalized: result.normalizedSummary,
        dedupeKeyPrefix: result.dedupeKey.slice(0, 16),
        latencyMs,
        requestId,
      });
      return { ok: true };
    } catch (error) {
      const reason = error instanceof Error ? error.message : 'unknown_error';
      const latencyMs = Date.now() - startedAt;
      this.logger.error('webhook_ingest_failed', { appId, reason, latencyMs, requestId });
      throw new InternalServerErrorException('Unable to ingest webhook');
    }
  }

  private extractRawBody(request: RawBodyRequest): string {
    if (typeof request.body === 'string') {
      return request.body;
    }
    if (Buffer.isBuffer(request.body)) {
      return request.body.toString('utf8');
    }
    if (request.rawBody) {
      return request.rawBody.toString('utf8');
    }
    if (request.body === null || request.body === undefined) {
      return '';
    }
    return JSON.stringify(request.body);
  }

  private headerToString(header: string | string[] | undefined): string | null {
    if (!header) {
      return null;
    }
    if (Array.isArray(header)) {
      return header.length > 0 ? header[0] : null;
    }
    return header;
  }

  private parseContentLength(rawValue: string | null): number | null {
    if (!rawValue) {
      return null;
    }
    const value = Number(rawValue);
    if (!Number.isFinite(value)) {
      return null;
    }
    return value;
  }

  private truncate(value: string | null, maxLength: number): string | null {
    if (!value) {
      return null;
    }
    if (value.length <= maxLength) {
      return value;
    }
    return `${value.slice(0, maxLength)}...[truncated]`;
  }
}
