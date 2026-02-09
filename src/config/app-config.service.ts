import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { DbConfig, resolveDbConfigFromEnv } from './db-config.util';

export interface WorkerConfig {
  batchSize: number;
  intervalMs: number;
  maxAttempts: number;
}

@Injectable()
export class AppConfigService {
  readonly port: number;
  readonly db: DbConfig;
  readonly gupshupWebhookSecret: string;
  readonly logLevel: string;
  readonly webhookVerboseLogs: boolean;
  readonly webhookPayloadPreviewChars: number;
  readonly userPhoneColumn: string;
  readonly blockedAsOptOut: boolean;
  readonly worker: WorkerConfig;
  readonly nodeEnv: string;

  constructor(private readonly configService: ConfigService) {
    this.port = this.getNumber('PORT');
    this.db = resolveDbConfigFromEnv({
      DB_HOST: this.configService.get<string>('DB_HOST'),
      DB_PORT: this.configService.get<string | number>('DB_PORT'),
      DB_USER: this.configService.get<string>('DB_USER'),
      DB_PASS: this.configService.get<string>('DB_PASS'),
      DB_NAME: this.configService.get<string>('DB_NAME'),
      DB_URL: this.configService.get<string>('DB_URL'),
      AWER_MARIADB_URL: this.configService.get<string>('AWER_MARIADB_URL'),
      'awer-mariadb-url': this.configService.get<string>('awer-mariadb-url'),
    });
    this.gupshupWebhookSecret = this.getString('GUPSHUP_WEBHOOK_SECRET');
    this.logLevel = this.getString('LOG_LEVEL');
    this.webhookVerboseLogs = this.getBoolean('WEBHOOK_VERBOSE_LOGS');
    this.webhookPayloadPreviewChars = this.getNumber('WEBHOOK_PAYLOAD_PREVIEW_CHARS');
    this.userPhoneColumn = this.getString('USER_PHONE_COLUMN');
    this.blockedAsOptOut = this.getBoolean('BLOCKED_AS_OPT_OUT');
    this.worker = {
      batchSize: this.getNumber('WEBHOOK_WORKER_BATCH_SIZE'),
      intervalMs: this.getNumber('WEBHOOK_WORKER_INTERVAL_MS'),
      maxAttempts: 10,
    };
    this.nodeEnv = this.getString('NODE_ENV');
  }

  private getString(key: string): string {
    const value = this.configService.get<string>(key);
    if (typeof value !== 'string' || value.length === 0) {
      throw new Error(`Missing required config value: ${key}`);
    }
    return value;
  }

  private getNumber(key: string): number {
    const value = this.configService.get<number>(key);
    if (typeof value !== 'number' || Number.isNaN(value)) {
      throw new Error(`Invalid numeric config value: ${key}`);
    }
    return value;
  }

  private getBoolean(key: string): boolean {
    const value = this.configService.get<string | boolean>(key);
    if (typeof value === 'boolean') {
      return value;
    }
    if (typeof value === 'string') {
      return value.toLowerCase() === 'true';
    }
    throw new Error(`Invalid boolean config value: ${key}`);
  }
}
