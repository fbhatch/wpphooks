import { Module, OnModuleInit } from '@nestjs/common';
import { AppConfigService } from './config/app-config.service';
import { AppConfigModule } from './config/config.module';
import { MysqlModule } from './db/mysql.module';
import { StructuredLoggerService } from './logging/structured-logger.service';
import { NormalizerService } from './normalizer/normalizer.service';
import { ConsentRepo } from './repos/consent.repo';
import { IntegrationRepo } from './repos/integration.repo';
import { RawRepo } from './repos/raw.repo';
import { RecipientRepo } from './repos/recipient.repo';
import { TemplateRepo } from './repos/template.repo';
import { WebhookController } from './webhook/webhook.controller';
import { WebhookService } from './webhook/webhook.service';
import { WorkerService } from './worker/worker.service';

@Module({
  imports: [AppConfigModule, MysqlModule],
  controllers: [WebhookController],
  providers: [
    StructuredLoggerService,
    NormalizerService,
    RawRepo,
    IntegrationRepo,
    RecipientRepo,
    TemplateRepo,
    ConsentRepo,
    WebhookService,
    WorkerService,
  ],
})
export class AppModule implements OnModuleInit {
  constructor(
    private readonly appConfigService: AppConfigService,
    private readonly logger: StructuredLoggerService,
  ) {}

  onModuleInit(): void {
    this.logger.setLevel(this.appConfigService.logLevel);
  }
}
