import { NestFactory } from '@nestjs/core';
import { FastifyAdapter, NestFastifyApplication } from '@nestjs/platform-fastify';
import { AppModule } from './app.module';
import { AppConfigService } from './config/app-config.service';
import { StructuredLoggerService } from './logging/structured-logger.service';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter({ logger: false }),
    { rawBody: true, bodyParser: false, logger: false },
  );

  const fastify = app.getHttpAdapter().getInstance();
  fastify.addContentTypeParser(
    'application/json',
    { parseAs: 'string' },
    (_request: unknown, body: string, done: (error: Error | null, parsed: string) => void) => {
      done(null, body);
    },
  );
  fastify.addContentTypeParser(
    'text/plain',
    { parseAs: 'string' },
    (_request: unknown, body: string, done: (error: Error | null, parsed: string) => void) => {
      done(null, body);
    },
  );

  app.enableShutdownHooks();
  const config = app.get(AppConfigService);
  const logger = app.get(StructuredLoggerService);

  await app.listen(config.port, '0.0.0.0');
  logger.info('http_server_started', { port: config.port, nodeEnv: config.nodeEnv });
}

void bootstrap();
