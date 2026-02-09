import { Global, Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import Joi from 'joi';
import { AppConfigService } from './app-config.service';

@Global()
@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      expandVariables: true,
      validationSchema: Joi.object({
        PORT: Joi.number().default(8080),
        DB_HOST: Joi.string(),
        DB_PORT: Joi.number().default(3306),
        DB_USER: Joi.string(),
        DB_PASS: Joi.string().allow('').default(''),
        DB_NAME: Joi.string(),
        DB_URL: Joi.string(),
        AWER_MARIADB_URL: Joi.string(),
        'awer-mariadb-url': Joi.string(),
        GUPSHUP_WEBHOOK_SECRET: Joi.string().required(),
        WEBHOOK_WORKER_BATCH_SIZE: Joi.number().integer().min(1).default(50),
        WEBHOOK_WORKER_INTERVAL_MS: Joi.number().integer().min(100).default(1000),
        WEBHOOK_VERBOSE_LOGS: Joi.boolean().default(true),
        WEBHOOK_PAYLOAD_PREVIEW_CHARS: Joi.number().integer().min(256).max(12000).default(2500),
        LOG_LEVEL: Joi.string().valid('fatal', 'error', 'warn', 'info', 'debug', 'trace').default('info'),
        USER_PHONE_COLUMN: Joi.string().pattern(/^[A-Za-z_][A-Za-z0-9_]*$/).default('phone'),
        BLOCKED_AS_OPT_OUT: Joi.boolean().default(true),
        NODE_ENV: Joi.string().default('production'),
      }).custom((value, helpers) => {
        const hasUrl = Boolean(value.DB_URL || value.AWER_MARIADB_URL || value['awer-mariadb-url']);
        if (hasUrl) {
          return value;
        }

        if (!value.DB_HOST || !value.DB_NAME || !value.DB_USER) {
          return helpers.error('any.custom', {
            message:
              'Set DB_URL/AWER_MARIADB_URL (jdbc:mysql://...) or provide DB_HOST + DB_NAME + DB_USER (DB_PASS optional).',
          });
        }

        return value;
      }, 'database-config-validation')
      .messages({
        'any.custom':
          '{{#message}}',
      }),
      validationOptions: {
        abortEarly: false,
        allowUnknown: true,
      },
    }),
  ],
  providers: [AppConfigService],
  exports: [AppConfigService],
})
export class AppConfigModule {}
