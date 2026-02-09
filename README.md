# wpphooks

Microservicio NestJS (Fastify) para recibir webhooks de Gupshup Partner (WhatsApp), persistir eventos crudos con deduplicación y procesarlos de forma asíncrona sobre MySQL existente.

## Características

- Endpoint: `POST /webhooks/gupshup/:appId/events`
- Healthcheck liviano: `GET /health` -> `ok`
- Seguridad por header `X-GUPSHUP-SECRET`
- Ingesta con persistencia raw + idempotencia (`dedupe_key` SHA-256)
- Worker interno por lotes (`FOR UPDATE SKIP LOCKED`) para múltiples instancias
- Actualización de estados en tablas existentes:
  - `wpp_campaign_recipient`
  - `wpp_template` + última `wpp_template_version`
  - `whatsapp_marketing_consent_event` + `whatsapp_marketing_current`
- Sin ORM (`mysql2/promise`)
- Dockerizable y listo para Cloud Run

## Requisitos

- Node.js 20+
- MySQL 8+

## Variables de entorno

### Requeridas

- `PORT` (default recomendado: `8080`)
- `GUPSHUP_WEBHOOK_SECRET`
- Config DB en uno de estos formatos:
  - URL: `AWER_MARIADB_URL=jdbc:mysql://host:puerto/db?...` o `DB_URL=mysql://user:pass@host:puerto/db`
  - Campos separados: `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASS`, `DB_NAME`

### Opcionales

- `WEBHOOK_WORKER_BATCH_SIZE` (default: `50`)
- `WEBHOOK_WORKER_INTERVAL_MS` (default: `1000`)
- `WEBHOOK_VERBOSE_LOGS` (default: `true`) habilita logs detallados por webhook/evento procesado
- `WEBHOOK_PAYLOAD_PREVIEW_CHARS` (default: `2500`) límite de preview del payload en logs
- `LOG_LEVEL` (`fatal|error|warn|info|debug|trace`, default: `info`)
- `USER_PHONE_COLUMN` (default: `phone`)
- `BLOCKED_AS_OPT_OUT` (default: `true`)
- `NODE_ENV` (default: `production`)

Referencia rápida: `.env`

## Instalación local

```bash
npm ci
npm run build
```

## Migración de buffer

Archivo: `migrations/001_create_wpp_webhook_event_raw.sql`

Aplicar por script:

```bash
npm run migrate
```

El script `migrate` lee `.env` automáticamente.

También puedes aplicar el SQL manualmente en tu MySQL.

## Ejecutar

```bash
npm start
```

Desarrollo:

```bash
npm run start:dev
```

## Ejemplo webhook

```bash
curl -X POST "http://localhost:8080/webhooks/gupshup/9c0b5b44-c983-xxxx/events" \
  -H "X-GUPSHUP-SECRET: your-secret" \
  -H "Content-Type: application/json" \
  -d '{"statuses":[{"id":"gs-msg-1","status":"delivered","timestamp":"1739112000"}]}'
```

## Docker

Build:

```bash
docker build -t wpphooks:latest .
```

Run:

```bash
docker run --rm -p 8080:8080 --env-file .env wpphooks:latest
```

Opcional dev con MySQL local:

```bash
docker compose up --build
```

## Cloud Run (recomendado)

- Puerto: `8080`
- Concurrency: `40-80`
- Min instances: `0-1`
- CPU always allocated: opcional según latencia del worker
- Usa `WEBHOOK_WORKER_INTERVAL_MS=1000` y ajusta `WEBHOOK_WORKER_BATCH_SIZE` según throughput real

## Notas de operación

- El endpoint responde `200` tras validar secreto e insertar el raw buffer (duplicados se ignoran por `UNIQUE dedupe_key`).
- El procesamiento de negocio es asíncrono por worker interno.
- Reintentos automáticos: hasta `attempts > 10`; luego se marca procesado con `last_error` final.
- Logging estructurado sin exponer teléfonos completos (solo últimos 4).
- Para fase de descubrimiento de payload en GCP, usa `WEBHOOK_VERBOSE_LOGS=true` y `LOG_LEVEL=info` o `LOG_LEVEL=debug`.
