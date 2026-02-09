const fs = require('fs');
const path = require('path');
const mysql = require('mysql2/promise');

function loadDotEnv() {
  const envPath = path.resolve(process.cwd(), '.env');
  if (!fs.existsSync(envPath)) {
    return;
  }

  const content = fs.readFileSync(envPath, 'utf8');
  for (const line of content.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) {
      continue;
    }

    const separator = trimmed.indexOf('=');
    if (separator <= 0) {
      continue;
    }

    const key = trimmed.slice(0, separator).trim();
    let value = trimmed.slice(separator + 1).trim();

    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }

    if (process.env[key] === undefined) {
      process.env[key] = value;
    }
  }
}

function getEnv(name) {
  const value = process.env[name];
  if (typeof value !== 'string') {
    return null;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function requiredEnv(name) {
  const value = getEnv(name);
  if (!value) {
    throw new Error(`Missing required env: ${name}`);
  }
  return value;
}

function parsePort(rawValue, fallback) {
  if (!rawValue) {
    return fallback;
  }
  const parsed = Number(rawValue);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`Invalid DB port value: ${rawValue}`);
  }
  return parsed;
}

function resolveDbConfig() {
  const dbUrl = getEnv('DB_URL') || getEnv('AWER_MARIADB_URL') || getEnv('awer-mariadb-url');
  if (dbUrl) {
    return parseDbUrl(dbUrl);
  }

  return {
    host: requiredEnv('DB_HOST'),
    port: parsePort(getEnv('DB_PORT'), 3306),
    user: requiredEnv('DB_USER'),
    password: getEnv('DB_PASS') || '',
    database: requiredEnv('DB_NAME'),
  };
}

function parseDbUrl(rawUrl) {
  const normalized = rawUrl.startsWith('jdbc:') ? rawUrl.slice(5) : rawUrl;
  let parsed;

  try {
    parsed = new URL(normalized);
  } catch {
    throw new Error('Invalid DB URL format. Use mysql://... or jdbc:mysql://...');
  }

  if (parsed.protocol !== 'mysql:') {
    throw new Error('Unsupported DB URL protocol. Use mysql://... or jdbc:mysql://...');
  }

  const host = parsed.hostname;
  if (!host) {
    throw new Error('DB URL is missing hostname');
  }

  const databaseFromUrl = decodeURIComponent(parsed.pathname.replace(/^\/+/, ''));
  const database = databaseFromUrl || getEnv('DB_NAME');
  if (!database) {
    throw new Error('DB URL is missing database name and DB_NAME is not set');
  }

  const user = (parsed.username ? decodeURIComponent(parsed.username) : '') || getEnv('DB_USER');
  if (!user) {
    throw new Error('DB URL is missing user and DB_USER is not set');
  }

  const password = (parsed.password ? decodeURIComponent(parsed.password) : '') || getEnv('DB_PASS') || '';

  return {
    host,
    port: parsePort(parsed.port || getEnv('DB_PORT'), 3306),
    user,
    password,
    database,
  };
}

async function run() {
  loadDotEnv();

  const migrationFile = process.argv[2] || path.join('migrations', '001_create_wpp_webhook_event_raw.sql');
  const sql = fs.readFileSync(path.resolve(process.cwd(), migrationFile), 'utf8');
  const dbConfig = resolveDbConfig();

  const connection = await mysql.createConnection({
    host: dbConfig.host,
    port: dbConfig.port,
    user: dbConfig.user,
    password: dbConfig.password,
    database: dbConfig.database,
    multipleStatements: true,
  });

  try {
    await connection.query(sql);
    console.log(`Migration applied: ${migrationFile}`);
  } finally {
    await connection.end();
  }
}

run().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
