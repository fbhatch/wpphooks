export interface DbConfig {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
}

type EnvMap = Record<string, string | number | boolean | undefined>;

export function resolveDbConfigFromEnv(env: EnvMap): DbConfig {
  const dbUrl = firstString(env, ['DB_URL', 'AWER_MARIADB_URL', 'awer-mariadb-url']);
  if (dbUrl) {
    return parseDbUrl(dbUrl, env);
  }

  const host = requiredString(env, 'DB_HOST');
  const database = requiredString(env, 'DB_NAME');
  const user = requiredString(env, 'DB_USER');
  const password = optionalString(env, 'DB_PASS') ?? '';
  const port = parsePort(optionalString(env, 'DB_PORT'), 3306);

  return {
    host,
    port,
    user,
    password,
    database,
  };
}

function parseDbUrl(rawUrl: string, env: EnvMap): DbConfig {
  const normalized = rawUrl.startsWith('jdbc:') ? rawUrl.slice(5) : rawUrl;
  let parsed: URL;

  try {
    parsed = new URL(normalized);
  } catch {
    throw new Error('Invalid DB URL format. Expected mysql://... or jdbc:mysql://...');
  }

  if (parsed.protocol !== 'mysql:') {
    throw new Error('Unsupported DB URL protocol. Use mysql://... or jdbc:mysql://...');
  }

  const host = parsed.hostname;
  if (!host) {
    throw new Error('DB URL is missing hostname');
  }

  const databaseFromUrl = decodeURIComponent(parsed.pathname.replace(/^\/+/, ''));
  const database = databaseFromUrl || optionalString(env, 'DB_NAME');
  if (!database) {
    throw new Error('DB URL is missing database name and DB_NAME is not set');
  }

  const userFromUrl = parsed.username ? decodeURIComponent(parsed.username) : '';
  const user = userFromUrl || optionalString(env, 'DB_USER');
  if (!user) {
    throw new Error('DB URL is missing user and DB_USER is not set');
  }

  const passwordFromUrl = parsed.password ? decodeURIComponent(parsed.password) : '';
  const password = passwordFromUrl || optionalString(env, 'DB_PASS') || '';
  const port = parsePort(parsed.port || optionalString(env, 'DB_PORT'), 3306);

  return {
    host,
    port,
    user,
    password,
    database,
  };
}

function requiredString(env: EnvMap, key: string): string {
  const value = optionalString(env, key);
  if (!value) {
    throw new Error(`Missing required config value: ${key}`);
  }
  return value;
}

function optionalString(env: EnvMap, key: string): string | null {
  const value = env[key];
  if (value === undefined || value === null) {
    return null;
  }
  const asString = String(value).trim();
  return asString.length > 0 ? asString : null;
}

function firstString(env: EnvMap, keys: string[]): string | null {
  for (const key of keys) {
    const value = optionalString(env, key);
    if (value) {
      return value;
    }
  }
  return null;
}

function parsePort(rawValue: string | null, fallback: number): number {
  if (!rawValue) {
    return fallback;
  }
  const parsed = Number(rawValue);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`Invalid DB port value: ${rawValue}`);
  }
  return parsed;
}
