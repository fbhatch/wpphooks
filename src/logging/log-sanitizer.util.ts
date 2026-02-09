const SENSITIVE_KEY_PATTERN = /(secret|token|password|authorization|auth|cipher|signature|api[_-]?key|bearer)/i;
const PHONE_KEY_PATTERN = /(phone|msisdn|wa[_-]?id|whatsapp)/i;

interface SanitizeOptions {
  maxDepth: number;
  maxArrayItems: number;
  maxObjectKeys: number;
  maxStringLength: number;
}

const DEFAULT_OPTIONS: SanitizeOptions = {
  maxDepth: 4,
  maxArrayItems: 20,
  maxObjectKeys: 40,
  maxStringLength: 400,
};

export function extractTopLevelKeys(payload: unknown): string[] {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return [];
  }
  return Object.keys(payload as Record<string, unknown>).slice(0, 30);
}

export function buildPayloadPreview(payload: unknown, maxStringLength: number): unknown {
  return sanitizeValue(payload, {
    ...DEFAULT_OPTIONS,
    maxStringLength,
  });
}

export function sanitizeForLog(value: unknown): unknown {
  return sanitizeValue(value, DEFAULT_OPTIONS);
}

function sanitizeValue(
  value: unknown,
  options: SanitizeOptions,
  depth = 0,
  keyHint: string | null = null,
  visited = new WeakSet<object>(),
): unknown {
  if (value === null || value === undefined) {
    return value;
  }

  if (typeof value === 'string') {
    return sanitizeString(value, options.maxStringLength, keyHint);
  }

  if (typeof value === 'number' || typeof value === 'boolean') {
    return value;
  }

  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : value.toISOString();
  }

  if (depth >= options.maxDepth) {
    if (Array.isArray(value)) {
      return `[Array(${value.length})]`;
    }
    return '[Object]';
  }

  if (Array.isArray(value)) {
    const items = value.slice(0, options.maxArrayItems).map((item) =>
      sanitizeValue(item, options, depth + 1, keyHint, visited),
    );
    if (value.length > options.maxArrayItems) {
      items.push(`[+${value.length - options.maxArrayItems} items]`);
    }
    return items;
  }

  if (typeof value === 'object') {
    const obj = value as Record<string, unknown>;
    if (visited.has(obj)) {
      return '[Circular]';
    }
    visited.add(obj);

    const result: Record<string, unknown> = {};
    const entries = Object.entries(obj);
    const limitedEntries = entries.slice(0, options.maxObjectKeys);

    for (const [key, node] of limitedEntries) {
      if (SENSITIVE_KEY_PATTERN.test(key)) {
        result[key] = '[REDACTED]';
        continue;
      }

      if (PHONE_KEY_PATTERN.test(key) && typeof node === 'string') {
        result[key] = maskPhone(node);
        continue;
      }

      result[key] = sanitizeValue(node, options, depth + 1, key, visited);
    }

    if (entries.length > options.maxObjectKeys) {
      result.__truncated_keys = entries.length - options.maxObjectKeys;
    }

    return result;
  }

  return String(value);
}

function sanitizeString(value: string, maxLength: number, keyHint: string | null): string {
  if (keyHint && PHONE_KEY_PATTERN.test(keyHint)) {
    return maskPhone(value);
  }

  if (looksLikePhone(value)) {
    return maskPhone(value);
  }

  if (value.length <= maxLength) {
    return value;
  }

  return `${value.slice(0, maxLength)}...[truncated:${value.length - maxLength}]`;
}

function looksLikePhone(input: string): boolean {
  const trimmed = input.trim();
  if (!/^\+?[\d\s().-]+$/.test(trimmed)) {
    return false;
  }
  const digits = input.replace(/\D/g, '');
  return digits.length >= 8 && digits.length <= 15;
}

export function maskPhone(phone: string | null | undefined): string {
  if (!phone) {
    return 'null';
  }
  const trimmed = phone.trim();
  if (trimmed.length <= 4) {
    return `***${trimmed}`;
  }
  return `***${trimmed.slice(-4)}`;
}
