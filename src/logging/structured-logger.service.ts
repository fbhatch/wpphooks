import { Injectable } from '@nestjs/common';
import { maskPhone as maskPhoneValue, sanitizeForLog } from './log-sanitizer.util';

type LogLevel = 'fatal' | 'error' | 'warn' | 'info' | 'debug' | 'trace';

const LEVEL_RANK: Record<LogLevel, number> = {
  fatal: 60,
  error: 50,
  warn: 40,
  info: 30,
  debug: 20,
  trace: 10,
};

@Injectable()
export class StructuredLoggerService {
  private minLevel: LogLevel = 'info';

  setLevel(level: string): void {
    if (level in LEVEL_RANK) {
      this.minLevel = level as LogLevel;
    }
  }

  info(message: string, meta?: Record<string, unknown>): void {
    this.write('info', message, meta);
  }

  warn(message: string, meta?: Record<string, unknown>): void {
    this.write('warn', message, meta);
  }

  error(message: string, meta?: Record<string, unknown>): void {
    this.write('error', message, meta);
  }

  debug(message: string, meta?: Record<string, unknown>): void {
    this.write('debug', message, meta);
  }

  private write(level: LogLevel, message: string, meta?: Record<string, unknown>): void {
    if (LEVEL_RANK[level] < LEVEL_RANK[this.minLevel]) {
      return;
    }

    const entry = {
      level,
      time: new Date().toISOString(),
      msg: message,
      ...(meta ? (sanitizeForLog(meta) as Record<string, unknown>) : {}),
    };
    const line = JSON.stringify(entry);

    if (level === 'error' || level === 'fatal') {
      // eslint-disable-next-line no-console
      console.error(line);
      return;
    }

    if (level === 'warn') {
      // eslint-disable-next-line no-console
      console.warn(line);
      return;
    }

    // eslint-disable-next-line no-console
    console.log(line);
  }

  static maskPhone(phone: string | null | undefined): string {
    return maskPhoneValue(phone);
  }
}
