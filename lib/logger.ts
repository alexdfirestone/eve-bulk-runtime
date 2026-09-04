type LogLevel = "info" | "warn" | "error";

export type LogFields = Record<string, unknown>;

function errorProperty(error: Error, key: string) {
  const value = (error as Error & Record<string, unknown>)[key];
  return typeof value === "string" || typeof value === "number" ? value : undefined;
}

export function serializeError(error: unknown, includeStack = true): Record<string, unknown> {
  if (!(error instanceof Error)) return { message: String(error) };

  const details: Record<string, unknown> = {
    name: error.name,
    message: error.message,
  };
  if (includeStack && error.stack) details.stack = error.stack;

  for (const key of ["code", "status", "statusCode"]) {
    const value = errorProperty(error, key);
    if (value !== undefined) details[key] = value;
  }

  if (error.cause !== undefined) {
    details.cause = error.cause instanceof Error
      ? serializeError(error.cause, false)
      : { message: String(error.cause) };
  }
  return details;
}

function write(level: LogLevel, event: string, fields: LogFields = {}) {
  const entry = JSON.stringify({
    timestamp: new Date().toISOString(),
    level,
    service: "eve-bulk-runtime",
    event,
    environment: process.env.VERCEL_ENV ?? process.env.NODE_ENV ?? "unknown",
    ...fields,
  });

  if (level === "error") console.error(entry);
  else if (level === "warn") console.warn(entry);
  else console.info(entry);
}

export const logger = {
  info: (event: string, fields?: LogFields) => write("info", event, fields),
  warn: (event: string, fields?: LogFields) => write("warn", event, fields),
  error: (event: string, error: unknown, fields: LogFields = {}) =>
    write("error", event, { ...fields, error: serializeError(error) }),
};
