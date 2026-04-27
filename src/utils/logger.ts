// src/utils/logger.ts
type LogLevel = "debug" | "info" | "warn" | "error";

const levelPriority: Record<LogLevel, number> = {
    debug: 0,
    info: 1,
    warn: 2,
    error: 3,
};

const currentLevel: LogLevel = (process.env.LOG_LEVEL as LogLevel) || "info";
const minPriority = levelPriority[currentLevel] ?? 1;

function formatMessage(level: string, message: string, ...meta: unknown[]): string {
    const timestamp = new Date().toISOString();
    const metaStr = meta.length ? ` ${JSON.stringify(meta)}` : "";
    return `[${timestamp}] ${level.toUpperCase()}: ${message}${metaStr}`;
}

function shouldLog(level: LogLevel): boolean {
    return levelPriority[level] >= minPriority;
}

function log(level: LogLevel, message: string, ...meta: unknown[]): void {
    if (!shouldLog(level)) return;
    const formatted = formatMessage(level, message, ...meta);
    switch (level) {
        case "error":
            console.error(formatted);
            break;
        case "warn":
            console.warn(formatted);
            break;
        default:
            console.log(formatted);
    }
}

export const logger = {
    debug: (message: string, ...meta: unknown[]) => log("debug", message, ...meta),
    info: (message: string, ...meta: unknown[]) => log("info", message, ...meta),
    warn: (message: string, ...meta: unknown[]) => log("warn", message, ...meta),
    error: (message: string, ...meta: unknown[]) => log("error", message, ...meta),
    performance: (message: string, data?: Record<string, unknown>) => {
        if (process.env.DEBUG === "true") {
            log("debug", `[PERF] ${message}`, data);
        } else {
            log("info", `[PERF] ${message}`, data);
        }
    },
};

export default logger;
