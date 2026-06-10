// src/index.ts
import dotenv from "dotenv";
import { MessageProcessor } from "./services/message-processor.service";
import { loadLuaScripts } from "./config/redis";
import { startQueueCleanup } from "./config/queue";
import { logger } from "./utils/logger";

dotenv.config();

async function bootstrap() {
    logger.info("Starting Polymarket Aggregator Service...");

    // 1. 加载 Redis Lua 脚本（用于秒级原子聚合）
    await loadLuaScripts();

    // 2. 启动队列定期清理（可选）
    startQueueCleanup();

    // 3. 创建消息处理器（包含 WebSocket 连接 + 批量聚合）
    const processor = new MessageProcessor({
        batchSize: parseInt(process.env.BATCH_SIZE || "50000"),
        batchTimeoutMs: parseInt(process.env.BATCH_TIMEOUT_MS || "1000"),
        enableStreaming: process.env.ENABLE_STREAMING === "true",
    });

    // 4. 监听新市场事件
    processor.on("new_market", marketKey => {
        logger.info(`New market event received in index.ts: ${marketKey}`);
    });

    // 可选：监听事件以便调试
    processor.on("connection_status", status => {
        logger.info(`WebSocket connection status: ${status}`);
    });

    processor.on("bars_updated", bars => {
        logger.debug(`Real-time update: ${bars.length} second bars updated`);
    });

    await processor.start();

    // 优雅退出处理
    const shutdown = async () => {
        logger.info("Shutting down...");
        await processor.stop();
        process.exit(0);
    };

    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);
}

bootstrap().catch(error => {
    logger.error("Fatal error during startup:", error);
    process.exit(1);
});
