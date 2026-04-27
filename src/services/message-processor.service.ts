// src/services/message-processor.service.ts
import { EventEmitter } from "events";
import { PolymarketClientService } from "./polymarket-client.service";
import { AggregationService } from "./aggregation.service";
import { OrderMatched } from "../models/types";
import { logger } from "../utils/logger";

interface ProcessorConfig {
    batchSize: number;
    batchTimeoutMs: number;
    enableStreaming?: boolean;
}

export class MessageProcessor extends EventEmitter {
    private client: PolymarketClientService;
    private aggregationService: AggregationService;
    private config: ProcessorConfig;

    // 批处理缓冲区
    private batchBuffer: OrderMatched[] = [];
    private batchTimer: NodeJS.Timeout | null = null;

    // 统计信息
    private stats = {
        ordersReceived: 0,
        ordersProcessed: 0,
        batchesProcessed: 0,
        lastBatchTime: Date.now(),
    };

    constructor(config?: Partial<ProcessorConfig>) {
        super();
        this.config = {
            batchSize: 100,
            batchTimeoutMs: 100,
            enableStreaming: true,
            ...config,
        };

        this.client = new PolymarketClientService();
        this.aggregationService = new AggregationService();

        this.setupEventHandlers();
    }

    /**
     * 设置事件处理器
     */
    private setupEventHandlers(): void {
        // 处理WebSocket接收到的订单
        this.client.on("order_matched", (order: OrderMatched) => {
            this.stats.ordersReceived++;
            this.handleOrder(order);
        });

        // 在 MessageProcessor 的 new_market 监听中
        this.aggregationService.on("new_market", async (marketKey: string) => {
            logger.info(`New market detected: ${marketKey}`);
            // 向上抛出
            this.emit("new_market", marketKey);
        });

        // WebSocket状态变化
        this.client.on("status_change", status => {
            logger.info(`WebSocket status: ${status}`);
            this.emit("connection_status", status);
        });

        // 重连事件
        this.client.on("reconnected", data => {
            logger.info(`WebSocket reconnected after ${data.attempts} attempts`);
            this.emit("reconnected", data);
        });
    }

    /**
     * 处理单个订单（支持批处理）
     */
    private handleOrder(order: OrderMatched): void {
        // 添加到批处理缓冲区
        this.batchBuffer.push(order);

        // 如果达到批量大小，立即处理
        if (this.batchBuffer.length >= this.config.batchSize) {
            this.flushBatch();
        } else if (!this.batchTimer) {
            // 设置定时器，超时后处理
            this.batchTimer = setTimeout(() => {
                this.flushBatch();
            }, this.config.batchTimeoutMs);
        }
    }

    /**
     * 刷新批处理缓冲区
     */
    private async flushBatch(): Promise<void> {
        if (this.batchBuffer.length === 0) return;

        // 清除定时器
        if (this.batchTimer) {
            clearTimeout(this.batchTimer);
            this.batchTimer = null;
        }

        const batch = [...this.batchBuffer];
        this.batchBuffer = [];

        // const startTime = Date.now();

        try {
            // 批量聚合
            const results = await this.aggregationService.batchProcessOrders(batch);

            this.stats.ordersProcessed += batch.length;
            this.stats.batchesProcessed++;
            this.stats.lastBatchTime = Date.now();

            // const processingTime = Date.now() - startTime;

            // logger.info(
            //     `📦 Batch processed: ${batch.length} orders, ${results.size} bars updated in ${processingTime}ms`,
            // );

            // 发送实时更新事件（用于其他服务）
            if (this.config.enableStreaming) {
                this.emit("bars_updated", Array.from(results.values()));
            }
        } catch (error) {
            logger.error("Failed to process batch:", error);
            // 失败时重新加入缓冲区
            this.batchBuffer.unshift(...batch);
        }
    }

    /**
     * 启动服务
     */
    async start(): Promise<void> {
        logger.info("Starting Message Processor...");

        // 连接WebSocket
        await this.client.connect();

        // 定期输出统计
        setInterval(() => {
            const stats = this.getStats();
            logger.info(
                `Stats: Received=${stats.ordersReceived} Processed=${stats.ordersProcessed} Batches=${stats.batchesProcessed} Buffer=${stats.bufferSize}`,
            );
        }, 60000);

        logger.info("✅ Message Processor started");
    }

    /**
     * 停止服务
     */
    async stop(): Promise<void> {
        logger.info("Stopping Message Processor...");

        // 处理剩余数据
        await this.flushBatch();

        // 断开连接
        this.client.disconnect();

        logger.info("✅ Message Processor stopped");
    }

    /**
     * 获取统计信息
     */
    getStats() {
        return {
            ...this.stats,
            bufferSize: this.batchBuffer.length,
            cacheStats: this.aggregationService.getStats(),
            wsStatus: this.client.getStatus(),
        };
    }

    /**
     * 获取秒级数据
     */
    async getSecondBars(marketKey: string, startTime: number, endTime: number) {
        return this.aggregationService.getSecondBars(marketKey, startTime, endTime);
    }

    /**
     * 获取所有活跃市场
     */
    async getActiveMarkets() {
        return this.aggregationService.getAllActiveMarkets();
    }
}
