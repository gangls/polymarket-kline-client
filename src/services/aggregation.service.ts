// src/services/aggregation.service.ts
import { EventEmitter } from "events";
import { redisClient, UPSERT_SECOND_BAR_SCRIPT } from "../config/redis";
import { OrderMatched, SecondBar, AggregationStats, KLine } from "../models/types";
import { logger } from "../utils/logger";

export class AggregationService extends EventEmitter {
    private readonly MAX_SECOND_BARS = 10000; // 最多返回 10000 条秒级数据
    private readonly ACTIVE_MARKETS_SET = "active_markets"; // Redis Set 名称

    constructor() {
        super();
    }

    private stats: AggregationStats = {
        totalOrdersProcessed: 0,
        totalSecondBarsUpdated: 0,
        avgProcessingTime: 0,
        marketsActive: 0,
        cacheSize: 0,
    };

    private processingTimes: number[] = [];

    /**
     * 从订单中获取市场标识
     */
    private getMarketKeyFromOrder(order: OrderMatched): string {
        return `${order.slug}`;
    }

    /**
     * 检查并记录新市场，如果是新市场则发出事件
     * @param marketKey 市场标识 `${slug}:${outcomeIndex}`
     */
    private async checkAndNotifyNewMarket(marketKey: string): Promise<void> {
        const isNew = await redisClient.sadd(this.ACTIVE_MARKETS_SET, marketKey);
        if (isNew === 1) {
            this.emit("new_market", marketKey);
            console.log(`🎉 New market discovered: ${marketKey}`);
        }
    }

    /**
     * 处理单个订单，更新秒级聚合数据
     */
    async processOrder(order: OrderMatched): Promise<SecondBar> {
        const startTime = Date.now();

        const marketKey = this.getMarketKeyFromOrder(order);
        const timestamp = order.timestamp;
        const currentTime = Math.floor(Date.now() / 1000);

        const redisKey = `second_bar:${marketKey}:${timestamp}`;

        try {
            const result = (await redisClient.eval(
                UPSERT_SECOND_BAR_SCRIPT,
                1,
                redisKey,
                order.price.toString(),
                order.size.toString(),
                timestamp.toString(),
                currentTime.toString(),
            )) as (number | string | null)[];

            const isNew = result[0] === 1;
            const secondBar: SecondBar = {
                timestamp: timestamp,
                marketKey: marketKey,
                open: result[1] as number,
                high: (result[2] as number) || (result[1] as number),
                low: (result[3] as number) || (result[1] as number),
                close: (result[4] as number) || (result[1] as number),
                volume: result[5] as number,
                tradeCount: result[6] as number,
                lastUpdate: currentTime,
            };

            this.updateStats(startTime, isNew);

            if (isNew) {
                logger.debug(`📊 New second bar created: ${marketKey} @ ${timestamp}`);
            } else {
                logger.debug(`📊 Second bar updated: ${marketKey} @ ${timestamp}`);
            }

            this.checkAndNotifyNewMarket(marketKey);

            return secondBar;
        } catch (error) {
            logger.error("Error processing order:", error);
            throw error;
        }
    }

    /**
     * 批量处理订单
     */
    async batchProcessOrders(orders: OrderMatched[]): Promise<Map<string, SecondBar>> {
        // const startTime = Date.now();
        const results = new Map<string, SecondBar>();

        if (orders.length === 0) return results;

        const pipeline = redisClient.pipeline();
        const currentTime = Math.floor(Date.now() / 1000);

        for (const order of orders) {
            const marketKey = this.getMarketKeyFromOrder(order);
            const timestamp = order.timestamp;
            const redisKey = `second_bar:${marketKey}:${timestamp}`;

            pipeline.eval(
                UPSERT_SECOND_BAR_SCRIPT,
                1,
                redisKey,
                order.price.toString(),
                order.size.toString(),
                timestamp.toString(),
                currentTime.toString(),
            );
        }

        const execResults = await pipeline.exec();

        let newCount = 0;
        for (let i = 0; i < orders.length; i++) {
            const order = orders[i];
            const result = execResults?.[i]?.[1] as (number | string | null)[];

            if (result) {
                const marketKey = this.getMarketKeyFromOrder(order);
                const timestamp = order.timestamp;
                const key = `${marketKey}:${timestamp}`;
                const isNew = result[0] === 1;

                if (isNew) newCount++;

                const secondBar: SecondBar = {
                    timestamp: timestamp,
                    marketKey: marketKey,
                    open: result[1] as number,
                    high: (result[2] as number) || (result[1] as number),
                    low: (result[3] as number) || (result[1] as number),
                    close: (result[4] as number) || (result[1] as number),
                    volume: result[5] as number,
                    tradeCount: result[6] as number,
                    lastUpdate: currentTime,
                };

                results.set(key, secondBar);
            }
        }

        // const processingTime = Date.now() - startTime;
        this.stats.totalOrdersProcessed += orders.length;
        this.stats.totalSecondBarsUpdated += newCount;
        // this.processingTimes.push(processingTime);

        // logger.info(
        //     `📦 Batch processed ${orders.length} orders, created/updated ${results.size} second bars in ${processingTime}ms`,
        // );

        const uniqueMarketKeys = new Set(orders.map(o => this.getMarketKeyFromOrder(o)));
        for (const marketKey of uniqueMarketKeys) {
            this.checkAndNotifyNewMarket(marketKey);
        }

        return results;
    }

    /**
     * 获取秒级数据（仅真实存在的数据，不填充）
     * @param slug 市场slug
     * @param outcomeIndex 结果索引（0或1）
     * @param startTime 开始时间戳（秒）
     * @param endTime 结束时间戳（秒）
     * @param limit 最大返回条数
     */
    async getRawSecondBars(
        slug: string,
        outcomeIndex: number,
        startTime: number,
        endTime: number,
        limit: number = this.MAX_SECOND_BARS,
    ): Promise<SecondBar[]> {
        if (startTime > endTime) {
            throw new Error("startTime must be <= endTime");
        }

        const MAX_RANGE_SECONDS = 30 * 86400;
        if (endTime - startTime > MAX_RANGE_SECONDS) {
            throw new Error(`Time range cannot exceed ${MAX_RANGE_SECONDS / 86400} days`);
        }

        const marketKey = this.getMarketKey(slug, outcomeIndex);
        const keys: string[] = [];
        for (let ts = startTime; ts <= endTime; ts++) {
            keys.push(`second_bar:${marketKey}:${ts}`);
        }

        const pipeline = redisClient.pipeline();
        for (const key of keys) pipeline.hgetall(key);
        const results = await pipeline.exec();

        const bars: SecondBar[] = [];
        for (let i = 0; i < keys.length; i++) {
            const [err, data] = results?.[i] || [];
            if (!err && data && Object.keys(data).length > 0) {
                const timestamp = startTime + i;
                bars.push(
                    this.hashToSecondBar(marketKey, timestamp, data as Record<string, string>),
                );
            }
        }

        if (bars.length > limit) {
            return bars.slice(-limit);
        }
        return bars;
    }

    /**
     * 获取连续秒级数据（缺失秒用上一个有效 close 填充）
     */
    async getContinuousSecondBars(
        slug: string,
        outcomeIndex: number,
        startTime: number,
        endTime: number,
        fillNullWithLastClose: boolean = true,
        limit: number = this.MAX_SECOND_BARS,
    ): Promise<SecondBar[]> {
        const now = Math.floor(Date.now() / 1000);
        let effectiveEnd = Math.min(endTime, now);
        let effectiveStart = startTime;

        const totalSeconds = effectiveEnd - effectiveStart + 1;
        if (totalSeconds > limit) {
            effectiveEnd = effectiveStart + limit - 1;
        }

        if (effectiveStart > effectiveEnd) return [];

        const realBars = await this.getRawSecondBars(
            slug,
            outcomeIndex,
            effectiveStart,
            effectiveEnd,
            Number.MAX_SAFE_INTEGER,
        );
        if (!fillNullWithLastClose) {
            return realBars;
        }

        if (realBars.length === 0) {
            const marketKey = this.getMarketKey(slug, outcomeIndex);
            const result: SecondBar[] = [];
            for (let ts = effectiveStart; ts <= effectiveEnd; ts++) {
                result.push({
                    timestamp: ts,
                    open: 0,
                    high: 0,
                    low: 0,
                    close: 0,
                    volume: 0,
                    marketKey,
                    tradeCount: 0,
                    lastUpdate: now,
                });
            }
            logger.warn(
                `No real data in range [${effectiveStart}, ${effectiveEnd}] for ${marketKey}, filling with 0`,
            );
            return result;
        }

        const barMap = new Map<number, SecondBar>();
        for (const bar of realBars) {
            barMap.set(bar.timestamp, bar);
        }

        const marketKey = this.getMarketKey(slug, outcomeIndex);
        const result: SecondBar[] = [];
        let lastClose: number | null = null;

        for (let ts = effectiveStart; ts <= effectiveEnd; ts++) {
            const realBar = barMap.get(ts);
            if (realBar) {
                result.push(realBar);
                lastClose = realBar.close;
            } else if (lastClose !== null) {
                result.push({
                    timestamp: ts,
                    open: lastClose,
                    high: lastClose,
                    low: lastClose,
                    close: lastClose,
                    volume: 0,
                    marketKey,
                    tradeCount: 0,
                    lastUpdate: now,
                });
            }
        }
        return result;
    }

    /**
     * 获取多周期 K 线数据
     */
    async getKlines(
        slug: string,
        outcomeIndex: number,
        startTime: number,
        endTime: number,
        interval: number = 1,
        fillGaps: boolean = true,
        limit: number = this.MAX_SECOND_BARS,
    ): Promise<KLine[]> {
        const now = Math.floor(Date.now() / 1000);
        let effectiveEnd = Math.min(endTime, now);
        const maxAllowedSeconds = limit * interval;
        const maxEnd = startTime + maxAllowedSeconds - 1;
        if (effectiveEnd > maxEnd) effectiveEnd = maxEnd;
        if (startTime > effectiveEnd) return [];

        const marketKey = this.getMarketKey(slug, outcomeIndex);
        const secondBars = await this.getContinuousSecondBars(
            slug,
            outcomeIndex,
            startTime,
            effectiveEnd,
            fillGaps,
            Number.MAX_SAFE_INTEGER,
        );

        if (secondBars.length === 0) return [];

        const groups = new Map<number, SecondBar[]>();
        for (const bar of secondBars) {
            const offset = bar.timestamp - startTime;
            const groupStart = startTime + Math.floor(offset / interval) * interval;
            if (!groups.has(groupStart)) groups.set(groupStart, []);
            groups.get(groupStart)!.push(bar);
        }

        const klines: KLine[] = [];
        for (let i = 0; i < limit; i++) {
            const groupStart = startTime + i * interval;
            const bars = groups.get(groupStart);
            if (!bars || bars.length === 0) continue;
            const sorted = bars.sort((a, b) => a.timestamp - b.timestamp);
            klines.push({
                marketKey,
                interval,
                startTime: groupStart,
                endTime: groupStart + interval - 1,
                open: sorted[0].open,
                high: Math.max(...sorted.map(b => b.high)),
                low: Math.min(...sorted.map(b => b.low)),
                close: sorted[sorted.length - 1].close,
                volume: sorted.reduce((sum, b) => sum + b.volume, 0),
                tradeCount: sorted.reduce((sum, b) => sum + b.tradeCount, 0),
                lastUpdate: now,
            });
        }
        return klines;
    }

    // 辅助方法
    private hashToSecondBar(
        marketKey: string,
        timestamp: number,
        data: Record<string, string>,
    ): SecondBar {
        return {
            timestamp,
            open: parseFloat(data.open),
            high: parseFloat(data.high),
            low: parseFloat(data.low),
            close: parseFloat(data.close),
            volume: parseFloat(data.volume),
            marketKey,
            tradeCount: parseInt(data.tradeCount || "0", 10),
            lastUpdate: parseInt(data.lastUpdate || "0", 10),
        };
    }

    // 以下未修改的方法保持不变（getSecondBars, getAllActiveMarkets, getCacheStats, getStats, cleanExpiredData 等）
    // 注意：getSecondBars 和 getAllActiveMarkets 仍使用旧 pattern，可能会扫描到旧格式 key，但为了方便过渡，暂时保留。
    // 若需完全迁移，可用新版方法替代。

    async getSecondBars(
        marketKey: string,
        startTime: number,
        endTime: number,
    ): Promise<SecondBar[]> {
        // 保留兼容旧调用的接口，推荐使用 getRawSecondBars
        return this.getRawSecondBars(
            marketKey.split(":")[0],
            parseInt(marketKey.split(":")[1]),
            startTime,
            endTime,
            this.MAX_SECOND_BARS,
        );
    }

    async getAllActiveMarkets(): Promise<string[]> {
        // 因 key 格式已变，扫描新格式
        const pattern = "second_bar:*:*";
        const keys: string[] = [];
        let cursor = "0";
        do {
            const reply = await redisClient.scan(cursor, "MATCH", pattern, "COUNT", 100);
            cursor = reply[0];
            keys.push(...reply[1]);
        } while (cursor !== "0");
        const markets = new Set<string>();
        for (const key of keys) {
            const parts = key.split(":");
            if (parts.length >= 3) {
                markets.add(`${parts[1]}:${parts[2]}`);
            }
        }
        this.stats.marketsActive = markets.size;
        this.stats.cacheSize = keys.length;
        return Array.from(markets);
    }

    /**
     * 获取缓存统计信息
     */
    async getCacheStats(): Promise<{
        totalKeys: number;
        totalMarkets: number;
        memoryUsage: number;
        oldestData: number | null;
        newestData: number | null;
    }> {
        const keys = await redisClient.keys("second_bar:*");
        const markets = await this.getAllActiveMarkets();

        const memoryInfo = await redisClient.info("memory");
        const usedMemoryMatch = memoryInfo.match(/used_memory:(\d+)/);
        const usedMemory = usedMemoryMatch ? parseInt(usedMemoryMatch[1]) : 0;

        // 获取数据时间范围
        let oldestData: number | null = null;
        let newestData: number | null = null;

        for (const key of keys) {
            const timestamp = parseInt(key.split(":").pop() || "0");
            if (oldestData === null || timestamp < oldestData) oldestData = timestamp;
            if (newestData === null || timestamp > newestData) newestData = timestamp;
        }

        return {
            totalKeys: keys.length,
            totalMarkets: markets.length,
            memoryUsage: usedMemory,
            oldestData,
            newestData,
        };
    }

    /**
     * 获取聚合统计
     */
    getStats(): AggregationStats {
        const avgTime =
            this.processingTimes.length > 0
                ? this.processingTimes.reduce((a, b) => a + b, 0) / this.processingTimes.length
                : 0;

        return {
            ...this.stats,
            avgProcessingTime: avgTime,
        };
    }

    /**
     * 生成市场Key
     */
    private getMarketKey(slug: string, outcomeIndex: number): string {
        return `${slug}:${outcomeIndex}`;
    }

    /**
     * 更新统计信息
     */
    private updateStats(processingStartTime: number, isNewBar: boolean): void {
        const processingTime = Date.now() - processingStartTime;

        this.stats.totalOrdersProcessed++;
        if (isNewBar) {
            this.stats.totalSecondBarsUpdated++;
        }

        this.processingTimes.push(processingTime);

        // 保留最近1000个处理时间
        if (this.processingTimes.length > 1000) {
            this.processingTimes.shift();
        }
    }

    /**
     * 清理过期数据（手动触发）
     */
    async cleanExpiredData(maxAgeSeconds: number = 86400): Promise<number> {
        const keys = await redisClient.keys("second_bar:*");
        const currentTime = Math.floor(Date.now() / 1000);
        let cleaned = 0;

        for (const key of keys) {
            const data = await redisClient.hgetall(key);
            if (data && data.timestamp) {
                const timestamp = parseInt(data.timestamp);
                if (currentTime - timestamp > maxAgeSeconds) {
                    await redisClient.del(key);
                    cleaned++;
                }
            }
        }

        logger.info(`🧹 Cleaned ${cleaned} expired keys (age > ${maxAgeSeconds}s)`);
        return cleaned;
    }
}
