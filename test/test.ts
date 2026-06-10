import { RealTimeDataClient } from "../src/polymarket-websocket/client";
import { MarketSubscriptionMessage } from "../src/polymarket-websocket/model";
import { MarketDiscoveryService } from "../src/services/market-discovery.service";

/**
 * 手动验证脚本：测试 Polymarket market WebSocket 是否能收到推送
 *
 * 用法: npx ts-node test/test.ts
 * 可选环境变量:
 *   WS_HOST        - WebSocket 地址（默认 wss://ws-subscriptions-clob.polymarket.com/ws/market）
 *   ASSET_IDS      - 逗号分隔的 asset/token IDs
 *   HEARTBEAT_MS   - 心跳日志间隔（默认 30000）
 *   MARKET_REQUIRED_TAG_IDS - 逗号分隔的必需 tag ids
 */

const WS_HOST = process.env.WS_HOST; // undefined 则使用 SDK 默认值
const HEARTBEAT_MS = Number(process.env.HEARTBEAT_MS) || 30_000;
const SUBSCRIPTION_BATCH_SIZE = Number(process.env.MARKET_SUBSCRIPTION_BATCH_SIZE) || 250;
const ASSET_IDS = (process.env.ASSET_IDS || "")
    .split(",")
    .map(assetId => assetId.trim())
    .filter(Boolean);

let client: RealTimeDataClient | null = null;
let messageCount = 0;
let lastMessageAt = 0;
let subscribedAssetIds: string[] = [];
const typeCounts = new Map<string, number>();
const heartbeatTimer = setInterval(() => {
    const lastMessageText = lastMessageAt ? new Date(lastMessageAt).toISOString() : "none";
    const counts = Array.from(typeCounts.entries())
        .map(([type, count]) => `${type}=${count}`)
        .join(" ");
    console.log(
        `[heartbeat] listening=true messages=${messageCount} lastMessageAt=${lastMessageText} types=${counts || "none"}`,
    );
}, HEARTBEAT_MS);

function cleanup(exitCode: number) {
    clearInterval(heartbeatTimer);
    if (client) {
        console.log("[cleanup] 断开 WebSocket 连接...");
        client.disconnect();
        client = null;
    }
    process.exit(exitCode);
}

// ---- 回调 ----

const onMessage = (_: RealTimeDataClient, message: unknown): void => {
    messageCount++;
    lastMessageAt = Date.now();
    const messageType = getMessageType(message);
    typeCounts.set(messageType, (typeCounts.get(messageType) || 0) + 1);

    console.log("========== 收到 market channel 推送 ==========");
    console.log(JSON.stringify(message, null, 2));
    console.log("=============================================");

    if (Array.isArray(message)) {
        console.log(`[msg] count=${messageCount} arrayLength=${message.length}`);

        if (message.length === 0) {
            console.log("[msg] type=empty_array");
            return;
        }

        message.forEach((item, index) => {
            console.log(`[array:${index}] ${formatMessageSummary(item)}`);
        });
        return;
    } else if (message && typeof message === "object") {
        console.log(`[msg] count=${messageCount} ${formatMessageSummary(message)}`);
        return;
    } else {
        console.log(`[msg] count=${messageCount} primitive=${String(message)}`);
        return;
    }
};

const onConnect = (c: RealTimeDataClient): void => {
    console.log("[connected] WebSocket 连接成功");

    const batches = chunk(subscribedAssetIds, SUBSCRIPTION_BATCH_SIZE);
    for (const [index, batch] of batches.entries()) {
        const subscribePayload: MarketSubscriptionMessage = {
            assets_ids: batch,
            type: "market",
            initial_dump: index === 0,
            level: 2,
            custom_feature_enabled: true,
        };

        console.log(
            `[subscribe] 发送订阅 batch=${index + 1}/${batches.length} assetCount=${batch.length}:`,
            JSON.stringify(payloadForLog(subscribePayload)),
        );
        c.subscribe(subscribePayload);
    }
};

// ---- 启动 ----

start().catch(error => {
    console.error("[fatal] 启动失败:", error);
    cleanup(1);
});

// 优雅退出
process.on("SIGINT", () => {
    console.log("\n[SIGINT] 手动中断，正在断开...");
    cleanup(0);
});

function formatMessageSummary(message: unknown): string {
    if (!message || typeof message !== "object") {
        return `type=${typeof message}`;
    }

    const record = message as Record<string, unknown>;
    const messageType =
        stringValue(record.event_type) ||
        stringValue(record.type) ||
        stringValue(record.topic) ||
        inferMessageType(record);

    return [
        `type=${messageType}`,
        formatField("market", record.market),
        formatField("asset_id", record.asset_id),
        formatField("timestamp", record.timestamp),
        formatCollectionField("price_changes", record.price_changes),
        formatCollectionField("bids", record.bids),
        formatCollectionField("asks", record.asks),
    ]
        .filter(Boolean)
        .join(" ");
}

function getMessageType(message: unknown): string {
    if (Array.isArray(message)) {
        return message.length === 0 ? "empty_array" : "array";
    }

    if (!message || typeof message !== "object") {
        return typeof message;
    }

    const record = message as Record<string, unknown>;
    return (
        stringValue(record.event_type) ||
        stringValue(record.type) ||
        stringValue(record.topic) ||
        inferMessageType(record)
    );
}

function inferMessageType(record: Record<string, unknown>): string {
    if ("price_changes" in record) return "price_change";
    if ("bids" in record || "asks" in record) return "orderbook_snapshot";
    if ("event_message" in record || "condition_id" in record) return "market_lifecycle";
    return "unknown_object";
}

function formatField(name: string, value: unknown): string {
    const normalized = stringValue(value);
    return normalized ? `${name}=${normalized}` : "";
}

function formatCollectionField(name: string, value: unknown): string {
    return Array.isArray(value) ? `${name}=${value.length}` : "";
}

function stringValue(value: unknown): string {
    return typeof value === "string" || typeof value === "number" || typeof value === "boolean"
        ? String(value)
        : "";
}

async function start(): Promise<void> {
    subscribedAssetIds = ASSET_IDS.length > 0 ? ASSET_IDS : await discoverAssetIds();

    if (subscribedAssetIds.length === 0) {
        throw new Error("未找到可订阅的 asset ids，请检查 MARKET_REQUIRED_TAG_IDS 或 Gamma 返回数据");
    }

    console.log(`[init] 目标 host: ${WS_HOST ?? "(SDK 默认)"}`);
    console.log(`[init] 订阅 asset 数量: ${subscribedAssetIds.length}`);
    console.log(`[init] 订阅 batch size: ${SUBSCRIPTION_BATCH_SIZE}`);
    console.log(`[init] WebSocket 将持续监听，heartbeat=${HEARTBEAT_MS}ms，按 Ctrl-C 断开`);

    client = new RealTimeDataClient({
        host: WS_HOST,
        onConnect,
        onMessage,
        onStatusChange: status => {
            console.log(`[status] ${status}`);
        },
        autoReconnect: true,
    }).connect();
}

async function discoverAssetIds(): Promise<string[]> {
    console.log("[discover] 未传 ASSET_IDS，按 MARKET_REQUIRED_TAG_IDS 自动发现订阅市场...");

    const discoveryService = new MarketDiscoveryService();
    const markets = await discoveryService.discoverCryptoUpDownMarkets();
    const assetIds = Array.from(new Set(markets.flatMap(market => market.assetIds)));
    const eventSlugs = Array.from(new Set(markets.map(market => market.eventSlug || market.slug).filter(Boolean)));

    console.log(`[discover] markets=${markets.length} assets=${assetIds.length}`);
    console.log(`[discover:event-slugs] count=${eventSlugs.length}`);
    for (const slug of eventSlugs) {
        console.log(slug);
    }

    for (const market of markets.slice(0, 10)) {
        console.log(
            `[discover:market] eventSlug=${market.eventSlug ?? "-"} conditionId=${market.conditionId ?? "-"} assets=${market.assetIds.length} question=${market.question ?? "-"}`,
        );
    }
    if (markets.length > 10) {
        console.log(`[discover] 仅展示前 10 个 market，剩余 ${markets.length - 10} 个已加入订阅`);
    }

    return assetIds;
}

function chunk<T>(items: T[], size: number): T[][] {
    const chunks: T[][] = [];
    for (let index = 0; index < items.length; index += size) {
        chunks.push(items.slice(index, index + size));
    }
    return chunks;
}

function payloadForLog(payload: MarketSubscriptionMessage): MarketSubscriptionMessage {
    return {
        ...payload,
        assets_ids: payload.assets_ids.slice(0, 5),
    };
}
