// src/services/polymarket-client.service.ts
import {
    RealTimeDataClient,
    ConnectionStatus as SDKConnectionStatus,
    Message,
} from "../polymarket-websocket";
import { EventEmitter } from "events";
import { MarketPriceChangeMessage, MarketSubscriptionMessage } from "../polymarket-websocket/model";
import { OrderMatched, OrderMatchedMessage, ConnectionStatus } from "../models/types";
import { DiscoveredMarket, MarketDiscoveryService } from "./market-discovery.service";
import { logger } from "../utils/logger";

const DEFAULT_POLYMARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const DEFAULT_SUBSCRIPTION_BATCH_SIZE = 20;
const DEFAULT_DISCOVERY_REFRESH_MS = 60_000;
const DEFAULT_ASSET_PARTITIONS: AssetPartition[] = [
    { id: "btc-5m", label: "BTC-5M", assetTagIds: ["235"], intervalTagIds: ["102892"] },
    { id: "btc-15m", label: "BTC-15M", assetTagIds: ["235"], intervalTagIds: ["102467"] },
    { id: "btc-1h", label: "BTC-1H", assetTagIds: ["235"], intervalTagIds: ["102175"] },
    { id: "btc-4h", label: "BTC-4H", assetTagIds: ["235"], intervalTagIds: ["102531"] },
    { id: "btc-daily", label: "BTC-Daily", assetTagIds: ["235"], intervalTagIds: ["102281"] },
    { id: "eth", label: "ETH", assetTagIds: ["39"] },
    { id: "sol", label: "SOL", assetTagIds: ["818"] },
    { id: "xrp", label: "XRP", assetTagIds: ["101267"] },
    { id: "doge", label: "DOGE", assetTagIds: ["100178"] },
    { id: "hype", label: "HYPE", assetTagIds: ["102331"] },
    { id: "bnb", label: "BNB", assetTagIds: ["102716"] },
];

interface AssetPartition {
    id: string;
    label: string;
    assetTagIds: string[];
    intervalTagIds?: string[];
}

interface ClientConfig {
    autoReconnect: boolean;
    pingInterval: number;
    host?: string;
    subscriptionBatchSize: number;
    discoveryRefreshMs: number;
    connectStaggerMs: number;
    reconnectJitterMs: number;
    initialDump: boolean;
    splitByAsset: boolean;
    assetPartitions: AssetPartition[];
}

interface AssetMetadata {
    assetId: string;
    conditionId: string;
    eventSlug: string;
    outcome: "Up" | "Down";
    outcomeIndex: number;
    slug: string;
    title: string;
}

interface MarketResolvedMessage {
    event_type: "market_resolved";
    market: string;
    assets_ids: string[];
    winning_asset_id?: string;
    winning_outcome?: string;
    timestamp?: string;
}

interface ClientContext {
    id: string;
    label: string;
    assetTagIds: string[];
    intervalTagIds?: string[];
    client: RealTimeDataClient | null;
    status: ConnectionStatus;
    reconnectAttempts: number;
    assetMetadataById: Map<string, AssetMetadata>;
    subscribedAssetIds: Set<string>;
    subscribedEventSlugs: Set<string>;
    refreshTimer: NodeJS.Timeout | null;
    refreshInFlight: boolean;
    connectionVersion: number;
}

export class PolymarketClientService extends EventEmitter {
    private status: ConnectionStatus = ConnectionStatus.DISCONNECTED;
    private config: ClientConfig;
    private maxReconnectAttempts: number = 10;
    private contexts: ClientContext[];

    constructor(config?: Partial<ClientConfig>) {
        super();
        const splitByAsset = process.env.MARKET_WS_SPLIT_BY_ASSET !== "false";
        const assetPartitions = parseAssetPartitions(process.env.MARKET_WS_ASSET_PARTITIONS);
        this.config = {
            autoReconnect: true,
            pingInterval: 5000,
            host: process.env.POLYMARKET_WS_URL || DEFAULT_POLYMARKET_WS_URL,
            subscriptionBatchSize: Math.min(
                Number(process.env.MARKET_SUBSCRIPTION_BATCH_SIZE) || DEFAULT_SUBSCRIPTION_BATCH_SIZE,
                DEFAULT_SUBSCRIPTION_BATCH_SIZE,
            ),
            discoveryRefreshMs: Number(process.env.MARKET_DISCOVERY_REFRESH_MS) || DEFAULT_DISCOVERY_REFRESH_MS,
            connectStaggerMs: Number(process.env.WS_CONNECT_STAGGER_MS) || 1000,
            reconnectJitterMs: Number(process.env.WS_RECONNECT_JITTER_MS) || 5000,
            initialDump: process.env.MARKET_SUBSCRIPTION_INITIAL_DUMP === "true",
            splitByAsset,
            assetPartitions,
            ...config,
        };
        this.contexts = this.createClientContexts();
    }

    /**
     * 连接到 Polymarket WebSocket
     */
    async connect(): Promise<void> {
        logger.info(
            `Connecting to Polymarket WebSocket partitions: ${this.contexts.map(context => context.label).join(",")}`,
        );
        this.updateOverallStatus();
        for (const [index, context] of this.contexts.entries()) {
            if (index > 0 && this.config.connectStaggerMs > 0) {
                await delay(this.config.connectStaggerMs);
            }
            await this.connectContext(context);
        }
        this.emit("connected");
    }

    private connectContext(context: ClientContext): Promise<void> {
        return new Promise((resolve, reject) => {
            let settled = false;
            const connectionVersion = ++context.connectionVersion;

            try {
                logger.info(`[${context.label}] Connecting to Polymarket WebSocket...`);
                if (context.client) {
                    context.client.disconnect();
                    context.client = null;
                }
                context.status = ConnectionStatus.CONNECTING;
                this.updateOverallStatus();

                const client = new RealTimeDataClient({
                    host: this.config.host,
                    pingInterval: this.config.pingInterval,
                    autoReconnect: false,
                    onConnect: connectedClient => {
                        if (context.connectionVersion !== connectionVersion) return;
                        this.handleConnect(context, connectedClient).then(() => {
                            if (!settled) {
                                settled = true;
                                clearTimeout(timeout);
                                resolve();
                            }
                        }).catch(error => {
                            if (context.connectionVersion !== connectionVersion) return;
                            if (!settled) {
                                settled = true;
                                clearTimeout(timeout);
                                reject(error);
                            }
                            this.emit("connection_error", error);
                        });
                    },
                    onMessage: (connectedClient, message) => {
                        if (context.connectionVersion !== connectionVersion) return;
                        this.handleMessage(context, connectedClient, message);
                    },
                    onStatusChange: status => {
                        if (context.connectionVersion !== connectionVersion) return;
                        this.handleStatusChange(context, status);
                    },
                });

                context.client = client;
                client.connect();

                const timeout = setTimeout(() => {
                    if (settled) return;
                    settled = true;
                    reject(new Error(`[${context.label}] Connection timeout`));
                }, Number(process.env.WS_CONNECT_TIMEOUT_MS) || 30000);
            } catch (error) {
                if (!settled) {
                    settled = true;
                    reject(error);
                }
                logger.error(`[${context.label}] Failed to create WebSocket client:`, error);
            }
        });
    }

    private async handleConnect(context: ClientContext, client: RealTimeDataClient): Promise<void> {
        logger.info(`[${context.label}] ✅ WebSocket connected successfully`);
        context.reconnectAttempts = 0;
        context.status = ConnectionStatus.CONNECTED;
        this.updateOverallStatus();
        await this.refreshSubscriptions(context, client, true);
        this.startSubscriptionRefresh(context);
    }

    /**
     * 处理状态变化
     */
    private handleStatusChange = (context: ClientContext, status: SDKConnectionStatus) => {
        logger.debug(`[${context.label}] Connection status changed: ${status}`);

        switch (status) {
            case SDKConnectionStatus.CONNECTED:
                context.status = ConnectionStatus.CONNECTED;
                this.updateOverallStatus();
                break;
            case SDKConnectionStatus.DISCONNECTED:
                context.status = ConnectionStatus.DISCONNECTED;
                this.updateOverallStatus();
                this.handleDisconnect(context);
                break;
            case SDKConnectionStatus.CONNECTING:
                context.status = ConnectionStatus.CONNECTING;
                this.updateOverallStatus();
                break;
        }
    };

    /**
     * 处理接收到的消息
     */
    private handleMessage = (context: ClientContext, client: RealTimeDataClient, message: unknown) => {
        try {
            // 只处理 orders_matched 消息
            if (isRealtimeMessage(message) && message.topic === "activity" && message.type === "orders_matched") {
                const orderMessage = message as OrderMatchedMessage;
                this.emit("order_matched", orderMessage.payload);
                logger.debug(
                    `📥 Order received: ${orderMessage.payload.slug} ${orderMessage.payload.outcome} @ ${orderMessage.payload.price}`,
                );
                return;
            }

            if (isMarketPriceChangeMessage(message)) {
                const orders = this.mapPriceChangesToOrders(context, message);
                for (const order of orders) {
                    this.emit("order_matched", order);
                }

                logger.debug(
                    `📥 Price changes mapped: market=${message.market} changes=${message.price_changes.length} orders=${orders.length}`,
                );
                return;
            }

            if (isMarketResolvedMessage(message)) {
                this.handleMarketResolved(context, client, message);
                return;
            }
        } catch (error) {
            logger.error(`[${context.label}] Error processing message:`, error);
        }
    };

    private handleMarketResolved(context: ClientContext, client: RealTimeDataClient, message: MarketResolvedMessage): void {
        const resolvedSubscribedAssets = message.assets_ids.filter(assetId => context.subscribedAssetIds.has(assetId));
        if (resolvedSubscribedAssets.length === 0) return;

        const resolvedEvents = Array.from(
            new Set(
                resolvedSubscribedAssets
                    .map(assetId => context.assetMetadataById.get(assetId)?.eventSlug)
                    .filter(Boolean) as string[],
            ),
        );

        logger.info(
            `[${context.label}] Subscribed Up/Down market resolved: events=${resolvedEvents.join(",") || "-"} market=${message.market} winning=${message.winning_outcome ?? "-"}`,
        );
        this.emit("up_down_event_resolved", {
            eventSlugs: resolvedEvents,
            market: message.market,
            assetsIds: message.assets_ids,
            winningAssetId: message.winning_asset_id,
            winningOutcome: message.winning_outcome,
            timestamp: message.timestamp,
        });

        this.refreshSubscriptions(context, client).catch(error => {
            logger.error(`[${context.label}] Failed to refresh subscriptions after market_resolved:`, error);
        });
    }

    private async refreshSubscriptions(
        context: ClientContext,
        client: RealTimeDataClient,
        forceSubscribeAll = false,
    ): Promise<void> {
        if (context.refreshInFlight) return;
        context.refreshInFlight = true;

        try {
            const discoveryService = new MarketDiscoveryService({
                assetTagIds: context.assetTagIds,
                ...(context.intervalTagIds ? { intervalTagIds: context.intervalTagIds } : {}),
            });
            const markets = await discoveryService.discoverCryptoUpDownMarkets();
            const nextAssetMetadata = this.createAssetMetadataIndex(markets);
            const nextAssetIds = new Set(nextAssetMetadata.keys());
            const nextEventSlugs = new Set(
                markets.map(market => market.eventSlug || market.slug).filter(Boolean) as string[],
            );
            const previousAssetIds = context.subscribedAssetIds;
            const previousEventSlugs = context.subscribedEventSlugs;

            if (nextAssetIds.size === 0) {
                throw new Error(`[${context.label}] No Polymarket asset ids discovered for subscription`);
            }

            const assetsToSubscribe = forceSubscribeAll
                ? Array.from(nextAssetIds)
                : Array.from(nextAssetIds).filter(assetId => !previousAssetIds.has(assetId));
            const assetsToUnsubscribe = Array.from(previousAssetIds).filter(assetId => !nextAssetIds.has(assetId));

            context.assetMetadataById = nextAssetMetadata;
            context.subscribedAssetIds = nextAssetIds;
            context.subscribedEventSlugs = nextEventSlugs;

            this.subscribeAssetIds(client, assetsToSubscribe, !forceSubscribeAll);
            this.unsubscribeAssetIds(client, assetsToUnsubscribe);
            this.emitNewUpDownEvents(context, markets, nextEventSlugs, previousEventSlugs, forceSubscribeAll);

            logger.info(
                `[${context.label}] Refreshed Polymarket subscriptions: markets=${markets.length} events=${nextEventSlugs.size} assets=${nextAssetIds.size} subscribed=${assetsToSubscribe.length} unsubscribed=${assetsToUnsubscribe.length}`,
            );
        } finally {
            context.refreshInFlight = false;
        }
    }

    private subscribeAssetIds(client: RealTimeDataClient, assetIds: string[], useUpdateOperation = false): void {
        if (assetIds.length === 0) return;

        const batches = chunk(assetIds, this.config.subscriptionBatchSize);
        logger.info(
            `Subscribing Polymarket market assets: assets=${assetIds.length} batches=${batches.length} operation=${useUpdateOperation ? "subscribe" : "initial"} sample=${assetIds.slice(0, 4).join(",")}`,
        );

        for (const [index, batch] of batches.entries()) {
            const shouldUseUpdateOperation = useUpdateOperation || index > 0;
            const payload: MarketSubscriptionMessage = {
                assets_ids: batch,
                type: "market",
                ...(shouldUseUpdateOperation
                    ? { operation: "subscribe" }
                    : {
                        ...(this.config.initialDump ? { initial_dump: true } : {}),
                        level: 2,
                        custom_feature_enabled: true,
                    }),
            };
            client.subscribe(payload);
        }
    }

    private unsubscribeAssetIds(client: RealTimeDataClient, assetIds: string[]): void {
        if (assetIds.length === 0) return;

        const batches = chunk(assetIds, this.config.subscriptionBatchSize);
        logger.info(
            `Unsubscribing Polymarket market assets: assets=${assetIds.length} batches=${batches.length} sample=${assetIds.slice(0, 4).join(",")}`,
        );

        for (const batch of batches) {
            const payload: MarketSubscriptionMessage = {
                assets_ids: batch,
                type: "market",
            };
            client.unsubscribe(payload);
        }
    }

    private createAssetMetadataIndex(markets: DiscoveredMarket[]): Map<string, AssetMetadata> {
        const assetMetadataById = new Map<string, AssetMetadata>();

        for (const market of markets) {
            const slug = market.eventSlug || market.slug || market.conditionId || "unknown";
            const title = market.question || slug;
            for (const [index, assetId] of market.assetIds.entries()) {
                const outcomeIndex = index === 0 ? 0 : 1;
                assetMetadataById.set(assetId, {
                    assetId,
                    conditionId: market.conditionId || "",
                    eventSlug: market.eventSlug || slug,
                    outcome: outcomeIndex === 0 ? "Up" : "Down",
                    outcomeIndex,
                    slug,
                    title,
                });
            }
        }

        return assetMetadataById;
    }

    private emitNewUpDownEvents(
        context: ClientContext,
        markets: DiscoveredMarket[],
        nextEventSlugs: Set<string>,
        previousEventSlugs: Set<string>,
        forceSubscribeAll: boolean,
    ): void {
        if (forceSubscribeAll && previousEventSlugs.size === 0) return;

        const firstMarketByEventSlug = new Map<string, DiscoveredMarket>();
        for (const market of markets) {
            const eventSlug = market.eventSlug || market.slug;
            if (eventSlug && !firstMarketByEventSlug.has(eventSlug)) {
                firstMarketByEventSlug.set(eventSlug, market);
            }
        }

        for (const eventSlug of nextEventSlugs) {
            if (previousEventSlugs.has(eventSlug)) continue;
            const market = firstMarketByEventSlug.get(eventSlug);
            const payload = {
                eventSlug,
                partition: context.id,
                assetTagIds: context.assetTagIds,
                intervalTagIds: context.intervalTagIds,
                assetIds: market?.assetIds ?? [],
                conditionId: market?.conditionId,
                title: market?.question,
            };
            logger.info(`[${context.label}] New Up/Down event discovered: ${eventSlug}`);
            this.emit("new_up_down_event", payload);
        }
    }

    private startSubscriptionRefresh(context: ClientContext): void {
        if (context.refreshTimer || this.config.discoveryRefreshMs <= 0) return;

        context.refreshTimer = setInterval(() => {
            if (!context.client || context.status !== ConnectionStatus.CONNECTED) return;
            this.refreshSubscriptions(context, context.client).catch(error => {
                logger.error(`[${context.label}] Failed to refresh Polymarket subscriptions:`, error);
            });
        }, this.config.discoveryRefreshMs);

        logger.info(`[${context.label}] Polymarket subscription refresh enabled: interval=${this.config.discoveryRefreshMs}ms`);
    }

    private stopSubscriptionRefresh(context: ClientContext): void {
        if (!context.refreshTimer) return;
        clearInterval(context.refreshTimer);
        context.refreshTimer = null;
    }

    private mapPriceChangesToOrders(context: ClientContext, message: MarketPriceChangeMessage): OrderMatched[] {
        const timestamp = Math.floor(Number(message.timestamp || Date.now()) / 1000);
        const latestPriceByAsset = new Map<string, MarketPriceChangeMessage["price_changes"][number]>();

        for (const change of message.price_changes) {
            latestPriceByAsset.set(change.asset_id, change);
        }

        return Array.from(latestPriceByAsset.values()).flatMap(change => {
            const metadata = context.assetMetadataById.get(change.asset_id);
            if (!metadata) {
                logger.debug(`[${context.label}] Skipping price_change for unsubscribed asset_id=${change.asset_id}`);
                return [];
            }

            const price = Number(change.price);
            const size = Number(change.size);
            if (!Number.isFinite(price) || !Number.isFinite(size)) {
                logger.warn("Skipping invalid price_change:", change);
                return [];
            }

            return [{
                asset: metadata.assetId,
                bio: "",
                conditionId: metadata.conditionId || message.market,
                eventSlug: metadata.eventSlug,
                icon: "",
                name: "",
                outcome: metadata.outcome,
                outcomeIndex: metadata.outcomeIndex,
                price,
                profileImage: "",
                proxyWallet: "",
                pseudonym: "",
                side: change.side === "SELL" ? "SELL" : "BUY",
                size,
                slug: metadata.slug,
                timestamp,
                title: metadata.title,
                transactionHash: change.hash,
            } satisfies OrderMatched];
        });
    }

    /**
     * 处理断开连接
     */
    private handleDisconnect = (context: ClientContext) => {
        logger.warn(`[${context.label}] WebSocket disconnected`);

        if (this.config.autoReconnect && context.reconnectAttempts < this.maxReconnectAttempts) {
            this.reconnect(context);
        } else if (context.reconnectAttempts >= this.maxReconnectAttempts) {
            logger.error(`[${context.label}] Max reconnection attempts reached`);
            this.emit("max_reconnect_reached", { partition: context.id });
        }
    };

    /**
     * 重连逻辑
     */
    private async reconnect(context: ClientContext): Promise<void> {
        context.reconnectAttempts++;
        const reconnectAttempt = context.reconnectAttempts;
        const baseDelay = Math.min(1000 * Math.pow(2, context.reconnectAttempts), 30000);
        const jitter = this.config.reconnectJitterMs > 0
            ? Math.floor(Math.random() * this.config.reconnectJitterMs)
            : 0;
        const delayMs = baseDelay + jitter;

        logger.info(
            `[${context.label}] Reconnecting in ${delayMs}ms... (Attempt ${context.reconnectAttempts}/${this.maxReconnectAttempts})`,
        );
        context.status = ConnectionStatus.RECONNECTING;
        this.updateOverallStatus();

        setTimeout(async () => {
            try {
                await this.connectContext(context);
                this.emit("reconnected", { partition: context.id, attempts: reconnectAttempt });
            } catch (error) {
                logger.error(`[${context.label}] Reconnection failed:`, error);
                this.handleDisconnect(context);
            }
        }, delayMs);
    }

    /**
     * 断开连接
     */
    disconnect(): void {
        this.config.autoReconnect = false;
        for (const context of this.contexts) {
            this.stopSubscriptionRefresh(context);
            if (context.client) {
                context.client.disconnect();
                context.client = null;
            }
            context.status = ConnectionStatus.DISCONNECTED;
        }
        this.updateOverallStatus();
    }

    /**
     * 更新状态
     */
    private updateOverallStatus(): void {
        const previousStatus = this.status;
        if (this.contexts.some(context => context.status === ConnectionStatus.RECONNECTING)) {
            this.status = ConnectionStatus.RECONNECTING;
        } else if (this.contexts.some(context => context.status === ConnectionStatus.CONNECTED)) {
            this.status = ConnectionStatus.CONNECTED;
        } else if (this.contexts.some(context => context.status === ConnectionStatus.CONNECTING)) {
            this.status = ConnectionStatus.CONNECTING;
        } else {
            this.status = ConnectionStatus.DISCONNECTED;
        }

        if (this.status !== previousStatus) {
            this.emit("status_change", this.status);
        }
    }

    private createClientContexts(): ClientContext[] {
        const partitions = this.config.splitByAsset
            ? this.config.assetPartitions
            : [{
                id: "all",
                label: "ALL",
                assetTagIds: parseCsv(process.env.MARKET_ASSET_TAG_IDS, DEFAULT_ASSET_PARTITIONS.flatMap(partition => partition.assetTagIds)),
                intervalTagIds: parseCsv(process.env.MARKET_INTERVAL_TAG_IDS, []),
            }];

        return partitions.map(partition => ({
            id: partition.id,
            label: partition.label,
            assetTagIds: partition.assetTagIds,
            intervalTagIds: partition.intervalTagIds,
            client: null,
            status: ConnectionStatus.DISCONNECTED,
            reconnectAttempts: 0,
            assetMetadataById: new Map<string, AssetMetadata>(),
            subscribedAssetIds: new Set<string>(),
            subscribedEventSlugs: new Set<string>(),
            refreshTimer: null,
            refreshInFlight: false,
            connectionVersion: 0,
        }));
    }

    /**
     * 获取当前状态
     */
    getStatus(): ConnectionStatus {
        return this.status;
    }

    /**
     * 检查是否已连接
     */
    isConnected(): boolean {
        return this.status === ConnectionStatus.CONNECTED;
    }
}

function isRealtimeMessage(value: unknown): value is Message {
    return (
        value !== null &&
        typeof value === "object" &&
        "topic" in value &&
        "type" in value &&
        "payload" in value
    );
}

function isMarketPriceChangeMessage(value: unknown): value is MarketPriceChangeMessage {
    if (!value || typeof value !== "object") return false;
    const record = value as Record<string, unknown>;
    return (
        record.event_type === "price_change" &&
        typeof record.market === "string" &&
        Array.isArray(record.price_changes)
    );
}

function isMarketResolvedMessage(value: unknown): value is MarketResolvedMessage {
    if (!value || typeof value !== "object") return false;
    const record = value as Record<string, unknown>;
    return (
        record.event_type === "market_resolved" &&
        typeof record.market === "string" &&
        Array.isArray(record.assets_ids)
    );
}

function chunk<T>(items: T[], size: number): T[][] {
    const chunks: T[][] = [];
    for (let index = 0; index < items.length; index += size) {
        chunks.push(items.slice(index, index + size));
    }
    return chunks;
}

function delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function parseAssetPartitions(value: string | undefined): AssetPartition[] {
    const parsed = (value || "")
        .split(",")
        .map(item => item.trim())
        .filter(Boolean)
        .map(item => {
            const [id, label, assetTagIds, intervalTagIds] = item.split(":");
            return {
                id: id || "",
                label: label || id || "",
                assetTagIds: parsePartitionTags(assetTagIds),
                intervalTagIds: parsePartitionTags(intervalTagIds),
            };
        })
        .filter(partition => partition.id && partition.assetTagIds.length > 0);

    return parsed.length > 0 ? parsed : DEFAULT_ASSET_PARTITIONS;
}

function parsePartitionTags(value: string | undefined): string[] {
    return (value || "")
        .split("+")
        .map(item => item.trim())
        .filter(Boolean);
}

function parseCsv(value: string | undefined, fallback: string[]): string[] {
    const parsed = (value || "")
        .split(",")
        .map(item => item.trim())
        .filter(Boolean);
    return parsed.length > 0 ? parsed : fallback;
}
