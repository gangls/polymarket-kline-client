// src/models/types.ts
import { Message as SDKMessage } from "../polymarket-websocket/model";

// 扩展SDK的Message类型
export interface OrderMatchedMessage extends SDKMessage {
    topic: "activity";
    type: "orders_matched";
    payload: OrderMatched;
}

export interface OrderMatched {
    asset: string;
    bio: string;
    conditionId: string;
    eventSlug: string;
    icon: string;
    name: string;
    outcome: "Up" | "Down";
    outcomeIndex: number;
    price: number;
    profileImage: string;
    proxyWallet: string;
    pseudonym: string;
    side: "BUY" | "SELL";
    size: number;
    slug: string;
    timestamp: number; // Unix timestamp (seconds)
    title: string;
    transactionHash: string;
}

// 秒级聚合数据
export interface SecondBar {
    timestamp: number; // UTC时间戳(秒)
    open: number;
    high: number;
    low: number;
    close: number;
    volume: number;
    marketKey: string; // 格式: `${slug}:${outcomeIndex}`
    tradeCount: number;
    lastUpdate: number;
}

// K线数据（聚合后的）
export interface KLine {
    marketKey: string; // `${slug}:${outcomeIndex}`
    interval: number; // 聚合粒度（秒）
    startTime: number;
    endTime: number;
    open: number;
    high: number;
    low: number;
    close: number;
    volume: number;
    tradeCount: number;
    lastUpdate: number;
}

// 聚合统计
export interface AggregationStats {
    totalOrdersProcessed: number;
    totalSecondBarsUpdated: number;
    avgProcessingTime: number;
    marketsActive: number;
    cacheSize: number;
}

export enum ConnectionStatus {
    CONNECTING = "CONNECTING",
    CONNECTED = "CONNECTED",
    DISCONNECTED = "DISCONNECTED",
    RECONNECTING = "RECONNECTING",
}
