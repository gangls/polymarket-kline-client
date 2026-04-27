// src/services/polymarket-client.service.ts
import {
    RealTimeDataClient,
    ConnectionStatus as SDKConnectionStatus,
    Message,
} from "../polymarket-websocket";
import { EventEmitter } from "events";
import { OrderMatchedMessage, ConnectionStatus } from "../models/types";
import { logger } from "../utils/logger";

interface ClientConfig {
    autoReconnect: boolean;
    pingInterval: number;
    host?: string;
}

export class PolymarketClientService extends EventEmitter {
    private client: RealTimeDataClient | null = null;
    private status: ConnectionStatus = ConnectionStatus.DISCONNECTED;
    private config: ClientConfig;
    private reconnectAttempts: number = 0;
    private maxReconnectAttempts: number = 10;

    constructor(config?: Partial<ClientConfig>) {
        super();
        this.config = {
            autoReconnect: true,
            pingInterval: 5000,
            host: process.env.POLYMARKET_WS_URL || "wss://ws-live-data.polymarket.com",
            ...config,
        };
    }

    /**
     * 连接到 Polymarket WebSocket
     */
    async connect(): Promise<void> {
        return new Promise((resolve, reject) => {
            try {
                logger.info("Connecting to Polymarket WebSocket...");
                this.updateStatus(ConnectionStatus.CONNECTING);

                this.client = new RealTimeDataClient({
                    host: this.config.host,
                    pingInterval: this.config.pingInterval,
                    autoReconnect: false, // 我们自己控制重连
                    onConnect: this.handleConnect,
                    onMessage: this.handleMessage,
                    onStatusChange: this.handleStatusChange,
                });

                this.client.connect();

                // 设置超时
                const timeout = setTimeout(() => {
                    reject(new Error("Connection timeout after 10 seconds"));
                }, 10000);

                this.once("connected", () => {
                    clearTimeout(timeout);
                    resolve();
                });

                this.once("connection_error", error => {
                    clearTimeout(timeout);
                    reject(error);
                });
            } catch (error) {
                logger.error("Failed to create WebSocket client:", error);
                reject(error);
            }
        });
    }

    /**
     * 处理连接成功
     */
    private handleConnect = (client: RealTimeDataClient) => {
        logger.info("✅ WebSocket connected successfully");
        this.reconnectAttempts = 0;
        this.updateStatus(ConnectionStatus.CONNECTED);
        // 订阅 orders_matched 事件
        this.subscribeToOrdersMatched();

        this.emit("connected");
    };

    /**
     * 处理状态变化
     */
    private handleStatusChange = (status: SDKConnectionStatus) => {
        logger.debug(`Connection status changed: ${status}`);

        switch (status) {
            case SDKConnectionStatus.CONNECTED:
                this.updateStatus(ConnectionStatus.CONNECTED);
                break;
            case SDKConnectionStatus.DISCONNECTED:
                this.updateStatus(ConnectionStatus.DISCONNECTED);
                this.handleDisconnect();
                break;
            case SDKConnectionStatus.CONNECTING:
                this.updateStatus(ConnectionStatus.CONNECTING);
                break;
        }
    };

    /**
     * 处理接收到的消息
     */
    private handleMessage = (client: RealTimeDataClient, message: Message) => {
        try {
            // 只处理 orders_matched 消息
            if (message.topic === "activity" && message.type === "orders_matched") {
                const orderMessage = message as OrderMatchedMessage;
                this.emit("order_matched", orderMessage.payload);
                logger.debug(
                    `📥 Order received: ${orderMessage.payload.slug} ${orderMessage.payload.outcome} @ ${orderMessage.payload.price}`,
                );
            }
        } catch (error) {
            logger.error("Error processing message:", error);
        }
    };

    /**
     * 处理断开连接
     */
    private handleDisconnect = () => {
        logger.warn("WebSocket disconnected");

        if (this.config.autoReconnect && this.reconnectAttempts < this.maxReconnectAttempts) {
            this.reconnect();
        } else if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            logger.error("Max reconnection attempts reached");
            this.emit("max_reconnect_reached");
        }
    };

    /**
     * 重连逻辑
     */
    private async reconnect(): Promise<void> {
        this.reconnectAttempts++;
        const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);

        logger.info(
            `Reconnecting in ${delay}ms... (Attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts})`,
        );
        this.updateStatus(ConnectionStatus.RECONNECTING);

        setTimeout(async () => {
            try {
                await this.connect();
                this.emit("reconnected", { attempts: this.reconnectAttempts });
            } catch (error) {
                logger.error("Reconnection failed:", error);
                this.handleDisconnect();
            }
        }, delay);
    }

    /**
     * 订阅 orders_matched
     */
    private subscribeToOrdersMatched(): void {
        if (!this.client) {
            logger.error("Cannot subscribe: client not initialized");
            return;
        }

        const subscriptionMessage = {
            subscriptions: [
                {
                    topic: "activity",
                    type: "orders_matched",
                },
            ],
        };

        this.client.subscribe(subscriptionMessage);
        logger.info("📡 Subscribed to orders_matched");
    }

    /**
     * 断开连接
     */
    disconnect(): void {
        this.config.autoReconnect = false;
        if (this.client) {
            this.client.disconnect();
            this.client = null;
        }
        this.updateStatus(ConnectionStatus.DISCONNECTED);
    }

    /**
     * 更新状态
     */
    private updateStatus(status: ConnectionStatus): void {
        this.status = status;
        this.emit("status_change", status);
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
