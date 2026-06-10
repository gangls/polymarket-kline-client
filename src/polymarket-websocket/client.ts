import WebSocket, { MessageEvent, CloseEvent, ErrorEvent } from "isomorphic-ws";
import { WebSocketSubscriptionMessage, Message, ConnectionStatus } from "./model";

const DEFAULT_HOST = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const DEFAULT_PING_INTERVAL = 5000;

/**
 * Interface representing the arguments for initializing a RealTimeDataClient.
 */
export interface RealTimeDataClientArgs {
    /**
     * Optional callback function that is called when the client successfully connects.
     * @param client - The instance of the RealTimeDataClient that has connected.
     */
    onConnect?: (client: RealTimeDataClient) => void;

    /**
     * Optional callback function that is called when the client receives a message.
     * @param client - The instance of the RealTimeDataClient that received the message.
     * @param message - The message received by the client.
     */
    onMessage?: (client: RealTimeDataClient, message: unknown) => void;

    /**
     * Optional callback function that is called when the client receives a connection status update.
     * @param status - The connection status of the client.
     */
    onStatusChange?: (status: ConnectionStatus) => void;

    /**
     * Optional host address to connect to.
     */
    host?: string;

    /**
     * Optional interval in milliseconds for sending ping messages to keep the connection alive.
     */
    pingInterval?: number;

    /**
     * Optional flag to enable or disable automatic reconnection when the connection is lost.
     */
    autoReconnect?: boolean;
}

/**
 * A client for managing real-time WebSocket connections, handling messages, subscriptions,
 * and automatic reconnections.
 */
export class RealTimeDataClient {
    /** WebSocket server host URL */
    private readonly host: string;

    /** Interval (in milliseconds) for sending ping messages */
    private readonly pingInterval: number;

    /** Determines whether the client should automatically reconnect on disconnection */
    private autoReconnect: boolean;

    /** Callback function executed when the connection is established */
    private readonly onConnect?: (client: RealTimeDataClient) => void;

    /** Callback function executed when a custom message is received */
    private readonly onCustomMessage?: (client: RealTimeDataClient, message: unknown) => void;

    /** Callback function executed on a connection status update */
    private readonly onStatusChange?: (status: ConnectionStatus) => void;

    /** WebSocket instance */
    private ws!: WebSocket;

    /** Reconnect attempt counter for exponential backoff */
    private reconnectAttempts: number = 0;

    /** Maximum reconnect attempts before giving up */
    private readonly maxReconnectAttempts: number = 10;

    /** Whether a reconnect is already scheduled */
    private reconnectPending: boolean = false;

    /** Whether the socket was intentionally closed */
    private closedByUser: boolean = false;

    /**
     * Constructs a new RealTimeDataClient instance.
     * @param args Configuration options for the client.
     */
    constructor(args?: RealTimeDataClientArgs) {
        this.host = args?.host || DEFAULT_HOST;
        this.pingInterval = args?.pingInterval || DEFAULT_PING_INTERVAL;
        this.autoReconnect = args?.autoReconnect ?? true;
        this.onCustomMessage = args?.onMessage;
        this.onConnect = args?.onConnect;
        this.onStatusChange = args?.onStatusChange;
    }

    /**
     * Establishes a WebSocket connection to the server.
     */
    public connect() {
        this.closedByUser = false;
        this.notifyStatusChange(ConnectionStatus.CONNECTING);
        this.ws = new WebSocket(this.host);
        if (this.ws) {
            this.ws.onopen = this.onOpen;
            this.ws.onmessage = this.onMessage;
            this.ws.onclose = this.onClose;
            this.ws.onerror = this.onError;
            this.ws.pong = this.onPong;
        }
        return this;
    }

    /**
     * Handles WebSocket 'open' event. Executes the `onConnect` callback and starts pinging.
     */
    private onOpen = async () => {
        this.reconnectAttempts = 0;
        if (!this.isMarketChannel()) {
            this.ping();
        }
        this.notifyStatusChange(ConnectionStatus.CONNECTED);
        if (this.onConnect) {
            this.onConnect(this);
        }
    };

    /**
     * Handles WebSocket 'pong' event. Continues the ping cycle.
     */
    private onPong = async () => {
        delay(this.pingInterval).then(() => this.ping());
    };

    /**
     * Handles WebSocket errors. Logs the error and attempts reconnection if `autoReconnect` is enabled.
     * @param err Error object describing the issue.
     */
    private onError = async (err: ErrorEvent) => {
        if (this.closedByUser) return;
        console.error("error", err);
        this.reconnectWithBackoff();
    };

    /**
     * Handles WebSocket 'close' event. Logs the disconnect reason and attempts reconnection if `autoReconnect` is enabled.
     * @param code Close event code.
     * @param reason Buffer containing the reason for closure.
     */
    private onClose = async (message: CloseEvent) => {
        if (this.closedByUser) return;
        console.error("disconnected", "code", message.code, "reason", message.reason);
        this.notifyStatusChange(ConnectionStatus.DISCONNECTED);
        this.reconnectWithBackoff();
    };

    /**
     * Sends a ping message to keep the connection alive.
     */
    private ping = async () => {
        if (this.ws.readyState !== WebSocket.OPEN) {
            return console.warn("Socket not open. Ready state is:", this.ws.readyState);
        }

        this.ws.send("ping", (err: Error | undefined) => {
            if (err) {
                console.error("ping error", err);
            }
        });
    };

    /**
     * Handles incoming WebSocket messages. Parses and processes custom messages if applicable.
     * @param event Raw WebSocket message data.
     */
    private onMessage = (event: MessageEvent): void => {
        if (typeof event.data === "string" && event.data.length > 0) {
            try {
                const message = JSON.parse(event.data);
                if (this.onCustomMessage) {
                    this.onCustomMessage(this, message as Message);
                }
            } catch {
                console.log("onMessage parse error", event.data);
            }
        }
    };

    /**
     * Reconnects with exponential backoff.
     */
    private reconnectWithBackoff() {
        if (this.closedByUser) return;
        if (!this.autoReconnect) return;
        if (this.reconnectPending) return;
        if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            console.error(`Max reconnect attempts (${this.maxReconnectAttempts}) reached, giving up`);
            return;
        }
        this.reconnectPending = true;
        const backoffMs = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);
        this.reconnectAttempts++;
        console.log(`Reconnecting in ${backoffMs}ms (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts})`);
        setTimeout(() => {
            this.reconnectPending = false;
            this.connect();
        }, backoffMs);
    }

    /**
     * Closes the WebSocket connection.
     */
    public disconnect() {
        this.closedByUser = true;
        this.autoReconnect = false;
        if (
            this.ws &&
            (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)
        ) {
            this.ws.close();
        }
    }

    /**
     * Subscribes to a data stream by sending a subscription message.
     * @param msg Subscription request message.
     */
    public subscribe(msg: WebSocketSubscriptionMessage) {
        if (this.ws.readyState !== WebSocket.OPEN) {
            return console.warn("Socket not open. Ready state is:", this.ws.readyState);
        }
        this.ws.send(JSON.stringify(this.createSubscribePayload(msg)), (err?: Error) => {
            if (err) {
                console.error("subscribe error", err);
                this.ws.close();
            }
        });
    }

    /**
     * Unsubscribes from a data stream by sending an unsubscription message.
     * @param msg Unsubscription request message.
     */
    public unsubscribe(msg: WebSocketSubscriptionMessage) {
        if (this.ws.readyState !== WebSocket.OPEN) {
            return console.warn("Socket not open. Ready state is:", this.ws.readyState);
        }
        this.ws.send(JSON.stringify(this.createUnsubscribePayload(msg)), (err?: Error) => {
            if (err) {
                console.error("unsubscribe error", err);
                this.ws.close();
            }
        });
    }

    /**
     * Callback for connection status changes
     * @param status status of the connection
     */
    private notifyStatusChange(status: ConnectionStatus) {
        if (this.onStatusChange) {
            this.onStatusChange(status);
        }
        return status;
    }

    private createSubscribePayload(msg: WebSocketSubscriptionMessage): unknown {
        if (this.isMarketChannel()) {
            const marketSubscription = normalizeMarketSubscription(msg);
            if (marketSubscription.operation) {
                return {
                    operation: marketSubscription.operation,
                    assets_ids: marketSubscription.assets_ids,
                };
            }

            return marketSubscription;
        }

        return { action: "subscribe", ...msg };
    }

    private createUnsubscribePayload(msg: WebSocketSubscriptionMessage): unknown {
        if (this.isMarketChannel()) {
            const marketSubscription = normalizeMarketSubscription(msg);
            return {
                operation: "unsubscribe",
                assets_ids: marketSubscription.assets_ids,
            };
        }

        return { action: "unsubscribe", ...msg };
    }

    private isMarketChannel(): boolean {
        return this.host.includes("/ws/market");
    }
}

function delay(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function normalizeMarketSubscription(msg: WebSocketSubscriptionMessage) {
    if ("assets_ids" in msg) {
        return msg;
    }

    const subscription = msg.subscriptions[0];
    return {
        assets_ids: subscription.assets_ids ?? [],
        type: "market" as const,
        initial_dump: subscription.initial_dump,
        level: subscription.level,
        custom_feature_enabled: subscription.custom_feature_enabled,
    };
}
