/**
 * API key credentials for CLOB authentication.
 */
export interface ClobApiKeyCreds {
    /** API key used for authentication */
    key: string;

    /** API secret associated with the key */
    secret: string;

    /** Passphrase required for authentication */
    passphrase: string;
}

/**
 * Authentication details for Gamma authentication.
 */
export interface GammaAuth {
    /** Address used for authentication */
    address: string;
}

/**
 * Message structure for subscription requests.
 */
export interface SubscriptionMessage {
    subscriptions: {
        /** Topic to subscribe to */
        topic?: string;

        /** Type of subscription */
        type: string;

        /** Optional filters for the subscription */
        filters?: string;

        /** Optional CLOB authentication credentials */
        clob_auth?: ClobApiKeyCreds;

        /** Optional Gamma authentication credentials */
        gamma_auth?: GammaAuth;

        /** Optional flag to enable custom features for the subscription */
        custom_feature_enabled?: boolean;
        assets_ids?: string[];
        initial_dump?: boolean;
        level?: number;
    }[];
}

/**
 * Subscription request for the Polymarket CLOB market channel.
 */
export interface MarketSubscriptionMessage {
    /** Asset IDs (token IDs) to subscribe to */
    assets_ids: string[];

    /** Market channel subscription type */
    type: "market";

    /** Dynamic market channel subscription operation */
    operation?: "subscribe" | "unsubscribe";

    /** Whether to send an initial orderbook snapshot on subscribe */
    initial_dump?: boolean;

    /** Orderbook depth level */
    level?: number;

    /** Optional flag to enable custom features */
    custom_feature_enabled?: boolean;
}

export type WebSocketSubscriptionMessage = SubscriptionMessage | MarketSubscriptionMessage;

export interface MarketPriceChange {
    asset_id: string;
    price: string;
    size: string;
    side: "BUY" | "SELL" | string;
    hash: string;
    best_bid: string;
    best_ask: string;
}

export interface MarketPriceChangeMessage {
    event_type: "price_change";
    market: string;
    price_changes: MarketPriceChange[];
    timestamp: string;
}

/**
 * Represents a real-time message received from the WebSocket server.
 */
export interface Message {
    /** Topic of the message */
    topic: string;

    /** Type of the message */
    type: string;

    /** Timestamp of when the message was sent */
    timestamp: number;

    /** Payload containing the message data */
    payload: object;

    /** Connection ID */
    connection_id: string;
}

/**
 * Represents websocket connection status
 */
export enum ConnectionStatus {
    CONNECTING = "CONNECTING",
    CONNECTED = "CONNECTED",
    DISCONNECTED = "DISCONNECTED",
}
