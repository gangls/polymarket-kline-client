import { RealTimeDataClient } from "../src/polymarket-websocket/client";
import { Message } from "../src/polymarket-websocket/model";

/**
 * 手动验证脚本：测试 Polymarket market WebSocket 是否能收到 new_market 事件
 *
 * 用法: npx ts-node test/test.ts
 * 可选环境变量:
 *   WS_HOST        - WebSocket 地址（默认 wss://ws-live-data.polymarket.com）
 *   TIMEOUT_MS     - 超时毫秒数（默认 120000，即 2 分钟）
 */

const WS_HOST = process.env.WS_HOST; // undefined 则使用 SDK 默认值
const TIMEOUT_MS = Number(process.env.TIMEOUT_MS) || 120_000;

let client: RealTimeDataClient | null = null;
let timeoutTimer: ReturnType<typeof setTimeout> | null = null;
let found = false;

function cleanup(exitCode: number) {
    if (timeoutTimer) {
        clearTimeout(timeoutTimer);
        timeoutTimer = null;
    }
    if (client) {
        console.log("[cleanup] 断开 WebSocket 连接...");
        client.disconnect();
        client = null;
    }
    process.exit(exitCode);
}

// ---- 回调 ----

const onMessage = (_: RealTimeDataClient, message: Message): void => {
    const { topic, type } = message;
    console.log(`[msg] topic=${topic}  type=${type}`);

    if (type === "new_market" || topic === "new_market") {
        found = true;
        console.log("========== ✅ 收到 new_market 事件 ==========");
        console.log(JSON.stringify(message, null, 2));
        console.log("==============================================");
        cleanup(0);
    }
};

const onConnect = (c: RealTimeDataClient): void => {
    console.log("[connected] WebSocket 连接成功");

    const subscribePayload = {
        subscriptions: [
            {
                topic: "activity",
                type: "orders_matched",
            },
        ],
    };

    console.log("[subscribe] 发送订阅:", JSON.stringify(subscribePayload));
    c.subscribe(subscribePayload);
};

// ---- 启动 ----

console.log(`[init] 目标 host: ${WS_HOST ?? "(SDK 默认)"}`);
console.log(`[init] 超时时间: ${TIMEOUT_MS}ms`);

client = new RealTimeDataClient({
    host: WS_HOST,
    onConnect,
    onMessage,
    autoReconnect: false,
}).connect();

// 超时退出
timeoutTimer = setTimeout(() => {
    if (!found) {
        console.log(`[timeout] ${TIMEOUT_MS}ms 内未观察到 new_market 事件，退出`);
        console.log("[hint] 可能原因:");
        console.log("  1. 当前时间段没有新市场创建");
        console.log("  2. market 频道不广播 new_market 到当前订阅方式");
        console.log("  3. SDK onMessage 中 payload 过滤逻辑丢弃了消息（见 client.ts:164）");
        cleanup(1);
    }
}, TIMEOUT_MS);

// 优雅退出
process.on("SIGINT", () => {
    console.log("\n[SIGINT] 手动中断，正在断开...");
    cleanup(found ? 0 : 1);
});
