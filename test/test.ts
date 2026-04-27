import { redisClient } from "../src/config/redis";

async function testRedis() {
    const testKey = `second_bar:bitcoin-up-or-down-april-25-2026-4am-et:Up:*`;
    const data = await redisClient.hgetall(testKey);
    console.log("Test key data:", data);
}

testRedis().catch(error => {
    console.error("Error testing Redis:", error);
});
