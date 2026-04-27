// src/config/redis.ts
import Redis from 'ioredis';
import { logger } from '../utils/logger';

class RedisManager {
    private static instance: Redis;
    private static subscriber: Redis;

    static getClient(): Redis {
        if (!this.instance) {
            this.instance = new Redis({
                host: process.env.REDIS_HOST || 'localhost',
                port: parseInt(process.env.REDIS_PORT || '6379'),
                password: process.env.REDIS_PASSWORD,
                db: 0,
                retryStrategy: (times) => {
                    const delay = Math.min(times * 50, 2000);
                    logger.warn(`Redis reconnecting in ${delay}ms...`);
                    return delay;
                },
                maxRetriesPerRequest: 3,
                enableReadyCheck: true,
                lazyConnect: false
            });

            this.instance.on('connect', () => {
                logger.info('✅ Redis connected');
            });

            this.instance.on('error', (error) => {
                logger.error('Redis error:', error);
            });
        }
        return this.instance;
    }

    static getSubscriber(): Redis {
        if (!this.subscriber) {
            this.subscriber = new Redis({
                host: process.env.REDIS_HOST || 'localhost',
                port: parseInt(process.env.REDIS_PORT || '6379'),
                password: process.env.REDIS_PASSWORD,
                db: 0,
            });
        }
        return this.subscriber;
    }
}

export const redisClient = RedisManager.getClient();
export const redisSubscriber = RedisManager.getSubscriber();

/**
 * Lua脚本：原子更新秒级K线
 */
export const UPSERT_SECOND_BAR_SCRIPT = `
local key = KEYS[1]
local price = tonumber(ARGV[1])
local size = tonumber(ARGV[2])
local timestamp = tonumber(ARGV[3])
local current_time = tonumber(ARGV[4])

local exists = redis.call('EXISTS', key)

if exists == 0 then
    redis.call('HMSET', key,
        'open', price,
        'high', price,
        'low', price,
        'close', price,
        'volume', size,
        'timestamp', timestamp,
        'tradeCount', 1,
        'lastUpdate', current_time)
    redis.call('EXPIRE', key, 86400)
    return {1, price, price, price, price, size, 1}
else
    local current_high = tonumber(redis.call('HGET', key, 'high'))
    local current_low = tonumber(redis.call('HGET', key, 'low'))
    local current_volume = tonumber(redis.call('HGET', key, 'volume'))
    local current_count = tonumber(redis.call('HGET', key, 'tradeCount'))
    
    local new_high = price > current_high and price or current_high
    local new_low = price < current_low and price or current_low
    local new_volume = current_volume + size
    local new_count = current_count + 1
    
    redis.call('HMSET', key,
        'high', new_high,
        'low', new_low,
        'close', price,
        'volume', new_volume,
        'tradeCount', new_count,
        'lastUpdate', current_time)
    redis.call('EXPIRE', key, 86400)
    
    return {0, nil, new_high, new_low, price, new_volume, new_count}
end
`;

/**
 * 加载 Lua 脚本到 Redis（缓存 SHA）
 * 使用 defineCommand 方法（推荐）
 */
export const loadLuaScripts = async () => {
    try {
        // 方法1：使用 defineCommand 定义命令
        redisClient.defineCommand('upsertSecondBar', {
            numberOfKeys: 1,
            lua: UPSERT_SECOND_BAR_SCRIPT
        });
        
        logger.info('✅ Lua script registered: upsertSecondBar');
        
        // 可选：预加载脚本获取 SHA（用于性能监控）
        const sha1 = await redisClient.script('LOAD', UPSERT_SECOND_BAR_SCRIPT);
        logger.info(`Lua script SHA: ${sha1}`);
        
        return sha1;
    } catch (error) {
        logger.error('Failed to load Lua script:', error);
        throw error;
    }
};

/**
 * 使用定义好的命令
 */
export const upsertSecondBar = async (
    key: string,
    price: number,
    size: number,
    timestamp: number,
    currentTime: number
) => {
    // @ts-expect-error - defineCommand 添加的动态方法
    return redisClient.upsertSecondBar(
        key,
        price.toString(),
        size.toString(),
        timestamp.toString(),
        currentTime.toString()
    );
};