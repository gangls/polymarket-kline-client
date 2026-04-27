// src/config/queue.ts
import Bull from 'bull';
import { redisClient } from './redis';
import { logger } from '../utils/logger';
import { OrderMatched } from '../models/types';

// Redis 配置
const redisConfig = {
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT || '6379'),
    password: process.env.REDIS_PASSWORD,
    db: parseInt(process.env.REDIS_QUEUE_DB || '1')
};

// 订单处理队列（直接使用 OrderMatched 作为数据类型）
export const orderQueue = new Bull<OrderMatched>('order-processing', {
    redis: redisConfig,
    defaultJobOptions: {
        attempts: 3,
        backoff: {
            type: 'exponential',
            delay: 1000
        },
        removeOnComplete: true,
        removeOnFail: false,
        timeout: 30000,
        delay: 0
    },
    limiter: {
        max: 1000,
        duration: 1000
    }
});

// 聚合队列（用于定时任务）
export const aggregationQueue = new Bull('aggregation-tasks', {
    redis: redisConfig,
    defaultJobOptions: {
        attempts: 2,
        backoff: {
            type: 'fixed',
            delay: 5000
        },
        removeOnComplete: true,
        removeOnFail: true,
        timeout: 60000
    }
});

// K线计算队列
export const klineQueue = new Bull('kline-calculation', {
    redis: redisConfig,
    defaultJobOptions: {
        attempts: 3,
        backoff: {
            type: 'exponential',
            delay: 2000
        },
        removeOnComplete: 100,
        removeOnFail: 500,
        timeout: 120000
    },
    limiter: {
        max: 10,
        duration: 1000
    }
});

// 队列事件监听
const setupQueueListeners = (queue: Bull.Queue, name: string): void => {
    queue.on('error', (error: Error) => {
        logger.error(`Queue ${name} error:`, error);
    });

    queue.on('failed', (job: Bull.Job | undefined, err: Error) => {
        if (job) {
            logger.error(`Queue ${name} job ${job.id} failed after ${job.attemptsMade} attempts:`, err.message);
        } else {
            logger.error(`Queue ${name} job failed (no job info):`, err.message);
        }
    });

    queue.on('completed', (job: Bull.Job, result: unknown) => {
        if (process.env.DEBUG === 'true') {
            logger.debug(`Queue ${name} job ${job.id} completed`, { result });
        }
    });

    queue.on('stalled', (job: Bull.Job) => {
        logger.warn(`Queue ${name} job ${job.id} stalled`);
    });

    queue.on('waiting', (jobId: Bull.JobId) => {
        if (process.env.DEBUG === 'true') {
            logger.debug(`Queue ${name} job ${String(jobId)} waiting`);
        }
    });

    queue.on('active', (job: Bull.Job) => {
        if (process.env.DEBUG === 'true') {
            logger.debug(`Queue ${name} job ${job.id} active`);
        }
    });
};

// 初始化监听器
setupQueueListeners(orderQueue, 'order-processing');
setupQueueListeners(aggregationQueue, 'aggregation-tasks');
setupQueueListeners(klineQueue, 'kline-calculation');

// 队列统计信息接口
export interface QueueStats {
    waiting: number;
    active: number;
    completed: number;
    failed: number;
    delayed: number;
    paused: boolean;
}

export const getQueueStats = async (queue: Bull.Queue): Promise<QueueStats> => {
    const [waiting, active, completed, failed, delayed, paused] = await Promise.all([
        queue.getWaitingCount(),
        queue.getActiveCount(),
        queue.getCompletedCount(),
        queue.getFailedCount(),
        queue.getDelayedCount(),
        queue.isPaused()
    ]);
    
    return { waiting, active, completed, failed, delayed, paused };
};

export const getAllQueueStats = async () => {
    const [orderStats, aggregationStats, klineStats] = await Promise.all([
        getQueueStats(orderQueue),
        getQueueStats(aggregationQueue),
        getQueueStats(klineQueue)
    ]);
    
    return {
        orderProcessing: orderStats,
        aggregationTasks: aggregationStats,
        klineCalculation: klineStats
    };
};

export const cleanOldJobs = async (queue: Bull.Queue, maxAge: number = 86400000): Promise<void> => {
    try {
        await queue.clean(maxAge, 'completed');
        await queue.clean(maxAge, 'failed');
        logger.info(`Cleaned ${queue.name} queue jobs older than ${maxAge}ms`);
    } catch (error) {
        logger.error(`Failed to clean ${queue.name} queue:`, error);
    }
};

export const pauseQueue = async (queue: Bull.Queue): Promise<void> => {
    await queue.pause();
    logger.info(`Queue ${queue.name} paused`);
};

export const resumeQueue = async (queue: Bull.Queue): Promise<void> => {
    await queue.resume();
    logger.info(`Queue ${queue.name} resumed`);
};

export const emptyQueue = async (queue: Bull.Queue): Promise<void> => {
    await queue.empty();
    logger.info(`Queue ${queue.name} emptied`);
};

export const closeAllQueues = async (): Promise<void> => {
    logger.info('Closing all queues...');
    await Promise.all([
        orderQueue.close(),
        aggregationQueue.close(),
        klineQueue.close()
    ]);
    logger.info('All queues closed');
};

// 根据订单大小计算优先级
const getOrderPriority = (size: number): number => {
    if (size >= 100) return 1;
    if (size >= 50) return 2;
    if (size >= 10) return 3;
    return 4;
};

/**
 * 添加单个订单到队列
 * @param order 订单数据
 * @param priority 可选优先级（1最高，4最低）
 * @returns Bull Job 实例
 */
export const addOrderToQueue = async (
    order: OrderMatched,
    priority?: number
): Promise<Bull.Job<OrderMatched>> => {
    const job = await orderQueue.add('process-order', order, {
        priority: priority || getOrderPriority(order.size),
        jobId: `${order.transactionHash}-${order.timestamp}`
    });
    
    logger.debug(`Order added to queue: ${job.id}`);
    return job;
};

/**
 * 批量添加订单到队列
 * @param orders 订单数组
 * @returns Bull Job 实例数组
 */
export const addOrdersToQueue = async (orders: OrderMatched[]): Promise<Bull.Job<OrderMatched>[]> => {
    const jobs = orders.map(order => ({
        name: 'process-order',
        data: order,
        opts: {
            priority: getOrderPriority(order.size),
            jobId: `${order.transactionHash}-${order.timestamp}`
        }
    }));
    
    const results = await orderQueue.addBulk(jobs);
    logger.info(`Added ${results.length} orders to queue`);
    return results;
};

/**
 * 获取 Redis 统计信息（使用主 redisClient）
 */
export const getQueueRedisInfo = async (): Promise<string | null> => {
    try {
        const info = await redisClient.info('stats');
        return info;
    } catch (error) {
        logger.error('Failed to get Redis info:', error);
        return null;
    }
};

/**
 * 检查所有队列的健康状态
 */
export const checkQueueHealth = async (): Promise<{
    healthy: boolean;
    queues: Record<string, { available: boolean; error?: string }>;
}> => {
    const result: {
        healthy: boolean;
        queues: Record<string, { available: boolean; error?: string }>;
    } = {
        healthy: true,
        queues: {}
    };
    
    // 检查 orderQueue
    try {
        await orderQueue.client.ping();
        result.queues['order-processing'] = { available: true };
    } catch (error) {
        result.healthy = false;
        result.queues['order-processing'] = { available: false, error: (error as Error).message };
    }
    
    // 检查 aggregationQueue
    try {
        await aggregationQueue.client.ping();
        result.queues['aggregation-tasks'] = { available: true };
    } catch (error) {
        result.healthy = false;
        result.queues['aggregation-tasks'] = { available: false, error: (error as Error).message };
    }
    
    // 检查 klineQueue
    try {
        await klineQueue.client.ping();
        result.queues['kline-calculation'] = { available: true };
    } catch (error) {
        result.healthy = false;
        result.queues['kline-calculation'] = { available: false, error: (error as Error).message };
    }
    
    // 检查主 Redis 客户端
    try {
        await redisClient.ping();
    } catch (error) {
        result.healthy = false;
        logger.error('Main Redis client is not healthy:', error);
    }
    
    return result;
};

/**
 * 获取所有队列中等待任务的总数（通过扫描 Redis 键）
 */
export const getTotalWaitingJobs = async (): Promise<number> => {
    try {
        const keys = await redisClient.keys('bull:*:waiting');
        let total = 0;
        for (const key of keys) {
            const count = await redisClient.llen(key);
            total += count;
        }
        return total;
    } catch (error) {
        logger.error('Failed to get total waiting jobs:', error);
        return 0;
    }
};

/**
 * 启动队列定期清理任务
 */
export const startQueueCleanup = (): void => {
    setInterval(async () => {
        await cleanOldJobs(orderQueue, 86400000);      // 24小时
        await cleanOldJobs(aggregationQueue, 604800000); // 7天
        await cleanOldJobs(klineQueue, 259200000);       // 3天
    }, 3600000); // 每小时执行一次
};

// 导出配置对象（避免匿名默认导出）
const queueConfig = {
    orderQueue,
    aggregationQueue,
    klineQueue,
    getQueueStats,
    getAllQueueStats,
    cleanOldJobs,
    closeAllQueues,
    startQueueCleanup,
    checkQueueHealth,
    getTotalWaitingJobs,
    pauseQueue,
    resumeQueue,
    emptyQueue,
    addOrderToQueue,
    addOrdersToQueue,
    getQueueRedisInfo
};

export default queueConfig;