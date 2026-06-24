import Redis from 'ioredis';
import { logger } from './index.js';

const redisUrl = process.env['REDIS_URL'] || 'redis://localhost:6379';

export const redis = new Redis(redisUrl, {
  maxRetriesPerRequest: null,
});

redis.on('connect', () => {
  logger.info('Connected to Redis successfully');
});

redis.on('error', (err) => {
  logger.error('Redis connection error', { error: err.message });
});
