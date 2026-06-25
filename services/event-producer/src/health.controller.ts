import { Request, Response, Router } from 'express';
import { prisma, redis, logger, RabbitMQClient } from '@miyabi/shared';

export function createHealthRouter(rabbitmqClient: RabbitMQClient): Router {
  const router = Router();

  router.get('/', async (_req: Request, res: Response) => {
    let hasError = false;
    const details = {
      postgres: 'unhealthy',
      redis: 'unhealthy',
      rabbitmq: 'unhealthy',
    };

    // 1. Check PostgreSQL
    try {
      await prisma.$queryRaw`SELECT 1`;
      details.postgres = 'healthy';
    } catch (err) {
      hasError = true;
      logger.error('Health check failed for PostgreSQL', { error: (err as Error).message });
    }

    // 2. Check Redis
    try {
      const pong = await redis.ping();
      if (pong === 'PONG') {
        details.redis = 'healthy';
      }
    } catch (err) {
      hasError = true;
      logger.error('Health check failed for Redis', { error: (err as Error).message });
    }

    // 3. Check RabbitMQ
    try {
      const channel = rabbitmqClient.getChannel();
      if (channel) {
        details.rabbitmq = 'healthy';
      }
    } catch (err) {
      hasError = true;
      logger.error('Health check failed for RabbitMQ', { error: (err as Error).message });
    }

    if (hasError) {
      return res.status(503).json({
        status: 'error',
        timestamp: new Date().toISOString(),
        services: details,
      });
    }

    return res.json({
      status: 'ok',
      timestamp: new Date().toISOString(),
      services: details,
    });
  });

  return router;
}
