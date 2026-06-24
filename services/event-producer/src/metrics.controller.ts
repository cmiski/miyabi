import { Request, Response, Router } from 'express';
import { prisma, redis, logger } from '@miyabi/shared';

export const metricsRouter = Router();

metricsRouter.get('/', async (_req: Request, res: Response) => {
  try {
    // 1. Get database outbox statistics
    const outboxCounts = await prisma.outboxEvent.groupBy({
      by: ['status'],
      _count: {
        id: true,
      },
    });

    const dbStats = {
      PENDING: 0,
      PROCESSING: 0,
      PUBLISHED: 0,
      FAILED: 0,
    };

    for (const group of outboxCounts) {
      if (group.status in dbStats) {
        dbStats[group.status as keyof typeof dbStats] = group._count.id;
      }
    }

    // 2. Get Redis consumer statistics
    const [
      notifProcessed,
      notifFailed,
      analyticsProcessed,
      analyticsFailed,
      globalProcessed,
      globalFailed,
    ] = await Promise.all([
      redis
        .get('metrics:consumer:notification.order-created:processed')
        .then((v) => Number(v || 0)),
      redis.get('metrics:consumer:notification.order-created:failed').then((v) => Number(v || 0)),
      redis.get('metrics:consumer:analytics.order-created:processed').then((v) => Number(v || 0)),
      redis.get('metrics:consumer:analytics.order-created:failed').then((v) => Number(v || 0)),
      redis.get('metrics:global:processed').then((v) => Number(v || 0)),
      redis.get('metrics:global:failed').then((v) => Number(v || 0)),
    ]);

    return res.json({
      timestamp: new Date().toISOString(),
      database: {
        outbox: dbStats,
      },
      consumers: {
        notification: {
          processed: notifProcessed,
          failed: notifFailed,
        },
        analytics: {
          processed: analyticsProcessed,
          failed: analyticsFailed,
        },
        global: {
          processed: globalProcessed,
          failed: globalFailed,
        },
      },
    });
  } catch (err) {
    logger.error('Failed to retrieve metrics', { error: (err as Error).message });
    return res.status(500).json({ error: 'Internal server error' });
  }
});
