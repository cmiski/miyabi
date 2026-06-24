import express from 'express';
import { logger } from '@miyabi/shared';
import dotenv from 'dotenv';
import { AnalyticsConsumer } from './analytics.consumer.js';

dotenv.config();

const app = express();
const port = process.env['PORT'] || 3002;
const rabbitmqUrl = process.env['RABBITMQ_URL'] || 'amqp://guest:guest@localhost:5672';

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'analytics-consumer' });
});

async function bootstrap() {
  try {
    // 1. Initialize Consumer
    const consumer = new AnalyticsConsumer({
      rabbitmqUrl,
      queueName: 'analytics.order-created',
      routingKey: 'order.created',
    });
    await consumer.start();

    // 2. Start Health Check API
    const server = app.listen(port, () => {
      logger.info(`Analytics Consumer health API listening on port ${port}`);
    });

    // Graceful shutdown handling
    const shutdown = async () => {
      logger.info('Shutting down analytics consumer...');
      server.close(() => {
        logger.info('Express health server closed');
      });
      await consumer.close();
      process.exit(0);
    };

    process.on('SIGTERM', shutdown);
    process.on('SIGINT', shutdown);
  } catch (err) {
    logger.error('Failed to bootstrap Analytics Consumer service', {
      error: (err as Error).message,
    });
    process.exit(1);
  }
}

bootstrap();
