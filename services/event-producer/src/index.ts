import express from 'express';
import { logger, RabbitMQClient } from '@miyabi/shared';
import dotenv from 'dotenv';
import { orderRouter } from './order.controller.js';
import { OutboxWorker } from './outbox.worker.js';

dotenv.config();

const app = express();
const port = process.env['PORT'] || 3000;
const rabbitmqUrl = process.env['RABBITMQ_URL'] || 'amqp://guest:guest@localhost:5672';

app.use(express.json());

// Mount routers
app.use('/orders', orderRouter);

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'event-producer' });
});

async function bootstrap() {
  try {
    // 1. Initialize RabbitMQ
    const rabbitmqClient = new RabbitMQClient(rabbitmqUrl);
    await rabbitmqClient.connect();

    // 2. Start Outbox Worker
    const outboxWorker = new OutboxWorker(rabbitmqClient);
    outboxWorker.start();

    // 3. Start Express Server
    const server = app.listen(port, () => {
      logger.info(`Event Producer service listening on port ${port}`);
    });

    // Graceful shutdown handling
    const shutdown = async () => {
      logger.info('Shutting down event producer...');
      outboxWorker.stop();
      server.close(() => {
        logger.info('Express server closed');
      });
      await rabbitmqClient.close();
      process.exit(0);
    };

    process.on('SIGTERM', shutdown);
    process.on('SIGINT', shutdown);
  } catch (err) {
    logger.error('Failed to bootstrap Event Producer service', { error: (err as Error).message });
    process.exit(1);
  }
}

bootstrap();
