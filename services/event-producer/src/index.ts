import express from 'express';
import { logger, RabbitMQClient, prisma, redis } from '@miyabi/shared';
import dotenv from 'dotenv';
import swaggerUi from 'swagger-ui-express';
import { orderRouter } from './order.controller.js';
import { OutboxWorker } from './outbox.worker.js';
import { metricsRouter } from './metrics.controller.js';
import fs from 'fs';
import path from 'path';
import { createHealthRouter } from './health.controller.js';

const openapiPath = path.join(__dirname, '../src/openapi.json');
const openapiSchema = JSON.parse(fs.readFileSync(openapiPath, 'utf8'));

dotenv.config();

const app = express();
const port = process.env['PORT'] || 3000;
const rabbitmqUrl = process.env['RABBITMQ_URL'] || 'amqp://guest:guest@localhost:5672';

app.use(express.json());

// Mount Swagger UI documentation
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(openapiSchema));

// Mount business routers
app.use('/orders', orderRouter);
app.use('/metrics', metricsRouter);

async function bootstrap() {
  try {
    // 1. Initialize RabbitMQ
    const rabbitmqClient = new RabbitMQClient(rabbitmqUrl);
    await rabbitmqClient.connect();

    // 2. Mount health check router (actively querying Postgres, Redis, and RabbitMQ)
    app.use('/health', createHealthRouter(rabbitmqClient));

    // 3. Start Outbox Worker
    const outboxWorker = new OutboxWorker(rabbitmqClient);
    outboxWorker.start();

    // 4. Start Express Server
    const server = app.listen(port, () => {
      logger.info(`Event Producer service listening on port ${port}`);
      logger.info(`OpenAPI documentation available at http://localhost:${port}/api-docs`);
    });

    // Graceful shutdown handling
    const shutdown = async (signal: string) => {
      logger.info(`Received ${signal}. Shutting down event producer gracefully...`);

      // Stop outbox worker polling
      outboxWorker.stop();

      // Stop Express server
      server.close(() => {
        logger.info('Express server closed');
      });

      // Disconnect clients in order
      try {
        await rabbitmqClient.close();
      } catch (err) {
        logger.error('Error closing RabbitMQ client', { error: (err as Error).message });
      }

      try {
        await redis.quit();
        logger.info('Redis connection closed gracefully');
      } catch (err) {
        logger.error('Error closing Redis connection', { error: (err as Error).message });
      }

      try {
        await prisma.$disconnect();
        logger.info('Prisma database client disconnected');
      } catch (err) {
        logger.error('Error disconnecting Prisma client', { error: (err as Error).message });
      }

      logger.info('Graceful shutdown completed');
      process.exit(0);
    };

    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT', () => shutdown('SIGINT'));
  } catch (err) {
    logger.error('Failed to bootstrap Event Producer service', { error: (err as Error).message });
    process.exit(1);
  }
}

bootstrap();
