import { prisma, RabbitMQClient, logger } from '@miyabi/shared';

interface PendingEvent {
  id: string;
  eventType: string;
  payload: Record<string, unknown>;
  version: string;
  retryCount: number;
}

export class OutboxWorker {
  private rabbitmqClient: RabbitMQClient;
  private intervalMs: number;
  private maxRetries: number;
  private batchSize: number;
  private isRunning = false;
  private timer?: NodeJS.Timeout;

  constructor(
    rabbitmqClient: RabbitMQClient,
    options?: { intervalMs?: number; maxRetries?: number; batchSize?: number },
  ) {
    this.rabbitmqClient = rabbitmqClient;
    this.intervalMs = options?.intervalMs ?? 1000;
    this.maxRetries = options?.maxRetries ?? 5;
    this.batchSize = options?.batchSize ?? 10;
  }

  start() {
    if (this.isRunning) return;
    this.isRunning = true;
    logger.info('Outbox worker started');
    this.runLoop();
  }

  stop() {
    this.isRunning = false;
    if (this.timer) {
      clearTimeout(this.timer);
    }
    logger.info('Outbox worker stopped');
  }

  private runLoop() {
    if (!this.isRunning) return;

    this.processOutbox()
      .catch((err) => {
        logger.error('Error in outbox processing loop', { error: (err as Error).message });
      })
      .finally(() => {
        if (this.isRunning) {
          this.timer = setTimeout(() => this.runLoop(), this.intervalMs);
        }
      });
  }

  async processOutbox() {
    // 1. Claim pending events transactionally using FOR UPDATE SKIP LOCKED
    const claimedEvents = await prisma.$transaction(async (tx) => {
      // Postgres raw query to lock pending rows
      const pending = await tx.$queryRawUnsafe<PendingEvent[]>(
        `
        SELECT id, event_type as "eventType", payload, version, retry_count as "retryCount"
        FROM outbox_events
        WHERE status = 'PENDING'
        ORDER BY created_at ASC
        LIMIT $1
        FOR UPDATE SKIP LOCKED
      `,
        this.batchSize,
      );

      if (pending.length === 0) {
        return [];
      }

      const ids = pending.map((e) => e.id);

      // Update status to PROCESSING to claim them
      await tx.outboxEvent.updateMany({
        where: { id: { in: ids } },
        data: { status: 'PROCESSING' },
      });

      return pending;
    });

    if (claimedEvents.length === 0) {
      return;
    }

    logger.info(`Claimed ${claimedEvents.length} outbox events for publishing`);

    // 2. Publish claimed events to RabbitMQ
    for (const event of claimedEvents) {
      try {
        await this.rabbitmqClient.publish(event.eventType, event.payload, event.version);

        // Success - update status to PUBLISHED
        await prisma.outboxEvent.update({
          where: { id: event.id },
          data: {
            status: 'PUBLISHED',
            processedAt: new Date(),
          },
        });
        logger.info('Successfully published event to RabbitMQ', {
          eventId: event.id,
          type: event.eventType,
        });
      } catch (err) {
        logger.error('Failed to publish event to RabbitMQ', {
          eventId: event.id,
          error: (err as Error).message,
        });

        const nextRetryCount = event.retryCount + 1;
        const shouldRetry = nextRetryCount < this.maxRetries;

        await prisma.outboxEvent.update({
          where: { id: event.id },
          data: {
            status: shouldRetry ? 'PENDING' : 'FAILED',
            retryCount: nextRetryCount,
            error: (err as Error).message || 'Unknown error',
          },
        });
      }
    }
  }
}
