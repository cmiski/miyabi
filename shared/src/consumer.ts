import amqp, { ChannelModel, Channel, ConsumeMessage } from 'amqplib';
import { logger } from './index.js';
import { EXCHANGE_NAME } from './rabbitmq.js';
import { redis } from './redis.js';

export const DLX_NAME = 'miyabi.events.dlx';

export interface ConsumerConfig {
  rabbitmqUrl: string;
  queueName: string;
  routingKey: string;
  prefetch?: number;
  maxRetries?: number;
}

export abstract class BaseConsumer {
  protected connection?: ChannelModel;
  protected channel?: Channel;
  protected config: ConsumerConfig;
  protected maxRetries: number;

  constructor(config: ConsumerConfig) {
    this.config = config;
    this.maxRetries = config.maxRetries ?? 3;
  }

  async start(): Promise<void> {
    const { rabbitmqUrl, queueName, routingKey, prefetch = 10 } = this.config;

    try {
      logger.info(`Starting consumer for queue ${queueName}...`);
      this.connection = await amqp.connect(rabbitmqUrl);
      this.channel = await this.connection.createChannel();

      // 1. Declare Main Exchange
      await this.channel.assertExchange(EXCHANGE_NAME, 'topic', { durable: true });

      // 2. Declare Dead Letter Exchange (DLX)
      await this.channel.assertExchange(DLX_NAME, 'topic', { durable: true });

      // 3. Declare and Bind Dead Letter Queue (DLQ)
      const dlqName = `${queueName}.dlq`;
      await this.channel.assertQueue(dlqName, { durable: true });
      await this.channel.bindQueue(dlqName, DLX_NAME, dlqName);

      // 4. Declare and Bind Retry Queue (acts as a backoff delay queue)
      const retryQueueName = `${queueName}.retry`;
      await this.channel.assertQueue(retryQueueName, {
        durable: true,
        arguments: {
          'x-message-ttl': 5000, // 5 seconds retry delay
          'x-dead-letter-exchange': EXCHANGE_NAME,
          'x-dead-letter-routing-key': routingKey, // routes back to the main queue
        },
      });
      await this.channel.bindQueue(retryQueueName, DLX_NAME, retryQueueName);

      // 5. Declare Main Queue with DLX configuration
      await this.channel.assertQueue(queueName, {
        durable: true,
        arguments: {
          'x-dead-letter-exchange': DLX_NAME,
          'x-dead-letter-routing-key': dlqName, // routes directly to DLQ on final rejection
        },
      });

      // 6. Bind Main Queue to Main Exchange
      await this.channel.bindQueue(queueName, EXCHANGE_NAME, routingKey);

      // Set Prefetch
      await this.channel.prefetch(prefetch);

      // 7. Start consuming
      await this.channel.consume(
        queueName,
        async (msg: ConsumeMessage | null) => {
          if (!msg) return;
          await this.handleMessage(msg);
        },
        { noAck: false },
      );

      logger.info(
        `Consumer for queue ${queueName} successfully started and bound to ${routingKey}`,
      );
    } catch (err) {
      logger.error(`Failed to start consumer for queue ${queueName}`, {
        error: (err as Error).message,
      });
      throw err;
    }
  }

  private async handleMessage(msg: ConsumeMessage): Promise<void> {
    const routingKey = msg.fields.routingKey;
    const content = msg.content.toString();
    let eventId = msg.properties.messageId || 'unknown';
    let payload: unknown = {};
    let metadata: Record<string, unknown> = {};

    try {
      const parsed = JSON.parse(content);
      payload = parsed.data || parsed;
      metadata = parsed.metadata || {};
      const castPayload = payload as { id?: string };
      if (castPayload && castPayload.id) {
        eventId = castPayload.id;
      }
    } catch (err) {
      logger.error('Failed to parse message content', { content, error: (err as Error).message });
      // Malformed message - ack immediately to discard or move to DLQ
      this.channel?.nack(msg, false, false);
      return;
    }

    const idempotencyKey = `idempotency:${this.config.queueName}:${eventId}`;

    try {
      // 1. Check Idempotency via Redis atomic lock
      // Set value "PROCESSING" with 1 hour TTL if not already set
      const acquired = await redis.set(idempotencyKey, 'PROCESSING', 'EX', 3600, 'NX');

      if (!acquired) {
        // Key already exists. Check status
        const status = await redis.get(idempotencyKey);
        if (status === 'COMPLETED') {
          logger.warn('Duplicate event detected. Already processed. Skipping.', {
            eventId,
            queue: this.config.queueName,
          });
          this.channel?.ack(msg);
          return;
        } else {
          // PROCESSING by another worker. Nack and requeue so it is tried again
          logger.warn('Event is currently being processed by another worker. Requeuing.', {
            eventId,
            queue: this.config.queueName,
          });
          this.channel?.nack(msg, false, true);
          return;
        }
      }

      logger.info(`Received event in ${this.config.queueName}`, {
        routingKey,
        eventId,
      });

      // 2. Process message
      await this.processMessage(payload, routingKey, metadata, msg.properties.headers || {});

      // 3. Mark processing as completed in Redis
      await redis.set(idempotencyKey, 'COMPLETED', 'EX', 3600);

      // Track successful metric
      await redis.incr(`metrics:consumer:${this.config.queueName}:processed`);
      await redis.incr('metrics:global:processed');

      // Acknowledge on success
      this.channel?.ack(msg);
      logger.debug(`Message acknowledged in ${this.config.queueName}`);
    } catch (err) {
      logger.error(`Error processing message in ${this.config.queueName}`, {
        error: (err as Error).message,
        routingKey,
        eventId,
      });

      // Release idempotency lock so it can be retried
      await redis.del(idempotencyKey);

      // Track failed metric
      await redis.incr(`metrics:consumer:${this.config.queueName}:failed`);
      await redis.incr('metrics:global:failed');

      // 4. Retry Policy with Exponential Backoff simulation via Retry Queue
      const headers = msg.properties.headers || {};
      const retryCount = Number(headers['x-retry-count'] || 0);

      if (retryCount < this.maxRetries) {
        const nextRetry = retryCount + 1;
        logger.warn(`Routing message to retry queue (attempt ${nextRetry}/${this.maxRetries})`, {
          eventId,
        });

        const retryQueueName = `${this.config.queueName}.retry`;

        // Publish to retry queue via DLX
        this.channel?.publish(DLX_NAME, retryQueueName, msg.content, {
          persistent: true,
          headers: {
            ...headers,
            'x-retry-count': nextRetry,
          },
        });

        // Acknowledge original message to remove it from the main queue
        this.channel?.ack(msg);
      } else {
        logger.error(`Max retries (${this.maxRetries}) exhausted. Rejecting to DLQ.`, { eventId });
        // Nack with requeue=false -> routes directly to DLQ
        this.channel?.nack(msg, false, false);
      }
    }
  }

  abstract processMessage(
    payload: unknown,
    routingKey: string,
    metadata: Record<string, unknown>,
    headers: Record<string, unknown>,
  ): Promise<void>;

  async close(): Promise<void> {
    try {
      await this.channel?.close();
      await this.connection?.close();
      logger.info(`Consumer ${this.config.queueName} closed successfully`);
    } catch (err) {
      logger.error(`Error closing consumer ${this.config.queueName}`, {
        error: (err as Error).message,
      });
    }
  }
}
