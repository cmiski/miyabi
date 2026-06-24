import amqp, { ChannelModel, Channel, ConsumeMessage } from 'amqplib';
import { logger } from './index.js';
import { EXCHANGE_NAME } from './rabbitmq.js';

export const DLX_NAME = 'miyabi.events.dlx';

export interface ConsumerConfig {
  rabbitmqUrl: string;
  queueName: string;
  routingKey: string;
  prefetch?: number;
}

export abstract class BaseConsumer {
  protected connection?: ChannelModel;
  protected channel?: Channel;
  protected config: ConsumerConfig;

  constructor(config: ConsumerConfig) {
    this.config = config;
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

      // 4. Declare Main Queue with DLX configuration
      await this.channel.assertQueue(queueName, {
        durable: true,
        arguments: {
          'x-dead-letter-exchange': DLX_NAME,
          'x-dead-letter-routing-key': dlqName,
        },
      });

      // 5. Bind Main Queue to Main Exchange
      await this.channel.bindQueue(queueName, EXCHANGE_NAME, routingKey);

      // Set Prefetch
      await this.channel.prefetch(prefetch);

      // 6. Start consuming
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

    try {
      const parsed = JSON.parse(content);
      const payload = parsed.data || parsed;
      const metadata = parsed.metadata || {};

      logger.info(`Received event in ${this.config.queueName}`, {
        routingKey,
        eventId: msg.properties.messageId || 'unknown',
      });

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      await this.processMessage(payload, routingKey, metadata, msg.properties.headers || {});

      // Acknowledge on success
      this.channel?.ack(msg);
      logger.debug(`Message acknowledged in ${this.config.queueName}`);
    } catch (err) {
      logger.error(`Error processing message in ${this.config.queueName}. Routing to DLQ.`, {
        error: (err as Error).message,
        routingKey,
        content,
      });

      // Nack and do NOT requeue -> routes directly to DLQ
      this.channel?.nack(msg, false, false);
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
