import amqp, { ChannelModel, ConfirmChannel } from 'amqplib';
import { logger } from './index.js';

export const EXCHANGE_NAME = 'miyabi.events';

export class RabbitMQClient {
  private connection?: ChannelModel;
  private channel?: ConfirmChannel;
  private url: string;

  constructor(url: string) {
    this.url = url;
  }

  async connect(): Promise<void> {
    let attempts = 0;
    const maxAttempts = 5;
    const delay = 2000;

    while (attempts < maxAttempts) {
      try {
        logger.info(`Connecting to RabbitMQ (attempt ${attempts + 1}/${maxAttempts})...`);
        this.connection = await amqp.connect(this.url);
        
        this.connection.on('error', (err) => {
          logger.error('RabbitMQ connection error', { error: err.message });
          this.handleDisconnect();
        });

        this.connection.on('close', () => {
          logger.warn('RabbitMQ connection closed');
          this.handleDisconnect();
        });

        this.channel = await this.connection.createConfirmChannel();
        await this.channel.assertExchange(EXCHANGE_NAME, 'topic', { durable: true });

        logger.info('Connected to RabbitMQ and declared exchange successfully');
        return;
      } catch (err: any) {
        attempts++;
        logger.error(`Failed to connect to RabbitMQ: ${err.message}. Retrying in ${delay}ms...`);
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
    throw new Error('Could not establish connection to RabbitMQ after maximum retries');
  }

  private handleDisconnect() {
    this.connection = undefined;
    this.channel = undefined;
    // Attempt reconnect
    setTimeout(() => {
      this.connect().catch((err) => {
        logger.error('Reconnection to RabbitMQ failed', { error: err.message });
      });
    }, 5000);
  }

  getChannel(): ConfirmChannel {
    if (!this.channel) {
      throw new Error('RabbitMQ channel is not initialized');
    }
    return this.channel;
  }

  async publish(routingKey: string, payload: Record<string, unknown>, version = '1.0.0'): Promise<boolean> {
    return new Promise((resolve, reject) => {
      try {
        const channel = this.getChannel();
        const messageBuffer = Buffer.from(
          JSON.stringify({
            data: payload,
            metadata: {
              timestamp: new Date().toISOString(),
              version,
            },
          })
        );

        channel.publish(
          EXCHANGE_NAME,
          routingKey,
          messageBuffer,
          { persistent: true },
          (err, _ok) => {
            if (err) {
              logger.error('RabbitMQ publish NACK received', { routingKey, error: err.message });
              reject(err);
            } else {
              logger.debug('RabbitMQ publish ACK received', { routingKey });
              resolve(true);
            }
          }
        );
      } catch (err) {
        logger.error('Error during publishing to RabbitMQ', { error: (err as Error).message });
        reject(err);
      }
    });
  }

  async close() {
    try {
      await this.channel?.close();
      await this.connection?.close();
      logger.info('RabbitMQ connection closed gracefully');
    } catch (err) {
      logger.error('Error closing RabbitMQ connection', { error: (err as Error).message });
    }
  }
}
