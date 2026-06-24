import { BaseConsumer, logger } from '@miyabi/shared';

export class AnalyticsConsumer extends BaseConsumer {
  override async processMessage(
    payload: unknown,
    routingKey: string,
    metadata: Record<string, unknown>,
    headers: Record<string, unknown>,
  ): Promise<void> {
    logger.info(`[Analytics Service] Processing event ${routingKey}`, {
      payload,
      metadata,
      headers,
    });

    const { id, customerId, amount } = payload as {
      id?: string;
      customerId?: string;
      amount?: string;
    };
    if (!id || !customerId || amount === undefined) {
      throw new Error('Invalid payload: missing order properties');
    }

    const orderAmount = parseFloat(amount);

    // Simulate database ingestion or real-time dashboard update
    logger.info(
      `[Analytics Service] Ingested order metrics for ID ${id}. Customer: ${customerId}, Revenue: $${orderAmount}`,
    );
  }
}
