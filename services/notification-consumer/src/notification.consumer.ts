import { BaseConsumer, logger } from '@miyabi/shared';

export class NotificationConsumer extends BaseConsumer {
  override async processMessage(
    payload: unknown,
    routingKey: string,
    metadata: Record<string, unknown>,
    headers: Record<string, unknown>,
  ): Promise<void> {
    logger.info(`[Notification Service] Processing event ${routingKey}`, {
      payload,
      metadata,
      headers,
    });

    // Validate payload shape
    const { id, customerId, amount } = payload as {
      id?: string;
      customerId?: string;
      amount?: string;
    };
    if (!id || !customerId || amount === undefined) {
      throw new Error('Invalid payload: missing order properties');
    }

    const orderAmount = parseFloat(amount);

    // Simulated failure for amount > 500 to demonstrate DLQ routing
    if (orderAmount > 500) {
      logger.warn(
        `[Notification Service] Simulated failure: order amount ${orderAmount} exceeds $500. Routing to DLQ.`,
      );
      throw new Error(`Email provider error: Simulated sending failure for high-value order ${id}`);
    }

    // Simulate sending email/sms
    logger.info(
      `[Notification Service] Email notification successfully sent to customer ${customerId} for order ${id} (Amount: $${orderAmount})`,
    );
  }
}
