import { Prisma } from '@prisma/client';

export interface CreateEventOptions {
  eventType: string;
  payload: Record<string, unknown>;
  version?: string;
}

/**
 * Creates an outbox event within a Prisma transaction.
 * This guarantees the event is saved if and only if the transaction succeeds.
 */
export async function createOutboxEvent(tx: Prisma.TransactionClient, options: CreateEventOptions) {
  return tx.outboxEvent.create({
    data: {
      eventType: options.eventType,
      payload: options.payload as Prisma.InputJsonValue,
      version: options.version ?? '1.0.0',
      status: 'PENDING',
    },
  });
}
