import dotenv from 'dotenv';
import path from 'path';

// Load environment variables from the workspace root .env file
dotenv.config({ path: path.resolve(process.cwd(), '.env') });

import { prisma, createOutboxEvent, redis } from '../index.js';

describe('Outbox Unit Tests', () => {
  beforeAll(async () => {
    await prisma.$connect();
  });

  afterAll(async () => {
    // Clean up created test outbox events
    await prisma.outboxEvent.deleteMany({
      where: {
        eventType: {
          startsWith: 'TEST_EVENT_',
        },
      },
    });
    await prisma.$disconnect();
    await redis.quit();
  });

  it('should successfully create an outbox event within a prisma transaction', async () => {
    const eventType = `TEST_EVENT_${Date.now()}`;
    const payload = { testKey: 'testValue' };

    const result = await prisma.$transaction(async (tx) => {
      return createOutboxEvent(tx, {
        eventType,
        payload,
        version: '1.2.3',
      });
    });

    expect(result).toBeDefined();
    expect(result.id).toBeDefined();
    expect(result.eventType).toBe(eventType);
    expect(result.payload).toEqual(payload);
    expect(result.version).toBe('1.2.3');
    expect(result.status).toBe('PENDING');

    // Verify it exists in database
    const dbEvent = await prisma.outboxEvent.findUnique({
      where: { id: result.id },
    });
    expect(dbEvent).toBeDefined();
    expect(dbEvent?.eventType).toBe(eventType);
  });
});
