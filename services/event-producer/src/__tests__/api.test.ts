import dotenv from 'dotenv';
import path from 'path';

// Load environment variables from the workspace root .env file
dotenv.config({ path: path.resolve(process.cwd(), '.env') });

import request from 'supertest';
import app from '../app.js';
import { prisma, redis } from '@miyabi/shared';

describe('Event Producer API Integration Tests', () => {
  const createdOrderIds: string[] = [];

  beforeAll(async () => {
    await prisma.$connect();
  });

  afterAll(async () => {
    // Clean up created orders and outbox events
    if (createdOrderIds.length > 0) {
      await prisma.outboxEvent.deleteMany({
        where: {
          payload: {
            path: ['id'],
            string_contains: createdOrderIds[0], // we can delete outbox events where payload contains the order IDs
          },
        },
      });

      // To be safe, let's just delete outbox events related to test orders
      // or delete all outbox events created in this test
      // Actually we can find outbox events matching the order ids in payload
      for (const orderId of createdOrderIds) {
        await prisma.order.delete({ where: { id: orderId } });
      }
    }

    // Clean up all PENDING outbox events created in tests by checking order ID mapping
    // To simplify, let's delete outbox events by eventType = 'order.created' and matching customer ID if we use a specific one
    await prisma.outboxEvent.deleteMany({
      where: {
        eventType: 'order.created',
        payload: {
          path: ['customerId'],
          equals: '00000000-0000-0000-0000-000000000000',
        },
      },
    });

    await prisma.order.deleteMany({
      where: {
        customerId: '00000000-0000-0000-0000-000000000000',
      },
    });

    await redis.quit();
    await prisma.$disconnect();
  });

  describe('POST /orders', () => {
    it('should return 400 when customerId is missing or invalid', async () => {
      const res = await request(app).post('/orders').send({ amount: 100 });

      expect(res.status).toBe(400);
      expect(res.body.error).toBe('Validation failed');
      expect(res.body.details).toBeDefined();
    });

    it('should return 400 when amount is negative or zero', async () => {
      const res = await request(app).post('/orders').send({
        customerId: '00000000-0000-0000-0000-000000000000',
        amount: -10,
      });

      expect(res.status).toBe(400);
      expect(res.body.error).toBe('Validation failed');
    });

    it('should successfully create an order and return 201', async () => {
      const orderData = {
        customerId: '00000000-0000-0000-0000-000000000000',
        amount: 150.5,
      };

      const res = await request(app).post('/orders').send(orderData);

      expect(res.status).toBe(201);
      expect(res.body.id).toBeDefined();
      expect(res.body.customerId).toBe(orderData.customerId);
      expect(Number(res.body.amount)).toBe(orderData.amount);
      expect(res.body.status).toBe('PENDING');

      createdOrderIds.push(res.body.id);

      // Verify DB contains the order
      const order = await prisma.order.findUnique({
        where: { id: res.body.id },
      });
      expect(order).toBeDefined();
      expect(order?.customerId).toBe(orderData.customerId);

      // Verify DB contains the outbox event
      const events = await prisma.outboxEvent.findMany({
        where: { eventType: 'order.created' },
      });
      const orderEvent = events.find((e) => {
        const payload = e.payload as Record<string, unknown>;
        return payload['id'] === res.body.id;
      });
      expect(orderEvent).toBeDefined();
      expect(orderEvent?.status).toBe('PENDING');
    });
  });

  describe('GET /metrics', () => {
    it('should return 200 and the current system metrics', async () => {
      const res = await request(app).get('/metrics');

      expect(res.status).toBe(200);
      expect(res.body.timestamp).toBeDefined();
      expect(res.body.database).toBeDefined();
      expect(res.body.database.outbox).toBeDefined();
      expect(res.body.consumers).toBeDefined();
    });
  });
});
