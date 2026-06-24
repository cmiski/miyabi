import { Request, Response, Router } from 'express';
import { z } from 'zod';
import { prisma, createOutboxEvent, logger } from '@miyabi/shared';

const CreateOrderSchema = z.object({
  customerId: z.string().uuid({ message: 'customerId must be a valid UUID' }),
  amount: z.number().positive({ message: 'amount must be a positive number' }),
});

export const orderRouter = Router();

orderRouter.post('/', async (req: Request, res: Response): Promise<any> => {
  try {
    const parseResult = CreateOrderSchema.safeParse(req.body);
    if (!parseResult.success) {
      return res.status(400).json({
        error: 'Validation failed',
        details: parseResult.error.errors,
      });
    }

    const { customerId, amount } = parseResult.data;

    // Execute in a database transaction
    const order = await prisma.$transaction(async (tx) => {
      const createdOrder = await tx.order.create({
        data: {
          customerId,
          amount,
          status: 'PENDING',
        },
      });

      // Write corresponding event to the outbox table
      await createOutboxEvent(tx, {
        eventType: 'order.created',
        payload: {
          id: createdOrder.id,
          customerId: createdOrder.customerId,
          amount: createdOrder.amount.toString(),
          status: createdOrder.status,
          createdAt: createdOrder.createdAt.toISOString(),
        },
        version: '1.0.0',
      });

      return createdOrder;
    });

    logger.info('Order and outbox event created successfully', { orderId: order.id });
    return res.status(201).json(order);
  } catch (err: any) {
    logger.error('Failed to create order', { error: err.message });
    return res.status(500).json({ error: 'Internal server error' });
  }
});
