import winston from 'winston';

export const logger = winston.createLogger({
  level: process.env['LOG_LEVEL'] || 'info',
  format: winston.format.combine(winston.format.timestamp(), winston.format.json()),
  defaultMeta: { service: 'miyabi-service' },
  transports: [new winston.transports.Console()],
});

export * from './db.js';
export * from './outbox.js';
export * from './rabbitmq.js';
export * from './consumer.js';
export * from './redis.js';
