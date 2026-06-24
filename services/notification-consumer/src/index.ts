import express from 'express';
import { logger } from '@miyabi/shared';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
const port = process.env['PORT'] || 3001;

app.get('/health', (_req, res) => {
  logger.info('Health check called in notification consumer');
  res.json({ status: 'ok', service: 'notification-consumer' });
});

app.listen(port, () => {
  logger.info(`Notification Consumer service listening on port ${port}`);
});
