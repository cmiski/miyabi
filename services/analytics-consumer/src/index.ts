import express from 'express';
import { logger } from '@miyabi/shared';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
const port = process.env['PORT'] || 3002;

app.get('/health', (_req, res) => {
  logger.info('Health check called in analytics consumer');
  res.json({ status: 'ok', service: 'analytics-consumer' });
});

app.listen(port, () => {
  logger.info(`Analytics Consumer service listening on port ${port}`);
});
