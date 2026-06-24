import express from 'express';
import { logger } from '@miyabi/shared';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
const port = process.env['PORT'] || 3000;

app.use(express.json());

app.get('/health', (_req, res) => {
  logger.info('Health check called');
  res.json({ status: 'ok', service: 'event-producer' });
});

app.listen(port, () => {
  logger.info(`Event Producer service listening on port ${port}`);
});
