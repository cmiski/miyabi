import express from 'express';
import swaggerUi from 'swagger-ui-express';
import fs from 'fs';
import path from 'path';
import { orderRouter } from './order.controller.js';
import { metricsRouter } from './metrics.controller.js';

const app = express();
app.use(express.json());

const openapiPath = path.join(__dirname, '../src/openapi.json');
const openapiSchema = JSON.parse(fs.readFileSync(openapiPath, 'utf8'));

// Mount Swagger UI documentation
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(openapiSchema));

// Mount business routers
app.use('/orders', orderRouter);
app.use('/metrics', metricsRouter);

export default app;
